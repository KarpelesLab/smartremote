package smartremote

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// dlClient is an internal HTTP connection handler that manages individual
// connections to remote URLs. It handles Range requests, connection reuse,
// and idle background downloading of missing blocks.
type dlClient struct {
	dlm      *DownloadManager
	url      string
	taskCnt  uintptr // currently running/pending tasks
	handler  *File
	failure  bool
	complete bool

	reader *http.Response
	rPos   int64 // in bytes
	lk     sync.Mutex
	expire time.Time
}

// Close closes the HTTP connection and signals completion to the manager.
func (dl *dlClient) Close() error {
	dl.lk.Lock()
	defer dl.lk.Unlock()

	if dl.reader != nil {
		err := dl.reader.Body.Close()
		dl.reader = nil
		return err
	}

	dl.dlm.cd.Broadcast()

	return nil
}

// dropDataCount reads and discards cnt bytes from the HTTP response body.
// If a handler is set, it opportunistically saves complete blocks to disk.
func (dl *dlClient) dropDataCount(cnt, startPos int64) error {
	if dl.handler == nil {
		_, err := io.CopyN(io.Discard, dl.reader.Body, cnt)
		return err
	}

	// download data in buffers
	sz := dl.handler.getBlockSize()
	if sz <= 0 || cnt < sz {
		// doesn't want data?
		_, err := io.CopyN(io.Discard, dl.reader.Body, cnt)
		return err
	}

	buf := make([]byte, sz)

	for cnt > 0 {
		if cnt < sz {
			// can't download enough so that it's worth it
			_, err := io.CopyN(io.Discard, dl.reader.Body, cnt)
			return err
		}

		_, err := io.ReadFull(dl.reader.Body, buf)
		if err != nil {
			return err
		}

		cnt -= sz

		err = dl.handler.ingestData(buf, startPos)
		if err != nil {
			// give up
			_, err := io.CopyN(io.Discard, dl.reader.Body, cnt)
			return err
		}

		startPos += sz
	}

	return nil
}

// ReadAt reads data from the remote URL at the specified offset into p.
// It reuses existing HTTP connections when possible, or opens new ones as needed.
func (dl *dlClient) ReadAt(p []byte, off int64) (int, error) {
	dl.lk.Lock()
	defer dl.lk.Unlock()

	// check if current reader can be used
	if dl.reader != nil {
		// check if can be used
		if dl.rPos > off {
			// nope
			dl.reader.Body.Close()
			dl.reader = nil
		} else if dl.rPos < off {
			if off-dl.rPos < dl.dlm.MaxDataJump {
				// drop that amount of data to move rPos forward
				err := dl.dropDataCount(off-dl.rPos, dl.rPos)
				if err != nil {
					// failed, drop connection & retry
					dl.reader.Body.Close()
					dl.reader = nil
				} else {
					dl.rPos = off
				}
			} else {
				dl.reader.Body.Close()
				dl.reader = nil
			}
		}
	}

	// instantiate a new reader if needed
	if dl.reader == nil {
		// spawn a new reader
		req, err := http.NewRequest("GET", dl.url, nil)
		if err != nil {
			return 0, err
		}

		if off != 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", off))
		}

		dl.dlm.logf("initializing HTTP connection download at byte %d~", off)

		// should respond with code 206 Partial Content
		resp, err := dl.dlm.Client.Do(req)
		if err != nil {
			return 0, err
		}
		if resp.StatusCode > 299 {
			// that's bad
			resp.Body.Close()
			return 0, fmt.Errorf("failed to download: %s", resp.Status)
		}
		dl.reader = resp
		dl.rPos = off
	}
	dl.expire = time.Now().Add(time.Minute)

	n, err := io.ReadFull(dl.reader.Body, p)
	if err != nil {
		dl.reader.Body.Close()
		dl.reader = nil
	} else {
		dl.rPos += int64(n)
	}

	return n, err
}

// idleTaskRun is called during idle periods to download missing blocks
// in the background. It runs in a separate goroutine.
func (dl *dlClient) idleTaskRun() {
	// this is run in a separate process

	defer func() {
		atomic.AddUintptr(&dl.taskCnt, ^uintptr(0))
		atomic.AddUintptr(&dl.dlm.taskCnt, ^uintptr(0))
		// do not block
		select {
		case dl.dlm.idleTrigger <- struct{}{}:
		default:
		}
	}()

	dl.handler.lk.Lock()
	defer dl.handler.lk.Unlock()
	dl.lk.Lock()
	defer dl.lk.Unlock()

	// increase timer now to avoid deletion
	dl.expire = time.Now().Add(time.Minute)

	if dl.reader != nil {
		cnt := dl.handler.wantsFollowing(dl.rPos)
		if cnt > 0 {
			rPos := dl.rPos
			// let's just read this from existing reader
			buf := make([]byte, cnt)
			n, err := io.ReadFull(dl.reader.Body, buf)
			if err != nil && err != io.ErrUnexpectedEOF {
				dl.dlm.logf("idle read failed: %s", err)
				dl.reader.Body.Close()
				dl.reader = nil
			}
			dl.rPos += int64(n)

			// feed it
			err = dl.handler.ingestData(buf[:n], rPos)
			if err != nil {
				dl.dlm.logf("idle write failed: %s", err)
				dl.failure = true
			}
			return
		}

		dl.reader.Body.Close()
		dl.reader = nil
	}

	// let's just ask where to start
	off := dl.handler.firstMissing()
	if off < 0 {
		// do not download
		if dl.handler.isComplete() {
			dl.complete = true
		}
		return
	}

	// spawn a new reader
	req, err := http.NewRequest("GET", dl.url, nil)
	if err != nil {
		dl.dlm.logf("idle: failed to create request: %s", err)
		return
	}

	if off != 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", off))
	}

	dl.dlm.logf("idle: initializing HTTP connection download at byte %d~", off)

	// should respond with code 206 Partial Content
	resp, err := dl.dlm.Client.Do(req)
	if err != nil {
		dl.dlm.logf("idle download failed: %s", err)
		return
	}
	if resp.StatusCode > 299 {
		// that's bad
		resp.Body.Close()
		dl.dlm.logf("idle download failed due to status %s", resp.Status)
		return
	}
	dl.reader = resp
	dl.rPos = off

	cnt := dl.handler.wantsFollowing(off)
	if cnt <= 0 {
		// why?
		return
	}

	buf := make([]byte, cnt)
	n, err := io.ReadFull(dl.reader.Body, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		dl.dlm.logf("idle read failed: %s", err)
		dl.reader.Body.Close()
		dl.reader = nil
	}
	dl.rPos += int64(n)

	// feed it (use separate thread to avoid deadlock)
	err = dl.handler.ingestData(buf[:n], off)
	if err != nil {
		dl.dlm.logf("idle write failed: %s", err)
		dl.failure = true
	}
}
