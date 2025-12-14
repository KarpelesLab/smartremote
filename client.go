package smartremote

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// httpReader represents a single HTTP connection for reading from a remote URL.
// Multiple readers can be maintained per file to handle random access patterns
// efficiently (e.g., reading ZIP central directory at end while also reading
// file contents from various positions).
type httpReader struct {
	resp       *http.Response
	pos        int64     // current read position in bytes
	lastAccess time.Time // when this reader was last used
}

// dlClient is an internal HTTP connection handler that manages individual
// connections to remote URLs. It handles Range requests, connection reuse,
// and idle background downloading of missing blocks. Supports multiple
// concurrent readers per file for efficient random access patterns.
type dlClient struct {
	dlm      *DownloadManager
	url      string
	taskCnt  uintptr // currently running/pending tasks
	handler  *File
	failure  bool
	complete bool

	readers []*httpReader // multiple readers for random access
	lk      sync.Mutex
	expire  time.Time
}

// Close closes all HTTP connections and signals completion to the manager.
func (dl *dlClient) Close() error {
	dl.lk.Lock()
	defer dl.lk.Unlock()

	var lastErr error
	for _, r := range dl.readers {
		if r != nil && r.resp != nil {
			if err := r.resp.Body.Close(); err != nil {
				lastErr = err
			}
		}
	}
	dl.readers = nil

	dl.dlm.cd.Broadcast()

	return lastErr
}

// dropDataCount reads and discards cnt bytes from the HTTP response body.
// If a handler is set, it opportunistically saves complete blocks to disk.
func (dl *dlClient) dropDataCount(r *httpReader, cnt, startPos int64) error {
	if dl.handler == nil {
		_, err := io.CopyN(io.Discard, r.resp.Body, cnt)
		return err
	}

	// download data in buffers
	sz := dl.handler.getBlockSize()
	if sz <= 0 || cnt < sz {
		// doesn't want data?
		_, err := io.CopyN(io.Discard, r.resp.Body, cnt)
		return err
	}

	buf := make([]byte, sz)

	for cnt > 0 {
		if cnt < sz {
			// can't download enough so that it's worth it
			_, err := io.CopyN(io.Discard, r.resp.Body, cnt)
			return err
		}

		_, err := io.ReadFull(r.resp.Body, buf)
		if err != nil {
			return err
		}

		cnt -= sz

		err = dl.handler.ingestData(buf, startPos)
		if err != nil {
			// give up
			_, err := io.CopyN(io.Discard, r.resp.Body, cnt)
			return err
		}

		startPos += sz
	}

	return nil
}

// ReadAt reads data from the remote URL at the specified offset into p.
// It manages multiple HTTP connections per file to handle random access patterns
// efficiently. Connections are reused when possible, and LRU eviction is used
// when the maximum number of readers is reached.
func (dl *dlClient) ReadAt(p []byte, off int64) (int, error) {
	dl.lk.Lock()
	defer dl.lk.Unlock()

	dl.expire = time.Now().Add(time.Minute)
	endPos := off + int64(len(p))

	// Find the best reader for this request
	var bestReader *httpReader
	var bestDistance int64 = -1

	for _, r := range dl.readers {
		if r == nil || r.resp == nil {
			continue
		}

		// Reader must be at or before our target offset
		if r.pos > off {
			continue
		}

		distance := off - r.pos
		if distance > dl.dlm.MaxDataJump {
			continue
		}

		// This reader can serve the request
		if bestReader == nil || distance < bestDistance {
			bestReader = r
			bestDistance = distance
		}
	}

	// If we found a suitable reader, check if reading would overlap another reader
	if bestReader != nil {
		// Close any readers that would be "passed" by this read
		dl.closeOverlappingReaders(bestReader, endPos)

		// Skip ahead if needed
		if bestDistance > 0 {
			err := dl.dropDataCount(bestReader, bestDistance, bestReader.pos)
			if err != nil {
				// Failed, close this reader and try to create new one
				bestReader.resp.Body.Close()
				dl.removeReader(bestReader)
				bestReader = nil
			} else {
				bestReader.pos = off
			}
		}
	}

	// Create a new reader if needed
	if bestReader == nil {
		var err error
		bestReader, err = dl.createReader(off)
		if err != nil {
			return 0, err
		}
	}

	// Update last access time
	bestReader.lastAccess = time.Now()

	// Read the data
	n, err := io.ReadFull(bestReader.resp.Body, p)
	if err != nil {
		bestReader.resp.Body.Close()
		dl.removeReader(bestReader)
	} else {
		bestReader.pos += int64(n)
	}

	return n, err
}

// closeOverlappingReaders closes any readers whose position would be passed
// by reading up to endPos with the selected reader.
func (dl *dlClient) closeOverlappingReaders(selected *httpReader, endPos int64) {
	for i := 0; i < len(dl.readers); i++ {
		r := dl.readers[i]
		if r == nil || r == selected || r.resp == nil {
			continue
		}

		// If this reader's position is between selected's position and our end position,
		// close it since we'd pass it anyway
		if r.pos >= selected.pos && r.pos < endPos {
			r.resp.Body.Close()
			dl.readers = append(dl.readers[:i], dl.readers[i+1:]...)
			i--
		}
	}
}

// removeReader removes a reader from the readers slice.
func (dl *dlClient) removeReader(r *httpReader) {
	for i, reader := range dl.readers {
		if reader == r {
			dl.readers = append(dl.readers[:i], dl.readers[i+1:]...)
			return
		}
	}
}

// createReader creates a new HTTP reader at the specified offset.
// If at the maximum number of readers, closes the least recently used one first.
func (dl *dlClient) createReader(off int64) (*httpReader, error) {
	maxReaders := dl.dlm.MaxReadersPerFile
	if maxReaders <= 0 {
		maxReaders = 3
	}

	// If at limit, close LRU reader
	if len(dl.readers) >= maxReaders {
		dl.closeLRUReader()
	}

	// Create new HTTP request
	req, err := http.NewRequest("GET", dl.url, nil)
	if err != nil {
		return nil, err
	}

	if off != 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", off))
	}

	dl.dlm.logf("initializing HTTP connection download at byte %d~ (readers: %d)", off, len(dl.readers)+1)

	resp, err := dl.dlm.Client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode > 299 {
		resp.Body.Close()
		return nil, fmt.Errorf("failed to download: %s", resp.Status)
	}

	r := &httpReader{
		resp:       resp,
		pos:        off,
		lastAccess: time.Now(),
	}
	dl.readers = append(dl.readers, r)

	return r, nil
}

// closeLRUReader closes the least recently used reader.
func (dl *dlClient) closeLRUReader() {
	if len(dl.readers) == 0 {
		return
	}

	var lru *httpReader
	var lruIdx int

	for i, r := range dl.readers {
		if r == nil || r.resp == nil {
			continue
		}
		if lru == nil || r.lastAccess.Before(lru.lastAccess) {
			lru = r
			lruIdx = i
		}
	}

	if lru != nil {
		lru.resp.Body.Close()
		dl.readers = append(dl.readers[:lruIdx], dl.readers[lruIdx+1:]...)
	}
}

// idleTaskRun is called during idle periods to download missing blocks
// in the background. It runs in a separate goroutine and downloads multiple
// consecutive blocks until approximately 1 second has elapsed.
// It releases locks during I/O to avoid blocking user ReadAt calls.
func (dl *dlClient) idleTaskRun() {
	defer func() {
		atomic.AddUintptr(&dl.taskCnt, ^uintptr(0))
		atomic.AddUintptr(&dl.dlm.taskCnt, ^uintptr(0))
		// do not block
		select {
		case dl.dlm.idleTrigger <- struct{}{}:
		default:
		}
	}()

	startTime := time.Now()
	blocksDownloaded := 0
	var buf []byte

	// Save progress at the end if we downloaded anything
	defer func() {
		if blocksDownloaded > 0 {
			dl.handler.lk.Lock()
			dl.handler.savePart()
			dl.handler.lk.Unlock()
			dl.dlm.logf("idle: downloaded %d blocks in %v", blocksDownloaded, time.Since(startTime))
		}
	}()

	// Download blocks until ~1 second has passed
	for time.Since(startTime) < time.Second {
		// Acquire locks to find/take a reader
		dl.handler.lk.Lock()
		dl.lk.Lock()

		dl.expire = time.Now().Add(time.Minute)

		// Initialize buffer on first iteration
		if buf == nil {
			buf = make([]byte, dl.handler.getBlockSize())
		}

		// Find and take a reader at a useful position
		var idleReader *httpReader
		for i, r := range dl.readers {
			if r == nil || r.resp == nil {
				continue
			}
			cnt := dl.handler.wantsFollowing(r.pos)
			if cnt > 0 {
				idleReader = r
				// Remove from slice - we're taking ownership
				dl.readers = append(dl.readers[:i], dl.readers[i+1:]...)
				break
			}
		}

		// If no suitable reader, find first missing block and create one
		var off int64 = -1
		if idleReader == nil {
			off = dl.handler.firstMissing()
			if off < 0 {
				if dl.handler.isComplete() {
					dl.complete = true
				}
				dl.lk.Unlock()
				dl.handler.lk.Unlock()
				return
			}
		}

		// Release locks before I/O
		dl.lk.Unlock()
		dl.handler.lk.Unlock()

		// Create new reader if needed (outside of lock)
		if idleReader == nil {
			req, err := http.NewRequest("GET", dl.url, nil)
			if err != nil {
				dl.dlm.logf("idle: failed to create request: %s", err)
				return
			}

			if off != 0 {
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-", off))
			}

			dl.dlm.logf("idle: initializing HTTP connection download at byte %d~", off)

			resp, err := dl.dlm.Client.Do(req)
			if err != nil {
				dl.dlm.logf("idle download failed: %s", err)
				return
			}
			if resp.StatusCode > 299 {
				resp.Body.Close()
				dl.dlm.logf("idle download failed due to status %s", resp.Status)
				return
			}

			idleReader = &httpReader{
				resp:       resp,
				pos:        off,
				lastAccess: time.Now(),
			}
		}

		// Read a block (outside of lock)
		dl.handler.lk.RLock()
		cnt := dl.handler.wantsFollowing(idleReader.pos)
		dl.handler.lk.RUnlock()

		if cnt <= 0 {
			// Position already downloaded, close reader and continue
			idleReader.resp.Body.Close()
			continue
		}

		readBuf := buf[:cnt]
		rPos := idleReader.pos
		n, err := io.ReadFull(idleReader.resp.Body, readBuf)
		if err != nil && err != io.ErrUnexpectedEOF {
			dl.dlm.logf("idle read failed: %s", err)
			idleReader.resp.Body.Close()
			if n == 0 {
				continue
			}
		}
		idleReader.pos += int64(n)
		idleReader.lastAccess = time.Now()

		// Ingest the data (needs lock)
		dl.handler.lk.Lock()
		err = dl.handler.ingestDataBatch(readBuf[:n], rPos)
		dl.handler.lk.Unlock()

		if err != nil {
			dl.dlm.logf("idle write failed: %s", err)
			idleReader.resp.Body.Close()
			dl.lk.Lock()
			dl.failure = true
			dl.lk.Unlock()
			return
		}
		blocksDownloaded++

		// Return reader to the pool if still useful, otherwise close it
		dl.handler.lk.RLock()
		stillUseful := dl.handler.wantsFollowing(idleReader.pos) > 0
		dl.handler.lk.RUnlock()

		if stillUseful {
			dl.lk.Lock()
			dl.readers = append(dl.readers, idleReader)
			dl.lk.Unlock()
		} else {
			idleReader.resp.Body.Close()
		}
	}
}
