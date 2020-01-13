package smartremote

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type DownloadTarget interface {
	WantsFollowing(offset int64) int         // returns >0 if the bytes starting at offset are useful
	FirstMissing() int64                     // returns first missing byte, or -1 if file is complete
	IngestData(b []byte, offset int64) error // stores data received
}

type downloadFeed interface {
	// feed intake can only be called during downloads for locking reasons
	// it does the same as IngestData() but without the lock, which is already acquired during download
	feed(b []byte, offset int64) error
	getBlockSize() int64
}

type DownloadManager struct {
	// MaxConcurrent is the maximum number of concurrent downloads.
	// changing it might not be effective immediately. Default is 10
	MaxConcurrent int

	// Client is the http client used to access urls to be downloaded
	Client *http.Client

	// TmpDir is where temporary files are created, and by default will be os.TempDir()
	TmpDir string

	// MaxDataJump is the maximum data that can be read & dropped when seeking forward
	// default is 128k
	MaxDataJump int64

	clients     map[string]*dlClient
	mapLock     sync.Mutex
	cd          *sync.Cond
	taskCnt     uintptr
	idleTrigger chan struct{}

	openFiles   map[[32]byte]*File
	openFilesLk sync.RWMutex
}

type dlClient struct {
	dlm     *DownloadManager
	url     string
	taskCnt uintptr // currently running/pending tasks
	handler DownloadTarget

	reader *http.Response
	rPos   int64 // in bytes
	lk     sync.Mutex
	expire time.Time
}

type dlReaderAt struct {
	dl  *DownloadManager
	url string
}

var DefaultDownloadManager = NewDownloadManager()

func NewDownloadManager() *DownloadManager {
	dl := &DownloadManager{
		MaxConcurrent: 10,
		Client:        http.DefaultClient,
		TmpDir:        os.TempDir(),
		MaxDataJump:   512 * 1024, // 512kB
		clients:       make(map[string]*dlClient),
		openFiles:     make(map[[32]byte]*File),
		idleTrigger:   make(chan struct{}),
	}
	dl.cd = sync.NewCond(&dl.mapLock)

	go dl.managerTask()

	return dl
}

func (dl *DownloadManager) For(u string) io.ReaderAt {
	return &dlReaderAt{dl, u}
}

func (dlr *dlReaderAt) ReadAt(p []byte, off int64) (int, error) {
	return dlr.dl.readUrl(dlr.url, p, off, nil)
}

func (dlm *DownloadManager) readUrl(url string, p []byte, off int64, handler DownloadTarget) (int, error) {
	dl := dlm.getClient(url, handler)
	defer atomic.AddUintptr(&dl.taskCnt, ^uintptr(0))
	defer atomic.AddUintptr(&dlm.taskCnt, ^uintptr(0))

	return dl.ReadAt(p, off)
}

func (dl *DownloadManager) getClient(u string, handler DownloadTarget) *dlClient {
	dl.mapLock.Lock()

	for {
		if cl, ok := dl.clients[u]; ok {
			atomic.AddUintptr(&cl.taskCnt, 1)
			atomic.AddUintptr(&dl.taskCnt, 1)
			dl.mapLock.Unlock()
			return cl
		}

		if dl.MaxConcurrent > 0 && len(dl.clients) >= dl.MaxConcurrent {
			// attempt to reap clients
			dl.internalReap()
		}
		if dl.MaxConcurrent > 0 && len(dl.clients) >= dl.MaxConcurrent {
			// still too many running. Let's wait a bit.
			dl.cd.Wait()
			continue
		}

		// can add a thread
		cl := &dlClient{
			dlm:     dl,
			url:     u,
			taskCnt: 1, // pre-init at 1 to avoid reap
			expire:  time.Now().Add(300 * time.Second),
			handler: handler,
		}
		atomic.AddUintptr(&dl.taskCnt, 1)
		dl.clients[u] = cl
		dl.mapLock.Unlock()
		return cl
	}
}

func (dlm *DownloadManager) managerTask() {
	t := time.NewTicker(time.Second)
	for {
		select {
		case <-t.C:
		case _, ok := <-dlm.idleTrigger:
			if !ok {
				// need to shut down
				return
			}
		}
		dlm.intervalProcess()
	}
}

func (dlm *DownloadManager) intervalProcess() bool {
	dlm.mapLock.Lock()
	defer dlm.mapLock.Unlock()

	if len(dlm.clients) == 0 {
		return true // idle
	}

	change := false
	now := time.Now()

	for u, cl := range dlm.clients {
		if atomic.LoadUintptr(&cl.taskCnt) != 0 {
			continue
		}

		if cl.expire.Before(now) {
			delete(dlm.clients, u)
			go cl.Close() // let close run in thread so we don't get locked
			change = true
		}

		if cl.handler == nil {
			continue
		}

		if atomic.LoadUintptr(&dlm.taskCnt) == 0 {
			atomic.AddUintptr(&dlm.taskCnt, 1)
			atomic.AddUintptr(&cl.taskCnt, 1)
			go cl.idleTaskRun()
		}
	}

	if change {
		dlm.cd.Broadcast()
	}
	return false
}

func (dl *dlClient) idleTaskRun() {
	// this is run in a separate process

	defer func() {
		atomic.AddUintptr(&dl.taskCnt, ^uintptr(0))
		atomic.AddUintptr(&dl.dlm.taskCnt, ^uintptr(0))
		dl.dlm.idleTrigger <- struct{}{}
	}()

	// increase timer now to avoid deletion
	dl.expire = time.Now().Add(time.Minute)

	dl.lk.Lock()
	defer dl.lk.Unlock()

	if dl.reader != nil {
		cnt := dl.handler.WantsFollowing(dl.rPos)
		if cnt > 0 {
			rPos := dl.rPos
			// let's just read this from existing reader
			buf := make([]byte, cnt)
			n, err := io.ReadFull(dl.reader.Body, buf)
			if err != nil && err != io.ErrUnexpectedEOF {
				log.Printf("idle read failed: %s", err)
				dl.reader.Body.Close()
				dl.reader = nil
			}
			dl.rPos += int64(n)

			// feed it
			err = dl.handler.IngestData(buf[:n], rPos)
			if err != nil {
				log.Printf("idle write failed: %s", err)
			}
			return
		}

		dl.reader.Body.Close()
		dl.reader = nil
	}

	// let's just ask where to start
	off := dl.handler.FirstMissing()
	if off < 0 {
		// do not download
		return
	}

	// spawn a new reader
	req, err := http.NewRequest("GET", dl.url, nil)

	if off != 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", off))
	}

	log.Printf("idle: initializing HTTP connection download at byte %d~", off)

	// should respond with code 206 Partial Content
	resp, err := dl.dlm.Client.Do(req)
	if err != nil {
		log.Printf("idle download failed: %s", err)
		return
	}
	if resp.StatusCode > 299 {
		// that's bad
		resp.Body.Close()
		log.Printf("idle download failed due to status %s", resp.Status)
		return
	}
	dl.reader = resp
	dl.rPos = off

	cnt := dl.handler.WantsFollowing(off)
	if cnt <= 0 {
		// why?
		return
	}

	buf := make([]byte, cnt)
	n, err := io.ReadFull(dl.reader.Body, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		log.Printf("idle read failed: %s", err)
		dl.reader.Body.Close()
		dl.reader = nil
	}
	dl.rPos += int64(n)

	// feed it
	err = dl.handler.IngestData(buf[:n], off)
	if err != nil {
		log.Printf("idle write failed: %s", err)
	}
	return
}

func (dl *DownloadManager) internalReap() {
	// attempt to reap at least one idle client
	// (lock already acquired by caller)
	for u, cl := range dl.clients {
		if atomic.LoadUintptr(&cl.taskCnt) == 0 {
			// can reap this
			delete(dl.clients, u)
			cl.Close()
			break
		}
	}
}

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

func (dl *dlClient) dropDataCount(cnt, startPos int64) error {
	feeder, ok := dl.handler.(downloadFeed)
	if !ok {
		_, err := io.CopyN(nullWriter{}, dl.reader.Body, cnt)
		return err
	}

	// download data in buffers
	sz := feeder.getBlockSize()
	if sz <= 0 || cnt < sz {
		// doesn't want data?
		_, err := io.CopyN(nullWriter{}, dl.reader.Body, cnt)
		return err
	}

	buf := make([]byte, sz)

	for cnt > 0 {
		if cnt < sz {
			// can't download enough so that it's worth it
			_, err := io.CopyN(nullWriter{}, dl.reader.Body, cnt)
			return err
		}

		_, err := io.ReadFull(dl.reader.Body, buf)
		if err != nil {
			return err
		}

		cnt -= sz

		err = feeder.feed(buf, startPos)
		if err != nil {
			// give up
			_, err := io.CopyN(nullWriter{}, dl.reader.Body, cnt)
			return err
		}

		startPos += sz
	}

	return nil
}

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

	// instanciate a new reader if needed
	if dl.reader == nil {
		// spawn a new reader
		req, err := http.NewRequest("GET", dl.url, nil)

		if off != 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", off))
		}

		log.Printf("initializing HTTP connection download at byte %d~", off)

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
