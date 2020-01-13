package smartremote

import (
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

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

func (dlm *DownloadManager) readUrl(url string, p []byte, off int64, handler *File) (int, error) {
	dl := dlm.getClient(url, handler)
	defer atomic.AddUintptr(&dl.taskCnt, ^uintptr(0))
	defer atomic.AddUintptr(&dlm.taskCnt, ^uintptr(0))

	return dl.ReadAt(p, off)
}

func (dl *DownloadManager) getClient(u string, handler *File) *dlClient {
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

		if cl.complete || cl.expire.Before(now) {
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
