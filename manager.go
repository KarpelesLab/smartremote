package smartremote

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
)

type DownloadManager struct {
	MaxConcurrent int          // maximum number of concurrent downloads, changing it might not be effective immediately. Default is 10
	Client        *http.Client // Client is the http client used to access urls to be downloaded
	TmpDir        string       // TmpDir is where temporary files are created, and by default will be os.TempDir()
	MaxDataJump   int64        // maximum data that can be read & dropped when seeking forward, default is 128k

	clients map[string]*dlClient
	mapLock sync.Mutex
	cd      *sync.Cond
}

type dlClient struct {
	dlm     *DownloadManager
	url     string
	taskCnt uintptr // currently running/pending tasks

	reader *http.Response
	rPos   int64 // in bytes
	lk     sync.Mutex
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
		MaxDataJump:   131072,
		clients:       make(map[string]*dlClient),
	}
	dl.cd = sync.NewCond(&dl.mapLock)

	return dl
}

func (dl *DownloadManager) For(u string) io.ReaderAt {
	return &dlReaderAt{dl, u}
}

func (dlr *dlReaderAt) ReadAt(p []byte, off int64) (int, error) {
	return dlr.dl.readUrl(dlr.url, p, off)
}

func (dlm *DownloadManager) readUrl(url string, p []byte, off int64) (int, error) {
	dl := dlm.getClient(url)
	defer atomic.AddUintptr(&dl.taskCnt, ^uintptr(0))

	return dl.ReadAt(p, off)
}

func (dl *DownloadManager) getClient(u string) *dlClient {
	dl.mapLock.Lock()

	for {
		if cl, ok := dl.clients[u]; ok {
			atomic.AddUintptr(&cl.taskCnt, 1)
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
		}
		dl.clients[u] = cl
		dl.mapLock.Unlock()
		return cl
	}
}

func (dl *DownloadManager) internalReap() {
	upd := false
	// reap (lock already acquired by caller)
	for u, cl := range dl.clients {
		if atomic.LoadUintptr(&cl.taskCnt) == 0 {
			// can reap this
			delete(dl.clients, u)
			cl.Close()
			upd = true
		}
	}

	if upd {
		dl.cd.Broadcast()
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
				_, err := io.CopyN(nullWriter{}, dl.reader.Body, off-dl.rPos)
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
		dl.reader = resp
		dl.rPos = off
	}

	n, err := io.ReadFull(dl.reader.Body, p)
	if err != nil {
		dl.reader.Body.Close()
		dl.reader = nil
	} else {
		dl.rPos += int64(n)
	}

	return n, err
}