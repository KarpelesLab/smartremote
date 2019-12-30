package smartremote

import (
	"crypto/sha256"
	"encoding/base64"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

var (
	openFiles   = make(map[[32]byte]*File)
	openFilesLk sync.RWMutex
)

const DefaultBlockSize = 65536

// Open a given URL and return a file pointer that will run partial downloads
// when reads are needed. Downloaded data will be stored in the system temp
// directory, and will be removed at the end if download is incomplete.
func Open(u string) (*File, error) {
	return DefaultDownloadManager.Open(u)
}

// Open a given URL and return a file pointer that will run partial downloads
// when reads are needed. Downloaded data will be stored in the system temp
// directory, and will be removed at the end if download is incomplete.
func (dlm *DownloadManager) Open(u string) (*File, error) {
	// generate hash
	hash := sha256.Sum256([]byte(u))

	openFilesLk.RLock()
	f, ok := openFiles[hash]
	openFilesLk.RUnlock()

	// if found, end there
	if ok {
		return f, nil
	}

	// we stay locked until end of op to avoid issues
	openFilesLk.Lock()
	defer openFilesLk.Unlock()

	// retry (just in case)
	if f, ok = openFiles[hash]; ok {
		return f, nil
	}

	// generate local path
	hashStr := base64.RawURLEncoding.EncodeToString(hash[:])
	localPath := filepath.Join(os.TempDir(), "remote-"+hashStr+".bin")

	f = &File{
		url:     u,
		path:    localPath,
		client:  http.DefaultClient,
		blkSize: DefaultBlockSize,
		dlm:     dlm,
		status:  roaring.New(),
	}

	fp, err := os.OpenFile(localPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		if !os.IsExist(err) {
			return nil, err
		}

		if _, err = os.Stat(localPath + ".part"); err == nil {
			// file is partial, attempt to load part
			fp, err = os.OpenFile(localPath, os.O_RDWR, 0600)
			if err != nil {
				// failed
				return nil, err
			}
			err = f.ReadPart()
			if err != nil {
				log.Printf("failed to resume download: %s", err)
				// truncate
				fp.Truncate(0)
			}
		} else {
			// file exists and is complete
			fp, err = os.Open(localPath)
			if err != nil {
				return nil, err
			}

			siz, err := fp.Seek(0, io.SeekEnd)
			if err == nil {
				f.size = siz
				f.hasSize = true
			}

			f.complete = true
		}
	}
	f.local = fp

	openFiles[hash] = f

	return f, nil
}
