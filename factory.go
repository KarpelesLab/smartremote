package smartremote

import (
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
)

var (
	openFiles   = make(map[[32]byte]*File)
	openFilesLk sync.RWMutex
)

func Simple(u string) (*File, error) {
	uObj, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	// generate hash
	hash := sha256.Sum256([]byte(u))

	openFilesLk.RLock()
	f, ok := openFiles[hash]
	openFilesLk.RUnlock()

	// if found, end there
	if ok {
		return f, nil
	}

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
		url:    uObj,
		path:   localPath,
		client: http.DefaultClient,
	}

	fp, err := os.OpenFile(localPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0700)
	if err != nil {
		if !os.IsExist(err) {
			return nil, err
		}

		// file exists
		fp, err = os.Open(localPath)
		if err != nil {
			return nil, err
		}

		f.complete = true
	}
	f.local = fp

	openFiles[hash] = f

	return f, nil
}
