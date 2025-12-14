package smartremote

import (
	"crypto/sha256"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"

	"github.com/RoaringBitmap/roaring"
)

// DefaultBlockSize is the size in bytes of each download block (64KB).
// Downloaded data is tracked and stored in blocks of this size.
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

	dlm.openFilesLk.RLock()
	f, ok := dlm.openFiles[hash]
	dlm.openFilesLk.RUnlock()

	// if found, end there
	if ok {
		return f, nil
	}

	// generate local path
	hashStr := base64.RawURLEncoding.EncodeToString(hash[:])
	localPath := filepath.Join(os.TempDir(), "remote-"+hashStr+".bin")

	return dlm.OpenTo(u, localPath)
}

// OpenTo opens a given URL and stores downloaded data at the specified local
// path. If the file already exists with a .part file, the download will resume.
// If the file exists without a .part file, it is assumed to be complete.
func (dlm *DownloadManager) OpenTo(u, localPath string) (*File, error) {
	// generate hash (again if called with Open)
	hash := sha256.Sum256([]byte(u))

	// we stay locked until end of op to avoid issues
	dlm.openFilesLk.Lock()
	defer dlm.openFilesLk.Unlock()

	// retry (just in case)
	if f, ok := dlm.openFiles[hash]; ok {
		return f, nil
	}

	f := &File{
		url:     u,
		path:    localPath,
		hash:    hash,
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
			err = f.readPart()
			if err != nil {
				dlm.logf("failed to resume download: %s", err)
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
	} else {
		// file was created, let's add .part too
		fp2, err := os.OpenFile(localPath+".part", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		if err == nil {
			fp2.Close()
		}
	}
	f.local = fp

	dlm.openFiles[hash] = f

	return f, nil
}
