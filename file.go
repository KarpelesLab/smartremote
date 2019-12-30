package smartremote

import (
	"net/http"
	"os"
	"sync"

	"github.com/KarpelesLab/idlock"
	"github.com/RoaringBitmap/roaring"
)

type File struct {
	path string // local path on disk
	url  string // url
	hash [32]byte

	client  *http.Client
	offset  int64 // offset in url
	size    int64 // size of url
	hasSize bool  // is size valid
	pos     int64 // read position in file

	local    *os.File
	complete bool            // file is fully local, no need for any network activity
	status   *roaring.Bitmap // download status (each bit is 1 block, 1=downloaded)

	blkSize int64

	lk  sync.RWMutex
	mlk *idlock.IntLock

	dlm *DownloadManager
}
