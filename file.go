package smartremote

import (
	"os"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

// File represents a remote file that can be accessed locally through partial
// downloads. It implements io.Reader, io.ReaderAt, and io.Seeker interfaces,
// allowing transparent access to remote HTTP content as if it were a local file.
// Downloaded data is cached locally in blocks, and only the required portions
// are fetched on demand. Partial download progress can be persisted to disk
// and resumed later.
type File struct {
	path string // local path on disk
	url  string // url
	hash [32]byte

	size    int64 // size of url
	hasSize bool  // is size valid
	pos     int64 // read position in file

	local    *os.File
	complete bool            // file is fully local, no need for any network activity
	status   *roaring.Bitmap // download status (each bit is 1 block, 1=downloaded)

	blkSize int64

	lk sync.RWMutex

	dlm *DownloadManager
}
