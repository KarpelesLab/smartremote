package smartremote

import (
	"net/http"
	"net/url"
	"os"
	"sync"
)

type File struct {
	path     string   // local path on disk
	pathMeta string   // local path on disk of metadata
	url      *url.URL // url

	client  *http.Client
	offset  int64 // offset in url
	size    int64 // size of url
	hasSize bool  // is size valid
	pos     int64 // read position in file

	local    *os.File
	complete bool // file is fully local, no need for any network activity

	blkSize int64

	lk sync.RWMutex
}
