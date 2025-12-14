package smartremote

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
)

// getSize fetches the file size from the remote server via a HEAD request.
// If the server doesn't support HEAD or doesn't return Content-Length,
// it falls back to downloading the entire file.
func (f *File) getSize() error {
	// note: this should run in lock

	if f.hasSize {
		return nil
	}

	// attempt to get size
	if f.complete {
		siz, err := f.local.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		f.size = siz
		f.hasSize = true
		return nil
	}

	// run query
	res, err := f.dlm.Client.Head(f.url)
	if err != nil {
		return err
	}
	res.Body.Close()

	if res.StatusCode != http.StatusOK {
		// could be linked to another kind of error (for example signed S3 urls won't let us do HEAD)
		// fallback to full download
		return f.downloadFull()
	}

	if res.ContentLength == -1 {
		// TODO: download whole file, return final size
		return fmt.Errorf("HTTP HEAD response has no Content-Length")
	}

	// might want to check for "Accept-Ranges" header

	f.size = res.ContentLength
	f.hasSize = true
	f.local.Truncate(f.size)

	return nil
}

// SetSize manually sets the file size for incomplete files. This is useful
// when the file size is known in advance but the server doesn't provide a
// Content-Length header. Has no effect on complete files.
func (f *File) SetSize(size int64) {
	if f.complete {
		// do not allow SetSize() on complete files
		return
	}

	// set size and truncate
	f.size = size
	f.hasSize = true
	f.local.Truncate(f.size)
}

// GetSize returns a file's size according to the remote server.
func (f *File) GetSize() (int64, error) {
	f.lk.Lock()
	defer f.lk.Unlock()

	if !f.hasSize {
		err := f.getSize()
		if err != nil {
			return 0, err
		}
	}

	return f.size, nil
}

// Stat() will obtain the information of the underlying file after checking
// its size matches that of the file to download.
func (f *File) Stat() (os.FileInfo, error) {
	_, err := f.GetSize()
	if err != nil {
		return nil, err
	}

	return f.local.Stat()
}

// Seek in file for next Read() operation. If you use io.SeekEnd but the file
// download hasn't started, a HEAD request will be made to obtain the file's
// size.
func (f *File) Seek(offset int64, whence int) (int64, error) {
	f.lk.Lock()
	defer f.lk.Unlock()

	switch whence {
	case io.SeekStart:
		if offset < 0 {
			return f.pos, errors.New("invalid seek")
		}
		f.pos = offset
		return f.pos, nil
	case io.SeekCurrent:
		if f.pos+offset < 0 {
			return f.pos, errors.New("invalid seek")
		}
		f.pos += offset
		return f.pos, nil
	case io.SeekEnd:
		err := f.getSize()
		if err != nil {
			return f.pos, err
		}

		if f.size+offset < 0 {
			return f.pos, errors.New("invalid seek")
		}
		f.pos = f.size + offset
		return f.pos, nil
	default:
		return f.pos, errors.New("invalid seek whence")
	}
}

// Read will read data from the file at the current position after checking
// it was successfully downloaded.
func (f *File) Read(p []byte) (n int, err error) {
	// read data
	f.lk.Lock()
	defer f.lk.Unlock()

	n, err = f.readAt(p, f.pos)
	if err == nil {
		f.pos += int64(n)
	}
	return
}

// ReadAt will read data from the disk at a specified offset after checking
// it was successfully downloaded.
func (f *File) ReadAt(p []byte, off int64) (int, error) {
	f.lk.Lock()
	defer f.lk.Unlock()

	return f.readAt(p, off)
}

// readAt is the internal implementation of ReadAt. It ensures the required
// blocks are downloaded before reading from the local file.
func (f *File) readAt(p []byte, off int64) (int, error) {
	if f.complete {
		// file is complete, let the OS handle that
		return f.local.ReadAt(p, off)
	}

	err := f.getSize()
	if err != nil {
		return 0, err
	}

	// let's check if off+p > f.size
	if off >= f.size {
		return 0, io.EOF
	}
	if off+int64(len(p)) > f.size {
		// reduce p to max len
		p = p[:f.size-off]
	}

	// ok so now we need to calculate the first and last block we'll need
	// first block is floor(off/blkSize), last block is floor((off+len)/blkSize)
	firstBlock := uint32(off / f.blkSize)
	lastBlock := uint32((off + int64(len(p))) / f.blkSize)

	// this will ensure blocks are downloaded and in f.local
	err = f.needBlocks(firstBlock, lastBlock)
	if err != nil {
		return 0, err
	}

	// let the OS handle the rest
	return f.local.ReadAt(p, off)
}

// Complete will download the whole file locally, returning errors in case of
// failure.
func (f *File) Complete() error {
	// read data
	f.lk.Lock()
	defer f.lk.Unlock()

	if f.complete {
		// file is complete, no need to do anything
		return nil
	}

	// get size
	err := f.getSize()
	if err != nil {
		return err
	}

	blkCount := uint32(f.getBlockCount())

	// find out first missing blocks
	for i := uint32(0); i < blkCount; i++ {
		if !f.status.Contains(uint32(i)) {
			err = f.needBlocks(i, i)
			if err != nil {
				return err
			}
		}
	}
	f.isComplete()

	return nil
}
