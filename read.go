package smartremote

import (
	"errors"
	"fmt"
	"io"
	"net/http"
)

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
	res, err := f.client.Head(f.url)
	if err != nil {
		return err
	}
	res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP request response: %s", res.Status)
	}

	if res.ContentLength == -1 {
		// TODO: download whole file, return final size
		return fmt.Errorf("HTTP HEAD response has no Content-Length")
	}

	// might want to check for "Accept-Ranges" header

	f.hasSize = true
	f.size = res.ContentLength
	f.local.Truncate(f.size)
	f.status = make([]byte, int64(f.size/f.blkSize))

	return nil
}

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

func (f *File) ReadAt(p []byte, off int64) (int, error) {
	f.lk.Lock()
	defer f.lk.Unlock()

	return f.readAt(p, off)
}

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
	firstBlock := int(off / f.blkSize)
	lastBlock := int((off + int64(len(p))) / f.blkSize)

	// this will ensure blocks are downloaded and in f.local
	err = f.needBlocks(firstBlock, lastBlock)
	if err != nil {
		return 0, err
	}

	// let the OS handle the rest
	return f.local.ReadAt(p, off)
}
