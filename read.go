package smartremote

import (
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

	f.hasSize = true
	f.size = res.ContentLength

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
