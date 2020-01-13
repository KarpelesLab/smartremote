package smartremote

import (
	"errors"
	"fmt"
	"io"
	"log"
)

func (f *File) downloadFull() error {
	// download the file fully
	// (we are in a lock)
	if f.complete {
		return nil
	}
	f.local.Seek(0, io.SeekStart)
	resp, err := f.dlm.Client.Get(f.url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// check response code
	if resp.StatusCode > 299 {
		// that's bad
		return fmt.Errorf("failed to download: %s", resp.Status)
	}

	// let's just perform a regular copy
	n, err := io.Copy(f.local, resp.Body)
	if err != nil {
		// ok that's a big failure
		f.status.Clear() // because likely corrupt
		f.savePart()
		return err
	}

	// now let's ensure our file size is correct
	f.local.Truncate(n)

	f.complete = true
	f.hasSize = true
	f.size = n
	f.savePart()

	return nil
}

func (f *File) needBlocks(start, end uint32) error {
	// ensure listed blocks exist and are downloaded
	// need to be called with lock acquired
	err := f.getSize()
	if err != nil {
		return err
	}

	if end < start {
		return errors.New("invalid values: end is lower than start")
	}

	// trim start/end based on known downloaded blocks
	for {
		if f.status.Contains(start) && (start < end) {
			start += 1
		} else {
			break
		}
	}

	for {
		if f.status.Contains(end) && (end > start) {
			end -= 1
		} else {
			break
		}
	}

	if start == end && f.status.Contains(start) {
		// we already have all blocks
		return nil
	}

	posByte := int64(start) * f.blkSize
	//f.local.Seek(posByte, io.SeekStart)
	buf := make([]byte, f.blkSize)

	for start <= end {
		// load a block
		n := f.blkSize
		if posByte+n > f.size {
			// special case: last block
			n = f.size - posByte
		}

		//log.Printf("downloading block %d (%d bytes)", start, n)
		_, err := f.dlm.readUrl(f.url, buf[:n], posByte, f)
		if err != nil {
			log.Printf("download error: %s", err)
			if f.status.IsEmpty() && posByte != 0 {
				// this is typically linked to backend refusing to let us do partial download
				return f.downloadFull()
			}
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return err
		}

		_, err = f.local.WriteAt(buf[:n], posByte)
		if err != nil {
			// failed to write (disk full?)
			log.Printf("write error: %s", err)
			return err
		}

		// write to local
		f.status.Add(start)

		// increment start
		start += 1
		posByte += f.blkSize
	}

	f.savePart()

	return nil
}
