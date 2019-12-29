package smartremote

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
)

func (f *File) needBlocks(start, end int) error {
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
		if f.hasBlock(start) && (start < end) {
			start += 1
		} else {
			break
		}
	}

	for {
		if f.hasBlock(end) && (end > start) {
			end -= 1
		} else {
			break
		}
	}

	if start == end && f.hasBlock(start) {
		// we already have all blocks
		return nil
	}

	// check if current reader can be used
	if f.reader != nil {
		// check if can be used
		if f.rPos > start {
			// nope
			f.reader.Body.Close()
			f.reader = nil
		} else if f.rPos < start {
			// check if diff is lower than 5
			if start-f.rPos < 5 {
				// we can read the extra data and commit it to disk
				start = f.rPos
			} else {
				f.reader.Body.Close()
				f.reader = nil
			}
		}
	}

	// instanciate a new reader if needed
	if f.reader == nil {
		// spawn a new reader
		req, err := http.NewRequest("GET", f.url, nil)

		if start != 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", int64(start)*f.blkSize))
		}

		log.Printf("initializing HTTP connection download at block %d~", start)

		// should respond with code 206 Partial Content
		resp, err := f.client.Do(req)
		if err != nil {
			return err
		}
		f.reader = resp
		f.rPos = start
	}

	blk := make([]byte, f.blkSize)

	for f.rPos <= end {
		log.Printf("downloading block %d", f.rPos)
		// load a block
		n, err := io.ReadFull(f.reader.Body, blk)
		if err != nil {
			f.reader.Body.Close()
			f.reader = nil
			return err
		}

		// write to local
		_, err = f.local.WriteAt(blk[:n], int64(f.rPos)*f.blkSize)
		if err != nil {
			f.reader.Body.Close()
			f.reader = nil
			return err
		}
		f.setBlock(f.rPos)

		// increment rPos
		f.rPos += 1
	}

	return nil
}

func (f *File) hasBlock(b int) bool {
	byt := b / 8
	if len(f.status) < byt {
		// too far?
		return false
	}
	v := f.status[byt]

	bit := byte(b % 8)
	mask := byte(1 << (7 - bit))

	return v&mask != 0
}

func (f *File) setBlock(b int) {
	byt := b / 8
	if len(f.status) < byt {
		return
	}

	bit := byte(b % 8)
	mask := byte(1 << (7 - bit))

	f.status[byt] |= mask
}
