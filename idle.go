package smartremote

import (
	"errors"
	"log"
)

func (f *File) WantsFollowing(offset int64) int {
	f.lk.Lock()
	defer f.lk.Unlock()

	block := offset / f.blkSize
	if block*f.blkSize != offset {
		// wrong offset (not block aligned), don't care
		return 0
	}

	if f.status.Contains(uint32(block)) {
		return 0
	}

	return int(f.blkSize)
}

func (f *File) FirstMissing() int64 {
	f.lk.Lock()
	defer f.lk.Unlock()

	if f.complete {
		// nothing to download
		return -1
	}

	if f.status.IsEmpty() {
		// everything to download
		return 0
	}

	err := f.getSize()
	if err != nil {
		log.Printf("failed to get file size: %s", err)
		return -1 // can't be helped
	}

	// computer number of blocks
	blkCount := f.size / f.blkSize
	if f.size%f.blkSize != 0 {
		blkCount += 1
	}

	if int64(f.status.GetCardinality()) == blkCount {
		// we already have all blocks
		f.complete = true
		f.savePart()
		return -1
	}

	// find out first missing block
	// TODO this can probably be optimized, roaring api may have something
	for i := int64(0); i < blkCount; i++ {
		if !f.status.Contains(uint32(i)) {
			return i * f.blkSize
		}
	}

	// ?????
	// did we have more blocks in status than we need? did file size change? this sounds like everything is likely corrupted...
	return -1
}

func (f *File) IngestData(b []byte, offset int64) error {
	f.lk.Lock()
	defer f.lk.Unlock()

	return f.feed(b, offset)
}

func (f *File) feed(b []byte, offset int64) error {
	block := offset / f.blkSize
	if block*f.blkSize != offset {
		return errors.New("invalid offset (not block aligned)")
	}
	count := int64(len(b)) / f.blkSize
	if f.blkSize*count != int64(len(b)) {
		if count > 1 {
			// trim
			b = b[:count*f.blkSize]
		} else {
			return errors.New("invalid buffer length (not block aligned)")
		}
	}

	_, err := f.local.WriteAt(b, offset)
	if err != nil {
		return err
	}

	// mark blocks as received
	f.status.AddRange(uint64(block), uint64(block+count)) // [rangeStart, rangeEnd)

	return nil
}
