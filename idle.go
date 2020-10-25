package smartremote

import (
	"errors"
)

func (f *File) wantsFollowing(offset int64) int {
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

func (f *File) getBlockCount() int64 {
	// computer number of blocks
	blkCount := f.size / f.blkSize
	if f.size%f.blkSize != 0 {
		blkCount += 1
	}

	return blkCount
}

func (f *File) isComplete() bool {
	if f.complete {
		return true
	}
	if f.status.IsEmpty() {
		return false
	}

	if int64(f.status.GetCardinality()) != f.getBlockCount() {
		return false
	}

	f.dlm.logf("file is now complete, marking as such")

	f.complete = true
	f.savePart()

	return true
}

func (f *File) firstMissing() int64 {
	if f.isComplete() {
		return -1
	}

	err := f.getSize()
	if err != nil {
		f.dlm.logf("failed to get file size: %s", err)
		return -1 // can't be helped
	}

	// get number of blocks
	blkCount := f.getBlockCount()

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

func (f *File) ingestData(b []byte, offset int64) error {
	if !f.hasSize {
		return errors.New("invalid operation, file size unknown")
	}

	block := offset / f.blkSize
	if offset%f.blkSize != 0 {
		return errors.New("invalid offset (not block aligned)")
	}

	blkCount := f.size / f.blkSize
	if f.size%f.blkSize != 0 {
		blkCount += 1
	}

	if block >= blkCount {
		return errors.New("invalid offset (over EOF)")
	}

	if block == blkCount-1 {
		// last block
		lastBlockSize := f.size % f.blkSize
		if lastBlockSize == 0 {
			lastBlockSize = f.blkSize
		}
		if int64(len(b)) != lastBlockSize {
			return errors.New("invalid buffer length (invalid final block size)")
		}
	} else {
		if int64(len(b)) != f.blkSize {
			return errors.New("invalid buffer length (not block size)")
		}
	}

	_, err := f.local.WriteAt(b, offset)
	if err != nil {
		return err
	}

	// mark blocks as received
	f.status.Add(uint32(block))

	f.savePart()

	return nil
}

func (f *File) getBlockSize() int64 {
	return f.blkSize
}
