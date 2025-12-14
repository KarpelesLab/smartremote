package smartremote

import (
	"errors"
)

// wantsFollowing returns the number of bytes to download starting at the
// given offset if the block at that offset hasn't been downloaded yet.
// Returns 0 if the offset is not block-aligned or the block is already present.
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

// getBlockCount returns the total number of blocks needed to store the file.
func (f *File) getBlockCount() int64 {
	// computer number of blocks
	blkCount := f.size / f.blkSize
	if f.size%f.blkSize != 0 {
		blkCount += 1
	}

	return blkCount
}

// isComplete checks if all blocks have been downloaded and marks the file
// as complete if so. Returns true if the file is fully downloaded.
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

// firstMissing returns the byte offset of the first missing block,
// or -1 if all blocks are downloaded.
func (f *File) firstMissing() int64 {
	if f.isComplete() {
		return -1
	}

	err := f.getSize()
	if err != nil {
		f.dlm.logf("failed to get file size: %s", err)
		return -1 // can't be helped
	}

	blkCount := f.getBlockCount()

	// If bitmap is empty, first missing is block 0
	if f.status.IsEmpty() {
		return 0
	}

	// Use iterator to efficiently find first gap in downloaded blocks.
	// The iterator returns set bits in ascending order, so we look for
	// the first position where the expected sequence breaks.
	it := f.status.Iterator()
	expected := uint32(0)

	for it.HasNext() {
		val := it.Next()
		if val > expected {
			// Found a gap - 'expected' is the first missing block
			return int64(expected) * f.blkSize
		}
		expected = val + 1
		if int64(expected) >= blkCount {
			// We've covered all blocks we need
			break
		}
	}

	// Check if there are blocks after the last downloaded block
	if int64(expected) < blkCount {
		return int64(expected) * f.blkSize
	}

	return -1
}

// ingestData writes a block of data to the local file at the specified offset.
// The offset must be block-aligned and the data must be exactly one block in size
// (or the correct size for the final block). This method saves progress to disk.
func (f *File) ingestData(b []byte, offset int64) error {
	if err := f.ingestDataBatch(b, offset); err != nil {
		return err
	}
	f.savePart()
	return nil
}

// ingestDataBatch writes a block of data without saving progress to disk.
// Use this for batch operations, then call savePart() once at the end.
func (f *File) ingestDataBatch(b []byte, offset int64) error {
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

	// mark block as received
	f.status.Add(uint32(block))

	return nil
}

// getBlockSize returns the block size used for this file.
func (f *File) getBlockSize() int64 {
	return f.blkSize
}
