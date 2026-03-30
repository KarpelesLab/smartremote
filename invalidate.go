package smartremote

import (
	"crypto/sha256"
	"errors"
	"io"
)

// ErrChecksumMismatch is returned when a checksum verification fails.
// The affected blocks are automatically invalidated so they will be
// re-downloaded on next access.
var ErrChecksumMismatch = errors.New("checksum mismatch")

// InvalidateRange marks all blocks overlapping the byte range [start, end)
// as not downloaded, causing them to be re-downloaded on next read access
// or by the idle background downloader. This is useful when a checksum
// verification detects corrupted data.
func (f *File) InvalidateRange(start, end int64) error {
	f.lk.Lock()
	defer f.lk.Unlock()

	return f.invalidateRange(start, end)
}

// invalidateRange is the internal implementation that assumes the write lock
// is already held.
func (f *File) invalidateRange(start, end int64) error {
	if start < 0 {
		return errors.New("invalid range: negative start")
	}
	if end < start {
		return errors.New("invalid range: end before start")
	}
	if start == end {
		return nil // empty range, nothing to do
	}

	if err := f.getSize(); err != nil {
		return err
	}

	if end > f.size {
		end = f.size
	}
	if start >= f.size {
		return nil // range is beyond file
	}

	firstBlock := uint64(start / f.blkSize)
	lastBlock := uint64((end - 1) / f.blkSize)

	f.status.RemoveRange(firstBlock, lastBlock+1)
	f.complete = false

	return f.savePart()
}

// Verify downloads the entire file if needed, then computes its SHA-256 hash
// and compares it to expected. If the hashes do not match, all blocks are
// invalidated and ErrChecksumMismatch is returned.
func (f *File) Verify(expected [32]byte) error {
	// Ensure the entire file is downloaded
	if err := f.Complete(); err != nil {
		return err
	}

	// Read size (file is complete so this is safe without lock)
	f.lk.RLock()
	size := f.size
	f.lk.RUnlock()

	// Compute SHA-256 of the local file (no lock needed - file is complete
	// and os.File.ReadAt is goroutine-safe)
	actual, err := f.hashRange(0, size)
	if err != nil {
		return err
	}

	if actual == expected {
		return nil
	}

	// Mismatch: invalidate everything
	f.lk.Lock()
	f.invalidateRange(0, f.size)
	f.lk.Unlock()

	return ErrChecksumMismatch
}

// VerifyRange downloads the blocks covering [start, end) if needed, then
// computes the SHA-256 hash of that byte range and compares it to expected.
// If the hashes do not match, the blocks in the range are invalidated and
// ErrChecksumMismatch is returned.
func (f *File) VerifyRange(start, end int64, expected [32]byte) error {
	if start < 0 {
		return errors.New("invalid range: negative start")
	}
	if end <= start {
		return errors.New("invalid range: end must be after start")
	}

	// Ensure blocks are downloaded
	f.lk.Lock()
	if err := f.getSize(); err != nil {
		f.lk.Unlock()
		return err
	}
	if end > f.size {
		end = f.size
	}
	if start >= f.size {
		f.lk.Unlock()
		return errors.New("invalid range: start beyond file size")
	}
	firstBlock := uint32(start / f.blkSize)
	lastBlock := uint32((end - 1) / f.blkSize)
	err := f.needBlocks(firstBlock, lastBlock)
	f.lk.Unlock()
	if err != nil {
		return err
	}

	// Compute SHA-256 of the range (no lock needed - blocks are downloaded
	// and os.File.ReadAt is goroutine-safe)
	actual, err := f.hashRange(start, end)
	if err != nil {
		return err
	}

	if actual == expected {
		return nil
	}

	// Mismatch: invalidate the range
	f.lk.Lock()
	f.invalidateRange(start, end)
	f.lk.Unlock()

	return ErrChecksumMismatch
}

// hashRange computes SHA-256 of bytes [start, end) from the local file.
func (f *File) hashRange(start, end int64) ([32]byte, error) {
	h := sha256.New()
	buf := make([]byte, f.blkSize)
	pos := start

	for pos < end {
		n := int64(len(buf))
		if pos+n > end {
			n = end - pos
		}
		rn, err := f.local.ReadAt(buf[:n], pos)
		if rn > 0 {
			h.Write(buf[:rn])
		}
		if err != nil && err != io.EOF {
			var zero [32]byte
			return zero, err
		}
		pos += int64(rn)
		if rn == 0 {
			break
		}
	}

	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result, nil
}
