package smartremote

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestInvalidateRangeBasic(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil
	localPath := filepath.Join(t.TempDir(), "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Read first two blocks to trigger download
	buf := make([]byte, DefaultBlockSize*2)
	n, err := f.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buf) {
		t.Fatalf("expected %d bytes, got %d", len(buf), n)
	}

	// Verify both blocks are in bitmap
	f.lk.RLock()
	has0 := f.status.Contains(0)
	has1 := f.status.Contains(1)
	f.lk.RUnlock()
	if !has0 || !has1 {
		t.Fatal("expected blocks 0 and 1 to be downloaded")
	}

	// Invalidate just the first block
	err = f.InvalidateRange(0, DefaultBlockSize)
	if err != nil {
		t.Fatal(err)
	}

	// Verify first block is gone, second still present
	f.lk.RLock()
	has0 = f.status.Contains(0)
	has1 = f.status.Contains(1)
	complete := f.complete
	f.lk.RUnlock()

	if has0 {
		t.Error("block 0 should be invalidated")
	}
	if !has1 {
		t.Error("block 1 should still be present")
	}
	if complete {
		t.Error("file should not be marked complete")
	}

	// Read first block again - should trigger re-download
	f.Seek(0, 0)
	rebuf := make([]byte, DefaultBlockSize)
	n, err = f.Read(rebuf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rebuf[:n], testData[:n]) {
		t.Error("re-downloaded data does not match")
	}
}

func TestInvalidateRangeNonAligned(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil
	localPath := filepath.Join(t.TempDir(), "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Download first 3 blocks
	buf := make([]byte, DefaultBlockSize*3)
	_, err = f.Read(buf)
	if err != nil {
		t.Fatal(err)
	}

	// Invalidate with non-aligned boundaries that span blocks 0 and 1
	err = f.InvalidateRange(100, DefaultBlockSize+100)
	if err != nil {
		t.Fatal(err)
	}

	// Both block 0 and block 1 should be invalidated
	f.lk.RLock()
	has0 := f.status.Contains(0)
	has1 := f.status.Contains(1)
	has2 := f.status.Contains(2)
	f.lk.RUnlock()

	if has0 {
		t.Error("block 0 should be invalidated")
	}
	if has1 {
		t.Error("block 1 should be invalidated")
	}
	if !has2 {
		t.Error("block 2 should still be present")
	}
}

func TestInvalidateRangeEntireFile(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil
	localPath := filepath.Join(t.TempDir(), "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Download everything
	err = f.Complete()
	if err != nil {
		t.Fatal(err)
	}

	f.lk.RLock()
	wasComplete := f.complete
	size := f.size
	f.lk.RUnlock()
	if !wasComplete {
		t.Fatal("file should be complete")
	}

	// Invalidate entire file
	err = f.InvalidateRange(0, size)
	if err != nil {
		t.Fatal(err)
	}

	f.lk.RLock()
	isEmpty := f.status.IsEmpty()
	isComplete := f.complete
	f.lk.RUnlock()

	if !isEmpty {
		t.Error("bitmap should be empty after full invalidation")
	}
	if isComplete {
		t.Error("file should not be complete")
	}

	// Verify .part file exists
	if _, err := os.Stat(localPath + ".part"); os.IsNotExist(err) {
		t.Error(".part file should exist after invalidation")
	}

	// Re-read to verify re-download works
	buf := make([]byte, 1024)
	n, err := f.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf[:n], testData[:n]) {
		t.Error("re-downloaded data doesn't match")
	}
}

func TestInvalidateRangeClampsBeyondEOF(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil
	localPath := filepath.Join(t.TempDir(), "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	err = f.Complete()
	if err != nil {
		t.Fatal(err)
	}

	f.lk.RLock()
	size := f.size
	f.lk.RUnlock()

	// Invalidate range extending far beyond EOF
	err = f.InvalidateRange(size-DefaultBlockSize, size+100000)
	if err != nil {
		t.Fatal(err)
	}

	// Last block should be invalidated
	f.lk.RLock()
	lastBlock := uint32((size - 1) / DefaultBlockSize)
	hasLast := f.status.Contains(lastBlock)
	// First block should still be present
	has0 := f.status.Contains(0)
	f.lk.RUnlock()

	if hasLast {
		t.Error("last block should be invalidated")
	}
	if !has0 {
		t.Error("first block should still be present")
	}
}

func TestInvalidateRangeEmptyRange(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil
	localPath := filepath.Join(t.TempDir(), "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Download first block
	buf := make([]byte, DefaultBlockSize)
	_, err = f.Read(buf)
	if err != nil {
		t.Fatal(err)
	}

	f.lk.RLock()
	cardBefore := f.status.GetCardinality()
	f.lk.RUnlock()

	// Empty range should be no-op
	err = f.InvalidateRange(100, 100)
	if err != nil {
		t.Fatal(err)
	}

	f.lk.RLock()
	cardAfter := f.status.GetCardinality()
	f.lk.RUnlock()

	if cardBefore != cardAfter {
		t.Errorf("empty range should not change bitmap: before=%d after=%d", cardBefore, cardAfter)
	}
}

func TestInvalidateRangeInvalidInputs(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil
	localPath := filepath.Join(t.TempDir(), "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Negative start
	err = f.InvalidateRange(-1, 100)
	if err == nil {
		t.Error("expected error for negative start")
	}

	// End before start
	err = f.InvalidateRange(200, 100)
	if err == nil {
		t.Error("expected error for end before start")
	}
}

func TestInvalidateRangeResetsComplete(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil
	localPath := filepath.Join(t.TempDir(), "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	err = f.Complete()
	if err != nil {
		t.Fatal(err)
	}

	// .part file should not exist for complete file
	if _, err := os.Stat(localPath + ".part"); !os.IsNotExist(err) {
		t.Log(".part file exists for complete file (may be expected)")
	}

	f.lk.RLock()
	if !f.complete {
		f.lk.RUnlock()
		t.Fatal("file should be complete")
	}
	f.lk.RUnlock()

	// Invalidate one block
	err = f.InvalidateRange(0, DefaultBlockSize)
	if err != nil {
		t.Fatal(err)
	}

	f.lk.RLock()
	isComplete := f.complete
	f.lk.RUnlock()

	if isComplete {
		t.Error("file should not be complete after invalidation")
	}

	// .part file should now exist
	if _, err := os.Stat(localPath + ".part"); os.IsNotExist(err) {
		t.Error(".part file should exist after invalidation of complete file")
	}
}

func TestVerifySuccess(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil
	localPath := filepath.Join(t.TempDir(), "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	expected := sha256.Sum256(testData)

	err = f.Verify(expected)
	if err != nil {
		t.Fatalf("Verify should succeed with correct hash: %v", err)
	}
}

func TestVerifyMismatch(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil
	localPath := filepath.Join(t.TempDir(), "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Use wrong hash
	wrong := sha256.Sum256([]byte("wrong data"))

	err = f.Verify(wrong)
	if err == nil {
		t.Fatal("Verify should fail with wrong hash")
	}
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got: %v", err)
	}

	// All blocks should be invalidated
	f.lk.RLock()
	isEmpty := f.status.IsEmpty()
	isComplete := f.complete
	f.lk.RUnlock()

	if !isEmpty {
		t.Error("bitmap should be empty after verify mismatch")
	}
	if isComplete {
		t.Error("file should not be complete after verify mismatch")
	}
}

func TestVerifyRangeSuccess(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil
	localPath := filepath.Join(t.TempDir(), "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Verify a sub-range
	start := int64(DefaultBlockSize)
	end := int64(DefaultBlockSize * 3)
	expected := sha256.Sum256(testData[start:end])

	err = f.VerifyRange(start, end, expected)
	if err != nil {
		t.Fatalf("VerifyRange should succeed with correct hash: %v", err)
	}
}

func TestVerifyRangeMismatch(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil
	localPath := filepath.Join(t.TempDir(), "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Download everything first so we can check selective invalidation
	err = f.Complete()
	if err != nil {
		t.Fatal(err)
	}

	start := int64(DefaultBlockSize)
	end := int64(DefaultBlockSize * 3)
	wrong := sha256.Sum256([]byte("wrong"))

	err = f.VerifyRange(start, end, wrong)
	if err == nil {
		t.Fatal("VerifyRange should fail with wrong hash")
	}
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got: %v", err)
	}

	// Only blocks 1 and 2 should be invalidated, block 0 and 3 should remain
	f.lk.RLock()
	has0 := f.status.Contains(0)
	has1 := f.status.Contains(1)
	has2 := f.status.Contains(2)
	has3 := f.status.Contains(3)
	f.lk.RUnlock()

	if !has0 {
		t.Error("block 0 should still be present")
	}
	if has1 {
		t.Error("block 1 should be invalidated")
	}
	if has2 {
		t.Error("block 2 should be invalidated")
	}
	if !has3 {
		t.Error("block 3 should still be present")
	}
}

func TestVerifyRangeNonAligned(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil
	localPath := filepath.Join(t.TempDir(), "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Non-aligned range
	start := int64(1000)
	end := int64(DefaultBlockSize + 500)
	expected := sha256.Sum256(testData[start:end])

	err = f.VerifyRange(start, end, expected)
	if err != nil {
		t.Fatalf("VerifyRange should succeed with correct non-aligned hash: %v", err)
	}
}

func TestVerifyRangeInvalidInputs(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil
	localPath := filepath.Join(t.TempDir(), "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var hash [32]byte

	// Negative start
	err = f.VerifyRange(-1, 100, hash)
	if err == nil {
		t.Error("expected error for negative start")
	}

	// End before start
	err = f.VerifyRange(200, 100, hash)
	if err == nil {
		t.Error("expected error for end <= start")
	}

	// Empty range
	err = f.VerifyRange(100, 100, hash)
	if err == nil {
		t.Error("expected error for empty range")
	}
}
