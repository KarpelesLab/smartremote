package smartremote

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// testData is random data used for testing
var testData []byte

func init() {
	// Generate 256KB of random test data
	testData = make([]byte, 256*1024)
	rand.Read(testData)
}

// generateOffsetData generates deterministic data where every 8 bytes contains
// the big-endian representation of that position's offset. This allows verification
// that the right bytes are at the right positions.
func generateOffsetData(size int64) []byte {
	data := make([]byte, size)
	for i := int64(0); i < size; i += 8 {
		remaining := size - i
		if remaining >= 8 {
			binary.BigEndian.PutUint64(data[i:], uint64(i))
		} else {
			// Handle partial last chunk
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], uint64(i))
			copy(data[i:], buf[:remaining])
		}
	}
	return data
}

// verifyOffsetData checks that data at the given offset contains the correct
// offset values. Returns true if valid, false if corrupted.
func verifyOffsetData(data []byte, offset int64) bool {
	for i := int64(0); i < int64(len(data)); i += 8 {
		pos := offset + i
		alignedPos := (pos / 8) * 8 // Align to 8-byte boundary
		remaining := int64(len(data)) - i

		if remaining >= 8 && pos%8 == 0 {
			// Full aligned 8-byte chunk
			expected := uint64(alignedPos)
			actual := binary.BigEndian.Uint64(data[i:])
			if actual != expected {
				return false
			}
		} else if pos%8 == 0 && remaining < 8 {
			// Partial chunk at end, aligned
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], uint64(alignedPos))
			if !bytes.Equal(data[i:], buf[:remaining]) {
				return false
			}
			break
		} else {
			// Unaligned read - check byte by byte
			for j := int64(0); j < 8 && i+j < int64(len(data)); j++ {
				bytePos := pos + j
				alignedStart := (bytePos / 8) * 8
				byteOffset := bytePos % 8
				var buf [8]byte
				binary.BigEndian.PutUint64(buf[:], uint64(alignedStart))
				if data[i+j] != buf[byteOffset] {
					return false
				}
			}
		}
	}
	return true
}

// newOffsetTestServer creates an HTTP server that serves offset-based deterministic data.
// Every 8 bytes contains the big-endian offset of that position.
func newOffsetTestServer(size int64) (*httptest.Server, []byte) {
	data := generateOffsetData(size)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			var start, end int64
			_, err := parseRange(rangeHeader, int64(len(data)), &start, &end)
			if err == nil {
				w.Header().Set("Content-Length", itoa(end-start+1))
				w.WriteHeader(http.StatusPartialContent)
				w.Write(data[start : end+1])
				return
			}
		}
		w.Header().Set("Content-Length", itoa(int64(len(data))))
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}))
	return server, data
}

// newTestServer creates an HTTP server that serves testData with Range support
func newTestServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Handle Range requests
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			var start, end int64
			_, err := parseRange(rangeHeader, int64(len(testData)), &start, &end)
			if err == nil {
				w.Header().Set("Content-Length", itoa(end-start+1))
				w.WriteHeader(http.StatusPartialContent)
				w.Write(testData[start : end+1])
				return
			}
		}

		// Full response
		w.Header().Set("Content-Length", itoa(int64(len(testData))))
		w.WriteHeader(http.StatusOK)
		w.Write(testData)
	}))
}

// newTestServerNoHead creates an HTTP server that rejects HEAD requests
func newTestServerNoHead() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Length", itoa(int64(len(testData))))
		w.WriteHeader(http.StatusOK)
		w.Write(testData)
	}))
}

func parseRange(rangeHeader string, size int64, start, end *int64) (bool, error) {
	// Parse "bytes=start-" or "bytes=start-end"
	var s, e int64
	n, _ := sscanf(rangeHeader, "bytes=%d-%d", &s, &e)
	if n >= 1 {
		*start = s
		if n == 2 {
			*end = e
		} else {
			*end = size - 1
		}
		return true, nil
	}
	return false, io.EOF
}

func sscanf(s, format string, args ...interface{}) (int, error) {
	var start, end int64
	n, err := parseRangeSimple(s, &start, &end)
	if n >= 1 && len(args) >= 1 {
		*args[0].(*int64) = start
	}
	if n >= 2 && len(args) >= 2 {
		*args[1].(*int64) = end
	}
	return n, err
}

func parseRangeSimple(s string, start, end *int64) (int, error) {
	// Simple parser for "bytes=X-" or "bytes=X-Y"
	if len(s) < 7 || s[:6] != "bytes=" {
		return 0, io.EOF
	}
	s = s[6:]
	var i int
	for i = 0; i < len(s) && s[i] >= '0' && s[i] <= '9'; i++ {
		*start = *start*10 + int64(s[i]-'0')
	}
	if i == 0 {
		return 0, io.EOF
	}
	if i >= len(s) || s[i] != '-' {
		return 1, nil
	}
	i++ // skip '-'
	if i >= len(s) {
		return 1, nil
	}
	for ; i < len(s) && s[i] >= '0' && s[i] <= '9'; i++ {
		*end = *end*10 + int64(s[i]-'0')
	}
	return 2, nil
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte(n%10) + '0'
		n /= 10
	}
	return string(buf[i:])
}

func TestOpenAndRead(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil // Suppress logging

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo failed: %v", err)
	}
	defer f.Close()

	// Read first block
	buf := make([]byte, 1024)
	n, err := f.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != 1024 {
		t.Errorf("Read returned %d bytes, want 1024", n)
	}
	if !bytes.Equal(buf, testData[:1024]) {
		t.Error("Read data doesn't match")
	}
}

func TestSeek(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo failed: %v", err)
	}
	defer f.Close()

	// Test SeekStart
	pos, err := f.Seek(1000, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek(SeekStart) failed: %v", err)
	}
	if pos != 1000 {
		t.Errorf("Seek returned %d, want 1000", pos)
	}

	// Test SeekCurrent
	pos, err = f.Seek(500, io.SeekCurrent)
	if err != nil {
		t.Fatalf("Seek(SeekCurrent) failed: %v", err)
	}
	if pos != 1500 {
		t.Errorf("Seek returned %d, want 1500", pos)
	}

	// Test SeekEnd
	pos, err = f.Seek(-100, io.SeekEnd)
	if err != nil {
		t.Fatalf("Seek(SeekEnd) failed: %v", err)
	}
	expectedPos := int64(len(testData)) - 100
	if pos != expectedPos {
		t.Errorf("Seek returned %d, want %d", pos, expectedPos)
	}

	// Test invalid seek (negative position)
	_, err = f.Seek(-1, io.SeekStart)
	if err == nil {
		t.Error("Seek to negative position should fail")
	}

	// Test invalid whence
	_, err = f.Seek(0, 999)
	if err == nil {
		t.Error("Seek with invalid whence should fail")
	}
}

func TestReadAt(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo failed: %v", err)
	}
	defer f.Close()

	// Read at specific offset
	buf := make([]byte, 512)
	n, err := f.ReadAt(buf, 1024)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != 512 {
		t.Errorf("ReadAt returned %d bytes, want 512", n)
	}
	if !bytes.Equal(buf, testData[1024:1536]) {
		t.Error("ReadAt data doesn't match")
	}
}

func TestGetSizeAndStat(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo failed: %v", err)
	}
	defer f.Close()

	// Test GetSize
	size, err := f.GetSize()
	if err != nil {
		t.Fatalf("GetSize failed: %v", err)
	}
	if size != int64(len(testData)) {
		t.Errorf("GetSize returned %d, want %d", size, len(testData))
	}

	// Test Stat
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info == nil {
		t.Error("Stat returned nil FileInfo")
	}
}

func TestSetSize(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo failed: %v", err)
	}
	defer f.Close()

	// Set size manually
	f.SetSize(int64(len(testData)))

	size, err := f.GetSize()
	if err != nil {
		t.Fatalf("GetSize failed: %v", err)
	}
	if size != int64(len(testData)) {
		t.Errorf("Size is %d, want %d", size, len(testData))
	}
}

func TestSavePart(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo failed: %v", err)
	}

	// Read some data to create partial download
	buf := make([]byte, 1024)
	_, err = f.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	// Save part explicitly
	err = f.SavePart()
	if err != nil {
		t.Fatalf("SavePart failed: %v", err)
	}

	// Check .part file exists
	_, err = os.Stat(localPath + ".part")
	if err != nil {
		t.Errorf(".part file should exist: %v", err)
	}

	f.Close()
}

func TestResumeDownload(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "test.bin")

	// First open and partial read
	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo failed: %v", err)
	}

	buf := make([]byte, DefaultBlockSize)
	_, err = f.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	f.Close()

	// Create new manager and reopen (simulating restart)
	dm2 := NewDownloadManager()
	dm2.Logger = nil

	f2, err := dm2.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo (resume) failed: %v", err)
	}
	defer f2.Close()

	// Read should work from resumed file
	buf2 := make([]byte, 1024)
	_, err = f2.Read(buf2)
	if err != nil {
		t.Fatalf("Read after resume failed: %v", err)
	}
	if !bytes.Equal(buf2, testData[:1024]) {
		t.Error("Data after resume doesn't match")
	}
}

func TestForReaderAt(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil

	reader := dm.For(server.URL)

	buf := make([]byte, 512)
	n, err := reader.ReadAt(buf, 100)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != 512 {
		t.Errorf("ReadAt returned %d bytes, want 512", n)
	}
	if !bytes.Equal(buf, testData[100:612]) {
		t.Error("ReadAt data doesn't match")
	}
}

func TestComplete(t *testing.T) {
	// Use smaller test data for this test
	smallData := make([]byte, 3*DefaultBlockSize)
	rand.Read(smallData)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			var start, end int64
			_, err := parseRange(rangeHeader, int64(len(smallData)), &start, &end)
			if err == nil {
				w.Header().Set("Content-Length", itoa(end-start+1))
				w.WriteHeader(http.StatusPartialContent)
				w.Write(smallData[start : end+1])
				return
			}
		}
		w.Header().Set("Content-Length", itoa(int64(len(smallData))))
		w.WriteHeader(http.StatusOK)
		w.Write(smallData)
	}))
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo failed: %v", err)
	}
	defer f.Close()

	// Complete the download
	err = f.Complete()
	if err != nil {
		t.Fatalf("Complete failed: %v", err)
	}

	// Verify all data
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek failed: %v", err)
	}

	allData, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(allData, smallData) {
		t.Error("Complete data doesn't match")
	}
}

func TestDownloadFullFallback(t *testing.T) {
	server := newTestServerNoHead()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo failed: %v", err)
	}
	defer f.Close()

	// Reading should trigger full download fallback
	buf := make([]byte, 1024)
	_, err = f.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if !bytes.Equal(buf, testData[:1024]) {
		t.Error("Read data doesn't match after fallback")
	}
}

func TestConcurrentReads(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil
	dm.MaxConcurrent = 2

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo failed: %v", err)
	}
	defer f.Close()

	// Concurrent ReadAt calls
	done := make(chan bool, 3)
	for i := 0; i < 3; i++ {
		go func(offset int64) {
			buf := make([]byte, 256)
			_, err := f.ReadAt(buf, offset)
			if err != nil {
				t.Errorf("Concurrent ReadAt failed: %v", err)
			}
			done <- true
		}(int64(i * 1024))
	}

	for i := 0; i < 3; i++ {
		<-done
	}
}

func TestIdleDownload(t *testing.T) {
	// Small data to make idle download fast
	smallData := make([]byte, 2*DefaultBlockSize)
	rand.Read(smallData)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			var start, end int64
			_, err := parseRange(rangeHeader, int64(len(smallData)), &start, &end)
			if err == nil {
				w.Header().Set("Content-Length", itoa(end-start+1))
				w.WriteHeader(http.StatusPartialContent)
				w.Write(smallData[start : end+1])
				return
			}
		}
		w.Header().Set("Content-Length", itoa(int64(len(smallData))))
		w.WriteHeader(http.StatusOK)
		w.Write(smallData)
	}))
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo failed: %v", err)
	}
	defer f.Close()

	// Read first block
	buf := make([]byte, 1024)
	_, err = f.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	// Wait for idle download to potentially run
	time.Sleep(2 * time.Second)

	// File might be complete now due to idle download
	// Just verify we can still read
	buf2 := make([]byte, 1024)
	_, err = f.ReadAt(buf2, DefaultBlockSize)
	if err != nil {
		t.Fatalf("ReadAt after idle wait failed: %v", err)
	}
}

func TestReadBeyondEOF(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo failed: %v", err)
	}
	defer f.Close()

	// Seek beyond EOF
	_, err = f.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatalf("Seek to end failed: %v", err)
	}

	// Read should return EOF
	buf := make([]byte, 100)
	_, err = f.Read(buf)
	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}

	// ReadAt beyond EOF
	_, err = f.ReadAt(buf, int64(len(testData)+1000))
	if err != io.EOF {
		t.Errorf("Expected EOF for ReadAt beyond end, got %v", err)
	}
}

func TestOpenExistingComplete(t *testing.T) {
	server := newTestServer()
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "test.bin")

	// Create a "complete" file (no .part file)
	err := os.WriteFile(localPath, testData, 0600)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Open should recognize it as complete
	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo failed: %v", err)
	}
	defer f.Close()

	// Read should work directly from local file
	buf := make([]byte, 1024)
	n, err := f.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != 1024 {
		t.Errorf("Read returned %d bytes, want 1024", n)
	}
}

func TestManagerLogf(t *testing.T) {
	dm := NewDownloadManager()

	// With logger, should log
	var buf bytes.Buffer
	dm.Logger.SetOutput(&buf)
	dm.logf("test %s", "message")

	if buf.Len() == 0 {
		t.Error("Expected log output")
	}

	// With nil logger, should not panic
	dm.Logger = nil
	dm.logf("test %s", "message") // Should not panic
}

// TestOffsetDataGeneration verifies that offset data generation and verification work correctly.
func TestOffsetDataGeneration(t *testing.T) {
	// Test various sizes
	sizes := []int64{64, 1000, 8192, 65536}
	for _, size := range sizes {
		data := generateOffsetData(size)
		if int64(len(data)) != size {
			t.Errorf("Generated data size %d, want %d", len(data), size)
		}

		// Verify entire data
		if !verifyOffsetData(data, 0) {
			t.Errorf("Generated data failed verification for size %d", size)
		}

		// Verify partial reads at various offsets
		offsets := []int64{0, 8, 16, 100, 1000}
		for _, off := range offsets {
			if off >= size {
				continue
			}
			readSize := int64(256)
			if off+readSize > size {
				readSize = size - off
			}
			if !verifyOffsetData(data[off:off+readSize], off) {
				t.Errorf("Partial verification failed at offset %d for size %d", off, size)
			}
		}
	}
}

// TestIdleDownloadIntegrity tests that idle downloading produces correct data.
func TestIdleDownloadIntegrity(t *testing.T) {
	// Use 4 blocks of data
	dataSize := int64(4 * DefaultBlockSize)
	server, expectedData := newOffsetTestServer(dataSize)
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo failed: %v", err)
	}
	defer f.Close()

	// Read first block to trigger connection
	buf := make([]byte, 1024)
	_, err = f.Read(buf)
	if err != nil {
		t.Fatalf("Initial read failed: %v", err)
	}
	if !verifyOffsetData(buf, 0) {
		t.Fatal("Initial read data corrupted")
	}

	// Wait for idle download to complete remaining blocks
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Log("Timeout waiting for idle download, verifying available data")
			goto verify
		case <-ticker.C:
			f.lk.Lock()
			complete := f.isComplete()
			f.lk.Unlock()
			if complete {
				t.Log("Idle download completed")
				goto verify
			}
		}
	}

verify:
	// Verify all data integrity
	allData := make([]byte, dataSize)
	n, err := f.ReadAt(allData, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("Full ReadAt failed: %v", err)
	}
	if int64(n) != dataSize {
		t.Errorf("Read %d bytes, expected %d", n, dataSize)
	}

	if !bytes.Equal(allData, expectedData) {
		t.Error("Data mismatch after idle download")
		// Find first difference
		for i := 0; i < len(allData); i++ {
			if allData[i] != expectedData[i] {
				t.Errorf("First difference at byte %d: got %02x, want %02x", i, allData[i], expectedData[i])
				break
			}
		}
	}

	if !verifyOffsetData(allData, 0) {
		t.Error("Offset verification failed - data corruption detected")
	}
}

// TestIdleDownloadWithConcurrentReads tests that concurrent reads during idle downloading
// don't cause data corruption or deadlocks.
func TestIdleDownloadWithConcurrentReads(t *testing.T) {
	dataSize := int64(8 * DefaultBlockSize)
	server, expectedData := newOffsetTestServer(dataSize)
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil
	dm.MaxReadersPerFile = 4

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo failed: %v", err)
	}
	defer f.Close()

	// Start concurrent readers that read random positions
	const numReaders = 5
	const readsPerReader = 20
	errors := make(chan error, numReaders*readsPerReader)
	done := make(chan bool, numReaders)

	for i := 0; i < numReaders; i++ {
		go func(readerID int) {
			defer func() { done <- true }()

			for j := 0; j < readsPerReader; j++ {
				// Read from various offsets
				offset := int64((readerID*readsPerReader + j) * 1024 % int(dataSize-1024))
				buf := make([]byte, 1024)
				n, err := f.ReadAt(buf, offset)
				if err != nil && err != io.EOF {
					errors <- err
					continue
				}

				// Verify data integrity
				if !verifyOffsetData(buf[:n], offset) {
					errors <- io.ErrUnexpectedEOF // Use as sentinel for corruption
				}

				// Also verify against expected data
				if !bytes.Equal(buf[:n], expectedData[offset:offset+int64(n)]) {
					errors <- io.ErrUnexpectedEOF
				}

				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}

	// Wait for all readers to complete
	for i := 0; i < numReaders; i++ {
		<-done
	}
	close(errors)

	// Check for errors
	errCount := 0
	for err := range errors {
		if err == io.ErrUnexpectedEOF {
			t.Error("Data corruption detected during concurrent reads")
		} else {
			t.Errorf("Read error: %v", err)
		}
		errCount++
	}

	if errCount > 0 {
		t.Errorf("Total errors: %d", errCount)
	}
}

// TestIdleDownloadDoesNotBlockReads verifies that idle downloading doesn't block user reads.
func TestIdleDownloadDoesNotBlockReads(t *testing.T) {
	dataSize := int64(10 * DefaultBlockSize)
	var requestCount atomic.Int64

	// Create a slow server that tracks request count
	data := generateOffsetData(dataSize)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		// Add small delay to simulate network latency
		time.Sleep(50 * time.Millisecond)

		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			var start, end int64
			_, err := parseRange(rangeHeader, int64(len(data)), &start, &end)
			if err == nil {
				w.Header().Set("Content-Length", itoa(end-start+1))
				w.WriteHeader(http.StatusPartialContent)
				w.Write(data[start : end+1])
				return
			}
		}
		w.Header().Set("Content-Length", itoa(int64(len(data))))
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}))
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo failed: %v", err)
	}
	defer f.Close()

	// Trigger idle download by reading first block
	buf := make([]byte, DefaultBlockSize)
	_, err = f.Read(buf)
	if err != nil {
		t.Fatalf("Initial read failed: %v", err)
	}

	// Wait a bit for idle download to start
	time.Sleep(200 * time.Millisecond)

	// Now do a read to a different location - this should not be blocked
	// for the full duration of an idle download cycle
	start := time.Now()
	buf2 := make([]byte, 1024)
	_, err = f.ReadAt(buf2, 5*DefaultBlockSize)
	readDuration := time.Since(start)

	if err != nil {
		t.Fatalf("Read during idle download failed: %v", err)
	}

	// Read should complete reasonably quickly (not blocked for full idle cycle)
	// Allow generous time for HTTP request but not 1+ seconds
	if readDuration > 500*time.Millisecond {
		t.Errorf("Read took %v, suggesting it was blocked by idle download", readDuration)
	}

	// Verify data integrity
	if !verifyOffsetData(buf2, 5*DefaultBlockSize) {
		t.Error("Data corruption in read during idle download")
	}

	t.Logf("Read during idle download completed in %v, total requests: %d", readDuration, requestCount.Load())
}

// TestMultipleReadersIntegrity tests that multiple readers produce correct data.
func TestMultipleReadersIntegrity(t *testing.T) {
	dataSize := int64(6 * DefaultBlockSize)
	server, expectedData := newOffsetTestServer(dataSize)
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil
	dm.MaxReadersPerFile = 3

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo failed: %v", err)
	}
	defer f.Close()

	// Simulate ZIP-like access pattern: read from end, then from various positions
	positions := []int64{
		5 * DefaultBlockSize,         // Near end (like ZIP central directory)
		0,                            // Beginning
		3 * DefaultBlockSize,         // Middle
		5*DefaultBlockSize + 1000,    // Near end again
		DefaultBlockSize,             // Second block
		4 * DefaultBlockSize,         // Fourth block
	}

	for i, pos := range positions {
		readSize := int64(2048)
		if pos+readSize > dataSize {
			readSize = dataSize - pos
		}

		buf := make([]byte, readSize)
		n, err := f.ReadAt(buf, pos)
		if err != nil && err != io.EOF {
			t.Fatalf("ReadAt %d at offset %d failed: %v", i, pos, err)
		}

		if !bytes.Equal(buf[:n], expectedData[pos:pos+int64(n)]) {
			t.Errorf("Data mismatch at position %d (read %d)", pos, i)
		}

		if !verifyOffsetData(buf[:n], pos) {
			t.Errorf("Offset verification failed at position %d (read %d)", pos, i)
		}
	}
}

// TestIdleDownloadStress performs heavy concurrent access while idle downloading.
func TestIdleDownloadStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	dataSize := int64(16 * DefaultBlockSize) // 1MB
	server, expectedData := newOffsetTestServer(dataSize)
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil
	dm.MaxReadersPerFile = 5

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo failed: %v", err)
	}
	defer f.Close()

	// Run stress test for a fixed duration
	const testDuration = 3 * time.Second
	const numGoroutines = 10

	var readCount atomic.Int64
	var errorCount atomic.Int64
	stopCh := make(chan struct{})

	// Start goroutines that continuously read random positions
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			readBuf := make([]byte, 4096)
			offset := int64(id * int(DefaultBlockSize/2))

			for {
				select {
				case <-stopCh:
					return
				default:
					// Vary offset to stress different patterns
					readOffset := offset % (dataSize - 4096)
					n, err := f.ReadAt(readBuf, readOffset)
					if err != nil && err != io.EOF {
						errorCount.Add(1)
						continue
					}

					readCount.Add(1)

					// Verify integrity
					if !bytes.Equal(readBuf[:n], expectedData[readOffset:readOffset+int64(n)]) {
						errorCount.Add(1)
						t.Errorf("Data mismatch at offset %d", readOffset)
					}

					// Move to next position
					offset += 1024
				}
			}
		}(i)
	}

	// Let it run
	time.Sleep(testDuration)
	close(stopCh)

	// Give goroutines time to finish
	time.Sleep(100 * time.Millisecond)

	t.Logf("Stress test completed: %d reads, %d errors", readCount.Load(), errorCount.Load())

	if errorCount.Load() > 0 {
		t.Errorf("Stress test had %d errors", errorCount.Load())
	}

	// Verify final file integrity
	allData := make([]byte, dataSize)
	n, err := f.ReadAt(allData, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("Final ReadAt failed: %v", err)
	}

	if !bytes.Equal(allData[:n], expectedData[:n]) {
		t.Error("Final data verification failed")
	}
}

// TestIdleDownloadCompletion verifies idle download eventually completes the file.
func TestIdleDownloadCompletion(t *testing.T) {
	dataSize := int64(4 * DefaultBlockSize)
	server, expectedData := newOffsetTestServer(dataSize)
	defer server.Close()

	dm := NewDownloadManager()
	dm.Logger = nil

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "test.bin")

	f, err := dm.OpenTo(server.URL, localPath)
	if err != nil {
		t.Fatalf("OpenTo failed: %v", err)
	}
	defer f.Close()

	// Read just 1 byte to establish connection and trigger idle downloading
	buf := make([]byte, 1)
	_, err = f.Read(buf)
	if err != nil {
		t.Fatalf("Initial read failed: %v", err)
	}

	// Wait for idle download to complete
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		f.lk.Lock()
		complete := f.isComplete()
		cardinality := f.status.GetCardinality()
		blockCount := f.getBlockCount()
		f.lk.Unlock()

		t.Logf("Progress: %d/%d blocks", cardinality, blockCount)

		if complete {
			t.Log("File download completed via idle downloading")
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Verify complete file
	allData := make([]byte, dataSize)
	n, err := f.ReadAt(allData, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("Final ReadAt failed: %v", err)
	}

	if int64(n) != dataSize {
		t.Errorf("Read %d bytes, expected %d", n, dataSize)
	}

	if !bytes.Equal(allData, expectedData) {
		t.Error("Final data doesn't match expected")
	}

	if !verifyOffsetData(allData, 0) {
		t.Error("Offset verification failed on complete file")
	}
}
