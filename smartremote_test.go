package smartremote

import (
	"bytes"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

// newTestServerNoRange creates an HTTP server without Range support
func newTestServerNoRange() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
