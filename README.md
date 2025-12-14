[![Test](https://github.com/KarpelesLab/smartremote/actions/workflows/test.yml/badge.svg)](https://github.com/KarpelesLab/smartremote/actions/workflows/test.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/KarpelesLab/smartremote.svg)](https://pkg.go.dev/github.com/KarpelesLab/smartremote)
[![Coverage Status](https://coveralls.io/repos/github/KarpelesLab/smartremote/badge.svg?branch=master)](https://coveralls.io/github/KarpelesLab/smartremote?branch=master)

# SmartRemote

SmartRemote is a Go library that provides seamless access to remote HTTP files with intelligent partial downloading and local caching. Rather than downloading entire files upfront, it allows you to open a URL and read from it like a regular file, automatically fetching only the needed portions on-demand.

This is particularly useful for large files like ISOs, ZIPs, and other archives where you might only need to access specific sections (e.g., reading the central directory of a ZIP file without downloading the entire archive).

## Features

- **Lazy Loading**: Downloads only the blocks you actually read
- **Resume Support**: Partial downloads can be saved and resumed via `.part` files
- **Intelligent Seeking**: Handles seekable HTTP connections using Range requests
- **Concurrent Downloads**: Manages multiple concurrent download clients with configurable limits
- **Idle Background Downloading**: Automatically fills gaps in partial downloads when not actively reading
- **Block-Based Tracking**: Uses efficient RoaringBitmap for tracking downloaded 64KB blocks
- **Standard Interfaces**: Implements `io.Reader`, `io.ReaderAt`, and `io.Seeker`

## Installation

```bash
go get github.com/KarpelesLab/smartremote
```

## Quick Start

```go
package main

import (
    "fmt"
    "io"
    "github.com/KarpelesLab/smartremote"
)

func main() {
    // Open a remote file
    f, err := smartremote.Open("https://example.com/largefile.zip")
    if err != nil {
        panic(err)
    }
    defer f.Close()

    // Use f as a regular read-only file
    // It will download parts as needed from the remote URL
    buf := make([]byte, 1024)
    n, err := f.Read(buf)
    if err != nil && err != io.EOF {
        panic(err)
    }
    fmt.Printf("Read %d bytes\n", n)
}
```

## How It Works

### Block-Based Downloads

SmartRemote divides remote files into 64KB blocks. When you read from the file, only the blocks containing the requested data are downloaded. Downloaded blocks are:

1. Stored in a local temporary file
2. Tracked using a RoaringBitmap for efficient status checking
3. Persisted to a `.part` file so downloads can be resumed

### HTTP Range Requests

The library uses HTTP Range requests (status 206 Partial Content) to download specific byte ranges. If the server doesn't support Range requests, SmartRemote falls back to downloading the entire file.

### Connection Pooling

The `DownloadManager` maintains a pool of HTTP connections (default: 10) that are reused across requests. Idle connections are automatically cleaned up after 5 minutes.

### Background Downloading

When there are no active read requests, SmartRemote opportunistically downloads missing blocks in the background, progressively completing the file.

## Advanced Usage

### Custom DownloadManager

Create a custom `DownloadManager` for more control:

```go
dm := smartremote.NewDownloadManager()
dm.MaxConcurrent = 5           // Limit to 5 concurrent connections
dm.MaxDataJump = 1024 * 1024   // Allow skipping up to 1MB when seeking
dm.TmpDir = "/custom/tmp"      // Custom temp directory
dm.Client = customHTTPClient   // Use a custom http.Client

f, err := dm.Open("https://example.com/file.iso")
if err != nil {
    panic(err)
}
defer f.Close()
```

### Specify Local Storage Path

Store the downloaded file at a specific path:

```go
dm := smartremote.NewDownloadManager()
f, err := dm.OpenTo("https://example.com/file.iso", "/path/to/local/file.iso")
if err != nil {
    panic(err)
}
defer f.Close()
```

### Simple ReaderAt Interface

For simple use cases where you just need `io.ReaderAt`:

```go
dm := smartremote.NewDownloadManager()
reader := dm.For("https://example.com/file.bin")

buf := make([]byte, 100)
n, err := reader.ReadAt(buf, 1000) // Read 100 bytes starting at offset 1000
```

### Force Complete Download

Download the entire file:

```go
f, err := smartremote.Open("https://example.com/file.zip")
if err != nil {
    panic(err)
}
defer f.Close()

// Download everything
err = f.Complete()
if err != nil {
    panic(err)
}
```

### Manual Progress Saving

Manually trigger a save of download progress:

```go
f, err := smartremote.Open("https://example.com/file.zip")
if err != nil {
    panic(err)
}

// ... perform some reads ...

// Save progress explicitly
err = f.SavePart()
if err != nil {
    panic(err)
}
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `MaxConcurrent` | `int` | 10 | Maximum number of concurrent HTTP connections |
| `MaxDataJump` | `int64` | 512KB | Maximum bytes to read and discard when seeking forward (vs opening a new connection) |
| `TmpDir` | `string` | `os.TempDir()` | Directory for temporary download files |
| `Client` | `*http.Client` | `http.DefaultClient` | HTTP client for making requests |
| `Logger` | `*log.Logger` | stderr | Logger for debug output |

## API Reference

### Package Functions

- `Open(url string) (*File, error)` - Open a remote URL using the default manager

### DownloadManager

- `NewDownloadManager() *DownloadManager` - Create a new download manager
- `Open(url string) (*File, error)` - Open a URL with auto-generated local path
- `OpenTo(url, localPath string) (*File, error)` - Open a URL with specific local path
- `For(url string) io.ReaderAt` - Get a simple ReaderAt for a URL

### File

- `Read(p []byte) (n int, err error)` - Read from current position
- `ReadAt(p []byte, off int64) (int, error)` - Read from specific offset
- `Seek(offset int64, whence int) (int64, error)` - Seek to position
- `Close() error` - Close file and save progress
- `GetSize() (int64, error)` - Get remote file size
- `SetSize(size int64)` - Manually set file size
- `Stat() (os.FileInfo, error)` - Get file info
- `Complete() error` - Download entire file
- `SavePart() error` - Manually save download progress

## Resume Behavior

When opening a URL:

1. If the local file doesn't exist, a new download begins
2. If the local file exists with a `.part` file, the download resumes from where it left off
3. If the local file exists without a `.part` file, it's assumed to be complete

On close:
- If download is incomplete and progress was saved successfully, both files are kept for resume
- If download is incomplete and progress save failed, the partial file is deleted
- If download is complete, the `.part` file is removed

## Requirements

- Go 1.18 or later
- Server must support HTTP Range requests for partial downloads (falls back to full download otherwise)

## TODO

- Add support for range invalidation (bad checksum causes re-download of affected area)
- Refactor idle downloader for better performance
