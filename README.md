[![Build Status](https://travis-ci.org/KarpelesLab/smartremote.svg)](https://travis-ci.org/KarpelesLab/smartremote)
[![GoDoc](https://godoc.org/github.com/KarpelesLab/smartremote?status.svg)](https://godoc.org/github.com/KarpelesLab/smartremote)
[![Coverage Status](https://coveralls.io/repos/github/KarpelesLab/smartremote/badge.svg?branch=master)](https://coveralls.io/github/KarpelesLab/smartremote?branch=master)


# SmartRemote

NOTE: this is not a remote for your TV, just an easy way to access remote (http) files.

How to use:

```Go
	f, err := smartremote.Open("http://...")
	if err != nil {
		panic(err)
	}

	// Use "f" as a regular readonly file, it'll download parts as needed from the remote url
```

This can be used with any kind of file as long as the server supports resume.
If it doesn't then this will just download the whole file, and still work the
same.

# TODO

* Add support for range invalidation (bad checksum causes re-download of affected area)
* Refactor idle downloader for better performances
