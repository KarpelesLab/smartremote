[![GoDoc](https://godoc.org/github.com/KarpelesLab/smartremote?status.svg)](https://godoc.org/github.com/KarpelesLab/smartremote)

# SmartRemote

NOTE: this is not a remote for your TV, just a smart way to access remote files.

How to use:

```Go
	f, err := smartremote.Open("http://...")
	if err != nil {
		panic(err)
	}

	// Use "f" as a regular readonly file, it'll download parts as needed from the remote url
```

This can be used with any kind of file as long as the server supports resume.
If it doesn't then this will not work (yet. In the future it'll just download
the whole file, but for now it just doesn't work).

