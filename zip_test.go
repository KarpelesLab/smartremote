package smartremote

import (
	"archive/zip"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"testing"
)

func TestZIP(t *testing.T) {
	// open a random .zip file off internet (a big one)
	f, err := Open("http://ftp.jaist.ac.jp/pub/qtproject/archive/qt/5.14/5.14.1/submodules/qtmultimedia-everywhere-src-5.14.1.zip")
	//f, err := Open("http://ftp.jaist.ac.jp/pub/qtproject/archive/qt/5.2/5.2.1/single/qt-everywhere-opensource-src-5.2.1.zip")
	if err != nil {
		t.Fatalf("unable to create smartremote object: %s", err)
		return
	}
	defer f.Close()

	siz, err := f.GetSize()
	if err != nil {
		t.Fatalf("failed to get file size: %s", err)
		return
	}

	// use a lib to load iso9660 data
	r, err := zip.NewReader(f, siz)
	if err != nil {
		t.Fatalf("unable to initialize ZIP object: %s", err)
		return
	}

	log.Printf("parsed zip file, has %d files", len(r.File))

	log.Printf("grabbing one file in zip file and checking hash")

	// let's read file qtmultimedia-opensource-src-5.2.1/src/src.pro
	for _, fl := range r.File {
		if fl.Name == "qtmultimedia-opensource-src-5.14.1/src/src.pro" {
			subfl, err := fl.Open()
			if err != nil {
				t.Fatalf("failed to open file: %s", err)
				return
			}

			h := sha256.New()
			io.Copy(h, subfl)
			subfl.Close()
			val := h.Sum(nil)

			if hex.EncodeToString(val) != "d9d79f7240562605cc41d900f41059e5ec3b1de2afc690ed0d5e1de20b7181f6" {
				t.Fatalf("invalid hash value for file, got %s", hex.EncodeToString(val))
			}
		}
	}

	log.Printf("trying to download whole file")
	err = f.Complete()
	if err != nil {
		t.Fatalf("error downloading: %s", err)
	}
}
