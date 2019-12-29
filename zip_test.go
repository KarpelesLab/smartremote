package smartremote

import (
	"archive/zip"
	"log"
	"testing"
)

func TestZIP(t *testing.T) {
	// open a random .zip file off internet (a big one)
	// http://download.qt.io/archive/qt/5.2/5.2.1/submodules/qtmultimedia-opensource-src-5.2.1.zip
	f, err := Open("http://ftp.jaist.ac.jp/pub/qtproject/archive/qt/5.2/5.2.1/submodules/qtmultimedia-opensource-src-5.2.1.zip")
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

	for _, fil := range r.File {
		log.Printf("file: %s", fil.Name)
	}
}
