package smartremote

import (
	"io"
	"testing"

	"github.com/hooklift/iso9660"
)

func TestISO(t *testing.T) {
	// open a random .iso file off internet (a big one)
	f, err := Open("http://releases.ubuntu.com/18.04.3/ubuntu-18.04.3-desktop-amd64.iso")
	if err != nil {
		t.Logf("unable to create smartremote object: %s", err)
		return
	}
	defer f.Close()

	// use a lib to load iso9660 data
	r, err := iso9660.NewReader(f)
	if err != nil {
		t.Logf("unable to initialize iso object: %s", err)
		return
	}

	cnt := 0
	for {
		_, err := r.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("error while reading: %s", err)
			break
		}
		cnt += 1
	}
	t.Logf("Confirmed %d files found in .iso", cnt)
}
