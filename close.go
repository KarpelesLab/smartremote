package smartremote

import "os"

// Close will close the file and make sure data is synced on the disk if the
// download is still partial.
func (f *File) Close() error {
	if !f.complete {
		// this is not complete, avoid keeping the partial file on disk
		err := f.local.Close()
		os.Remove(f.path)
		return err
	}

	return f.local.Close()
}
