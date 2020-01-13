package smartremote

import "os"

// Close will close the file and make sure data is synced on the disk if the
// download is still partial.
func (f *File) Close() error {
	f.lk.Lock()
	defer f.lk.Unlock()

	err := f.savePart()

	f.dlm.openFilesLk.Lock()
	delete(f.dlm.openFiles, f.hash)
	f.dlm.openFilesLk.Unlock()

	if !f.complete {
		// this is not complete
		if err != nil {
			// failed to save part, remove partial data
			err = f.local.Close()
			os.Remove(f.path)
			return err
		}

		// we managed to save the part data, ok to keep partial data
		return f.local.Close()
	}

	return f.local.Close()
}
