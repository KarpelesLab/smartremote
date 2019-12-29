package smartremote

import "os"

func (f *File) Close() error {
	if !f.complete {
		// this is not complete, avoid keeping the partial file on disk
		err := f.local.Close()
		os.Remove(f.path)
		return err
	}

	return f.local.Close()
}
