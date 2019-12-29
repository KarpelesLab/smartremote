package smartremote

import "log"

func (f *File) needBlocks(start, end int) error {
	// ensure listed blocks exist and are downloaded
	// need to be called with lock acquired
	err := f.getSize()
	if err != nil {
		return err
	}

	log.Printf("TODO: download data blocks %d => %d", start, end)

	return nil // TODO
}
