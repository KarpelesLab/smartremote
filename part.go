package smartremote

import (
	"bufio"
	"encoding/binary"
	"os"
)

// SavePart triggers an immediate save of the download status to a .part file
// on disk, allowing resume to happen if the program terminates and is opened
// again.
func (f *File) SavePart() error {
	f.lk.Lock()
	defer f.lk.Unlock()

	return f.savePart()
}

func (f *File) savePart() error {
	// save partial file
	if f.complete {
		// remove part file if any
		os.Remove(f.path + ".part")
		os.Remove(f.path + ".wpart")
		return nil
	}

	out, err := os.Create(f.path + ".wpart")
	if err != nil {
		return err
	}

	// write block size
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, f.blkSize)
	_, err = out.Write(buf[:n])
	if err != nil {
		out.Close()
		os.Remove(f.path + ".wpart")
		return err
	}

	// write bitmap
	_, err = f.status.WriteTo(out)
	out.Close()

	if err != nil {
		os.Remove(f.path + ".wpart")
		return err
	}

	return os.Rename(f.path+".wpart", f.path+".part")
}

func (f *File) readPart() error {
	in, err := os.Open(f.path + ".part")
	if err != nil {
		return err
	}
	buf := bufio.NewReader(in)

	// read blksize
	t, err := binary.ReadVarint(buf)

	if err != nil {
		in.Close()
		return err
	}

	// read map
	_, err = f.status.ReadFrom(buf)
	in.Close()

	if err != nil {
		f.status.Clear()
		return err
	}

	f.blkSize = t
	return nil
}
