package smartremote

import (
	"bufio"
	"encoding/binary"
	"os"
)

func (f *File) SavePart() error {
	// save partial file
	if f.complete {
		// remove part file if any
		os.Remove(f.path + ".part")
		return nil
	}

	out, err := os.Create(f.path + ".part")
	if err != nil {
		return err
	}

	// write block size
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, f.blkSize)
	_, err = out.Write(buf[:n])
	if err != nil {
		out.Close()
		os.Remove(f.path + ".part")
		return err
	}

	// write bitmap
	_, err = f.status.WriteTo(out)
	out.Close()

	if err != nil {
		os.Remove(f.path + ".part")
		return err
	}

	return nil
}

func (f *File) ReadPart() error {
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