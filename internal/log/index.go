package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

const (
	// Each entry in the index consists of <4-byte offset><8-byte pos within store>.
	offWidth   = 4
	posWidth   = 8
	entryWidth = offWidth + posWidth
)

type Index struct {
	*os.File
	mmap   gommap.MMap
	size   uint64
	config Config
}

func NewIndex(f *os.File, c Config) (*Index, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())

	// Grow the index file to its maximum size before memory mapping
	// because we cannot increase the file size after it is mmapped.
	if err := os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	// memory map.
	mmap, err := gommap.Map(f.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return &Index{
		File:   f,
		mmap:   mmap,
		size:   size,
		config: c,
	}, nil
}

func (i *Index) Read(off int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if off == -1 {
		out = uint32((i.size / uint64(entryWidth)) - 1)
	} else {
		out = uint32(off)
	}

	startingPos := uint64(out) * entryWidth
	if i.size < startingPos+entryWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[startingPos : startingPos+offWidth])
	pos = enc.Uint64(i.mmap[startingPos+offWidth : startingPos+entryWidth])

	return out, pos, nil
}

func (i *Index) Write(off uint32, pos uint64) error {
	// Check that the index file is not full.
	if i.IsMaxed() {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entryWidth], pos)
	i.size += entryWidth
	return nil
}

func (i *Index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.File.Sync(); err != nil {
		return err
	}

	// Truncate file back to actual size. Remember we grew the file to its maximum size
	// before mmap.
	if err := i.File.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.File.Close()
}

func (i *Index) IsMaxed() bool {
	return uint64(len(i.mmap)) < i.size+entryWidth
}
