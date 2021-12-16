package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

type Store struct {
	*os.File
	sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func NewStore(f *os.File) (*Store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &Store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *Store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.Lock()
	defer s.Unlock()
	pos = s.size

	// Write the record length (in 8 bytes).
	if err = binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// Write the record itself.
	bytesWritten, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	bytesWritten += RecordLengthWidth
	s.size += uint64(bytesWritten)
	return uint64(bytesWritten), pos, nil
}

func (s *Store) Read(pos uint64) ([]byte, error) {
	s.Lock()
	defer s.Unlock()

	// Flush cached data to disk before reading.
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// Read the length of the record.
	recordLength := make([]byte, RecordLengthWidth)
	if _, err := s.File.ReadAt(recordLength, int64(pos)); err != nil {
		return nil, err
	}

	// Read the actual record.
	p := make([]byte, enc.Uint64(recordLength))
	if _, err := s.File.ReadAt(p, int64(pos+RecordLengthWidth)); err != nil {
		return nil, err
	}

	return p, nil
}

func (s *Store) ReadAt(p []byte, off int64) (int, error) {
	s.Lock()
	defer s.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *Store) Close() error {
	s.Lock()
	defer s.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}

var (
	enc = binary.BigEndian
)

const (
	// Number of bytes needed to write the length of a record in the store file.
	// Every store file entry consists of <8-byte record length><actual record>.
	RecordLengthWidth = 8
)
