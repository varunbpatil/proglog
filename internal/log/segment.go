package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/varunbpatil/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type Segment struct {
	store  *Store
	index  *Index
	config Config

	// baseOffset is needed to calculate relative offset for index entries.
	baseOffset uint64

	// nextOffset is the offset to append new records under.
	nextOffset uint64
}

func NewSegment(dir string, baseOffset uint64, c Config) (*Segment, error) {
	// Create a new store.
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	store, err := NewStore(storeFile)
	if err != nil {
		return nil, err
	}

	// Create a new index.
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	index, err := NewIndex(indexFile, c)
	if err != nil {
		return nil, err
	}

	// Find the nextOffset by reading the last entry of the index file.
	var nextOffset uint64
	if off, _, err := index.Read(-1); err != nil {
		nextOffset = baseOffset
	} else {
		nextOffset = baseOffset + uint64(off) + 1
	}

	return &Segment{
		store:      store,
		index:      index,
		config:     c,
		baseOffset: baseOffset,
		nextOffset: nextOffset,
	}, nil
}

func (s *Segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur

	// Convert record to bytes
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	// TODO: Writing to store and index should be atomic.

	// Write record to store.
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	// Write index entry (with relative offset).
	if err = s.index.Write(
		// offsets in the index file are stored relative to the segments baseOffset.
		uint32(s.nextOffset-s.baseOffset),
		pos,
	); err != nil {
		return 0, err
	}

	s.nextOffset++
	return cur, nil
}

func (s *Segment) Read(off uint64) (*api.Record, error) {
	// Get position from index file.
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	// Read record from store file.
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	// Convert bytes to record.
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

func (s *Segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes ||
		s.index.IsMaxed()
}

func (s *Segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

func (s *Segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}
