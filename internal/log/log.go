package log

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/varunbpatil/proglog/api/v1"
)

type Log struct {
	sync.RWMutex
	Dir           string
	Config        Config
	activeSegment *Segment
	segments      []*Segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

func (l *Log) setup() error {
	files, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	// Get a list of baseOffset's from the file names in the directory.
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	// Recreate the segments.
	for i := 0; i < len(baseOffsets); i++ {
		if err := l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffsets contains dups (one for index file and one for store file).
		i++
	}

	// Create a new segment if none exist already
	if l.segments == nil {
		if err := l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) Append(record *api.Record) (off uint64, err error) {
	highestOffset, err := l.HighestOffset()
	if err != nil {
		return 0, err
	}

	l.Lock()
	defer l.Unlock()

	if l.activeSegment.IsMaxed() {
		err = l.newSegment(highestOffset + 1)
		if err != nil {
			return 0, err
		}
	}

	off, err = l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	return off, err
}

func (l *Log) Read(off uint64) (*api.Record, error) {
	l.RLock()
	defer l.RUnlock()

	// Find the segment in which the offset lies.
	var s *Segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}
	if s == nil || s.nextOffset <= off {
		return nil, api.ErrOffsetOutOfRange{Offset: off}
	}

	return s.Read(off)
}

func (l *Log) newSegment(baseOffset uint64) error {
	s, err := NewSegment(l.Dir, baseOffset, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

func (l *Log) Close() error {
	l.Lock()
	defer l.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

func (l *Log) LowestOffset() (uint64, error) {
	l.RLock()
	defer l.RUnlock()
	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.RLock()
	defer l.RUnlock()
	off := l.activeSegment.nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

func (l *Log) Truncate(lowestOff uint64) error {
	l.Lock()
	defer l.Unlock()

	var segments []*Segment
	for _, segment := range l.segments {
		if segment.nextOffset <= lowestOff+1 {
			if err := segment.Remove(); err != nil {
				return err
			}
		} else {
			segments = append(segments, segment)
		}
	}
	l.segments = segments
	return nil
}

func (l *Log) Reader() io.Reader {
	l.RLock()
	defer l.RUnlock()
	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{Store: segment.store, off: 0}
	}
	return io.MultiReader(readers...)
}

type originReader struct {
	*Store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}
