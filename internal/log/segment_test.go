package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/varunbpatil/proglog/api/v1"
)

func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment-test")
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxIndexBytes = entryWidth * 3
	c.Segment.MaxStoreBytes = 1024

	s, err := NewSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	// Test maxed out index (MaxIndexBytes).
	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)
	require.True(t, s.IsMaxed())

	// Test maxed out store (MaxStoreBytes).
	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024
	s, err = NewSegment(dir, 16, c)
	require.NoError(t, err)
	require.True(t, s.IsMaxed())

	err = s.Remove()
	require.NoError(t, err)
	s, err = NewSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())

}
