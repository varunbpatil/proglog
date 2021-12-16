package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile("", "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	// Create a dummy config.
	c := Config{}
	c.Segment.MaxIndexBytes = 100 * entryWidth

	idx, err := NewIndex(f, c)
	require.NoError(t, err)

	_, _, err = idx.Read(-1)
	require.Equal(t, io.EOF, err)

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, want := range entries {
		err := idx.Write(want.Off, want.Pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}

	// Reading past end of index should error.
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	_ = idx.Close()

	// index should build its state from the existing file​
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = NewIndex(f, c)
	require.NoError(t, err)
	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)
}
