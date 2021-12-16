package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	testData      = []byte("hello world")
	testDataWidth = uint64(len(testData)) + RecordLengthWidth
)

func TestStoreAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := NewStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = NewStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *Store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(testData)
		require.NoError(t, err)
		require.Equal(t, testDataWidth*i, n+pos)
	}
}

func testRead(t *testing.T, s *Store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		p, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, testData, p)
		pos += testDataWidth
	}
}

func testReadAt(t *testing.T, s *Store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		// Read length of the record.
		recordLength := make([]byte, RecordLengthWidth)
		n, err := s.ReadAt(recordLength, off)
		require.NoError(t, err)
		require.Equal(t, RecordLengthWidth, n)
		off += RecordLengthWidth

		p := make([]byte, enc.Uint64(recordLength))
		n, err = s.ReadAt(p, off)
		require.NoError(t, err)
		require.Equal(t, testData, p)
		require.Equal(t, int(enc.Uint64(recordLength)), n)
		off += int64(n)
	}
}
