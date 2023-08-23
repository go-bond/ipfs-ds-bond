package dsbond

import (
	"os"
	"testing"

	ds "github.com/ipfs/go-datastore"
	dstest "github.com/ipfs/go-datastore/test"
	"github.com/stretchr/testify/require"
)

func newDS(t *testing.T) (ds.Datastore, func()) {
	dir := os.TempDir()
	store, err := NewDataStore(dir, nil)
	require.NoError(t, err)
	return store, func() {
		store.Close()
		os.RemoveAll(dir)
	}
}

func TestSuite(t *testing.T) {
	d, done := newDS(t)
	defer done()

	dstest.SubtestAll(t, d)
}
