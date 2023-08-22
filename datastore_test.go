package dsbond

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/go-bond/bond"
	"github.com/stretchr/testify/require"
)

func TestBondPrefix(t *testing.T) {
	type Entry struct {
		Key string
	}

	db, err := bond.Open("./tmp", nil)
	defer os.RemoveAll("./tmp")
	require.NoError(t, err)
	table := bond.NewTable[*Entry](bond.TableOptions[*Entry]{
		DB:      db,
		TableID: bond.TableID(1),
		TablePrimaryKeyFunc: func(builder bond.KeyBuilder, t *Entry) []byte {
			builder = builder.AddStringField(t.Key)
			return builder.Bytes()
		},
	})
	for i := 0; i < 25; i++ {
		err = table.Insert(context.TODO(), []*Entry{{Key: fmt.Sprintf("/blocks/%d", i)}})
		require.NoError(t, err)
	}

	q := table.Query().With(table.PrimaryIndex(), bond.NewSelectorRange[*Entry](&Entry{Key: "/blocks"}, &Entry{Key: "/blocks"}))
	var out []*Entry
	err = q.Execute(context.TODO(), &out)
	require.NoError(t, err)
	// require.Equal(t, len(out), 3)
	for _, item := range out {
		fmt.Println(item.Key)
		require.Contains(t, item.Key, "/blocks")
	}
}
