package dsbond

import (
	"context"
	"strings"

	"github.com/go-bond/bond"
	"github.com/go-bond/bond/cond"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
)

type DatastoreEntry struct {
	Key   ds.Key
	Value []byte
}

type Datastore struct {
	table bond.Table[*DatastoreEntry]
	db    bond.DB
}

type txn struct {
	table bond.Table[*DatastoreEntry]
	batch bond.Batch
}

func (t *txn) Put(ctx context.Context, key ds.Key, value []byte) error {
	return t.table.Upsert(ctx, []*DatastoreEntry{{Key: key, Value: value}}, func(old, new *DatastoreEntry) *DatastoreEntry {
		return new
	}, t.batch)
}

func (t *txn) Delete(ctx context.Context, key ds.Key) error {
	return t.table.Delete(ctx, []*DatastoreEntry{{Key: key}})
}

func (t *txn) Get(ctx context.Context, key ds.Key) ([]byte, error) {
	entries, err := t.table.Get(ctx, bond.NewSelectorPoint[*DatastoreEntry](&DatastoreEntry{Key: key}))
	if err == nil {
		return entries[0].Value, nil
	}
	if strings.Contains(err.Error(), "not found") {
		return nil, ds.ErrNotFound
	}
	return nil, err
}

func (t *txn) Query(ctx context.Context, userQuery query.Query) (query.Results, error) {
	q := t.table.Query()

	if len(userQuery.Orders) > 0 {
		q = q.Order(func(r, r2 *DatastoreEntry) bool {
			return query.Less(userQuery.Orders, query.Entry{
				Key:   r.Key.String(),
				Value: r.Value,
				Size:  len(r.Value),
			}, query.Entry{
				Key:   r2.Key.String(),
				Value: r2.Value,
				Size:  len(r2.Value),
			})
		})
	}

	if len(userQuery.Filters) > 0 {
		conds := []cond.Cond[*DatastoreEntry]{}
		for _, filter := range userQuery.Filters {
			conds = append(conds, cond.Func(func(r *DatastoreEntry) bool {
				return filter.Filter(query.Entry{
					Key:   r.Key.String(),
					Value: r.Value,
					Size:  len(r.Value),
				})
			}))
		}
		q = q.Filter(cond.And(conds...))
	}

	if userQuery.Limit != 0 {
		q = q.Limit(uint64(userQuery.Limit))
	}

	if userQuery.Offset != 0 {
		q = q.Offset(uint64(userQuery.Offset))
	}

	if userQuery.Prefix != "" {
		q.With(t.table.PrimaryIndex(), bond.NewSelectorRange(&DatastoreEntry{Key: ds.NewKey(userQuery.Prefix)}, &DatastoreEntry{Key: ds.NewKey(userQuery.Prefix)}))
	}

	var out []*DatastoreEntry
	err := q.Execute(ctx, &out)
	if err != nil {
		builder := query.NewResultBuilder(userQuery)
		return builder.Results(), err
	}

	var entries []query.Entry
	for _, entry := range out {
		entries = append(entries, query.Entry{
			Key:   entry.Key.String(),
			Value: entry.Value,
			Size:  len(entry.Value),
		})
	}
	return query.ResultsWithEntries(userQuery, entries), nil
}

func NewDataStore(path string, opt *bond.Options) (*Datastore, error) {
	db, err := bond.Open(path, opt)
	if err != nil {
		return nil, err
	}
	table := bond.NewTable[*DatastoreEntry](bond.TableOptions[*DatastoreEntry]{
		DB:      db,
		TableID: bond.TableID(1),
		TablePrimaryKeyFunc: func(builder bond.KeyBuilder, t *DatastoreEntry) []byte {
			builder = builder.AddStringField(t.Key.String())
			return builder.Bytes()
		},
	})
	return &Datastore{
		db:    db,
		table: table,
	}, nil
}

type batch struct {
	b bond.Batch
}
