package dsbond

import (
	"context"
	"fmt"
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

var _ ds.Datastore = &Datastore{}

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

func (d *Datastore) Get(ctx context.Context, key ds.Key) ([]byte, error) {
	txn, _ := d.NewTransaction(ctx, true)
	return txn.Get(ctx, key)
}

func (d *Datastore) Has(ctx context.Context, key ds.Key) (bool, error) {
	txn, _ := d.NewTransaction(ctx, true)
	return txn.Has(ctx, key)
}

func (d *Datastore) GetSize(ctx context.Context, key ds.Key) (int, error) {
	txn, _ := d.NewTransaction(ctx, true)
	return txn.GetSize(ctx, key)
}

func (d *Datastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	txn, _ := d.NewTransaction(ctx, true)
	return txn.Query(ctx, q)
}

func (d *Datastore) Put(ctx context.Context, key ds.Key, value []byte) error {
	txn, _ := d.NewTransaction(ctx, false)
	return txn.Put(ctx, key, value)
}

func (d *Datastore) Delete(ctx context.Context, key ds.Key) error {
	txn, _ := d.NewTransaction(ctx, false)
	return txn.Delete(ctx, key)
}

func (d *Datastore) Sync(ctx context.Context, _ ds.Key) error {
	return d.db.Backend().Flush()
}

func (d *Datastore) Close() error {
	return d.db.Close()
}

func (d *Datastore) Batch(ctx context.Context) (ds.Batch, error) {
	return d.NewTransaction(ctx, false)
}

func (d *Datastore) NewTransaction(ctx context.Context, readOnly bool) (ds.Txn, error) {
	batch := d.db.Batch()
	return &txn{
		batch:    batch,
		table:    d.table,
		readOnly: readOnly,
	}, nil
}

type txn struct {
	table    bond.Table[*DatastoreEntry]
	batch    bond.Batch
	readOnly bool
}

var _ ds.Txn = &txn{}

func (t *txn) Put(ctx context.Context, key ds.Key, value []byte) error {
	if t.readOnly {
		return fmt.Errorf("readonly txn")
	}
	return t.table.Upsert(ctx, []*DatastoreEntry{{Key: key, Value: value}}, func(old, new *DatastoreEntry) *DatastoreEntry {
		return new
	}, t.batch)
}

func (t *txn) Delete(ctx context.Context, key ds.Key) error {
	return t.table.Delete(ctx, []*DatastoreEntry{{Key: key}})
}

func (t *txn) Get(ctx context.Context, key ds.Key) ([]byte, error) {
	entries, err := t.table.Get(ctx, bond.NewSelectorPoint[*DatastoreEntry](&DatastoreEntry{Key: key}), t.batch)
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
	err := q.Execute(ctx, &out, t.batch)
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

func (t *txn) Has(ctx context.Context, key ds.Key) (bool, error) {
	_, err := t.Get(ctx, key)
	if err == nil {
		return true, nil
	}
	if err == ds.ErrNotFound {
		return false, nil
	}
	return false, err
}

func (t *txn) GetSize(ctx context.Context, key ds.Key) (int, error) {
	entry, err := t.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	return len(entry), nil
}

func (t *txn) Commit(ctx context.Context) error {
	return t.batch.Commit(bond.Sync)
}

func (t *txn) Discard(ctx context.Context) {
	t.batch.Reset()
}
