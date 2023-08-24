package dsbond

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-bond/bond"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
)

type DatastoreEntry struct {
	Key   string
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
			builder = builder.AddStringField(t.Key)
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
	err := txn.Put(ctx, key, value)
	if err != nil {
		return err
	}
	return txn.Commit(ctx)
}

func (d *Datastore) Delete(ctx context.Context, key ds.Key) error {
	txn, _ := d.NewTransaction(ctx, false)
	err := txn.Delete(ctx, key)
	if err != nil {
		return err
	}
	return txn.Commit(ctx)
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
	return t.table.Upsert(ctx, []*DatastoreEntry{{Key: key.String(), Value: value}}, func(old, new *DatastoreEntry) *DatastoreEntry {
		return new
	}, t.batch)
}

func (t *txn) Delete(ctx context.Context, key ds.Key) error {
	return t.table.Delete(ctx, []*DatastoreEntry{{Key: key.String()}}, t.batch)
}

func (t *txn) Get(ctx context.Context, key ds.Key) ([]byte, error) {
	entries, err := t.table.Get(ctx, bond.NewSelectorPoint[*DatastoreEntry](&DatastoreEntry{Key: key.String()}), t.batch)
	if err == nil {
		return entries[0].Value, nil
	}
	if strings.Contains(err.Error(), "not found") {
		return nil, ds.ErrNotFound
	}
	return nil, err
}

func (t *txn) Query(ctx context.Context, userQuery query.Query) (query.Results, error) {
	prefix := ds.NewKey(userQuery.Prefix).String()
	if prefix != "/" {
		prefix = prefix + "/"
	}
	reverse := false

	if len(userQuery.Orders) > 0 {
		switch userQuery.Orders[0].(type) {
		case query.OrderByKey, *query.OrderByKey:
		// We order by key by default.
		case query.OrderByKeyDescending, *query.OrderByKeyDescending:
			// Reverse order by key
			reverse = true
		default:
			// Skip the stuff we can't apply.
			baseQuery := userQuery
			baseQuery.Limit = 0
			baseQuery.Offset = 0
			baseQuery.Orders = nil

			// perform the base query.
			res, err := t.Query(ctx, baseQuery)
			if err != nil {
				return nil, err
			}

			// fix the query
			res = query.ResultsReplaceQuery(res, userQuery)

			// Remove the parts we've already applied.
			naiveQuery := userQuery
			naiveQuery.Prefix = ""
			naiveQuery.Filters = nil

			// Apply the rest of the query
			return query.NaiveQueryApply(naiveQuery, res), nil
		}
	}

	var entires []query.Entry
	skipped := 0
	err := t.table.ScanIndexForEach(ctx, t.table.PrimaryIndex(), bond.NewSelectorPoint(&DatastoreEntry{Key: prefix}),
		func(keyBytes bond.KeyBytes, t bond.Lazy[*DatastoreEntry]) (bool, error) {
			key := string(keyBytes.ToKey().PrimaryKey)[1:]
			if !strings.HasPrefix(key, prefix) {
				return false, nil
			}
			// skip if offset is applied.
			if skipped < userQuery.Offset && len(userQuery.Filters) == 0 {
				skipped++
				return true, nil
			}

			var entry query.Entry
			if userQuery.KeysOnly {
				entry = query.Entry{
					Key: key,
				}
			} else {
				item, err := t.Get()
				if err != nil {
					return false, err
				}
				entry = query.Entry{
					Key:   item.Key,
					Value: item.Value,
					Size:  len(item.Value),
				}
			}

			// we must go through filter to apply offset.
			if skipped < userQuery.Offset {
				if !filter(userQuery.Filters, entry) {
					skipped++
				}
				return true, nil
			}

			if userQuery.Limit != 0 && len(entires) >= userQuery.Limit {
				return false, nil
			}

			if len(userQuery.Filters) > 0 && filter(userQuery.Filters, entry) {
				return true, nil
			}
			entires = append(entires, entry)
			return true, nil
		}, reverse, t.batch)
	if err != nil {
		return nil, err
	}

	return query.ResultsWithEntries(userQuery, entires), nil
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
		return -1, err
	}
	return len(entry), nil
}

func (t *txn) Commit(ctx context.Context) error {
	return t.batch.Commit(bond.Sync)
}

func (t *txn) Discard(ctx context.Context) {
	t.batch.Reset()
}

func filter(filters []query.Filter, entry query.Entry) bool {
	for _, f := range filters {
		if !f.Filter(entry) {
			return true
		}
	}
	return false
}
