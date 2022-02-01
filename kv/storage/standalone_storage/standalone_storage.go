package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db   *badger.DB
	conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	db := engine_util.CreateDB(conf.DBPath, false)
	kv := &StandAloneStorage{db: db}
	return kv
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	ssr := &standaloneStorageReader{db: s.db}
	return ssr, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, v := range batch {
		switch v.Data.(type) {
		case storage.Put:
			if err := engine_util.PutCF(s.db, v.Cf(), v.Key(), v.Value()); err != nil {
				return err
			}
		case storage.Delete:
			if err := engine_util.DeleteCF(s.db, v.Cf(), v.Key()); err != nil {
				return err
			}
		}

	}
	return nil
}

type standaloneStorageReader struct {
	db     *badger.DB
	dbIter *engine_util.BadgerIterator
	txn    *badger.Txn
}

func (s *standaloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCF(s.db, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (s *standaloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	s.txn = s.db.NewTransaction(false)
	s.dbIter = engine_util.NewCFIterator(cf, s.txn)
	return s.dbIter
}

func (s *standaloneStorageReader) Close() {
	s.txn.Discard()
	s.dbIter.Close()
}
