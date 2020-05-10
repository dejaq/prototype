package server

import (
	"github.com/dgraph-io/badger/v2"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type PartitionStorage struct {
	topicID   string
	db        *badger.DB
	logger    logrus.FieldLogger
	partition uint16
}

func (p *PartitionStorage) GetOldestMsgs(count int) []Msg {
	result := make([]Msg, 0, count)

	// TODO manage error here
	_ = p.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: true,
			PrefetchSize:   count,
			Reverse:        false,
			AllVersions:    false,
			Prefix:         nil,
			InternalAccess: false,
		})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			result = append(result, Msg{
				Key: item.Key(),
				Val: val,
			})
			if len(result) >= count {
				break
			}
		}
		return nil
	})

	return result
}

// the Op is done in a transaction, all or nothing
func (p *PartitionStorage) AddMsgs(batch []Msg) error {
	//write to DB
	wb := p.db.NewWriteBatch()
	defer wb.Cancel()

	for _, msg := range batch {
		err := wb.Set(msg.Key, msg.Val)
		if err != nil {
			return errors.Wrap(err, "cannot write to DB batch AddMsgs")
		}
	}
	err := wb.Flush()
	if err != nil {
		return errors.Wrap(err, "failed to flush AddMsgs")
	}
	return nil
}

// the Op is done in a transaction, all or nothing
func (p *PartitionStorage) RemoveMsgs(keys [][]byte) error {
	//write to DB
	wb := p.db.NewWriteBatch()
	defer wb.Cancel()

	for _, key := range keys {
		err := wb.Delete(key)
		if err != nil {
			return errors.Wrap(err, "cannot write to DB batch RemoveMsgs")
		}
	}
	err := wb.Flush()
	if err != nil {
		return errors.Wrap(err, "failed to flush RemoveMsgs")
	}
	return nil
}
