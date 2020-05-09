package server

import (
	"github.com/dgraph-io/badger/v2"
	"github.com/sirupsen/logrus"
)

type PriorityStorage struct {
	db        *badger.DB
	logger    logrus.FieldLogger
	partition uint16
}

func (p *PriorityStorage) GetOldestMsgs(count int) []Msg {
	result := make([]Msg, 0, count)

	p.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: true,
			PrefetchSize:   count,
			Reverse:        false,
			AllVersions:    false,
			Prefix:         prefixPriority(p.partition),
			InternalAccess: false,
		})
		defer it.Close()
		for ; it.Valid(); it.Next() {
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
