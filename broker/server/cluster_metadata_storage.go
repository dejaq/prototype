package server

import (
	"github.com/dgraph-io/badger/v2"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type ClusterMetadata struct {
	db     *badger.DB
	logger logrus.FieldLogger
}

func (m *ClusterMetadata) Get(keys [][]byte) []Msg {
	var result []Msg
	err := m.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			v, err := txn.Get(key)
			if err != nil {
				m.logger.WithError(err).Errorf("failed fetching %s", string(key))
				continue
			}
			msg := Msg{
				Key: key,
			}
			_, err = v.ValueCopy(msg.Val)
			if err != nil {
				m.logger.WithError(err).Errorf("failed fetching value for %s", string(key))
				continue
			}
			result = append(result, msg)
		}
		return nil
	})
	if err != nil {
		m.logger.WithError(err).Error("failed Read TX get")
	}

	return result
}

func (m *ClusterMetadata) Set(batch []Msg) error {
	//write to DB
	wb := m.db.NewWriteBatch()
	defer wb.Cancel()

	for _, msg := range batch {
		err := wb.Set(msg.Key, msg.Val)
		if err != nil {
			return errors.Wrap(err, "cannot write to DB batch Set")
		}
	}
	err := wb.Flush()
	if err != nil {
		return errors.Wrap(err, "failed to flush Set")
	}
	return nil
}
