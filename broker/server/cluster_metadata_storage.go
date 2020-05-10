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

//
//// Update lowest priorities for a series of partitions
//// set Overwrite false to update only if they are smaller than the current
//// values (For producers).
//// ACK/Consumers should send overwrite true
//// TODO add tx time - replicatin combined with the fact that different brokers can
//// update these at the same time, an ack cam come before a Produce but reach
//// the replicas in different times can result to bad data
//func (m *ClusterMetadata) UpdateLowestPriority(topic string, mins map[uint16]uint64, overwrite bool) error {
//	rw := m.db.NewTransaction(true)
//	defer rw.Discard()
//
//	for partition, shouldMinPrio := range mins {
//		key := getMinPriorityKey(topic, partition)
//		_, rerr := rw.Get(key)
//		if rerr != nil && rerr != badger.ErrKeyNotFound {
//			return errors.Wrap(rerr, "failed to get the previous value")
//		}
//		if overwrite {
//			werr := rw.Set(key, UInt64ToBytes(shouldMinPrio))
//			if werr != nil {
//				return errors.Wrap(werr, "failed to set new value (1)")
//			}
//			continue
//		}
//	}
//}
//
//func getMinPriorityKey(topic string, partition uint16) []byte {
//	result := []byte("min_prio_" + topic + "_")
//	return append(result, UInt16ToBytes(partition)...)
//}
