package server

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"

	"github.com/dgraph-io/badger/v2"

	"github.com/sirupsen/logrus"
)

type TopicLocalData struct {
	topicID            string
	db                 *badger.DB
	logger             logrus.FieldLogger
	noPartitions       uint16
	consumersPartition map[string]uint16
	m                  sync.Mutex
}

func NewTopicLocalData(topicID string, dataDirectory string, logger logrus.FieldLogger, noPartitions uint16) *TopicLocalData {
	//TODO get the no of partitions from the ClusterMETATDAT table
	topicDirectory := fmt.Sprintf("%s/topic/%s", dataDirectory, topicID)
	//group can have read it for backups, only us for execute
	err := os.MkdirAll(topicDirectory, 0740)
	if err != nil {
		logger.WithError(err).Fatalf("failed to mkdir %s", topicDirectory)
	}
	db, err := badger.Open(badger.DefaultOptions(topicDirectory))
	if err != nil {
		logger.WithError(err).Fatalf("failed to open DB at %s", topicDirectory)
	}
	logger.Infof("created local DB for topic %s at %s", topicID, topicDirectory)

	return &TopicLocalData{
		db:                 db,
		logger:             logger,
		noPartitions:       noPartitions,
		consumersPartition: make(map[string]uint16),
	}
}

func (m *TopicLocalData) AddNewConsumer(id string) error {
	m.m.Lock()
	defer m.m.Unlock()

	//seek a free partition
	for i := uint16(0); i < m.noPartitions; i++ {
		isFree := true
		for _, p := range m.consumersPartition {
			if p == i {
				isFree = false
			}
		}
		if isFree {
			//else we found a free one
			m.consumersPartition[id] = i
			return nil
		}
	}

	return errors.New("more consumers than partitions")
}

func (m *TopicLocalData) RemoveConsumer(id string) {
	m.m.Lock()
	defer m.m.Unlock()
	delete(m.consumersPartition, id)
}

func (m *TopicLocalData) GetPartitionForConsumer(id string) (uint16, error) {
	m.m.Lock()
	defer m.m.Unlock()

	if partitionForConsumer, exists := m.consumersPartition[id]; exists {
		return partitionForConsumer, nil
	}
	return 0, errors.New("Consumer was not added")
}

func (m *TopicLocalData) GetPartitionStorage(partition uint16) (*PartitionStorage, error) {
	m.m.Lock()
	defer m.m.Unlock()

	prefixForThisPartition := []byte("dejaq_pq_" + m.topicID + "_")
	prefixForThisPartition = append(prefixForThisPartition, UInt16ToBytes(partition)...)
	prefixForThisPartition = append(prefixForThisPartition, '_')

	return &PartitionStorage{
		topicID:   m.topicID,
		db:        m.db,
		logger:    m.logger.WithField("priorityStorage", partition),
		partition: partition,
		prefix:    prefixForThisPartition,
	}, nil
}

func (m *TopicLocalData) GetRandomPartition() uint16 {
	//TODO put local rand source to avoid mutex between topics
	return uint16(rand.Intn(int(m.noPartitions)))
}

func (m *TopicLocalData) GetTopicID() string {
	return m.topicID
}

func (m *TopicLocalData) Close() {
	err := m.db.Close()
	if err != nil {
		m.logger.WithError(err).Error("failed to close DB")
	}
}
