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
	localPartitions    map[uint16]*badger.DB
	logger             logrus.FieldLogger
	noPartitions       uint16
	consumersPartition map[string]uint16
	m                  sync.Mutex
}

func NewTopicLocalData(topicID string, dataDirectory string, logger logrus.FieldLogger, noPartitions uint16) *TopicLocalData {
	localPartitions := make(map[uint16]*badger.DB, 12)

	//TODO get the no of partitions from the ClusterMETATDAT table
	for i := uint16(0); i < noPartitions; i++ {
		partitionDBDirectory := fmt.Sprintf("%s/topics/%s/%d", dataDirectory, topicID, i)
		//group can have read it for backups, only us for execute
		err := os.MkdirAll(partitionDBDirectory, 0740)
		if err != nil {
			logger.WithError(err).Fatalf("failed to mkdir %s", partitionDBDirectory)
		}
		db, err := badger.Open(badger.DefaultOptions(partitionDBDirectory))
		if err != nil {
			logger.WithError(err).Fatalf("failed to open DB at %s", partitionDBDirectory)
		}
		//TODO if the overhead is too large and keeping thousands of open DBs is too much for a node
		// we can group more partitions in one DB and add a prefix
		localPartitions[i] = db
		logger.Infof("created local DB for topic %s partition %d at %s", topicID, i, partitionDBDirectory)
	}

	return &TopicLocalData{
		localPartitions:    localPartitions,
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
		for _, p := range m.consumersPartition {
			if p == i {
				continue
			}
		}
		//else we found a free one
		m.consumersPartition[id] = i
		return nil
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

	localDB, exists := m.localPartitions[partition]
	if !exists {
		return nil, errors.New("partition not found on this node")
	}

	return &PartitionStorage{
		topicID:   m.topicID,
		db:        localDB,
		logger:    m.logger.WithField("priorityStorage", partition),
		partition: partition,
	}, nil
}

func (m *TopicLocalData) GetRandomPartition() uint16 {
	return uint16(rand.Intn(int(m.noPartitions)))
}

func (m *TopicLocalData) GetTopicID() string {
	return m.topicID
}

func (m *TopicLocalData) Close() {
	for p, db := range m.localPartitions {
		err := db.Close()
		if err != nil {
			m.logger.WithError(err).WithField("partition", p).Error("failed to close DB")
		}
	}
}
