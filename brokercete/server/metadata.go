package server

import (
	"errors"
	"math/rand"
	"sync"

	"github.com/dgraph-io/badger/v2"

	"github.com/sirupsen/logrus"
)

type Metadata struct {
	db           *badger.DB
	logger       logrus.FieldLogger
	noPartitions uint16
	//minPrioritiesPerPartition map[uint16][]byte
	consumersPartition map[string]uint16
	m                  sync.Mutex
}

func NewMetadata(db *badger.DB, logger logrus.FieldLogger, noPartitions uint16) *Metadata {
	return &Metadata{
		db:                 db,
		logger:             logger,
		noPartitions:       noPartitions,
		consumersPartition: make(map[string]uint16),
	}
}

func (m *Metadata) AddNewConsumer(id string) error {
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

func (m *Metadata) RemoveConsumer(id string) {
	m.m.Lock()
	defer m.m.Unlock()
	delete(m.consumersPartition, id)
}

func (m *Metadata) GetStorageForConsumer(id string) (*PriorityStorage, error) {
	m.m.Lock()
	defer m.m.Unlock()

	if _, exists := m.consumersPartition[id]; !exists {
		return nil, errors.New("Consumer was not added")
	}

	return &PriorityStorage{
		db:        m.db,
		logger:    m.logger.WithField("priorityStorage", id),
		partition: m.consumersPartition[id],
	}, nil
}

func (m *Metadata) GetRandomPartition() uint16 {
	return uint16(rand.Intn(int(m.noPartitions)))
}
