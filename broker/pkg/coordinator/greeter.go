package coordinator

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"sync"

	"github.com/bgadrian/dejaq-broker/common/protocol"

	"github.com/bgadrian/dejaq-broker/common/timeline"
)

var (
	ErrConsumerNotSubscribed  = errors.New("consumer is not subscribed")
	ErrConsumerIsNotConnected = errors.New("cannot find the channel connection")
	ErrNotFound               = errors.New("not found")
)

// a 2 dimensional map of strings.
// first layer is the consumer/producerID, 2nd the topicID and the 3rd a string
type idsPerTopic struct {
	data map[string]map[string]string
}

func newIDsPerTopic() *idsPerTopic {
	return &idsPerTopic{
		make(map[string]map[string]string, 100),
	}
}

func (d *idsPerTopic) Set(id, topicID, value string) {
	if _, exists := d.data[id]; !exists {
		d.data[id] = make(map[string]string, 1)
	}
	d.data[id][topicID] = value
}

func (d *idsPerTopic) Get(id, topicID string) (string, bool) {
	if v, ok := d.data[id]; ok {
		if v2, ok2 := v[topicID]; ok2 {
			return v2, true
		}
	}
	return "", false
}

func NewGreeter() *Greeter {
	return &Greeter{
		opMutex:                        sync.RWMutex{},
		consumerIDsAndSessionIDs:       newIDsPerTopic(),
		consumerSessionIDAndID:         make(map[string]string, 128),
		consumerSessionsIDs:            make(map[string]*Consumer, 128),
		consumerSessionIDsAndPipelines: make(map[string]*ConsumerPipelineTuple),
		producerSessionIDs:             make(map[string]*Producer),
		producerIDsAndSessionIDs:       newIDsPerTopic(),
	}
}

type ConsumerPipelineTuple struct {
	C *Consumer
	// Pipeline can be used to push messages, while ConsumerConnected is open
	Pipeline chan timeline.Lease
	// Connected will be closed to signal when the consumer disconnects
	Connected chan struct{}
}

// Greeter is in charge of keeping the local state of all Clients
type Greeter struct {
	baseCtx                        context.Context
	opMutex                        sync.RWMutex
	consumerIDsAndSessionIDs       *idsPerTopic
	consumerSessionIDAndID         map[string]string
	consumerSessionsIDs            map[string]*Consumer
	consumerSessionIDsAndPipelines map[string]*ConsumerPipelineTuple

	producerSessionIDs       map[string]*Producer
	producerIDsAndSessionIDs *idsPerTopic
}

func randomSessionID() string {
	session := make([]byte, 128, 128)
	rand.Read(session)
	return string(session)
}

func (s *Greeter) ConsumerHandshake(c *Consumer) (string, error) {
	s.opMutex.Lock()
	defer s.opMutex.Unlock()

	if _, exists := s.consumerIDsAndSessionIDs.Get(c.GetID(), c.Topic); exists {
		return "", errors.New("handshake already exists")
	}

	sessionID := randomSessionID()
	if _, duplicateSessionID := s.consumerSessionsIDs[sessionID]; duplicateSessionID {
		log.Fatal("duplicate random sessionID")
	}
	if c, ok := s.consumerSessionsIDs[sessionID]; ok {
		c.HydrateStatus = protocol.Hydration_None
	}
	s.consumerIDsAndSessionIDs.Set(c.GetID(), c.Topic, sessionID)
	s.consumerSessionIDAndID[sessionID] = c.GetID()
	s.consumerSessionsIDs[sessionID] = c
	s.consumerSessionIDsAndPipelines[sessionID] = &ConsumerPipelineTuple{
		C:         c,
		Pipeline:  nil,
		Connected: nil,
	}

	return sessionID, nil
}

func (s *Greeter) ProducerHandshake(req *Producer) (string, error) {
	s.opMutex.Lock()
	defer s.opMutex.Unlock()

	if _, alreadyExists := s.producerIDsAndSessionIDs.Get(req.ProducerID, req.Topic); alreadyExists {
		return "", errors.New("producerID already has a handshake")
	}

	sessionID := randomSessionID()
	s.producerIDsAndSessionIDs.Set(req.ProducerID, req.Topic, sessionID)
	s.producerSessionIDs[sessionID] = req

	return sessionID, nil
}

func (s *Greeter) ConsumerConnected(sessionID string) (chan timeline.Lease, error) {
	s.opMutex.Lock()
	defer s.opMutex.Unlock()

	_, hadHandshake := s.consumerSessionIDAndID[sessionID]
	if !hadHandshake {
		return nil, errors.New("session was not found")
	}
	if s.consumerSessionIDsAndPipelines[sessionID].Pipeline != nil {
		return nil, errors.New("there is already an active connection for this consumerID")
	}

	s.consumerSessionsIDs[sessionID].HydrateStatus = protocol.Hydration_Requested
	s.consumerSessionIDsAndPipelines[sessionID].Pipeline = make(chan timeline.Lease) //not buffered!!!
	s.consumerSessionIDsAndPipelines[sessionID].Connected = make(chan struct{})
	return s.consumerSessionIDsAndPipelines[sessionID].Pipeline, nil
}

func (s *Greeter) ConsumerDisconnected(sessionID string) {
	s.opMutex.Lock()
	defer s.opMutex.Unlock()

	//this should trigger the Loader to stop pushing, if is currently doing so
	close(s.consumerSessionIDsAndPipelines[sessionID].Connected)
	close(s.consumerSessionIDsAndPipelines[sessionID].Pipeline)
	s.consumerSessionIDsAndPipelines[sessionID].Pipeline = nil
	s.consumerSessionIDsAndPipelines[sessionID].Connected = nil
}

func (s *Greeter) GetConsumer(sessionID string) (*Consumer, error) {
	s.opMutex.RLock()
	defer s.opMutex.RUnlock()
	consumer, ok := s.consumerSessionsIDs[sessionID]
	if !ok {
		return nil, errors.New("consumer not found")
	}
	return consumer, nil
}

func (s *Greeter) GetTopicFor(sessionID string) (string, error) {
	s.opMutex.RLock()
	defer s.opMutex.RUnlock()

	//for now we do not know if is a producer or a consumer
	if sessionData, isProducer := s.producerSessionIDs[sessionID]; isProducer {
		return sessionData.Topic, nil
	}

	if sessionData, isConsumer := s.consumerSessionsIDs[sessionID]; isConsumer {
		return sessionData.Topic, nil
	}

	return "", ErrNotFound
}

func (s *Greeter) GetProducerSessionData(sessionID string) (*Producer, error) {
	s.opMutex.RLock()
	defer s.opMutex.RUnlock()

	sessionData, hadHandshake := s.producerSessionIDs[sessionID]
	if hadHandshake {
		return sessionData, nil
	}
	return nil, ErrNotFound
}

func (s *Greeter) GetAllConnectedConsumersWithHydrateStatus(topicID string, hydrateStatus protocol.HydrationStatus) []*ConsumerPipelineTuple {
	s.opMutex.RLock()
	defer s.opMutex.RUnlock()

	result := make([]*ConsumerPipelineTuple, 0, len(s.consumerSessionIDsAndPipelines))
	for sessionID, pipe := range s.consumerSessionIDsAndPipelines {
		consumer := s.consumerSessionsIDs[sessionID]
		if consumer.HydrateStatus != hydrateStatus || consumer.Topic != topicID ||
			pipe.Connected == nil {
			continue
		}
		result = append(result, pipe)
	}
	return result
}

func (s *Greeter) LeasesSent(c *Consumer, count int) {
	//logrus.Infof("sent %d msgs to consumer: %s topic: %s", count, c.ID, c.Topic)
	//TODO increment leases
}
