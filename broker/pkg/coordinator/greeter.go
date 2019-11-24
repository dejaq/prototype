package coordinator

import (
	"errors"
	"github.com/bgadrian/dejaq-broker/common/protocol"
	"log"
	"math/rand"
	"sync"

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
		consumerSessionIDsAndPipelines: make(map[string]chan timeline.PushLeases), //not buffered!!!
		producerSessionIDs:             make(map[string]*Producer),
		producerIDsAndSessionIDs:       newIDsPerTopic(),
	}
}

type ConsumerPipelineTuple struct {
	C        *Consumer
	Pipeline chan timeline.PushLeases
}

// Greeter is in charge of keeping the local state of all Clients
type Greeter struct {
	opMutex                        sync.RWMutex
	consumerIDsAndSessionIDs       *idsPerTopic
	consumerSessionIDAndID         map[string]string
	consumerSessionsIDs            map[string]*Consumer
	consumerSessionIDsAndPipelines map[string]chan timeline.PushLeases

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
	s.consumerSessionIDsAndPipelines[sessionID] = nil

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

func (s *Greeter) ConsumerConnected(sessionID string) (chan timeline.PushLeases, error) {
	s.opMutex.Lock()
	defer s.opMutex.Unlock()

	_, hadHandshake := s.consumerSessionIDAndID[sessionID]
	if !hadHandshake {
		return nil, errors.New("session was not found")
	}
	if pipe, alreadyHasAPipeline := s.consumerSessionIDsAndPipelines[sessionID]; alreadyHasAPipeline && pipe != nil {
		return nil, errors.New("there is already an active connection for this consumerID")
	}

	if c, ok := s.consumerSessionsIDs[sessionID]; ok {
		c.HydrateStatus = protocol.Hydration_Requested
	}
	s.consumerSessionIDsAndPipelines[sessionID] = make(chan timeline.PushLeases) //not buffered!!!
	return s.consumerSessionIDsAndPipelines[sessionID], nil
}

func (s *Greeter) ConsumerDisconnected(sessionID string) {
	s.opMutex.Lock()
	defer s.opMutex.Unlock()

	close(s.consumerSessionIDsAndPipelines[sessionID])
	s.consumerSessionIDsAndPipelines[sessionID] = nil
}

func (s *Greeter) GetPipelineFor(c *Consumer) (chan timeline.PushLeases, error) {
	s.opMutex.RLock()
	defer s.opMutex.RUnlock()

	sessionID, hasSession := s.consumerIDsAndSessionIDs.Get(c.GetID(), c.Topic)
	if !hasSession {
		return nil, ErrConsumerNotSubscribed
	}

	pipeline, isConnectedNow := s.consumerSessionIDsAndPipelines[sessionID]
	if !isConnectedNow || pipeline == nil {
		return nil, ErrConsumerIsNotConnected
	}
	return pipeline, nil
}

func (s *Greeter) GetConsumer(sessionID string) (*Consumer, error){
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

func (s *Greeter) GetAllActiveConsumers() []ConsumerPipelineTuple {
	s.opMutex.RLock()
	defer s.opMutex.RUnlock()

	result := make([]ConsumerPipelineTuple, 0, len(s.consumerSessionIDsAndPipelines))
	for sessionID, pipe := range s.consumerSessionIDsAndPipelines {
		result = append(result, ConsumerPipelineTuple{
			C:        s.consumerSessionsIDs[sessionID],
			Pipeline: pipe,
		})
	}
	return result
}

func (s *Greeter) GetAllConsumersWithHydrateStatus(hydrateStatus protocol.HydrationStatus) []ConsumerPipelineTuple {
	s.opMutex.RLock()
	defer s.opMutex.RUnlock()

	result := make([]ConsumerPipelineTuple, 0, len(s.consumerSessionIDsAndPipelines))
	for sessionID, pipe := range s.consumerSessionIDsAndPipelines {
		consumer := s.consumerSessionsIDs[sessionID]
		if consumer.HydrateStatus != hydrateStatus {
			continue
		}
		result = append(result, ConsumerPipelineTuple{
			C:        consumer,
			Pipeline: pipe,
		})
	}
	return result
}

func (s *Greeter) LeasesSent(c *Consumer, count int) {
	//TODO increment leases
}
