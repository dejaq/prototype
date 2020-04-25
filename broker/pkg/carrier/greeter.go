package carrier

import (
	"context"
	"errors"
	"math/rand"
	"sync"

	"github.com/sirupsen/logrus"

	derrors "github.com/dejaq/prototype/common/errors"

	"github.com/dejaq/prototype/common/protocol"

	"github.com/dejaq/prototype/common/timeline"
)

var (
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
		logger:                         logrus.New().WithField("component", "greeter"),
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
	logger                         logrus.FieldLogger
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

	if _, exists := s.consumerIDsAndSessionIDs.Get(c.GetID(), c.GetTopic()); exists {
		s.logger.Errorf("consumer %s on topic %s already has a handshake, removing the old one", c.GetID(), c.topic)
	}

	sessionID := randomSessionID()
	c.SetHydrateStatus(protocol.Hydration_None)
	s.consumerIDsAndSessionIDs.Set(c.GetID(), c.GetTopic(), sessionID)
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
		s.logger.Errorf("producerID %s on topic %s already has a handshake, removing the old one", req.ProducerID, req.Topic)
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

	s.consumerSessionsIDs[sessionID].SetHydrateStatus(protocol.Hydration_Requested)
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
		return nil, derrors.ErrConsumerNotSubscribed
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
		return sessionData.GetTopic(), nil
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
		if consumer.GetHydrateStatus() != hydrateStatus || consumer.GetTopic() != topicID ||
			pipe.Connected == nil {
			continue
		}
		result = append(result, pipe)
	}
	return result
}

func (s *Greeter) LeasesSent(c *Consumer, count int) {
	//logrus.Infof("sent %d msgs to consumer: %s topic: %s", count, c.id, c.topic)
	//TODO increment leases
}
