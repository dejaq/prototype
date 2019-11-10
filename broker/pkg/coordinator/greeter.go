package coordinator

import (
	"errors"
	"math/rand"
	"sync"

	"github.com/bgadrian/dejaq-broker/common/timeline"
)

var (
	ErrConsumerNotSubscribed  = errors.New("consumer is not subscribed")
	ErrConsumerIsNotConnected = errors.New("cannot find the channel connection")
	ErrNotFound               = errors.New("not found")
)

func NewGreeter() *Greeter {
	return &Greeter{
		opMutex:                       sync.RWMutex{},
		consumerIDsAndSessionIDs:      make(map[string]string, 128),
		consumerSessionIDAndID:        make(map[string]string, 128),
		consumerSessionsIDs:           make(map[string]*Consumer, 128),
		consumerIDsAndPipelines:       make(map[string]chan timeline.PushLeases), //not buffered!!!
		producerSessionIDs:            make(map[string]Producer),
		producerGroupIDsAndSessionIDs: make(map[string]string),
	}
}

type ConsumerPipelineTuple struct {
	C        *Consumer
	Pipeline chan timeline.PushLeases
}

// Greeter is in charge of keeping the local state of all Clients
type Greeter struct {
	opMutex                  sync.RWMutex
	consumerIDsAndSessionIDs map[string]string
	consumerSessionIDAndID   map[string]string
	consumerSessionsIDs      map[string]*Consumer
	consumerIDsAndPipelines  map[string]chan timeline.PushLeases

	producerSessionIDs            map[string]Producer
	producerGroupIDsAndSessionIDs map[string]string
}

func (s *Greeter) ConsumerHandshake(c Consumer) (string, error) {
	s.opMutex.Lock()
	defer s.opMutex.Unlock()

	if _, alreadyExists := s.consumerIDsAndSessionIDs[c.GetID()]; alreadyExists {
		return "", errors.New("handshake already existed")
	}

	session := make([]byte, 128, 128)
	rand.Read(session)
	sessionID := string(session)

	s.consumerIDsAndSessionIDs[c.GetID()] = sessionID
	s.consumerSessionIDAndID[sessionID] = c.GetID()
	s.consumerSessionsIDs[sessionID] = &c
	s.consumerIDsAndPipelines[c.GetID()] = nil

	return sessionID, nil
}

func (s *Greeter) ProducerHandshake(req Producer) (string, error) {
	s.opMutex.Lock()
	defer s.opMutex.Unlock()

	producerID := string(req.GroupID)
	sessionID, alreadyExists := s.producerGroupIDsAndSessionIDs[producerID]

	if alreadyExists {
		return "", errors.New("handshake already done")
	}

	session := make([]byte, 128, 128)
	rand.Read(session)
	sessionID = string(session)

	s.producerGroupIDsAndSessionIDs[producerID] = sessionID
	s.producerSessionIDs[sessionID] = req

	return sessionID, nil
}

func (s *Greeter) ConsumerConnected(sessionID string) (chan timeline.PushLeases, error) {
	s.opMutex.Lock()
	defer s.opMutex.Unlock()

	consumerID, hadHandshake := s.consumerSessionIDAndID[sessionID]
	if !hadHandshake {
		return nil, errors.New("session was not found")
	}
	s.consumerIDsAndPipelines[consumerID] = make(chan timeline.PushLeases) //not buffered!!!
	return s.consumerIDsAndPipelines[consumerID], nil
}

func (s *Greeter) ConsumerDisconnected(sessionID string) {
	s.opMutex.Lock()
	defer s.opMutex.Unlock()

	consumerID := s.consumerSessionIDAndID[sessionID]
	s.consumerIDsAndPipelines[consumerID] = nil
}

func (s *Greeter) GetPipelineFor(consumerID string) (chan timeline.PushLeases, error) {
	s.opMutex.RLock()
	defer s.opMutex.RUnlock()

	if _, hasSession := s.consumerIDsAndSessionIDs[consumerID]; !hasSession {
		return nil, ErrConsumerNotSubscribed
	}

	pipeline, isConnectedNow := s.consumerIDsAndPipelines[consumerID]
	if !isConnectedNow {
		return nil, ErrConsumerIsNotConnected
	}
	return pipeline, nil
}

func (s *Greeter) GetTopicFor(sessionID []byte) (string, error) {
	s.opMutex.RLock()
	defer s.opMutex.RUnlock()

	//for now we do not know if is a producer or a consumer
	if sessionData, isProducer := s.producerSessionIDs[string(sessionID)]; isProducer {
		return sessionData.Topic, nil
	}

	if sessionData, isConsumer := s.consumerSessionsIDs[string(sessionID)]; isConsumer {
		return sessionData.Topic, nil
	}

	return "", ErrNotFound
}

func (s *Greeter) GetProducerSessionData(sessionID []byte) (Producer, error) {
	s.opMutex.RLock()
	defer s.opMutex.RUnlock()

	sessionData, hadHandshake := s.producerSessionIDs[string(sessionID)]
	if hadHandshake {
		return sessionData, nil
	}
	return Producer{}, ErrNotFound
}

func (s *Greeter) GetAllActiveConsumers() []ConsumerPipelineTuple {
	s.opMutex.RLock()
	defer s.opMutex.RUnlock()

	result := make([]ConsumerPipelineTuple, 0, len(s.consumerIDsAndPipelines))
	for consumerID := range s.consumerIDsAndPipelines {
		sessionID := s.consumerIDsAndSessionIDs[consumerID]
		result = append(result, ConsumerPipelineTuple{
			C:        s.consumerSessionsIDs[sessionID],
			Pipeline: s.consumerIDsAndPipelines[consumerID],
		})
	}
	return result
}
