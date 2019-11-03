package coordinator

import (
	"errors"
	"math/rand"
	"sync"

	"github.com/bgadrian/dejaq-broker/common/timeline"
	grpc "github.com/bgadrian/dejaq-broker/grpc/DejaQ"
)

var (
	ErrConsumerNotSubscribed  = errors.New("consumer is not subscribed")
	ErrConsumerIsNotConnected = errors.New("cannot find the channel connection")
	ErrNotFound               = errors.New("not found")
)

func NewGreeter() *Greeter {
	return &Greeter{
		RWMutex:                       sync.RWMutex{},
		consumerIDsAndSessionIDs:      make(map[string]string, 128),
		consumerSessionIDAndID:        make(map[string]string, 128),
		consumerSessionsIDs:           make(map[string]*grpc.TimelineConsumerHandshakeRequest, 128),
		consumerIDsAndPipelines:       make(map[string]chan timeline.PushLeases), //not buffered!!!
		producerSessionIDs:            make(map[string]*grpc.TimelineProducerHandshakeRequest),
		producerGroupIDsAndSessionIDs: make(map[string]string),
	}
}

// Greeter is in charge of keeping the local state of all Clients
type Greeter struct {
	sync.RWMutex
	consumerIDsAndSessionIDs map[string]string
	consumerSessionIDAndID   map[string]string
	consumerSessionsIDs      map[string]*grpc.TimelineConsumerHandshakeRequest
	consumerIDsAndPipelines  map[string]chan timeline.PushLeases

	producerSessionIDs            map[string]*grpc.TimelineProducerHandshakeRequest
	producerGroupIDsAndSessionIDs map[string]string
}

func (s *Greeter) ConsumerHandshake(req *grpc.TimelineConsumerHandshakeRequest) (string, error) {
	s.Lock()
	defer s.Unlock()

	consumerID := string(req.ConsumerID())
	if _, alreadyExists := s.consumerIDsAndSessionIDs[consumerID]; alreadyExists {
		return "", errors.New("handshake already existed")
	}

	session := make([]byte, 128, 128)
	rand.Read(session)
	sessionID := string(session)

	s.consumerIDsAndSessionIDs[consumerID] = sessionID
	s.consumerSessionIDAndID[sessionID] = consumerID
	s.consumerSessionsIDs[sessionID] = req
	s.consumerIDsAndPipelines[consumerID] = nil

	return sessionID, nil
}

func (s *Greeter) ProducerHandshake(req *grpc.TimelineProducerHandshakeRequest) (string, error) {
	s.Lock()
	defer s.Unlock()

	producerID := string(req.ProducerGroupID())
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
	s.Lock()
	defer s.Unlock()

	consumerID, hadHandshake := s.consumerSessionIDAndID[sessionID]
	if !hadHandshake {
		return nil, errors.New("session was not found")
	}
	s.consumerIDsAndPipelines[consumerID] = make(chan timeline.PushLeases) //not buffered!!!
	return s.consumerIDsAndPipelines[consumerID], nil
}

func (s *Greeter) ConsumerDisconnected(sessionID string) {
	s.Lock()
	defer s.Unlock()

	consumerID := s.consumerSessionIDAndID[sessionID]
	s.consumerIDsAndPipelines[consumerID] = nil
}

func (s *Greeter) GetPipelineFor(consumerID string) (chan timeline.PushLeases, error) {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()

	if _, hasSession := s.consumerIDsAndSessionIDs[string(consumerID)]; !hasSession {
		return nil, ErrConsumerNotSubscribed
	}

	pipeline, isConnectedNow := s.consumerIDsAndPipelines[string(consumerID)]
	if !isConnectedNow {
		return nil, ErrConsumerIsNotConnected
	}
	return pipeline, nil
}

func (s *Greeter) GetTopicFor(sessionID []byte) ([]byte, error) {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()

	//for now we do not know if is a producer or a consumer
	if sessionData, isProducer := s.producerSessionIDs[string(sessionID)]; isProducer {
		return sessionData.TopicID(), nil
	}

	if sessionData, isConsumer := s.consumerSessionsIDs[string(sessionID)]; isConsumer {
		return sessionData.TopicID(), nil
	}

	return nil, ErrNotFound
}

func (s *Greeter) GetProducerSessionData(sessionID []byte) (*grpc.TimelineProducerHandshakeRequest, error) {
	s.RLock()
	defer s.RUnlock()

	sessionData, hadHandshake := s.producerSessionIDs[string(sessionID)]
	if hadHandshake {
		return sessionData, nil
	}
	return nil, ErrNotFound
}