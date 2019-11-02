package coordinator

import (
	"errors"
	"math/rand"
	"sync"

	"github.com/bgadrian/dejaq-broker/common/timeline"
	grpc "github.com/bgadrian/dejaq-broker/grpc/DejaQ"
)

type Greeter struct {
	sync.RWMutex
	consumerIDsAndSessionIDs map[string]string
	consumerSessionIDAndID   map[string]string
	//consumerSessions   map[string]*grpc.TimelineConsumerHandshakeRequest
	consumerIDsAndPipelines map[string]chan timeline.PushLeases

	producerSessionIDs            map[string]*grpc.TimelineProducerHandshakeRequest
	producerGroupIDsAndSessionIDs map[string]string
}

func (s *Greeter) AddConsumer(req *grpc.TimelineConsumerHandshakeRequest) (string, error) {
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
	//s.consumerSessions[sessionID] = req
	s.consumerIDsAndPipelines[consumerID] = nil

	return sessionID, nil
}

func (s *Greeter) AddProducer(req *grpc.TimelineProducerHandshakeRequest) (string, error) {
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

func (s *Greeter) consumerStarted(sessionID string) (chan timeline.PushLeases, error) {
	s.Lock()
	defer s.Unlock()

	consumerID, hadHandshake := s.consumerSessionIDAndID[sessionID]
	if !hadHandshake {
		return nil, errors.New("session was not found")
	}
	s.consumerIDsAndPipelines[consumerID] = make(chan timeline.PushLeases) //not buffered!!!
	return s.consumerIDsAndPipelines[consumerID], nil
}
func (s *Greeter) consumerStopped(sessionID string) {
	s.Lock()
	defer s.Unlock()

	consumerID := s.consumerSessionIDAndID[sessionID]
	s.consumerIDsAndPipelines[consumerID] = nil
}
