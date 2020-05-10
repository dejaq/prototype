package server

import "github.com/dgraph-io/badger/v2"

type RaftListener struct {
	//to get and set snapshots
	db *badger.DB
	//to update and read metadata
	metadata *ClusterMetadata
	//to update and read messages
	topic *TopicLocalData
}

func NewRaftListener(db *badger.DB, metadata *ClusterMetadata, topic *TopicLocalData) *RaftListener {
	return &RaftListener{
		db:       db,
		metadata: metadata,
		topic:    topic,
	}

}

type AddMsgPayload struct {
	Partition uint16
	Msgs      []Msg
}

// send trough raft the request
func (r *RaftListener) AddMsgs(p AddMsgPayload) error {
	return nil
}

type AckMsgPayload struct {
	Partition uint16
	MsgIDs    [][]byte
}

// send trough raft the request
func (r *RaftListener) AckMsgs(p AckMsgPayload) error {
	return nil
}
