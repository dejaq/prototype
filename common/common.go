package common

type TopicType uint8
const (
	TopicType_Timeline TopicType = iota
	TopicType_ProrityQueue TopicType = 1
	TopicType_Crons  TopicType = 2
)
