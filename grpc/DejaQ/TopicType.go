// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package DejaQ

import "strconv"

type TopicType int8

const (
	TopicTypeTimeline      TopicType = 1
	TopicTypePriorityQueue TopicType = 2
	TopicTypeCronjob       TopicType = 3
)

var EnumNamesTopicType = map[TopicType]string{
	TopicTypeTimeline:      "Timeline",
	TopicTypePriorityQueue: "PriorityQueue",
	TopicTypeCronjob:       "Cronjob",
}

var EnumValuesTopicType = map[string]TopicType{
	"Timeline":      TopicTypeTimeline,
	"PriorityQueue": TopicTypePriorityQueue,
	"Cronjob":       TopicTypeCronjob,
}

func (v TopicType) String() string {
	if s, ok := EnumNamesTopicType[v]; ok {
		return s
	}
	return "TopicType(" + strconv.FormatInt(int64(v), 10) + ")"
}
