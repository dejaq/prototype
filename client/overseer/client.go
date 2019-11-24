package overseer

import (
	"context"

	"github.com/bgadrian/dejaq-broker/client"
	"github.com/bgadrian/dejaq-broker/common/timeline"
	dejaq "github.com/bgadrian/dejaq-broker/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
)

var _ = client.Overseer(&Chief{})

func New(overseers []dejaq.BrokerClient) *Chief {
	return &Chief{overseers: overseers}
}

type Chief struct {
	overseers []dejaq.BrokerClient
}

func (c *Chief) CreateTimelineTopic(ctx context.Context, id string, topicSettings timeline.TopicSettings) error {
	var builder *flatbuffers.Builder

	builder = flatbuffers.NewBuilder(128)
	topicIDPos := builder.CreateString(id)

	dejaq.TimelineCreateRequestStart(builder)
	dejaq.TimelineCreateRequestAddId(builder, topicIDPos)
	dejaq.TimelineCreateRequestAddBucketCount(builder, topicSettings.BucketCount)
	dejaq.TimelineCreateRequestAddChecksumBodies(builder, topicSettings.ChecksumBodies)
	dejaq.TimelineCreateRequestAddMaxBodySizeBytes(builder, topicSettings.MaxBodySizeBytes)
	dejaq.TimelineCreateRequestAddMaxSecondsLease(builder, topicSettings.MaxSecondsLease)
	dejaq.TimelineCreateRequestAddMinimumDriverVersion(builder, topicSettings.MinimumDriverVersion)
	dejaq.TimelineCreateRequestAddMinimumProtocolVersion(builder, topicSettings.MinimumProtocolVersion)
	dejaq.TimelineCreateRequestAddReplicaCount(builder, topicSettings.ReplicaCount)
	dejaq.TimelineCreateRequestAddRqsLimitPerClient(builder, topicSettings.RQSLimitPerClient)
	root := dejaq.TimelineCreateRequestEnd(builder)

	builder.Finish(root)
	_, err := c.overseers[0].TimelineCreate(ctx, builder)
	if err != nil {
		return err
	}
	return nil
}
