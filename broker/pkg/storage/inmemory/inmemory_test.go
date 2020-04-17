package inmemory

import (
	"context"
	"reflect"
	"testing"

	"github.com/dejaq/prototype/common/timeline"

	"github.com/dejaq/prototype/broker/pkg/overseer"
	"github.com/dejaq/prototype/broker/pkg/synchronization"
)

func createTopic(buckets uint16) (*Memory, error) {
	c := overseer.NewCatalog()
	ctx := context.Background()
	err := c.AddTopic(ctx, synchronization.Topic{Topic: timeline.Topic{
		ID: "id",
		Settings: timeline.TopicSettings{
			BucketCount: buckets,
		},
	}})

	if err != nil {
		return nil, err
	}
	return New(c), nil
}

func Test_TopicNames(t *testing.T) {
	m, err := createTopic(1)
	ctx := context.Background()
	if err != nil {
		t.Error(err)
	}
	err = m.CreateTopic(ctx, "id")
	if err != nil {
		t.Error(reflect.TypeOf(m), err)
	}
	// create same topic should fail
	err = m.CreateTopic(ctx, "id")
	if err == nil {
		t.Error(reflect.TypeOf(m), "expect error, got nil")
	}
	// empty topic ID should rise an error
	err = m.CreateTopic(ctx, "")
	if err == nil {
		t.Error(reflect.TypeOf(m), "expect error, got nil")
	}
}

func Test_TopicBuckets(t *testing.T) {
	m, err := createTopic(0)
	ctx := context.Background()
	if err != nil {
		t.Error(err)
	}
	// bucket count less than 1 should rise an error
	err = m.CreateTopic(ctx, "id")
	if err == nil {
		t.Error(reflect.TypeOf(m), "expect error, got nil")
	}
}
