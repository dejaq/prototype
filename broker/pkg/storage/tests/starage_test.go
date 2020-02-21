package tests

import (
	"context"
	"reflect"
	"testing"

	"github.com/dejaq/prototype/broker/pkg/storage/inmemory"

	"github.com/dejaq/prototype/broker/pkg/overseer"
	"github.com/dejaq/prototype/broker/pkg/storage/timeline"
)

func Test_TopicNames(t *testing.T) {
	m := inmemory.New(overseer.GetDefaultCatalog())
	topicNameTest(t, m)
}

func Test_TopicBuckets(t *testing.T) {
	m := inmemory.New(overseer.GetDefaultCatalog())
	topicBucketTest(t, m)
}

func Test_Insert(t *testing.T) {
	m := inmemory.New(overseer.GetDefaultCatalog())
	insertTest(t, m)
}

func topicNameTest(t *testing.T, m timeline.Repository) {
	err := m.CreateTopic(context.Background(), "id")
	if err != nil {
		t.Error(reflect.TypeOf(m), err)
	}
	// create same topic should fail
	err = m.CreateTopic(context.Background(), "id")
	if err == nil {
		t.Error(reflect.TypeOf(m), "expect error, got nil")
	}
	// empty topic ID should rise an error
	err = m.CreateTopic(context.Background(), "")
	if err == nil {
		t.Error(reflect.TypeOf(m), "expect error, got nil")
	}
}

func topicBucketTest(t *testing.T, m timeline.Repository) {
	// bucket count less than 1 should rise an error
	err := m.CreateTopic(context.Background(), "id")
	if err == nil {
		t.Error(reflect.TypeOf(m), "expect error, got nil")
	}
	// create topic with one bucket should be an success
	err = m.CreateTopic(context.Background(), "id")
	if err != nil {
		t.Error(reflect.TypeOf(m), err)
	}
}

func insertTest(t *testing.T, m timeline.Repository) {

}
