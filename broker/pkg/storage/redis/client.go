package redis

import (
	"fmt"
	"math/rand"

	"github.com/go-redis/redis"

	"github.com/bgadrian/dejaq-broker/common/timeline"

	"context"

	storage "github.com/bgadrian/dejaq-broker/broker/pkg/storage/timeline"
	derrors "github.com/bgadrian/dejaq-broker/common/errors"
)

type Client struct {
	host   string
	client *redis.Client
}

// NewClient ...
func NewClient(host string) (*Client, error) {

	client := redis.NewClient(&redis.Options{Addr: host})
	_, err := client.Ping().Result()

	if err != nil {
		return nil, err
	}

	return &Client{
		host:   host,
		client: client,
	}, nil
}

func (c *Client) createTimelineKey(clusterName string, timelineId string, bucketId string) string {
	return clusterName + ":" + timelineId + ":" + bucketId + ":timeline"
}

func (c *Client) createMessageKey(clusterName string, timelineId string, bucketId string, messageId string) string {
	return clusterName + ":" + timelineId + ":" + bucketId + ":" + messageId
}

func (c *Client) Insert(ctx context.Context, timelineID []byte, msgs []timeline.Message) []derrors.MessageIDTuple {
	// TODO use unsafe for conversions
	// keep messageId on sorted set, app use range queries
	for _, msg := range msgs {

		// TODO need a real timestamp instead of fake one
		msg.TimestampMS = uint64(rand.Intn(1000))

		// sorted set could keep 4 billions of records
		timelineKey := c.createTimelineKey("cluster_name", string(timelineID), string(msg.BucketID))
		err := c.client.ZAdd(timelineKey, &redis.Z{
			Member: string(msg.ID),
			Score:  float64(msg.TimestampMS),
		}).Err()

		// TODO return derrors here
		if err != nil {
			fmt.Println(err)
		}

		fmt.Printf("insert timelineKey: %s", timelineKey)

		// TODO improve here, find a better solution to translate a type into map
		messageKey := c.createMessageKey("cluster_name:", string(timelineID), string(msg.BucketID), string(msg.ID))
		data := make(map[string]interface{})
		data["ID"] = string(msg.ID)
		data["TimestampMS"] = string(msg.TimestampMS)
		data["BodyID"] = string(msg.BodyID)
		data["Body"] = string(msg.Body)
		data["ProducerGroupID"] = string(msg.ProducerGroupID)
		data["LockConsumerID"] = string(msg.LockConsumerID)
		data["BucketID"] = string(msg.BucketID)
		data["Version"] = string(msg.Version)

		err = c.client.HMSet(messageKey, data).Err()
		// TODO return derrors here
		if err != nil {
			fmt.Println(err)
		}
	}

	return nil
}

func (c *Client) Select(ctx context.Context, timelineID []byte, buckets []uint16, limit int, maxTimestamp uint64) ([]timeline.Message, bool, error) {
	var results []timeline.Message

	// TODO implement limit
	// TODO use maxTimestamp

	// TODO timelineID is empty
	timelineID = []byte("default_timeline")

	for _, bucketId := range buckets {
		timelineKey := c.createTimelineKey("cluster_name", string(timelineID), string(bucketId))

		msgIds, err := c.client.ZRangeByScore(timelineKey, &redis.ZRangeBy{
			Min:    "-inf",
			Max:    "+inf",
			Offset: 0,
			Count:  int64(limit),
		}).Result()

		// TODO return derrors here
		if err != nil {
			fmt.Println(err)
		}

		for _, msgId := range msgIds {
			messageKey := c.createMessageKey("cluster_name:", string(timelineID), string(bucketId), string(msgId))
			rawMessage, err := c.client.HMGet(
				messageKey,
				"ID", "TimestampMS", "BodyID", "Body", "ProducerGroupID", "LockConsumerID", "BucketID", "Version").Result()

			// TODO return derrors here
			if err != nil {
				fmt.Println(err)
			}

			_ = rawMessage

			var message timeline.Message
			// TODO convert data here
			message.ID = []byte("have to convert messageId")
			//message.BodyID = rawMessage[1].([]byte)
			//message.Body = rawMessage[2].([]byte)
			//message.ProducerGroupID = rawMessage[3].([]byte)
			//message.LockConsumerID = rawMessage[4].([]byte)
			//message.BucketID = rawMessage[5].(uint16)
			//message.Version = rawMessage[6].(uint16)

			results = append(results, message)
			limit--
		}
	}

	return results, false, nil
}

func (c *Client) selectFromBucket(ctx context.Context, bucket uint16, limit int) ([]timeline.Message, bool, error) {
	var result []timeline.Message

	return result, false, nil
}

func (c *Client) Delete(ctx context.Context, timelineID []byte, msgs []timeline.Message) []derrors.MessageIDTuple {
	return nil
}

//------------------------------- We do not need these for now
func (c *Client) Update(ctx context.Context, timelineID []byte, messageTimestamps []storage.MsgTime) []derrors.MessageIDTuple {
	return nil
}

func (c *Client) UpdateLeases(ctx context.Context, timelineID []byte, msgs []timeline.Message) []derrors.MessageIDTuple {
	return nil
}

//LOOKUP message by messageID (owner control, lease operations)
func (c *Client) Lookup(ctx context.Context, timelineID []byte, messageIDs [][]byte) ([]timeline.Message, []derrors.MessageIDTuple) {
	return nil, nil
}

//COUNT messages BY RANGE (spike detection/consumer scaling and metrics)
func (c *Client) CountByRange(ctx context.Context, timelineID []byte, a, b uint64) uint64 { return 0 }

//COUNT messages BY RANGE and LockConsumerID is empty (count processing status)
func (c *Client) CountByRangeProcessing(ctx context.Context, timelineID []byte, a, b uint64) uint64 {
	return 0
}

//COUNT messages BY RANGE and LockConsumerID is not empty (count waiting status)
func (c *Client) CountByRangeWaiting(ctx context.Context, timelineID []byte, a, b uint64) uint64 {
	return 0
}

//SELECT messages by LockConsumerID (when consumer restarts)
func (c *Client) SelectByConsumer(ctx context.Context, timelineID []byte, consumerID []byte) []timeline.Message {
	return nil
}

//SELECT messages by ProducerOwnerID (ownership control)
func (c *Client) SelectByProducer(ctx context.Context, timelineID []byte, producerID []byte) []timeline.Message {
	return nil
}
