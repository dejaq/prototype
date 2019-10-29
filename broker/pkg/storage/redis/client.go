package redis

import (
	"fmt"
	"math/rand"
	"strconv"

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

func (c *Client) createTimelineKey(clusterName string, timelineId []byte, bucketId uint16) string {
	idBucket := strconv.Itoa(int(bucketId))
	return clusterName + "::" + string(timelineId) + "::" + idBucket + "::timeline"
}

func (c *Client) createMessageKey(clusterName string, timelineId []byte, bucketId uint16, messageId []byte) string {
	idBucket := strconv.Itoa(int(bucketId))
	return clusterName + "::" + string(timelineId) + "::" + idBucket + "::" + string(messageId)
}

func (c *Client) Insert(ctx context.Context, timelineID []byte, msgs []timeline.Message) []derrors.MessageIDTuple {
	// TODO use unsafe for conversions
	// keep messageId on sorted set, app use range queries
	for _, msg := range msgs {

		// TODO need a real timestamp instead of fake one
		msg.TimestampMS = uint64(rand.Intn(1000))

		// sorted set could keep 4 billions of records
		timelineKey := c.createTimelineKey("cluster_name", timelineID, msg.BucketID)
		err := c.client.ZAdd(timelineKey, &redis.Z{
			Member: msg.ID,
			Score:  float64(msg.TimestampMS),
		}).Err()

		// TODO return derrors here
		if err != nil {
			fmt.Println(err)
		}

		// TODO improve here, find a better solution to translate a type into map
		messageKey := c.createMessageKey("cluster_name:", timelineID, msg.BucketID, msg.ID)
		data := make(map[string]interface{})
		data["ID"] = string(msg.ID)
		data["TimestampMS"] = string(msg.TimestampMS)
		data["BodyID"] = string(msg.BodyID)
		data["Body"] = string(msg.Body)
		data["ProducerGroupID"] = string(msg.ProducerGroupID)
		data["LockConsumerID"] = string(msg.LockConsumerID)
		data["BucketID"] = []byte(strconv.Itoa(int(msg.BucketID)))
		data["Version"] = []byte(strconv.Itoa(int(msg.Version)))

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
		timelineKey := c.createTimelineKey("cluster_name", timelineID, bucketId)

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
			messageKey := c.createMessageKey("cluster_name:", timelineID, bucketId, []byte(msgId))
			rawMessage, err := c.client.HMGet(
				messageKey,
				"ID", "TimestampMS", "BodyID", "Body", "ProducerGroupID", "LockConsumerID", "BucketID", "Version").Result()

			// TODO return derrors here
			if err != nil {
				fmt.Println(err)
			}

			// TODO better conversion with unsafe
			var message timeline.Message
			message.ID = []byte(fmt.Sprintf("%v", rawMessage[0]))
			message.BodyID = []byte(fmt.Sprintf("%v", rawMessage[1]))
			message.Body = []byte(fmt.Sprintf("%v", rawMessage[2]))
			message.ProducerGroupID = []byte(fmt.Sprintf("%v", rawMessage[3]))
			message.LockConsumerID = []byte(fmt.Sprintf("%v", rawMessage[4]))

			// format bucketID and version
			bucketIdUint16, _ := strconv.ParseUint(fmt.Sprintf("%v", rawMessage[6]), 10, 16)
			message.BucketID = uint16(bucketIdUint16)
			version, _ := strconv.ParseUint(fmt.Sprintf("%v", rawMessage[7]), 10, 16)
			message.Version = uint16(version)

			results = append(results, message)
		}
	}

	return results, false, nil
}

func (c *Client) Delete(ctx context.Context, timelineID []byte, msgs []timeline.Message) []derrors.MessageIDTuple {
	// TODO use transaction here MULTI and EXEC

	for _, msg := range msgs {
		// TODO messageId always is 0
		msg.BucketID = 53126

		// deleted from sorted set
		timelineKey := c.createTimelineKey("cluster_name", timelineID, msg.BucketID)
		ok, err := c.client.ZRem(timelineKey, msg.ID).Result()

		// TODO return derrors here
		if err != nil {
			fmt.Println(err)
		}

		// TODO check if ok is 1
		_ := ok

		messageKey := c.createMessageKey("cluster_name:", timelineID, msg.BucketID, msg.ID)
		fmt.Println(messageKey)
		ok, err = c.client.HDel(
			messageKey,
			"ID", "TimestampMS", "BodyID", "Body", "ProducerGroupID", "LockConsumerID", "BucketID", "Version",
		).Result()

		if err != nil {
			fmt.Println(err)
		}

		// TODO check if ok is equal with number of fields that have to be deleted
		_ := ok
	}

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
