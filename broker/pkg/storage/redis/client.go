package redis

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/bgadrian/dejaq-broker/broker/domain"

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
	idBucket := fmt.Sprintf("%v", bucketId)
	return "dejaq::" + clusterName + "::" + string(timelineId) + "::timeline::" + idBucket
}

func (c *Client) createMessageKey(clusterName string, timelineId []byte, bucketId uint16, messageId []byte) string {
	idBucket := fmt.Sprintf("%v", bucketId)
	return "dejaq::" + clusterName + "::" + string(timelineId) + "::" + idBucket + "::" + string(messageId)
}

func (c *Client) createLeaseKey(clusterName string, timelineId []byte) string {
	return "dejaq::" + clusterName + "::leased" + string(timelineId) + "::"
}

// Insert ...
func (c *Client) Insert(ctx context.Context, timelineID []byte, messages []timeline.Message) []derrors.MessageIDTuple {
	// TODO use unsafe for conversions

	var errors []derrors.MessageIDTuple

	for _, msg := range messages {
		timelineKey := c.createTimelineKey("cluster_name", timelineID, msg.BucketID)
		ok, err := c.client.ZAdd(timelineKey, redis.Z{
			Member: msg.ID,
			Score:  float64(msg.TimestampMS),
		}).Result()

		if err != nil {
			var derror derrors.Dejaror
			derror.Message = err.Error()
			errors = append(errors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
		}

		if ok != 1 {
			var derror derrors.Dejaror
			derror.Message = "MessageId was not written on redis"
			errors = append(errors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
		}

		// TODO improve here, find a better solution to translate a type into map
		messageKey := c.createMessageKey("cluster_name:", timelineID, msg.BucketID, msg.ID)
		data := make(map[string]interface{})
		data["ID"] = msg.GetID()
		data["TimestampMS"] = string(msg.TimestampMS)
		data["BodyID"] = string(msg.BodyID)
		data["Body"] = string(msg.Body)
		data["ProducerGroupID"] = msg.GetProducerGroupID()
		data["LockConsumerID"] = msg.GetLockConsumerID()
		data["BucketID"] = fmt.Sprintf("%v", msg.BucketID)
		data["Version"] = fmt.Sprintf("%v", msg.Version)

		isOk, err := c.client.HMSet(messageKey, data).Result()

		if err != nil {
			var derror derrors.Dejaror
			derror.Message = err.Error()
			errors = append(errors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
		}

		if isOk != "OK" {
			var derror derrors.Dejaror
			derror.Message = "Message data was not written on redis"
			errors = append(errors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
		}
	}

	return errors
}

// Select ...
func (c *Client) Select(ctx context.Context, timelineID []byte, buckets []domain.BucketRange, limit int, maxTimestamp uint64) ([]timeline.Message, bool, error) {
	// TODO implement limit
	// TODO use maxTimestamp
	// TODO use unsafe for a better conversion
	// TODO use transaction select, get message, lease

	var results []timeline.Message
	var processingError error

	for _, bucketRange := range buckets {
		for i := bucketRange.MinInclusive; i < bucketRange.MaxExclusive; i++ {
			timelineKey := c.createTimelineKey("cluster_name", timelineID, i)

			messagesIds, err := c.client.ZRangeByScore(timelineKey, redis.ZRangeBy{
				Min:    "-inf",
				Max:    "+inf",
				Offset: 0,
				Count:  int64(limit),
			}).Result()

			// TODO maybe not best practice, assign to same var in a loop
			if err != nil {
				processingError = err
				continue
			}

			for _, msgId := range messagesIds {
				leasedKey := c.createLeaseKey("cluster_name", timelineID)

				// continue if message is already leased
				isMember, err := c.client.SIsMember(leasedKey, msgId).Result()
				if err != nil {
					processingError = err
					continue
				}

				if isMember {
					continue
				}

				messageKey := c.createMessageKey("cluster_name:", timelineID, i, []byte(msgId))
				rawMessage, err := c.client.HMGet(
					messageKey,
					"ID", "TimestampMS", "BodyID", "Body", "ProducerGroupID", "LockConsumerID", "BucketID", "Version").Result()

				if err != nil {
					processingError = err
					continue
				}

				timelineMessage, err := convertMessageToTimelineMsg(rawMessage)
				if err != nil {
					processingError = err
					continue
				}

				// add lease to message
				ok, err := c.client.SAdd(leasedKey, msgId).Result()
				if err != nil {
					processingError = err
					continue
				}

				if ok != 1 {
					processingError = errors.New("could not set lease on message")
					continue
				}

				results = append(results, timelineMessage)
			}
		}
	}

	return results, false, processingError
}

func convertMessageToTimelineMsg(rawMessage []interface{}) (timeline.Message, error) {
	// TODO implement errors on strconv
	var message timeline.Message
	message.ID = []byte(fmt.Sprintf("%v", rawMessage[0]))

	timestamp, _ := strconv.ParseUint(fmt.Sprintf("%v", rawMessage[1]), 10, 64)
	message.TimestampMS = uint64(timestamp)

	message.BodyID = []byte(fmt.Sprintf("%v", rawMessage[2]))
	message.Body = []byte(fmt.Sprintf("%v", rawMessage[3]))
	message.ProducerGroupID = []byte(fmt.Sprintf("%v", rawMessage[4]))
	message.LockConsumerID = []byte(fmt.Sprintf("%v", rawMessage[5]))

	bucketIdUint16, _ := strconv.ParseUint(fmt.Sprintf("%v", rawMessage[6]), 10, 16)
	message.BucketID = uint16(bucketIdUint16)
	version, _ := strconv.ParseUint(fmt.Sprintf("%v", rawMessage[7]), 10, 16)
	message.Version = uint16(version)

	return message, nil
}

// Update - not used at this time
func (c *Client) Update(ctx context.Context, timelineID []byte, messageTimestamps []storage.MsgTime) []derrors.MessageIDTuple {
	return nil
}

// UpdateLeases - not used at this time
func (c *Client) UpdateLeases(ctx context.Context, timelineID []byte, msgs []timeline.Message) []derrors.MessageIDTuple {
	return nil
}

// Lookup - not used at this time
func (c *Client) Lookup(ctx context.Context, timelineID []byte, messageIDs [][]byte) ([]timeline.Message, []derrors.MessageIDTuple) {
	return nil, nil
}

// Delete ...
func (c *Client) Delete(ctx context.Context, timelineID []byte, messages []timeline.Message) []derrors.MessageIDTuple {
	// TODO use transaction here MULTI and EXEC

	var errors []derrors.MessageIDTuple

	for _, msg := range messages {
		// deleted from sorted set
		timelineKey := c.createTimelineKey("cluster_name", timelineID, msg.BucketID)
		ok, err := c.client.ZRem(timelineKey, msg.GetID()).Result()

		if err != nil {
			var derror derrors.Dejaror
			derror.Message = err.Error()
			errors = append(errors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
		}

		// TODO remove after fix bug of memory leak
		_ = ok

		// TODO uncomment after fix bug of memory leak
		//if ok != 1 {
		//	var derror derrors.Dejaror
		//	derror.Message = "MessageId was not deleted from redis"
		//	errors = append(errors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
		//}

		// delete message data from hashMap
		messageKey := c.createMessageKey("cluster_name:", timelineID, msg.BucketID, msg.ID)
		ok, err = c.client.HDel(
			messageKey,
			"ID", "TimestampMS", "BodyID", "Body", "ProducerGroupID", "LockConsumerID", "BucketID", "Version",
		).Result()

		if err != nil {
			var derror derrors.Dejaror
			derror.Message = err.Error()
			errors = append(errors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
		}

		// TODO uncomment after fix bug of memory leak
		//if ok != 8 {
		//	var derror derrors.Dejaror
		//	derror.Message = "Message data was not deleted from redis"
		//	errors = append(errors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
		//}
	}

	return errors
}

// CountByRange - not used at this time
func (c *Client) CountByRange(ctx context.Context, timelineID []byte, a, b uint64) uint64 {
	return 0
}

// CountByRangeProcessing - not used at this time
func (c *Client) CountByRangeProcessing(ctx context.Context, timelineID []byte, a, b uint64) uint64 {
	return 0
}

// CountByRangeWaiting - not used at this time
func (c *Client) CountByRangeWaiting(ctx context.Context, timelineID []byte, a, b uint64) uint64 {
	return 0
}

// SelectByConsumer - not used at this time
func (c *Client) SelectByConsumer(ctx context.Context, timelineID []byte, consumerID []byte) []timeline.Message {
	return nil
}

// SelectByProducer - not used at this time
func (c *Client) SelectByProducer(ctx context.Context, timelineID []byte, producerID []byte) []timeline.Message {
	return nil
}
