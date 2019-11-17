package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/bgadrian/dejaq-broker/broker/domain"
	derrors "github.com/bgadrian/dejaq-broker/common/errors"
	dtime "github.com/bgadrian/dejaq-broker/common/time"
	"github.com/bgadrian/dejaq-broker/common/timeline"
	"github.com/go-redis/redis"
)

type Client struct {
	host   string
	client *redis.Client
	// map[operationType]redisHash
	script map[string]string
}

var errorsType = map[string]string{
	"1": "messageId already exists, you can not set it again",
	"2": "cannot add new entry on timeline sorted set",
	"3": "cannot insert all properties into hashMap",
}

var scripts = map[string]string{
	"insert":      Scripts.insert,
	"getAndLease": Scripts.getAndLease,
	"delete":      Scripts.delete,
}

// NewClient ...
func NewClient(host string) (*Client, error) {
	c := redis.NewClient(&redis.Options{Addr: host})
	_, err := c.Ping().Result()

	if err != nil {
		return nil, err
	}

	client := Client{
		host:   host,
		client: c,
	}

	// load scripts
	if err := loadScripts(&client, scripts); err != nil {
		return nil, err
	}

	return &client, nil
}

func loadScripts(c *Client, s map[string]string) error {
	hashes := make(map[string]string)
	for k, v := range s {
		hash, err := c.client.ScriptLoad(v).Result()
		if err != nil {
			return err
		}
		hashes[k] = hash
	}

	c.script = hashes

	return nil
}

func (c *Client) createTimelineKey(clusterName string, timelineId []byte, bucketId uint16) string {
	idBucket := strconv.Itoa(int(bucketId))
	return "dejaq::" + clusterName + "::" + string(timelineId) + "::timeline::" + idBucket
}

func (c *Client) createMessageKey(clusterName string, timelineId []byte, bucketId uint16, messageId []byte) string {
	idBucket := strconv.Itoa(int(bucketId))
	return "dejaq::" + clusterName + "::" + string(timelineId) + "::" + idBucket + "::" + string(messageId)
}

// Insert ...
func (c *Client) Insert(ctx context.Context, timelineID []byte, messages []timeline.Message) []derrors.MessageIDTuple {
	// TODO use unsafe for conversions
	// TODO create batches and insert

	var insertErrors []derrors.MessageIDTuple

	for _, msg := range messages {
		timelineKey := c.createTimelineKey("cluster_name", timelineID, msg.BucketID)
		messageKey := c.createMessageKey("cluster_name:", timelineID, msg.BucketID, msg.ID)

		data := []string{
			"ID", string(msg.ID),
			"TimestampMS", string(msg.TimestampMS),
			"BodyID", string(msg.BodyID),
			"Body", string(msg.Body),
			"ProducerGroupID", string(msg.ProducerGroupID),
			"LockConsumerID", string(msg.LockConsumerID),
			"BucketID", strconv.Itoa(int(msg.BucketID)),
			"Version", strconv.Itoa(int(msg.Version)),
		}

		keys := []string{timelineKey, messageKey, string(msg.ID), strconv.Itoa(int(msg.TimestampMS))}
		ok, err := c.client.EvalSha(c.script["insert"], keys, data).Result()

		if err != nil {
			var derror derrors.Dejaror
			derror.Message = err.Error()
			insertErrors = append(insertErrors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
		}

		if ok != "0" {
			var derror derrors.Dejaror
			derror.Message = errorsType[ok.(string)]
			insertErrors = append(insertErrors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})

			if ok == "3" {
				// TODO rollback here, could not insert into hashmap, here we can use retry mechanism
			}
		}
	}

	return insertErrors
}

// GetAndLease ...
func (c *Client) GetAndLease(ctx context.Context, timelineID []byte, buckets domain.BucketRange, consumerId string, leaseMs uint64, limit int, maxTimestamp uint64) ([]timeline.PushLeases, bool, error) {
	// TODO use unsafe for a better conversion
	// TODO use transaction select, get message, lease

	var results []timeline.PushLeases
	var processingError error

	// TODO get data here in revers order if Start is less than End
	for bucketID := buckets.Min(); bucketID <= buckets.Max(); bucketID++ {
		timelineKey := c.createTimelineKey("cluster_name", timelineID, bucketID)

		messagesIds, err := c.client.ZRangeByScore(timelineKey, redis.ZRangeBy{
			Min:    "-inf",
			Max:    fmt.Sprintf("%v", maxTimestamp),
			Offset: 0,
			Count:  int64(limit),
		}).Result()

		// TODO maybe not best practice, assign to same var in a loop
		if err != nil {
			processingError = err
			continue
		}

		for _, msgId := range messagesIds {
			limit--

			// there are more messages
			if limit < 0 {
				return results, true, processingError
			}

			messageKey := c.createMessageKey("cluster_name:", timelineID, bucketID, []byte(msgId))

			// set lease on message hashMap
			data := make(map[string]interface{})
			data["LockConsumerID"] = consumerId
			endLeaseTimeMs := uint64(dtime.TimeToMS(time.Now().UTC())) + leaseMs
			data["TimestampMS"] = fmt.Sprintf("%v", endLeaseTimeMs)

			ok, err := c.client.HMSet(messageKey, data).Result()
			if err != nil {
				processingError = err
				continue
			}

			if ok != "OK" {
				processingError = errors.New("message data was not updated on lease")
				continue
			}

			// Confirmation has to be 0 on update (1 on creation)
			_, err = c.client.ZAdd(timelineKey, redis.Z{
				Member: msgId,
				Score:  float64(endLeaseTimeMs),
			}).Result()

			if err != nil {
				processingError = err
				continue
			}

			rawMessage, err := c.client.HMGet(
				messageKey,
				"ID", "TimestampMS", "BodyID", "Body", "ProducerGroupID", "LockConsumerID", "BucketID", "Version").Result()

			if err != nil {
				processingError = err
				continue
			}

			timelineMessage, err := convertMessageToTimelineMsg(rawMessage, bucketID)
			if err != nil {
				processingError = err
				continue
			}

			timelineMessage.TimestampMS -= leaseMs

			results = append(results, timeline.PushLeases{
				ExpirationTimestampMS: endLeaseTimeMs,
				ConsumerID:            []byte(consumerId),
				Message:               timeline.NewLeaseMessage(timelineMessage),
			})
		}
	}

	return results, false, processingError
}

func convertMessageToTimelineMsg(rawMessage []interface{}, bucketID uint16) (timeline.Message, error) {
	// TODO implement errors on strconv
	var message timeline.Message
	message.ID = []byte(fmt.Sprintf("%v", rawMessage[0]))

	timestamp, _ := strconv.ParseUint(fmt.Sprintf("%v", rawMessage[1]), 10, 64)
	message.TimestampMS = uint64(timestamp)

	message.BodyID = []byte(fmt.Sprintf("%v", rawMessage[2]))
	message.Body = []byte(fmt.Sprintf("%v", rawMessage[3]))
	message.ProducerGroupID = []byte(fmt.Sprintf("%v", rawMessage[4]))
	message.LockConsumerID = []byte(fmt.Sprintf("%v", rawMessage[5]))

	//bucketIdUint16, _ := strconv.ParseUint(fmt.Sprintf("%v", rawMessage[6]), 10, 16)
	message.BucketID = uint16(bucketID)
	version, _ := strconv.ParseUint(fmt.Sprintf("%v", rawMessage[7]), 10, 16)
	message.Version = uint16(version)

	return message, nil
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

		if ok != 1 {
			var derror derrors.Dejaror
			derror.Message = "MessageId was not deleted from redis"
			errors = append(errors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
		}

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

		if ok != 8 {
			var derror derrors.Dejaror
			derror.Message = "Message data was not deleted from redis"
			errors = append(errors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
		}
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
