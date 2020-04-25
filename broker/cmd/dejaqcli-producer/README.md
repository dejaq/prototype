# DejaQ Test Sync Consumer

To start a sync producer for testing purposes use this binary/image.

# Install

## Docker (recommended)

```bash
docker run -it --rm dejaq/testsyncproducer:latest
```

## binary
Not yet available 

## build from source

```bash
git clone git@github.com:dejaq/prototype.git
cd prototype/broker/
go install github.com/dejaq/prototype/broker/cmd/dejaqcli-producer

#from anywhere 
TIMEOUT=3s dejaqcli-producer

```

# Parameters

All the variables are served using env variables.

```go

type Config struct {
	OverseerSeed  string `env:"OVERSEER" env-default:"localhost:9000"`
	Topic         string `env:"TOPIC"`
	TopicBuckets  int    `env:"TOPIC_BUCKETS" env-default:"100"`
	ProducerGroup string `env:"NAME"`

	// after this duration the process wil close
	TimeoutDuration string `env:"TIMEOUT" env-default:"3s"`
	// the process will close after it sends this amount of messages
	SingleBurstEventsCount int `env:"SINGLE_BURST_EVENTS"`

	// as alternative to SINGLE_BURST_EVENTS, set these to keep writing messages indefinately
	ConstantBurstsTickDuration    string `env:"CONSTANT_TICK_DURATION"`
	ConstantBurstsTickEventsCount int    `env:"CONSTANT_TICK_COUNT"`

	// the number of events to be sent in a single GRPC call
	BatchSize int `env:"BATCH_SIZE" env-default:"3000"`
	// the size of the messages
	BodySizeBytes int `env:"BODY_SIZE" env-default:"12000"`
	// The event timestamp will be determined with a Time.Now() + Rand(-MinDelta,MaxDelta)
	//set MaxDelta = 0 to have all the events in the past
	//set MinDelta > 0 to have all of them in the future
	EventTimelineMinDelta string `env:"EVENT_TIME_MIN_DELTA" env-default:"3s"`
	EventTimelineMaxDelta string `env:"EVENT_TIME_MAX_DELTA" env-default:"0s"`

	// messages will have a deterministic ID for debuging purposes
	DeterministicEventID bool `env:"DETERMINISTIC_ID"`
}
```