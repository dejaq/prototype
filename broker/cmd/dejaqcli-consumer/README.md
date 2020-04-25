# DejaQ Test Sync Consumer

To start a sync consumer for testing purposes use this binary/image.

# Install

## Docker (recommended)

```bash
docker run -it --rm dejaq/testsyncconsumer:latest
```

## binary
Not yet available 

## build from source

```bash
git clone git@github.com:dejaq/prototype.git
cd prototype/broker/
go install github.com/dejaq/prototype/broker/cmd/dejaqcli-consumer

#from anywhere start the server 
TIMEOUT=3s dejaqcli-consumer

```

# Parameters

All the variables are served using env variables.

```go

type Config struct {
	// the broker address
	OverseerSeed string `env:"OVERSEER" env-default:"localhost:9000"`
	Topic        string `env:"TOPIC"`
	// this consumer instance
	ConsumerID string `env:"CONSUMER_ID"`

	// the max amount of messages to prefetch/stored in buffer
	MaxBufferSize int `env:"MAX_BUFFER_SIZE" env-default:"100"`
	// the time to own the leases
	LeaseDuration string `env:"LEASE_DURATION" env-default:"1m"`
	// consumer will send info to broker at specific duration
	UpdatePreloadStatsTick string `env:"PRELOAD_STATS_TICK" env-default:"1s"`
	// stop after consume n messages, -1 will run continuously
	StopAfterCount int `env:"STOP_AFTER" env-default:"-1"`
	// If false the messages will be served to another consumer after this ones lease expires
	DeleteMessages bool `env:"DELETE_MESSAGES" env-default:"true"`

	// the process will close after this
	TimeoutDuration string `env:"TIMEOUT" env-default:"3s"`
}
```