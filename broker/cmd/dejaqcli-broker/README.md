# DejaQ Test broker

To start a broker for testing purposes use this binary/image.

# Install

## Docker (recommended)

```bash
docker run -it --rm dejaq/testbroker:latest
```

## binary
Not yet available 

## build from source

```bash
git clone git@github.com:dejaq/prototype.git
cd prototype/broker/
go install github.com/dejaq/prototype/broker/cmd/dejaqcli-broker

#from anywhere start the server 
TIMEOUT=3s dejaqcli-broker

```

# Parameters

All the variables are served using env variables.

```go

type Config struct {
	//default listens on all interfaces, this is standard for containers
	BindingAddress string `env:"BINDING_ADDRESS" env-default:"0.0.0.0:9000"`
	// memory, redis or cockroach
	StorageType string `env:"STORAGE_TYPE" env-default:"memory"`
	// used to connect to redis or cockroach
	StorageHost string `env:"STORAGE_HOST"`

	//max no of messages for insert and delete
	CockroachMaxBatchSize int `env:"STORAGE_CRDB_MAXBATCH_COUNT" env-default:"10"`

	// max amount of concurrent GRPC streams
	MaxConnectionsLimit int `env:"CONNECTIONS_LIMIT" env-default:"1000"`
	// timeout for a GRPC idle connection
	ConnectionTimeoutDuration string `env:"CONNECTION_TIMEOUT" env-default:"120s"`

	// after this timeout the process will close automatically
	TimeoutDuration string `env:"TIMEOUT"`
	//max amount of leases to be fetched from the DB and sent to a consumer
	LoaderMaxBatchSize int `env:"LOADER_MAX_BATCH_SIZE" env-default:"100"`
}
```