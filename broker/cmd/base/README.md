# Base command

A simple tool we are using to jump start a broker and test how fast can we insert and consumer messages.

## Config

A config file is required and all fields are mandatory. 
ENV variables can be used to override the values.

```bash
cp config.example config.yml
```

## Simple test
Start a broker, set of consumers and producers.It will stop after all messages are consumed (so the entire flow is working, including deleting the messages).

```bash
go run main.go
```

## Different processes

To start each component in its own process, for more advanced setups and tests.
The default will read the config from `./config.yml`, where the timeouts, topics and count of messages are provided.

```bash
#start the broker, this will also create the topics, wait for 1hour then close
RUN_TIMEOUT_MS=3600000 START_PRODUCERS=false START_CONSUMERS=false go run main.go

#start the producers, they will stop after inserting messages_per_topic count of message, for all topics
START_BROKER=false START_CONSUMERS=false go run main.go

#start the consumers, they will stop after consuming the required messages from all topics
START_BROKER=false START_PRODUCERS=false go run main.go

```