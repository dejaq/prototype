# Base command (deprecated)

A simple tool we are using to jump start a broker and test that all systems are functional.

## Config

A config file is required and all fields are mandatory. 
ENV variables can be used to override the values.

```bash
cp config.example config.yml
```

## Simple test
Start a broker, set of consumers and producers. It will stop after all messages are consumed (so the entire flow is working, including deleting the messages).

```bash
go run main.go
```

## Different processes

To start each component in its own process see dejaq-{broker,producer,consumer} binaries.