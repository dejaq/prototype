#!/usr/bin/env bash

export GOOS=linux GOARCH=amd64

mkdir bin
go build -o bin/dejaqcli-broker ../../broker/cmd/dejaqcli-broker/main.go
go build -o bin/dejaqcli-producer ../../broker/cmd/dejaqcli-producer/main.go
go build -o bin/dejaqcli-consumer ../../broker/cmd/dejaqcli-consumer/main.go
cp start-all-local/* bin/

zip dejaq-linux bin/*
rm -rf ./bin
