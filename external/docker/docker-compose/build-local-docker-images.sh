#!/usr/bin/env bash

cd ../../
docker build . --file producer.dockerfile --tag dejaq/testsyncproducer:latest
docker build . --file broker.dockerfile --tag dejaq/testbroker:latest
docker build . --file consumer.dockerfile --tag dejaq/testsyncconsumer:latest