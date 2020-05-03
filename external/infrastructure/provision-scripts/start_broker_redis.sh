#!/usr/bin/env bash


cd "$(dirname "$0")"
REDIS_HOSTNAME=$(<redis.privateip)
METRICS_PORT=9100 STORAGE_TYPE=redis STORAGE_HOST=${REDIS_HOSTNAME}:6379 ./dejaqcli-broker