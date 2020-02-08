#!/usr/bin/env bash

echo "Starting a single node cockroach and create the DB and user"
cockroach quit --insecure
cockroach start-single-node --insecure --background --listen-addr=localhost
cockroach sql --insecure --execute "CREATE USER IF NOT EXISTS duser;"
cockroach sql --insecure --execute "CREATE DATABASE IF NOT EXISTS dejaq;"
cockroach sql --insecure --execute "GRANT ALL ON DATABASE dejaq TO duser;"
