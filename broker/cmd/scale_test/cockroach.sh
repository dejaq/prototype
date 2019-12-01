#!/usr/bin/env bash

cockroach quit --insecure
cockroach start --insecure --background
cockroach sql --insecure --execute "CREATE USER IF NOT EXISTS duser;"
cockroach sql --insecure --execute "CREATE DATABASE dejaq;"
cockroach sql --insecure --execute "GRANT ALL ON DATABASE dejaq TO duser;"
