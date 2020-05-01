# DejaQ test framework docker compose

Use this to start a local setup of a multi-node cluter of dejaq and its dependencies.

The images will be downloaded from dockerhub automatically OR you can build them locally:
```bash
./build-local-docker-images.sh
```

Run simple cluster: 1 broker, 1 consumer, 1 producer with INMEMORY storage

```bash
docker-compose up
```

Run simple cluster + addons
```bash
docker-compose -f docker-compose.yml -f file2 -f file3 up 
```
Where file2, file3 ..can be any of the following:

plus-2c-15kmsgs.yml: adds 2 more consumer and increase the producers rate
with-prometheus-volatile.yml: adds a container with prometheus that only targets the broker!

Storage: choose only 1

with-crdb-volatile.yml: adds a cockroachDB cluster of 3 nodes that will be used by the broker
with-redis-volatile.yml: adds a redis instance that will be used by the broker as storage
