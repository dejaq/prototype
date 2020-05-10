# DejaQ prototype with BadgerDB

On this branch we are testing a concept with brokers having embeded syncronization and storage systems, using BadgerDB and etcd.

It is the 2nd prototype that is meant to be simpler, test the idea that DejaQ can be a simple single deployment without extra storage/syncronization.

Scope of this prototype:
* only a cluster of 3 nodes for High availability, NOT distributed storage (all nodes have all data)
* any number of producers
* producers write to any/all partitions (broker choose a random partition for each msg)
* only 1 topic with any number of partitions
* 1 consumer per topic partition (so there can be max of [Partitions] of active consumers)
* all writes are ack a quorum of 2 out of 3
* consumers can consume from any node (stale data is excepted, worst case the consumer does not receive the lowest priority msg but it will in the next batch)
* only Priority Queue (not timeline/schedule time based messages)
* is NOT fault tolerant (doesn't handle node data loss or replication)


BadgerDB embeded instances are on each node to persist the messages:
* each partition of a topic has its own BadgerDB instance, replicated on each node
* keys are formed by priority_msgID that forms a 8byte slice/64bit number
* priority is an uint16 determined by the producer
* msgID is a random 6 bytes entry
* being sorted by priority the consumer only fetches the first messages

etcd embeded instance on each node to keep the metadata of the cluster consistent and persistent, like:
* topic partitions count
* consumer to partition assigment




