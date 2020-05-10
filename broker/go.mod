module github.com/dejaq/prototype/broker

go 1.13

require (
	github.com/coreos/bbolt v0.0.0-00010101000000-000000000000 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/dejaq/prototype/grpc v0.0.0-20200503153213-a05f730deb56
	github.com/dgraph-io/badger/v2 v2.0.3
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/google/flatbuffers v1.12.0
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.14.5 // indirect
	github.com/ilyakaznacheev/cleanenv v1.2.3
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v1.6.0 // indirect
	github.com/sirupsen/logrus v1.6.0
	github.com/tmc/grpc-websocket-proxy v0.0.0-20200427203606-3cfed13b9966 // indirect
	go.etcd.io/etcd v3.3.20+incompatible // indirect
	go.etcd.io/etcd/v3 v3.3.0-rc.0.0.20200510060526-b95f135e1011
	go.uber.org/zap v1.15.0
	golang.org/x/time v0.0.0-20200416051211-89c76fbcd5d1 // indirect
	google.golang.org/grpc v1.29.1
)

//etcd Go modules are broken see https://github.com/etcd-io/etcd/issues/11829 and
// https://github.com/etcd-io/etcd/issues?q=is%3Aissue+module+declares+its+path+as%3A+go.etcd.io%2Fbbolt
replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.3

replace github.com/dejaq/prototype/grpc => ../grpc/
