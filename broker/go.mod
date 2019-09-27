module github.com/bgadrian/dejaq-broker/broker

require (
	github.com/bgadrian/dejaq-broker/common v0.0.0
	github.com/bgadrian/dejaq-broker/grpc v0.0.0
	github.com/coreos/etcd v3.3.15+incompatible // indirect
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/gocql/gocql v0.0.0-20190915153252-16cf9ea1b3e2
	github.com/gogo/protobuf v1.3.0 // indirect
	github.com/google/flatbuffers v1.11.0
	github.com/google/uuid v1.1.1 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20190826022208-cac0b30c2563
	github.com/sirupsen/logrus v1.4.2
	go.etcd.io/etcd v3.3.15+incompatible
	go.uber.org/atomic v1.4.0 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.10.0 // indirect
	google.golang.org/grpc v1.23.1
)

replace github.com/bgadrian/dejaq-broker/common => ../common/

replace github.com/bgadrian/dejaq-broker/grpc => ../grpc/

go 1.13
