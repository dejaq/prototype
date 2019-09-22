module github.com/bgadrian/dejaq-broker/broker

require (
	github.com/bgadrian/dejaq-broker/common v0.0.0
	github.com/bgadrian/dejaq-broker/grpc v0.0.0
	github.com/gocql/gocql v0.0.0-20190915153252-16cf9ea1b3e2
	github.com/google/flatbuffers v1.11.0
	github.com/sirupsen/logrus v1.4.2 // indirect
	google.golang.org/grpc v1.23.1
)

replace github.com/bgadrian/dejaq-broker/common => ../common/

replace github.com/bgadrian/dejaq-broker/grpc => ../grpc/

go 1.13
