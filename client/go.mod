module github.com/bgadrian/dejaq-broker/client

go 1.13

require (
	github.com/bgadrian/dejaq-broker/common v0.0.0-00010101000000-000000000000
	github.com/bgadrian/dejaq-broker/grpc v0.0.0-00010101000000-000000000000
	github.com/google/flatbuffers v1.11.0
	github.com/pkg/errors v0.8.0
	github.com/prometheus/common v0.6.0
	github.com/sirupsen/logrus v1.2.0
	go.uber.org/atomic v1.5.1
	google.golang.org/grpc v1.24.0
)

replace github.com/bgadrian/dejaq-broker/common => ../common

replace github.com/bgadrian/dejaq-broker/grpc => ../grpc
