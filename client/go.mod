module github.com/dejaq/prototype/client

go 1.13

require (
	github.com/dejaq/prototype/common v0.0.0-00010101000000-000000000000
	github.com/dejaq/prototype/grpc v0.0.0-00010101000000-000000000000
	github.com/google/flatbuffers v1.12.0
	github.com/google/uuid v1.1.1 // indirect
	github.com/pkg/errors v0.8.0
	github.com/prometheus/client_golang v1.1.0
	github.com/prometheus/common v0.6.0
	github.com/sirupsen/logrus v1.2.0
	go.uber.org/atomic v1.5.1
	google.golang.org/grpc v1.29.1
)

replace github.com/dejaq/prototype/common => ../common

replace github.com/dejaq/prototype/grpc => ../grpc
