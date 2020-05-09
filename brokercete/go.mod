module github.com/dejaq/prototype/brokercete

go 1.13

require (
	github.com/dejaq/prototype/grpc v0.0.0-20200503153213-a05f730deb56
	github.com/dgraph-io/badger/v2 v2.0.3
	github.com/google/flatbuffers v1.12.0
	github.com/ilyakaznacheev/cleanenv v1.2.3 // indirect
	github.com/mosuka/cete v0.3.1 // indirect
	github.com/pkg/errors v0.8.1
	github.com/sirupsen/logrus v1.6.0
)

replace github.com/dejaq/prototype/grpc => ../grpc/
