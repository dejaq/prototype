module github.com/dejaq/prototype/producer

go 1.13

require (
	github.com/dejaq/prototype/grpc v0.0.0-00010101000000-000000000000
	github.com/google/flatbuffers v1.12.0
	github.com/google/go-cmp v0.3.0 // indirect
	github.com/ilyakaznacheev/cleanenv v1.2.3
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.6.0
	github.com/stretchr/testify v1.3.0 // indirect
	golang.org/x/net v0.0.0-20191002035440-2ec189313ef0 // indirect
	google.golang.org/genproto v0.0.0-20190927181202-20e1ac93f88c // indirect
	google.golang.org/grpc v1.29.1
)

replace github.com/dejaq/prototype/grpc => ../grpc/
