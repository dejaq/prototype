module github.com/dejaq/prototype/consumer

go 1.13

replace github.com/dejaq/prototype/grpc => ../grpc/

require (
	github.com/dejaq/prototype/grpc v0.0.0-00010101000000-000000000000
	github.com/ilyakaznacheev/cleanenv v1.2.3 // indirect
	github.com/sirupsen/logrus v1.6.0
)
