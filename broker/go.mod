module github.com/bgadrian/dejaq-broker/broker

require (
	github.com/bgadrian/dejaq-broker/common v0.0.0
	github.com/bgadrian/dejaq-broker/grpc v0.0.0
	github.com/gocql/gocql v0.0.0-20190915153252-16cf9ea1b3e2
)

replace github.com/bgadrian/dejaq-broker/common => ../common/

replace github.com/bgadrian/dejaq-broker/grpc => ../grpc/

go 1.13
