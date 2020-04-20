module github.com/dejaq/prototype/common

go 1.13

require (
	github.com/dejaq/prototype/grpc v0.0.0-00010101000000-000000000000
	github.com/mihaioprea/go-metrics-prometheus v0.0.0-20190927062427-83c6e4a84584
	github.com/prometheus/client_golang v1.1.0
	github.com/rcrowley/go-metrics v0.0.0-20190826022208-cac0b30c2563
)

replace github.com/dejaq/prototype/grpc => ../grpc
