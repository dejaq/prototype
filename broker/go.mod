module github.com/bgadrian/dejaq-broker/broker

require (
	github.com/bgadrian/dejaq-broker/client v0.0.0-00010101000000-000000000000
	github.com/bgadrian/dejaq-broker/common v0.0.0
	github.com/bgadrian/dejaq-broker/grpc v0.0.0
	github.com/coreos/bbolt v1.3.3 // indirect
	github.com/coreos/etcd v3.3.15+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/gocql/gocql v0.0.0-20190915153252-16cf9ea1b3e2
	github.com/gogo/protobuf v1.3.0 // indirect
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/google/btree v1.0.0 // indirect
	github.com/google/flatbuffers v1.11.0
	github.com/google/uuid v1.1.1 // indirect
	github.com/gorilla/websocket v1.4.1 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.11.2 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/prometheus/client_model v0.0.0-20190812154241-14fe0d1b01d4 // indirect
	github.com/prometheus/common v0.6.0
	github.com/rcrowley/go-metrics v0.0.0-20190826022208-cac0b30c2563
	github.com/sirupsen/logrus v1.4.2
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/bbolt v1.3.3 // indirect
	go.etcd.io/etcd v3.3.15+incompatible
	golang.org/x/time v0.0.0-20190921001708-c4c64cad1fd0 // indirect
	google.golang.org/grpc v1.24.0
	sigs.k8s.io/yaml v1.1.0 // indirect
)

replace github.com/bgadrian/dejaq-broker/common => ../common/

replace github.com/bgadrian/dejaq-broker/grpc => ../grpc/

replace github.com/bgadrian/dejaq-broker/client => ../client/

go 1.13
