module github.com/dejaq/prototype/broker

require (
	github.com/coreos/bbolt v1.3.3 // indirect
	github.com/coreos/etcd v3.3.15+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/dejaq/prototype/client v0.0.0-00010101000000-000000000000
	github.com/dejaq/prototype/common v0.0.0
	github.com/dejaq/prototype/grpc v0.0.0
	github.com/go-redis/redis v6.15.6+incompatible
	github.com/gocql/gocql v0.0.0-20190915153252-16cf9ea1b3e2
	github.com/gogo/protobuf v1.3.0 // indirect
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/google/flatbuffers v1.11.0
	github.com/gorilla/websocket v1.4.1 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.11.2 // indirect
	github.com/ilyakaznacheev/cleanenv v1.2.1
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/lib/pq v1.2.0
	github.com/onsi/ginkgo v1.10.3 // indirect
	github.com/onsi/gomega v1.7.1 // indirect
	github.com/pelletier/go-toml v1.6.0 // indirect
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v1.1.0
	github.com/prometheus/client_model v0.0.0-20190812154241-14fe0d1b01d4 // indirect
	github.com/prometheus/common v0.6.0
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/afero v1.2.2 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/spf13/viper v1.5.0
	go.etcd.io/bbolt v1.3.3 // indirect
	go.etcd.io/etcd v3.3.15+incompatible
	go.uber.org/atomic v1.5.1
	golang.org/x/crypto v0.0.0-20190820162420-60c769a6c586 // indirect
	golang.org/x/net v0.0.0-20190813141303-74dc4d7220e7 // indirect
	golang.org/x/sys v0.0.0-20191120155948-bd437916bb0e // indirect
	golang.org/x/text v0.3.2 // indirect
	golang.org/x/time v0.0.0-20190921001708-c4c64cad1fd0 // indirect
	google.golang.org/grpc v1.24.0
	gopkg.in/yaml.v2 v2.2.7 // indirect
	sigs.k8s.io/yaml v1.1.0 // indirect
)

replace github.com/dejaq/prototype/common => ../common/

replace github.com/dejaq/prototype/grpc => ../grpc/

replace github.com/dejaq/prototype/client => ../client/

go 1.13
