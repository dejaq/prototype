package etcd

import (
	"context"
	"github.com/bgadrian/dejaq-broker/broker/pkg/synchronization"
	"go.etcd.io/etcd/clientv3"
)

type Etcd struct {
	client *clientv3.Client
}

func NewEtcd(client *clientv3.Client) *Etcd {
	return &Etcd{
		client: client,
	}
}

func (e *Etcd) UpdateTopic(ctx context.Context, topic synchronization.Topic) error {
	return nil
}
