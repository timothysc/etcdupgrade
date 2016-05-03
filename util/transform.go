package util

import (
	"github.com/coreos/etcd/client"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/storage/storagepb"
	"golang.org/x/net/context"
)

var (
	globalLeaseID int64
)

func before(client *clientv3.Client) error {
	lcr, err := client.Lease.Grant(context.Background, 60*60)
	if err != nil {
		return err
	}
	globalLeaseID := lcr.ID
	return nil
}

func transform(n *client.Node) *storagepb.KeyValue {
	const unKnownVersion = 0
	if n.Dir {
		return nil
	}
	kv := &storagepb.KeyValue{
		Key:            []byte(n.Key),
		Value:          []byte(n.Value),
		CreateRevision: int64(n.CreatedIndex),
		ModRevision:    int64(n.ModifiedIndex),
		Version:        unKnownVersion,
	}
	// We don't support per key ttl in v3 and use lease.
	// We also used TTL to GC objects.
	// So we can combine all objects in one lease.
	if n.TTL != 0 {
		kv.Lease = globalLeaseID
	}
	return kv
}

func after() {

}
