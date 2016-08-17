package main

import (
	"flag"
	"log"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/client"
	"github.com/coreos/etcd/clientv3"
)

var etcdHosts string

func init() {
	flag.StringVar(&etcdHosts, "etcd-addr", "http://localhost:2379", "")
	flag.Parse()
}
func main() {
	cfg := clientv3.Config{Endpoints: strings.Split(etcdHosts, ",")}
	etcdcli, err := clientv3.New(cfg)
	if err != nil {
		panic(err)
	}
	v2cfg := client.Config{Endpoints: strings.Split(etcdHosts, ",")}
	v2cli, err := client.New(v2cfg)
	if err != nil {
		panic(err)
	}

	v2kv := client.NewKeysAPI(v2cli)

	key := "/"
	for {
		opts := []clientv3.OpOption{clientv3.WithLimit(100), clientv3.WithPrefix()}
		resp, err := etcdcli.KV.Get(context.TODO(), key, opts...)
		if err != nil {
			panic(err)
		}

		// copy to v2
		for _, kv := range resp.Kvs {
			if kv.Lease != 0 {
				v2kv.Set(context.TODO(), string(kv.Key), string(kv.Value), &client.SetOptions{
					TTL: 1 * time.Hour,
				})
				continue
			}
			v2kv.Set(context.TODO(), string(kv.Key), string(kv.Value), nil)
		}

		if !resp.More {
			log.Println("finished")
			return
		}

		// move to next key
		key = string(append(resp.Kvs[len(resp.Kvs)-1].Key, 0))
	}
}
