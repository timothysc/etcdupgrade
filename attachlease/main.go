package main

import (
	"bufio"
	"flag"
	"os"
	"strings"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/clientv3"
)

var keysFile string
var etcdHosts string

func init() {
	flag.StringVar(&keysFile, "file", "", "The file of ttls keys. Each line of format: \"TTL Keys:...\"")
	flag.StringVar(&etcdHosts, "etcd-addr", "localhost:2379", "")
	flag.Parse()
}

func main() {
	if keysFile == "" {
		panic("unexpected")
	}
	cfg := clientv3.Config{Endpoints: strings.Split(etcdHosts, ",")}
	etcdcli, err := clientv3.New(cfg)
	if err != nil {
		panic(err)
	}

	f, err := os.Open(keysFile)
	if err != nil {
		panic(err)
	}

	lresp, err := etcdcli.Lease.Grant(context.TODO(), 5)
	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "TTL key:") {
			continue
		}
		key := strings.TrimSpace(line[len("TTL key:"):])
		gresp, err := etcdcli.KV.Get(context.TODO(), key)
		if err != nil {
			panic(err)
		}
		etcdcli.KV.Put(context.TODO(), key, string(gresp.Kvs[0].Value), clientv3.WithLease(lresp.ID))
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}

}
