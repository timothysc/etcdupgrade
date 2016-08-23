package main

import (
	"flag"
	"fmt"
	"path"
	"time"

	"github.com/coreos/etcd/mvcc/backend"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

var (
	migrateDatadir string
)

func init() {
	flag.StringVar(&migrateDatadir, "data-dir", "", "Path to the data directory")
	flag.Parse()
}

func main() {
	dbpath := path.Join(migrateDatadir, "member", "snap", "db")

	be := backend.New(dbpath, time.Second, 10000)
	tx := be.BatchTx()

	tx.Lock()
	tx.UnsafeForEach([]byte("key"), func(k, v []byte) error {
		kv := &mvccpb.KeyValue{}
		kv.Unmarshal(v)
		fmt.Printf("%s %d %d %d\n", string(kv.Key), kv.CreateRevision, kv.ModRevision, kv.Lease)
		return nil
	})
	tx.Unlock()

}
