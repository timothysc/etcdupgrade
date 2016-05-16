package main

import (
	"bufio"
	"encoding/json"
	"os"

	"github.com/coreos/etcd/client"
	"github.com/coreos/etcd/storage/storagepb"
	"github.com/golang/protobuf/proto"
)

func transform(n *client.Node) *storagepb.KeyValue {
	if n.Dir {
		return nil
	}
	kv := &storagepb.KeyValue{
		Key:            []byte(n.Key),
		Value:          []byte(n.Value),
		CreateRevision: int64(n.CreatedIndex),
		ModRevision:    int64(n.ModifiedIndex),
		// We can't get version from etcd2 nodes. Assuming all KVs has version 1.
		Version: 1,
	}
	return kv
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	decoder := json.NewDecoder(reader)
	writer := bufio.NewWriter(os.Stdout)
	for {
		if !decoder.More() {
			break
		}
		node := &client.Node{}
		if err := decoder.Decode(v); err != nil {
			panic(err)
		}
		kv := transform(node)
		if kv == nil {
			continue
		}
		data, err := proto.Marshal(kv)
		if err != nil {
			panic(err)
		}
		writer.Write(data)
	}
}
