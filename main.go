package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"os"

	"github.com/coreos/etcd/client"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/protobuf/proto"
)

func transform(n *client.Node) *mvccpb.KeyValue {
	if n.Dir {
		return nil
	}
	kv := &mvccpb.KeyValue{
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
		if err := binary.Write(writer, binary.LittleEndian, len(data)); err != nil {
			panic(err)
		}
		writer.Write(data)
	}
}
