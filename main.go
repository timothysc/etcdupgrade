package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"

	"github.com/coreos/etcd/client"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/protobuf/proto"
)

func transform(n *client.Node) *mvccpb.KeyValue {
	if n.Dir {
		return nil
	}
	fmt.Fprintf(os.Stderr, "Key: %s\n", n.Key)
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

	fmt.Fprintf(os.Stderr, "Starting...\n")
	for {
		if !decoder.More() {
			fmt.Fprintf(os.Stderr, "No More...\n")
			break
		}
		node := &client.Node{}
		if err := decoder.Decode(node); err != nil {
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
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(len(data)))
		if _, err := writer.Write(buf); err != nil {
			panic(err)
		}
		if _, err := writer.Write(data); err != nil {
			panic(err)
		}
	}
	fmt.Fprintf(os.Stderr, "Exit...\n")
}
