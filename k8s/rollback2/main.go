package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/coreos/etcd/etcdserver"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc/backend"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/store"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
)

var (
	migrateDatadir string
	ttl            time.Duration
)

func init() {
	flag.StringVar(&migrateDatadir, "data-dir", "", "Path to the data directory")
	flag.DurationVar(&ttl, "ttl", time.Hour, "TTL of event keys (default 1 hour)")
	flag.Parse()
}

func main() {
	dbpath := path.Join(migrateDatadir, "member", "snap", "db")

	be := backend.New(dbpath, time.Second, 10000)
	tx := be.BatchTx()

	st := store.New("/0", "/1")
	expireTime := time.Now().Add(ttl)

	tx.Lock()
	err := tx.UnsafeForEach([]byte("key"), func(k, v []byte) error {
		tb := isTombstone(k)

		kv := &mvccpb.KeyValue{}
		kv.Unmarshal(v)

		// This is a compact key..
		if !strings.HasPrefix(string(kv.Key), "/") {
			return nil
		}

		// fmt.Printf("%s %d %d %d\n", string(kv.Key), kv.CreateRevision, kv.ModRevision, kv.Lease)
		ttlOpt := store.TTLOptionSet{}
		if kv.Lease != 0 {
			ttlOpt = store.TTLOptionSet{ExpireTime: expireTime}
		}

		if !tb {
			_, err := st.Set(path.Join("1", string(kv.Key)), false, string(kv.Value), ttlOpt)
			if err != nil {
				return err
			}
		} else {
			st.Delete(string(kv.Key), false, false)
		}

		return nil
	})
	if err != nil {
		panic(err)
	}
	tx.Unlock()

	traverse(st, "/")

	snapshotter := snap.New(path.Join(migrateDatadir, "member", "snap"))
	raftSnap, err := snapshotter.Load()
	if err != nil {
		panic(err)
	}

	metadata, hardstate, oldSt := rebuild(migrateDatadir)

	if err := os.RemoveAll(migrateDatadir); err != nil {
		panic(err)
	}
	if err := os.MkdirAll(path.Join(migrateDatadir, "member", "snap"), 0700); err != nil {
		panic(err)
	}
	walDir := path.Join(migrateDatadir, "member", "wal")

	w, err := wal.Create(walDir, metadata)
	if err != nil {
		panic(err)
	}
	err = w.SaveSnapshot(walpb.Snapshot{Index: hardstate.Commit, Term: hardstate.Term})
	if err != nil {
		panic(err)
	}
	w.Close()

	event, err := oldSt.Get("/0", true, false)
	if err != nil {
		panic(err)
	}
	q := []*store.NodeExtern{}
	q = append(q, event.Node)
	for len(q) > 0 {
		n := q[0]
		q = q[1:]
		v := ""
		if !n.Dir {
			v = *n.Value
		}
		if n.Key != "/0" {
			if n.Key == path.Join("/0", "version") {
				v = "2.3.7"
			}
			if _, err := st.Set(n.Key, n.Dir, v, store.TTLOptionSet{}); err != nil {
				panic(err)
			}
		}
		for _, next := range n.Nodes {
			q = append(q, next)
		}
	}

	data, err := st.Save()
	if err != nil {
		panic(err)
	}
	raftSnap.Data = data
	if err := snapshotter.SaveSnap(*raftSnap); err != nil {
		panic(err)
	}
	fmt.Println("Finished.")
}

func printNode(node *store.NodeExtern) {
	// fmt.Println(node.Key[len("/1"):])
	// fmt.Printf("key:%s ttl:%d mod_index:%d\n", node.Key[len("/1"):], node.TTL, node.ModifiedIndex)
}

const (
	revBytesLen            = 8 + 1 + 8
	markedRevBytesLen      = revBytesLen + 1
	markBytePosition       = markedRevBytesLen - 1
	markTombstone     byte = 't'
)

func isTombstone(b []byte) bool {
	return len(b) == markedRevBytesLen && b[markBytePosition] == markTombstone
}

func traverse(st store.Store, dir string) {
	e, err := st.Get(dir, true, false)
	if err != nil {
		panic(err)
	}
	if len(e.Node.Nodes) == 0 {
		st.Delete(dir, true, true)
		return
	}
	for _, node := range e.Node.Nodes {
		if !node.Dir {
			printNode(node)
		} else {
			// fmt.Println(node.Key[len("/1"):])
			traverse(st, node.Key)
		}
	}
}

func rebuild(datadir string) ([]byte, raftpb.HardState, store.Store) {
	waldir := path.Join(datadir, "member", "wal")
	snapdir := path.Join(datadir, "member", "snap")

	ss := snap.New(snapdir)
	snapshot, err := ss.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		panic(err)
	}

	var walsnap walpb.Snapshot
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}

	w, err := wal.OpenForRead(waldir, walsnap)
	if err != nil {
		panic(err)
	}
	defer w.Close()

	meta, hardstate, ents, err := w.ReadAll()
	if err != nil {
		panic(err)
	}

	st := store.New()
	if snapshot != nil {
		err := st.Recovery(snapshot.Data)
		if err != nil {
			panic(err)
		}
	}

	applier := etcdserver.NewApplierV2(st, nil)
	for _, ent := range ents {
		if ent.Type != raftpb.EntryNormal {
			continue
		}

		var raftReq pb.InternalRaftRequest
		if !pbutil.MaybeUnmarshal(&raftReq, ent.Data) { // backward compatible
			var r pb.Request
			pbutil.MustUnmarshal(&r, ent.Data)
			applyRequest(&r, applier)
		} else {
			if raftReq.V2 != nil {
				req := raftReq.V2
				applyRequest(req, applier)
			}
		}
	}

	return meta, hardstate, st
}

func toTTLOptions(r *pb.Request) store.TTLOptionSet {
	refresh, _ := pbutil.GetBool(r.Refresh)
	ttlOptions := store.TTLOptionSet{Refresh: refresh}
	if r.Expiration != 0 {
		ttlOptions.ExpireTime = time.Unix(0, r.Expiration)
	}
	return ttlOptions
}

func applyRequest(r *pb.Request, applyV2 etcdserver.ApplierV2) {
	toTTLOptions(r)
	switch r.Method {
	case "POST":
		applyV2.Post(r)
	case "PUT":
		applyV2.Put(r)
	case "DELETE":
		applyV2.Delete(r)
	case "QGET":
		applyV2.QGet(r)
	case "SYNC":
		applyV2.Sync(r)
	default:
		panic("unknown command")
	}
}
