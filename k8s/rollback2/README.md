# Rollback workflow

Running the rollback tool is very simple:
```
$ go build .
$ ./rollback2 --data-dir $ETCDDATADIR --ttl 1h
```

This will rollback KV pairs from v3 into v2.
If a key was attached to a lease before, it will be created with given TTL (default to 1h).

On success, it will print at the end:
```
Finished.
```

You can do simple check on keys (if any exists):
```
etcdctl ls /
```

Caveats:
- No guarantee on versions.
- No pervious v2 data. You don't need to remove them. It won't be preserved.
- No v3 data left after rollback.
