# kvs

> Part of the **Strata** platform.

Raft-replicated, versioned key-value store used as the backing store for Guardian and MonoFS internal state. Each key keeps a version chain with optimistic concurrency, batch revisions, prefix listing, and watch subscriptions. Hot blobs are served locally and older versions are archived on disk or offloaded to the MonoFS fetcher tier.

See [docs/guardian-versioned-kv-architecture.md](docs/guardian-versioned-kv-architecture.md) for the architecture, data model, and implementation status.

## Build and test

```bash
go build ./...
go test ./...
```

The store is consumed as a library (`pkg/raftstore`, `pkg/grpcserver`, `pkg/kvsapi`) and as an embedded local store (`pkg/localstore`). `internal/store/local` and `internal/service/grpcapi` contain the implementations behind those public packages.

## Authentication scope

`grpcserver.NewWithAuth` (backed by `grpcapi.NewServerWithAuth`) requires a
bearer token from `Config.AuthToken` or `KVS_AUTH_TOKEN` on `KVStore` RPCs
(health checks excepted). The interceptors can be installed on a gRPC server
that also hosts other services: non-`KVStore` methods are left to their own
authentication rather than being rejected or bypassed by the KVS token.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
