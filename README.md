# kvs

> Part of the **Strata** platform.

Raft-replicated, versioned key-value store. MonoFS embeds it as the backing store for managed namespaces and Guardian's storage paths; Guardian reads that state through MonoFS and deploys KVS bootstrap resources, but does not import the `kvs` Go packages. Each key keeps a version chain with optimistic concurrency, batch revisions, prefix listing, and watch subscriptions. Hot blobs are served locally and older versions are archived on disk or offloaded to the MonoFS fetcher tier.

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

`NewWithAuthAndTLS` adds the shared `STRATA_TLS_*` contract through
`TLSOptions`; the same transport is used for follower-to-leader forwarding.
`KVS_FORWARD_AUTH_TOKEN` authenticates that forwarding hop when it differs from
the client token. `KVS_AUDIT_LOG` enables the append-only JSONL audit log.

## Retention and offload

`StartPurge` runs background retention. Versions beyond `MaxHotVersions` move to
the local archive, are offloaded through a `FetcherOffloader`, or are deleted
once they exceed `MaxArchivedVersions`. Manifests are retained after blob
deletion so version history remains inspectable.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
