# kvs

> Part of the **Strata** platform.

Raft-replicated, versioned key-value store used as the backing store for Guardian and MonoFS internal state. Each key keeps a version chain with optimistic concurrency, batch revisions, prefix listing, and watch subscriptions. Hot blobs are served locally and older versions are archived on disk or offloaded to the MonoFS fetcher tier.

See [docs/guardian-versioned-kv-architecture.md](docs/guardian-versioned-kv-architecture.md) for the architecture, data model, and implementation status.

## Build and test

```bash
go build ./...
go test ./...
```

The store is consumed as a library (`pkg/raftstore`, `internal/service/grpcapi`) and as an embedded local store (`internal/store/local`).

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
