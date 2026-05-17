package grpcserver

import (
	kvsv1 "github.com/rydzu/ainfra/kvs/api/proto/kvs/v1"
	grpcapi "github.com/rydzu/ainfra/kvs/internal/service/grpcapi"
	"github.com/rydzu/ainfra/kvs/pkg/kvsapi"
	"google.golang.org/grpc"
)

type Config struct {
	ChunkSize int
}

func New(store kvsapi.Store, cfg Config) kvsv1.KVStoreServer {
	return grpcapi.NewServer(store, grpcapi.Config{ChunkSize: cfg.ChunkSize})
}

func Register(server grpc.ServiceRegistrar, store kvsapi.Store, cfg Config) {
	if server == nil || store == nil {
		return
	}
	kvsv1.RegisterKVStoreServer(server, New(store, cfg))
}
