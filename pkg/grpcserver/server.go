package grpcserver

import (
	kvsv1 "github.com/radryc/kvs/api/proto/kvs/v1"
	grpcapi "github.com/radryc/kvs/internal/service/grpcapi"
	"github.com/radryc/kvs/pkg/kvsapi"
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
