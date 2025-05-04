package slinky_prices

import (
	"context"

	"github.com/rs/zerolog"

	grpcClient "github.com/bro-n-bro/spacebox-crawler/v2/client/grpc"
	rpcClient "github.com/bro-n-bro/spacebox-crawler/v2/client/rpc"
	"github.com/bro-n-bro/spacebox-crawler/v2/modules/utils"
	"github.com/bro-n-bro/spacebox-crawler/v2/types"
)

const (
	ModuleName = "slinky_prices"
)

var (
	_ types.Module       = &Module{}
	_ types.BlockHandler = &Module{}
)

type Config struct {
	StartSlinkyHeight int64
}

type Module struct {
	log        *zerolog.Logger
	rpcClient  *rpcClient.Client
	grpcClient *grpcClient.Client
	broker     broker
	cfg        Config
}

type broker interface {
	PublishRawSlinkyPrices(ctx context.Context, data interface{}) error
}

func New(cfg Config, b broker, rpcCli *rpcClient.Client, grpcCli *grpcClient.Client) *Module {
	return &Module{
		cfg:        cfg,
		log:        utils.NewModuleLogger(ModuleName),
		broker:     b,
		rpcClient:  rpcCli,
		grpcClient: grpcCli,
	}
}

func (m *Module) Name() string { return ModuleName }
