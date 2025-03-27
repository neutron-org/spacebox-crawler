package app

import (
	"context"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"

	"github.com/bro-n-bro/spacebox-crawler/v2/adapter/storage"
	"github.com/bro-n-bro/spacebox-crawler/v2/adapter/storage/model"
	grpcClient "github.com/bro-n-bro/spacebox-crawler/v2/client/grpc"
	rpcClient "github.com/bro-n-bro/spacebox-crawler/v2/client/rpc"
	"github.com/bro-n-bro/spacebox-crawler/v2/delivery/broker"
	"github.com/bro-n-bro/spacebox-crawler/v2/delivery/server"
	"github.com/bro-n-bro/spacebox-crawler/v2/internal/rep"
	"github.com/bro-n-bro/spacebox-crawler/v2/modules"
	rawModule "github.com/bro-n-bro/spacebox-crawler/v2/modules/raw"
	healthchecker "github.com/bro-n-bro/spacebox-crawler/v2/pkg/health_checker"
	ts "github.com/bro-n-bro/spacebox-crawler/v2/pkg/mapper/to_storage"
	"github.com/bro-n-bro/spacebox-crawler/v2/pkg/worker"
	neutronapp "github.com/neutron-org/neutron/v5/app"
)

const (
	FmtCannotStart = "cannot start %q"
)

var (
	ErrStartTimeout    = errors.New("start timeout")
	ErrShutdownTimeout = errors.New("shutdown timeout")
)

type (
	App struct {
		log     *zerolog.Logger
		version string
		cmps    []cmp
		cfg     Config
	}
	cmp struct {
		Service rep.Lifecycle
		Name    string
	}
)

func New(cfg Config, version string, l zerolog.Logger) *App {
	l = l.With().Str("version", version).Str("cmp", "app").Logger()

	return &App{
		log:     &l,
		cfg:     cfg,
		version: version,
	}
}

func (a *App) Start(ctx context.Context) error {
	a.log.Info().Msg("starting app")

	if a.cfg.MetricsEnabled {
		promauto.NewGauge(prometheus.GaugeOpts{
			Namespace:   "spacebox_crawler",
			Name:        "version",
			Help:        "Crawler version",
			ConstLabels: prometheus.Labels{"version": a.version},
		}).Inc()
	}

	var (
		cod     = neutronapp.MakeEncodingConfig().Marshaler
		sto     = storage.New(a.cfg.StorageConfig, *a.log)
		rpcCli  = rpcClient.New(a.cfg.RPCConfig)
		grpcCli = grpcClient.New(a.cfg.GRPCConfig, *a.log, sto)

		brk = broker.New(a.cfg.BrokerConfig, *a.log)

		raw  = rawModule.New(brk, rpcCli, grpcCli)
		mods = modules.NewModuleLoader().WithLogger(a.log).WithModules(raw)

		tos = ts.NewToStorage()
		wrk = worker.New(a.cfg.WorkerConfig, *a.log, brk, rpcCli, grpcCli, mods.Build(), sto, cod, *tos)
		srv = server.New(a.cfg.Server, sto, *a.log)
		hc  = healthchecker.New(*a.log, checkLastBlockDiff(a.cfg.HealthcheckConfig.MaxBlockLag, sto), a.cfg.HealthcheckConfig) //nolint:lll
	)

	MakeSDKConfig(a.cfg, sdk.GetConfig())

	a.cmps = append(a.cmps,
		cmp{sto, "storage"},
		cmp{grpcCli, "grpc_client"},
		cmp{rpcCli, "rpc_client"},
		cmp{brk, "broker"},
		cmp{wrk, "worker"},
		cmp{srv, "server"},
		cmp{hc, "health_checker"},
	)

	okCh, errCh := make(chan struct{}), make(chan error)

	go func() {
		for _, c := range a.cmps {
			a.log.Info().Str("service", c.Name).Msg("starting")

			if err := c.Service.Start(ctx); err != nil {
				a.log.Error().Err(err).Msgf(FmtCannotStart, c.Name)
				errCh <- errors.Wrapf(err, FmtCannotStart, c.Name)

				return
			}

			a.log.Info().Str("service", c.Name).Msg("started")
		}

		okCh <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return ErrStartTimeout
	case err := <-errCh:
		return err
	case <-okCh:
		a.log.Info().Msg("application started")
		return nil
	}
}

func (a *App) Stop(ctx context.Context) error {
	a.log.Info().Msg("shutting down service...")

	okCh, errCh := make(chan struct{}), make(chan error)

	go func() {
		for i := len(a.cmps) - 1; i > 0; i-- {
			c := a.cmps[i]

			a.log.Info().Str("service", c.Name).Msg("stopping")

			if err := c.Service.Stop(ctx); err != nil {
				a.log.Error().Str("service", c.Name).Err(err).Msg("cannot stop")
				errCh <- err

				return
			}
		}

		okCh <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return ErrShutdownTimeout
	case err := <-errCh:
		return err
	case <-okCh:
		a.log.Info().Msg("application stopped")
		return nil
	}
}

func (a *App) GetStartTimeout() time.Duration { return a.cfg.StartTimeout }
func (a *App) GetStopTimeout() time.Duration  { return a.cfg.StopTimeout }

// MakeSDKConfig represents a handy implementation of SdkConfigSetup that simply setups the prefix
// inside the configuration
func MakeSDKConfig(cfg Config, sdkConfig *sdk.Config) {
	prefix := cfg.ChainPrefix

	sdkConfig.SetBech32PrefixForAccount(
		prefix,
		prefix+sdk.PrefixPublic,
	)

	sdkConfig.SetBech32PrefixForValidator(
		prefix+sdk.PrefixValidator+sdk.PrefixOperator,
		prefix+sdk.PrefixValidator+sdk.PrefixOperator+sdk.PrefixPublic,
	)

	sdkConfig.SetBech32PrefixForConsensusNode(
		prefix+sdk.PrefixValidator+sdk.PrefixConsensus,
		prefix+sdk.PrefixValidator+sdk.PrefixConsensus+sdk.PrefixPublic,
	)
}

// checkLastBlockDiff checks whether the block was created no later than maxDiff.
func checkLastBlockDiff(maxDiff time.Duration, storage interface {
	GetLatestBlock(ctx context.Context) (*model.Block, error)
}) func(context.Context, *zerolog.Logger) bool {

	return func(ctx context.Context, log *zerolog.Logger) bool {
		lastBlock, err := storage.GetLatestBlock(ctx)
		if err != nil {
			log.Error().Err(err).Msg("cannot get latest block")
			return true
		}

		if lastBlock == nil {
			return true
		}

		return time.Since(lastBlock.Created) <= maxDiff
	}
}
