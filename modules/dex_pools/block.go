package dex_pools

import (
	"context"
	"fmt"
	"time"

	"github.com/bro-n-bro/spacebox-crawler/v2/types"
	"github.com/cosmos/cosmos-sdk/types/query"
	dex "github.com/neutron-org/neutron/v6/x/dex/types"
	"google.golang.org/grpc/metadata"
)

func (m *Module) HandleBlock(ctx context.Context, block *types.Block) error {
	if err := m.publishDexPoolMetadata(ctx, block.Height, block.Timestamp); err != nil {
		return fmt.Errorf("failed to publish dex pool metadata: %w", err)
	}
	m.log.Debug().Int64("height", block.Height).Msg("Published DEX pool metadata")
	return nil
}

func (m *Module) publishDexPoolMetadata(ctx context.Context, height int64, timestamp time.Time) error {
	// Get previous number of pools known
	previousHeightResp, err := m.grpcClient.DexService.PoolMetadataAll(
		metadata.NewOutgoingContext(ctx, metadata.Pairs("x-cosmos-block-height", fmt.Sprintf("%d", height-1))),
		&dex.QueryAllPoolMetadataRequest{
			Pagination: &query.PageRequest{
				Limit:      1,
				CountTotal: true,
			},
		},
	)
	if err != nil {
		if height > m.cfg.StartDexHeight {
			return fmt.Errorf("failed to get previous height pool metadata count: %w", err)
		} else {
			previousHeightResp = &dex.QueryAllPoolMetadataResponse{
				Pagination: &query.PageResponse{},
			}
		}
	}

	heightResp, err := m.grpcClient.DexService.PoolMetadataAll(
		metadata.NewOutgoingContext(ctx, metadata.Pairs("x-cosmos-block-height", fmt.Sprintf("%d", height))),
		&dex.QueryAllPoolMetadataRequest{
			Pagination: &query.PageRequest{
				Offset:     previousHeightResp.Pagination.Total,
				CountTotal: true,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to get first page of pool metadata: %w", err)
	}

	// get pool metadata
	poolMetadata := heightResp.PoolMetadata

	// get multiple pages of data if needed
	nextKey := heightResp.Pagination.NextKey
	for {
		if nextKey != nil {
			nextPageHeightResp, err := m.grpcClient.DexService.PoolMetadataAll(
				metadata.NewOutgoingContext(ctx, metadata.Pairs("x-cosmos-block-height", fmt.Sprintf("%d", height))),
				&dex.QueryAllPoolMetadataRequest{
					Pagination: &query.PageRequest{
						Offset:     previousHeightResp.Pagination.Total,
						Key:        nextKey,
						CountTotal: true,
					},
				},
			)
			if err != nil {
				return fmt.Errorf("failed to get next page of pool metadata: %w", err)
			}
			// append rows into collection
			poolMetadata = append(poolMetadata, nextPageHeightResp.PoolMetadata...)
			nextKey = nextPageHeightResp.Pagination.NextKey
		} else {
			break
		}
	}

	// publish metadata array (or empty array) with block height
	if poolMetadata == nil {
		poolMetadata = []dex.PoolMetadata{}
	}
	rawDexPoolMetadata := struct {
		Timestamp    time.Time          `json:"timestamp"`
		Height       int64              `json:"height"`
		PoolMetadata []dex.PoolMetadata `json:"pool_metadata"`
	}{
		Timestamp:    timestamp,
		Height:       height,
		PoolMetadata: poolMetadata,
	}
	err = m.broker.PublishRawDexPoolMetadata(ctx, rawDexPoolMetadata)
	if err != nil {
		return fmt.Errorf("failed to publish DEX pool metadata: %w", err)
	}

	return nil
}
