package raw

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	"github.com/cosmos/cosmos-sdk/types/query"
	jsoniter "github.com/json-iterator/go"
	dex "github.com/neutron-org/neutron/v7/x/dex/types"
	oracle "github.com/skip-mev/slinky/x/oracle/types"

	"github.com/bro-n-bro/spacebox-crawler/v2/types"
	"google.golang.org/grpc/metadata"
)

func (m *Module) HandleBlock(ctx context.Context, block *types.Block) error {
	rawBlock := struct {
		Hash            string          `json:"hash"`
		ProposerAddress string          `json:"proposer_address"`
		Block           json.RawMessage `json:"block"`
		TotalGas        uint64          `json:"total_gas"`
		NumTxs          uint16          `json:"num_txs"`
	}{
		TotalGas:        block.TotalGas,
		Hash:            block.Hash,
		ProposerAddress: block.ProposerAddress,
		NumTxs:          uint16(block.TxNum),
	}

	var err error
	rawBlock.Block, err = jsoniter.Marshal(block.Raw().Block)
	if err != nil {
		return fmt.Errorf("failed to marshal block: %w", err)
	}

	if err = m.broker.PublishRawBlock(ctx, rawBlock); err != nil {
		return fmt.Errorf("failed to publish raw block: %w", err)
	}

	if err = m.publishBlockResults(ctx, block.Height, block.Timestamp); err != nil {
		return fmt.Errorf("failed to publish raw block results: %w", err)
	}

	if err := m.publishDexPoolMetadata(ctx, block.Height, block.Timestamp); err != nil {
		return err
	}

	if m.cfg.StartSlinkyHeight >= 0 && block.Height >= m.cfg.StartSlinkyHeight {
		if err := m.publishBlockPrices(ctx, block.Height); err != nil {
			return err
		}
	}

	return nil
}

func (m *Module) publishBlockResults(ctx context.Context, height int64, timestamp time.Time) error {
	brResp, err := m.rpcClient.GetBlockResults(ctx, height)
	if err != nil {
		return fmt.Errorf("failed to get block results: %w", err)
	}

	rawBR := struct {
		*coretypes.ResultBlockResults
		Timestamp time.Time `json:"timestamp"`
	}{
		ResultBlockResults: brResp,
		Timestamp:          timestamp,
	}

	return m.broker.PublishRawBlockResults(ctx, rawBR)
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

func (m *Module) publishBlockPrices(ctx context.Context, height int64) error {
	// Get currency pairs
	header := metadata.Pairs("x-cosmos-block-height", fmt.Sprintf("%d", height))
	ctxWithHeader := metadata.NewOutgoingContext(ctx, header)

	pairsResp, err := m.grpcClient.OracleService.GetCurrencyPairMappingList(ctxWithHeader, &oracle.GetCurrencyPairMappingListRequest{})
	if err != nil {
		return fmt.Errorf("failed to get currency pair mappings: %w", err)
	}

	// Create currency pairs request format
	pairs := make([]string, 0, len(pairsResp.Mappings))
	for _, mapping := range pairsResp.Mappings {
		pair := fmt.Sprintf("%s/%s", mapping.CurrencyPair.Base, mapping.CurrencyPair.Quote)
		pairs = append(pairs, pair)
	}

	// Get prices
	pricesResp, err := m.grpcClient.OracleService.GetPrices(ctxWithHeader, &oracle.GetPricesRequest{
		CurrencyPairIds: pairs,
	})
	if err != nil {
		return fmt.Errorf("failed to get prices: %w", err)
	}

	// Combine data and publish
	rawPrices := struct {
		Mappings []oracle.CurrencyPairMapping `json:"mappings"`
		Prices   []oracle.GetPriceResponse    `json:"prices"`
	}{
		Mappings: pairsResp.Mappings,
		Prices:   pricesResp.Prices,
	}

	err = m.broker.PublishRawSlinkyPrices(ctx, rawPrices)
	if err != nil {
		return fmt.Errorf("failed to publish slinky prices: %w", err)
	}

	return nil
}
