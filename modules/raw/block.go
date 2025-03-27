package raw

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	jsoniter "github.com/json-iterator/go"
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

	// Add debug logging
	m.log.Info().Int64("start_slinky_height", m.cfg.StartSlinkyHeight).Int64("block_height", block.Height).Msg("might publish block prices")

	if m.cfg.StartSlinkyHeight >= 0 && block.Height >= m.cfg.StartSlinkyHeight {
		m.log.Info().Int64("block_height", block.Height).Msg("attempting to publish block prices")
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

func (m *Module) publishBlockPrices(ctx context.Context, height int64) error {
	// Get currency pairs
	header := metadata.Pairs("x-cosmos-block-height", fmt.Sprintf("%d", height))
	ctxWithHeader := metadata.NewOutgoingContext(ctx, header)

	// Add debug logging
	m.log.Debug().Int64("height", height).Msg("attempting to publish block prices")

	pairsResp, err := m.grpcClient.OracleService.GetCurrencyPairMappingList(ctxWithHeader, &oracle.GetCurrencyPairMappingListRequest{})
	if err != nil {
		m.log.Error().Int64("height", height).Msg("failed to get currency pair mappings")
		return fmt.Errorf("failed to get currency pair mappings: %w", err)
	}

	// Log successful response
	m.log.Debug().
		Int64("height", height).
		Int("pairs_count", len(pairsResp.Mappings)).
		Msg("got prices from oracle service")

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
		m.log.Error().Int64("height", height).Interface("mappings", pairsResp.Mappings).Str("pairs", strings.Join(pairs, ",")).Msg("failed to get prices")
		return fmt.Errorf("failed to get prices: %w", err)
	}

	m.log.Debug().Int64("height", height).Int("price_count", len(pricesResp.Prices)).Msg("got prices from oracle service")

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
		m.log.Error().Int64("height", height).Interface("prices", pricesResp.Prices).Msg("failed to publish slinky prices")
		return fmt.Errorf("failed to publish slinky prices: %w", err)
	}

	m.log.Debug().Int64("height", height).Msg("successfully published block prices")
	return nil
}
