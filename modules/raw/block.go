package raw

import (
	"context"
	"encoding/json"
	"fmt"
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

	return m.publishBlockPrices(ctx, block.Height)
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

	return m.broker.PublishRawSlinkyPrices(ctx, rawPrices)
}
