package slinky_prices

import (
	"context"
	"fmt"

	"github.com/bro-n-bro/spacebox-crawler/v2/types"
	oracle "github.com/skip-mev/slinky/x/oracle/types"
	"google.golang.org/grpc/metadata"
)

func (m *Module) HandleBlock(ctx context.Context, block *types.Block) error {
	if m.cfg.StartSlinkyHeight >= 0 && block.Height >= m.cfg.StartSlinkyHeight {
		if err := m.publishBlockPrices(ctx, block.Height); err != nil {
			return fmt.Errorf("failed to publish block prices: %w", err)
		}
	}
	m.log.Debug().Int64("height", block.Height).Msg("Published Slinky prices")
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
