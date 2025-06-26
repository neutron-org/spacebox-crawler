package grpc

import (
	"context"
	"encoding/hex"
	"time"

	cometbfttypes "github.com/cometbft/cometbft/types"
	"github.com/cosmos/cosmos-sdk/types/tx"

	"github.com/bro-n-bro/spacebox-crawler/v2/adapter/storage/model"
)

// Txs queries for all the transactions in a block. Transactions are returned
// in sdk.TxResponse format which internally contains a sdk.Tx. An error is
// returned if any query fails.
func (c *Client) Txs(ctx context.Context, height int64, txs cometbfttypes.Txs) ([]*tx.GetTxResponse, error) {
	txResponses := make([]*tx.GetTxResponse, 0, len(txs))

	for i, tmTx := range txs {
		hash := hex.EncodeToString(tmTx.Hash())
		respPb, err := c.TxService.GetTx(ctx, &tx.GetTxRequest{Hash: hash})
		if err != nil {
			// treat common known error differently
			if i == 0 && err.Error() == "rpc error: code = Unknown desc = codespace sdk code 2: "+
				"tx parse error: expected 2 wire type, got 0" {
				// just log this one quietly
				c.log.Debug().Err(err).Int64("height", height).Msgf("GetTx error on txhash: %s", hash)
			} else {
				_ = c.storage.InsertErrorTx(ctx, model.Tx{
					Created:      time.Now(),
					ErrorMessage: err.Error(),
					Hash:         hash,
					Height:       height,
				})

				c.log.Warn().Err(err).Int64("height", height).Msgf("GetTx error on txhash: %s", hash)
			}
			continue
		}

		txResponses = append(txResponses, &tx.GetTxResponse{Tx: respPb.Tx, TxResponse: respPb.TxResponse})
	}

	return txResponses, nil
}
