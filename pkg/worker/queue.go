package worker

import (
	"context"
	"sync"
	"time"
)

func (w *Worker) enqueueHeight(ctx context.Context, wg *sync.WaitGroup, startHeight, stopHeight int64) {
	defer wg.Done()

	w.log.Debug().Msgf("try to parse: %d count of blocks", stopHeight-startHeight+1)

	ctx, w.stopEnqueueHeight = context.WithCancel(ctx)
	defer w.stopEnqueueHeight()

	// send zero height to process genesis
	if startHeight != 0 && w.cfg.ProcessGenesis {
		w.heightCh <- 0
	}

	for height := startHeight; height >= 0 && height <= stopHeight; height++ {
		// safe from closed channel
		select {
		case <-ctx.Done():
			w.log.Info().Msg("stop enqueueHeight")
			return
		case w.heightCh <- height: // put height to channel for processing the block
		}
	}
}

func (w *Worker) enqueueNewBlocks(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(w.cfg.ProcessNewBlocksInterval)
	defer ticker.Stop()

	ctx, w.stopEnqueueNewBlocks = context.WithCancel(ctx)
	defer w.stopEnqueueNewBlocks()

	w.log.Info().Msg("parsing new blocks")

	startHeight, err := w.rpcClient.GetLastBlockHeight(ctx)
	if err != nil {
		w.log.Error().Err(err).Str("func", "GetLastBlockHeight").Msg("can't enqueueNewBlocks")
		return
	}

	// process the first new block height to prevent double processing attempts later
	w.heightCh <- startHeight

	for {
		select {
		case <-ctx.Done():
			w.log.Info().Msg("stop new block parsing")
			return
		case <-ticker.C:
			stopHeight, err := w.rpcClient.GetLastBlockHeight(ctx)
			if err != nil {
				w.log.Warn().Err(err).Str("func", "GetLastBlockHeight").Msg("can't enqueueNewBlocks")
				continue
			}

			for height := startHeight + 1; height <= stopHeight; height++ {
				w.log.Info().Int64("height", height).Msg("enqueueing new block")

				// safe from closed channel
				select {
				case <-ctx.Done():
					w.log.Info().Msg("stop new block parsing")
					return
				case w.heightCh <- height:
				}
			}

			startHeight = stopHeight
		}
	}
}

func (w *Worker) enqueueErrorBlocks(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(w.cfg.ProcessErrorBlocksInterval)
	defer ticker.Stop()

	ctx, w.stopEnqueueErrorBlocks = context.WithCancel(ctx)
	defer w.stopEnqueueErrorBlocks()

	for {
		select {
		case <-ctx.Done():
			w.log.Info().Msg("stop GetErrorBlockHeights")
			return
		case <-ticker.C:
			heights, err := w.storage.GetErrorBlockHeights(ctx)
			if err != nil {
				w.log.Error().Err(err).Str("func", "GetErrorBlockHeights").Msg("can't enqueueErrorBlocks")
				return
			}

			for _, height := range heights {
				// safe from closed channel
				select {
				case <-ctx.Done():
					w.log.Info().Msg("stop GetErrorBlockHeights")
					return
				case w.heightCh <- height:
				}
			}
		}
	}
}
