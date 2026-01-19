package metrics

import (
	"github.com/ethereum/go-ethereum/metrics"
)

var (
	// Block processing metrics
	BlockProcessTimer       = metrics.NewRegisteredTimer("debank/block/process", nil)
	BlockTxExecutionTimer   = metrics.NewRegisteredTimer("debank/block/tx/execution", nil)
	BlockHeaderUploadTimer  = metrics.NewRegisteredTimer("debank/upload/header", nil)
	StateDiffUploadTimer    = metrics.NewRegisteredTimer("debank/upload/statediff", nil)
	BlockFileUploadTimer    = metrics.NewRegisteredTimer("debank/upload/blockfile", nil)
	BlockFileValidationTimer = metrics.NewRegisteredTimer("debank/upload/blockfile/validation", nil)
	BlockPushTimer          = metrics.NewRegisteredTimer("debank/push/block", nil)

	// Block number metrics
	LatestBlockNumber         = metrics.NewRegisteredGauge("debank/block/latest", nil)
	LatestBlockTime           = metrics.NewRegisteredGauge("debank/block/latest/time", nil)
	LatestUploadedBlockNumber = metrics.NewRegisteredGauge("debank/block/uploaded", nil)

	// Node info
	NodeInfo = metrics.NewRegisteredGaugeInfo("debank/node/info", nil)
)
