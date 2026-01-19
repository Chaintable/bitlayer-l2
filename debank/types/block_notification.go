package types

import (
	"github.com/ethereum/go-ethereum/common"
)

type BlockChangeNotification struct {
	ChangeType uint64         `json:"changeType"` // 1 for new, 2 for fork
	NewBlocks  []BlockContext `json:"newBlocks"`  // new block, sorted by height
	DropBlocks []BlockContext `json:"dropBlocks"` // blocks to drop due to fork, sorted by height
}

type BlockContext struct {
	Hash        common.Hash `json:"hash"`
	ParentHash  common.Hash `json:"parentHash"`
	BlockNumber uint64      `json:"blockNumber"`
	Timestamp   uint64      `json:"timestamp"`
}

type OuterBlockChangeNotification struct {
	ChainID     int64       `json:"chain_id"`
	Hash        common.Hash `json:"block_id"`
	BlockNumber uint64      `json:"block_height"`
	Timestamp   uint64      `json:"block_timestamp"`
	IsFork      bool        `json:"is_fork"`
}
