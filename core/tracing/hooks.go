package tracing

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
)

type (
	/*
		- VM events -
	*/

	// TxStartHook is called before the execution of a transaction starts.
	// Call simulations don't come with a valid signature. `from` field
	// to be used for address of the caller.
	TxStartHook = func(tx *types.Transaction, from common.Address)

	// TxEndHook is called after the execution of a transaction ends.
	TxEndHook = func(receipt *types.Receipt, err error)

	// BlockchainInitHook is called when the blockchain is initialized.
	BlockchainInitHook = func(chainConfig *params.ChainConfig)

	CloseHook = func()

	// BlockStartHook is called before executing `block`.
	// `td` is the total difficulty prior to `block`.
	BlockStartHook = func(block *types.Block)

	// BlockEndHook is called after executing a block.
	BlockEndHook = func(err error)

	// GenesisBlockHook is called when the genesis block is being processed.
	// Note: alloc type is interface{} to avoid import cycle (actual type is core.GenesisAlloc)
	GenesisBlockHook = func(genesis *types.Block, alloc interface{})

	// CommitHook is called when the state is committed.
	// Note: bitlayer-l2 uses standard 6-parameter version (no sharePrice like Blast)
	CommitHook = func(originRoot common.Hash, root common.Hash, destructs map[common.Hash]struct{}, accounts map[common.Hash][]byte, storages map[common.Hash]map[common.Hash][]byte, codes map[common.Hash][]byte)

	// LogHook is called when a log is emitted.
	LogHook = func(log *types.Log)
)

type Hooks struct {
	// VM events
	OnTxStart TxStartHook
	OnTxEnd   TxEndHook
	// Chain events
	OnBlockchainInit BlockchainInitHook
	OnClose          CloseHook
	OnBlockStart     BlockStartHook
	OnBlockEnd       BlockEndHook
	OnGenesisBlock   GenesisBlockHook
	OnLog            LogHook
	// custom hook
	OnCommit CommitHook
}

// PipelineTracerInterface defines the interface needed to avoid import cycle
// This matches the methods in debank/tracer.PipelineTracer
type PipelineTracerInterface interface {
	OnBlockchainInit(chainConfig *params.ChainConfig)
	OnClose()
	OnBlockStart(block *types.Block)
	OnTxStart(tx *types.Transaction, from common.Address)
	OnTxEnd(receipt *types.Receipt, err error)
	OnLog(log *types.Log)
	OnGenesisBlock(genesis *types.Block, alloc interface{})
	OnCommit(originRoot common.Hash, root common.Hash, destructs map[common.Hash]struct{}, accounts map[common.Hash][]byte, storages map[common.Hash]map[common.Hash][]byte, codes map[common.Hash][]byte)
}

func BuildHooks(t PipelineTracerInterface) *Hooks {
	return &Hooks{
		OnBlockchainInit: t.OnBlockchainInit,
		OnClose:          t.OnClose,
		OnBlockStart:     t.OnBlockStart,
		OnTxStart:        t.OnTxStart,
		OnTxEnd:          t.OnTxEnd,
		OnLog:            t.OnLog,
		OnGenesisBlock:   t.OnGenesisBlock,
		OnCommit:         t.OnCommit,
	}
}
