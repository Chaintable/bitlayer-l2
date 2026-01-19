package util

import (
	"encoding/hex"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	ptypes "github.com/ethereum/go-ethereum/debank/types"
)

func BuildPilelineBlockHeader(block *types.Block) *ptypes.Header {
	header := block.Header()
	h := &ptypes.Header{
		Number:           (*hexutil.Big)(header.Number),
		Hash:             block.Hash(),
		ParentHash:       header.ParentHash,
		Nonce:            header.Nonce,
		MixHash:          header.MixDigest,
		Sha3Uncles:       header.UncleHash,
		LogsBloom:        header.Bloom,
		StateRoot:        header.Root,
		Miner:            header.Coinbase,
		Difficulty:       (*hexutil.Big)(header.Difficulty),
		ExtraData:        header.Extra,
		GasLimit:         hexutil.Uint64(header.GasLimit),
		GasUsed:          hexutil.Uint64(header.GasUsed),
		Timestamp:        hexutil.Uint64(header.Time),
		TransactionsRoot: header.TxHash,
		ReceiptsRoot:     header.ReceiptHash,
	}

	if header.BaseFee != nil {
		h.BaseFeePerGas = (*hexutil.Big)(header.BaseFee)
	}
	if header.WithdrawalsHash != nil {
		h.WithdrawalsRoot = header.WithdrawalsHash
	}
	if header.BlobGasUsed != nil {
		h.BlobGasUsed = (*hexutil.Uint64)(header.BlobGasUsed)
	}
	if header.ExcessBlobGas != nil {
		h.ExcessBlobGas = (*hexutil.Uint64)(header.ExcessBlobGas)
	}
	if header.ParentBeaconRoot != nil {
		h.ParentBeaconBlockRoot = header.ParentBeaconRoot
	}
	return h
}

func BuildPipelineBlock(block *types.Block) ptypes.Block {
	header := block.Header()
	baseFee := big.NewInt(0)
	if header.BaseFee != nil {
		baseFee = header.BaseFee
	}
	return ptypes.Block{
		ID:                    strings.ToLower(block.Hash().Hex()),
		Height:                block.Number(),
		ParentID:              strings.ToLower(block.ParentHash().Hex()),
		BaseFeePerGas:         baseFee,
		Miner:                 strings.ToLower(header.Coinbase.Hex()),
		GasLimit:              big.NewInt(int64(header.GasLimit)),
		GasUsed:               big.NewInt(int64(header.GasUsed)),
		Timestamp:             header.Time,
		ProcessStartTimestamp: time.Now().UnixMilli(),
	}
}

func BuildPipelineTransaction(tx *types.Transaction, receipt *types.Receipt, from common.Address, baseFee *big.Int) ptypes.Transaction {
	to := ""
	if tx.To() != nil {
		to = strings.ToLower(tx.To().Hex())
	}

	effectiveGasPrice := tx.GasPrice()
	if baseFee != nil && tx.Type() == types.DynamicFeeTxType {
		effectiveGasPrice = new(big.Int).Add(baseFee, tx.GasTipCap())
		if effectiveGasPrice.Cmp(tx.GasFeeCap()) > 0 {
			effectiveGasPrice = tx.GasFeeCap()
		}
	}

	ptx := ptypes.Transaction{
		ID:                strings.ToLower(tx.Hash().Hex()),
		From:              strings.ToLower(from.Hex()),
		To:                to,
		Value:             tx.Value(),
		GasPrice:          tx.GasPrice(),
		GasLimit:          big.NewInt(int64(tx.Gas())),
		GasUsed:           big.NewInt(int64(receipt.GasUsed)),
		Nonce:             tx.Nonce(),
		Index:             uint64(receipt.TransactionIndex),
		Input:             "0x" + hex.EncodeToString(tx.Data()),
		Status:            receipt.Status,
		EffectiveGasPrice: effectiveGasPrice,
	}

	if tx.Type() == types.DynamicFeeTxType {
		ptx.MaxFeePerGas = tx.GasFeeCap()
		ptx.MaxPriorityFeePerGas = tx.GasTipCap()
	}

	return ptx
}
