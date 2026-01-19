package types

import "math/big"

type Trace struct {
	Type         string   `json:"type"`
	From         string   `json:"from"`
	To           string   `json:"to"`
	Value        *big.Int `json:"value"`
	Gas          uint64   `json:"gas"`
	GasUsed      uint64   `json:"gas_used"`
	Input        string   `json:"input"`
	Output       string   `json:"output"`
	Error        string   `json:"error,omitempty"`
	TxIndex      uint64   `json:"tx_index"`
	TraceAddress []uint64 `json:"trace_address"`
}
