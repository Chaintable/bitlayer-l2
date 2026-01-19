package types

import "math/big"

type Transaction struct {
	ID                   string   `json:"id"`
	From                 string   `json:"from"`
	To                   string   `json:"to"`
	Value                *big.Int `json:"value"`
	GasPrice             *big.Int `json:"gas_price"`
	GasLimit             *big.Int `json:"gas_limit"`
	GasUsed              *big.Int `json:"gas_used"`
	Nonce                uint64   `json:"nonce"`
	Index                uint64   `json:"index"`
	Input                string   `json:"input"`
	Status               uint64   `json:"status"`
	EffectiveGasPrice    *big.Int `json:"effective_gas_price"`
	MaxFeePerGas         *big.Int `json:"max_fee_per_gas,omitempty"`
	MaxPriorityFeePerGas *big.Int `json:"max_priority_fee_per_gas,omitempty"`
}
