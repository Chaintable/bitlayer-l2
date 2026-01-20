package types

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// GenesisAccount represents an account in the genesis state
// This is a simplified version to avoid importing core package (avoids import cycle)
type GenesisAccount struct {
	Code    []byte                      `json:"code,omitempty"`
	Storage map[common.Hash]common.Hash `json:"storage,omitempty"`
	Balance *big.Int                    `json:"balance" gencodec:"required"`
	Nonce   uint64                      `json:"nonce,omitempty"`
}

// GenesisAlloc specifies the initial state at genesis
type GenesisAlloc map[common.Address]GenesisAccount
