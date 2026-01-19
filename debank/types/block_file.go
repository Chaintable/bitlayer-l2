package types

type BlockFile struct {
	Block            Block         `json:"block"`
	Events           []Event       `json:"events"`
	Txs              []Transaction `json:"txs"`
	Traces           []Trace       `json:"traces"`
	ErrorEvents      []Event       `json:"error_events"`
	ErrorTraces      []Trace       `json:"error_traces"`
	StorageContracts []string      `json:"storage_contracts"`
}
