package types

type Event struct {
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	TxIndex     uint64   `json:"tx_index"`
	LogIndex    uint64   `json:"log_index"`
	BlockNumber uint64   `json:"block_number"`
}
