package processor

import (
	"fmt"

	"github.com/ethereum/go-ethereum/debank/types"
	"github.com/ethereum/go-ethereum/debank/util"
)

type DataFile struct {
	S3key string
	Data  []byte
	Kind  string // block_file, block_header, state_diff, block_file_validation
}

func SerializeHeader(chainID string, header *types.Header) (*DataFile, error) {
	data, err := util.EncodeToJsonGzip(header)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		S3key: fmt.Sprintf("%s/block/%d/header.json.gz", chainID, header.Number.ToInt().Uint64()),
		Data:  data,
		Kind:  "block_header",
	}, nil
}

func SerializeStateDiff(chainID string, stateDiff *types.BlockStorageDiff) (*DataFile, error) {
	data, err := util.EncodeToRlp(stateDiff)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		S3key: fmt.Sprintf("%s/state/%s/diff.rlp", chainID, stateDiff.Hash.Hex()),
		Data:  data,
		Kind:  "state_diff",
	}, nil
}

func SerializeFile(chainID string, blockFile *types.BlockFile) (*DataFile, error) {
	data, err := util.EncodeToJsonGzip(blockFile)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		S3key: fmt.Sprintf("%s/block/%s/file.json.gz", chainID, blockFile.Block.Height.String()),
		Data:  data,
		Kind:  "block_file",
	}, nil
}

type BlockFileValidation struct {
	TxCount     int `json:"tx_count"`
	EventCount  int `json:"event_count"`
	TraceCount  int `json:"trace_count"`
	ErrorCount  int `json:"error_count"`
}

func SerializeFileValidation(chainID string, blockFile *types.BlockFile) (*DataFile, error) {
	validation := BlockFileValidation{
		TxCount:    len(blockFile.Txs),
		EventCount: len(blockFile.Events),
		TraceCount: len(blockFile.Traces),
		ErrorCount: len(blockFile.ErrorEvents) + len(blockFile.ErrorTraces),
	}
	data, err := util.EncodeToJsonGzip(validation)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		S3key: fmt.Sprintf("%s/block/%s/validation.json.gz", chainID, blockFile.Block.Height.String()),
		Data:  data,
		Kind:  "block_file_validation",
	}, nil
}
