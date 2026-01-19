package tracer

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/debank/metrics"
	"github.com/ethereum/go-ethereum/debank/processor"
	ptypes "github.com/ethereum/go-ethereum/debank/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

type ExtraInfo struct {
	BlockNumber     uint64
	BlockHash       common.Hash
	BlockFile       *ptypes.BlockFile
	Tx              *types.Transaction
	From            common.Address
	BlockHeader     *ptypes.Header
	BlockDiff       *ptypes.BlockStorageDiff
	BlockChange     *ptypes.BlockChangeNotification
	Committed       bool
	ChangeContracts map[common.Address]struct{}
	// metrics timer
	TxStartTime    time.Time
	BlockStartTime time.Time
}

var (
	NodeXPusher            *processor.PushProcessor
	ChainTableBucketPusher *processor.PushProcessor
	BlockCtx               *ExtraInfo
	BizChainID             string
)

func InitPipeline(region string, nodeXBucket string, chainTableBucket string, brokers []string, topic string, bizChainID string, s3TmpDir string, isBackup bool) (err error) {
	NodeXPusher, err = processor.NewPushProcessor(region, nodeXBucket, brokers, topic, s3TmpDir, isBackup)
	if err != nil {
		return err
	}
	ChainTableBucketPusher, err = processor.NewPushProcessor(region, chainTableBucket, brokers, topic, s3TmpDir, isBackup)
	if err != nil {
		return err
	}
	BizChainID = bizChainID
	return nil
}

func stateUpdateToStateDiff(originRoot common.Hash, root common.Hash, destructs map[common.Hash]struct{}, accounts map[common.Hash][]byte, storages map[common.Hash]map[common.Hash][]byte, codes map[common.Hash][]byte) *ptypes.BlockStorageDiff {
	stateDiff := &ptypes.BlockStorageDiff{}
	for addrhash := range destructs {
		stateDiff.DeletedAccounts = append(stateDiff.DeletedAccounts, addrhash)
	}
	for k, v := range accounts {
		account, err := DecodeFullAccount(v)
		if err != nil {
			log.Error("Failed to decode account", "err", err)
			continue
		}
		stateDiff.NewAccounts = append(stateDiff.NewAccounts, ptypes.NewAccount{
			Address:  k,
			Balance:  account.Balance,
			Nonce:    account.Nonce,
			CodeHash: common.BytesToHash(account.CodeHash),
		})
	}
	for account, storage := range storages {
		Values := make([]ptypes.IndexValuePair, 0, len(storage))
		for index, v := range storage {
			value := uint256.NewInt(0)
			if len(v) > 0 {
				_, content, _, err := rlp.Split(v)
				if err != nil {
					log.Error("Failed to split storage", "err", err)
				}
				value = uint256.NewInt(0).SetBytes(content)
			}
			Values = append(Values, ptypes.IndexValuePair{
				Index: index,
				Value: value,
			})
		}
		stateDiff.StorageDiff = append(stateDiff.StorageDiff, ptypes.AccountStorageDiff{
			Address: account,
			Values:  Values,
		})
	}
	for hash, code := range codes {
		stateDiff.NewCodes = append(stateDiff.NewCodes, ptypes.NewCode{
			CodeHash: hash,
			Code:     code,
		})
	}
	if originRoot == (common.Hash{}) {
		originRoot = types.EmptyRootHash
	}
	if root == (common.Hash{}) {
		root = types.EmptyRootHash
	}
	stateDiff.Hash = root
	stateDiff.ParentHash = originRoot
	return stateDiff
}

func uploadBlockHeader(blockHeader *ptypes.Header) error {
	start := time.Now()
	defer func() {
		metrics.BlockHeaderUploadTimer.UpdateSince(start)
	}()
	s3BlockFile, err := processor.SerializeHeader(BizChainID, blockHeader)
	if err != nil {
		return fmt.Errorf("failed to serialize block header: %v", err)
	}
	err = NodeXPusher.UploadFile(s3BlockFile)
	if err != nil {
		return fmt.Errorf("failed to upload block header: %v", err)
	}
	return nil
}

func uploadBlockDiff(blockDiff *ptypes.BlockStorageDiff) error {
	start := time.Now()
	defer func() {
		metrics.StateDiffUploadTimer.UpdateSince(start)
	}()
	s3file, err := processor.SerializeStateDiff(BizChainID, blockDiff)
	if err != nil {
		return fmt.Errorf("failed to serialize state diff: %v", err)
	}
	err = NodeXPusher.UploadFile(s3file)
	if err != nil {
		return fmt.Errorf("failed to upload state diff: %v", err)
	}
	return nil
}

func uploadBlockFile(blockFile *ptypes.BlockFile) error {
	s3file, err := processor.SerializeFile(BizChainID, blockFile)
	if err != nil {
		return fmt.Errorf("failed to serialize block file: %v", err)
	}
	err = ChainTableBucketPusher.UploadFile(s3file)
	if err != nil {
		return fmt.Errorf("failed to upload block file: %v", err)
	}
	return nil
}

func uploadblockFileValidation(blockFile *ptypes.BlockFile) error {
	start := time.Now()
	defer func() {
		metrics.BlockFileValidationTimer.UpdateSince(start)
	}()
	blockFileValidation, err := processor.SerializeFileValidation(BizChainID, blockFile)
	if err != nil {
		return fmt.Errorf("failed to serialize block file validation: %v", err)
	}
	err = ChainTableBucketPusher.UploadFile(blockFileValidation)
	if err != nil {
		return fmt.Errorf("failed to upload block file validation: %v", err)
	}
	return nil
}

// FullAccount is the account structure used for RLP decoding
type FullAccount struct {
	Nonce    uint64
	Balance  *uint256.Int
	Root     common.Hash
	CodeHash []byte
}

// DecodeFullAccount decodes the data on the 'slim RLP' format and returns
// the consensus format account.
func DecodeFullAccount(data []byte) (*FullAccount, error) {
	var account FullAccount
	if err := rlp.DecodeBytes(data, &account); err != nil {
		return nil, err
	}
	return &account, nil
}
