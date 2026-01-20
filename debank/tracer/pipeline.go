package tracer

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/debank/leader"
	"github.com/ethereum/go-ethereum/debank/metrics"
	"github.com/ethereum/go-ethereum/debank/processor"
	ptypes "github.com/ethereum/go-ethereum/debank/types"
	"github.com/ethereum/go-ethereum/debank/writer"
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
	LeaderManager          *leader.Manager
	WriterRegistry         *writer.WriterRegistry
)

func InitPipeline(region string, nodeXBucket string, chainTableBucket string, brokers []string, topic string, bizChainID string, s3TmpDir string) (err error) {
	NodeXPusher, err = processor.NewPushProcessor(region, nodeXBucket, brokers, topic, s3TmpDir)
	if err != nil {
		return err
	}
	ChainTableBucketPusher, err = processor.NewPushProcessor(region, chainTableBucket, brokers, topic, s3TmpDir)
	if err != nil {
		return err
	}
	BizChainID = bizChainID
	return nil
}

// WriterRegistryConfig holds configuration for writer node registration
type WriterRegistryConfig struct {
	TTL              int64
	NodeXBucket      string
	ChainTableBucket string
	Region           string
	Brokers          []string
	Topic            string
}

// SetupLeaderElection sets up manual leader election for the processors
func SetupLeaderElection(etcdEndpoints []string, electionKey string, nodeID string, isBackup *bool, gracePeriod int, writerConfig *WriterRegistryConfig) error {
	// Create a single leader manager for both processors
	config := leader.ManagerConfig{
		EtcdEndpoints: etcdEndpoints,
		ElectionKey:   electionKey,
		NodeID:        nodeID,
		IsBackup:      isBackup,
		GracePeriod:   time.Duration(gracePeriod) * time.Second,
		OnBecomeLeader: func() error {
			// Update last block when becoming leader
			log.Info("Updating last block info on leader transition")
			if NodeXPusher != nil {
				if err := NodeXPusher.UpdateLastBlock(); err != nil {
					log.Error("Failed to update NodeX last block", "err", err)
				}
			}
			if ChainTableBucketPusher != nil {
				if err := ChainTableBucketPusher.UpdateLastBlock(); err != nil {
					log.Error("Failed to update ChainTable last block", "err", err)
				}
			}
			return nil
		},
		OnLoseLeader: func() error {
			return nil
		},
	}

	var err error
	leader.GlobalManager, err = leader.NewManager(&config)
	if err != nil {
		return fmt.Errorf("failed to create leader manager: %w", err)
	}

	// Initialize writer registry in failover mode
	if writerConfig != nil {
		// Use the same etcd client from leader manager
		etcdClient := leader.GlobalManager.GetEtcdClient()

		// Create writer node info
		nodeInfo := writer.WriterNodeInfo{
			NodeXBucket:      writerConfig.NodeXBucket,
			ChainTableBucket: writerConfig.ChainTableBucket,
			Region:           writerConfig.Region,
			Brokers:          writerConfig.Brokers,
			Topic:            writerConfig.Topic,
		}

		WriterRegistry = writer.NewWriterRegistry(etcdClient, BizChainID, nodeID, nodeInfo, writerConfig.TTL)

		// Register node immediately when initialized (not waiting to become leader)
		if err := WriterRegistry.RegisterNode(); err != nil {
			log.Error("Failed to register writer node during initialization", "err", err)
		} else {
			log.Info("Writer node registered during initialization", "chainID", BizChainID, "nodeID", nodeID)
		}
	}

	if err := leader.GlobalManager.Start(); err != nil {
		return fmt.Errorf("failed to start leader manager: %w", err)
	}

	log.Info("Leader election setup completed", "nodeID", nodeID, "electionKey", electionKey)

	return nil
}

// getAccountBalance returns the balance of an account
// Note: Removed Blast-specific sharePrice logic for bitlayer-l2
func getAccountBalance(account *types.StateAccount) *big.Int {
	return account.Balance
}

func stateUpdateToStateDiff(originRoot common.Hash, root common.Hash, destructs map[common.Hash]struct{}, accounts map[common.Hash][]byte, storages map[common.Hash]map[common.Hash][]byte, codes map[common.Hash][]byte) *ptypes.BlockStorageDiff {
	stateDiff := &ptypes.BlockStorageDiff{}
	for addrhash := range destructs {
		stateDiff.DeletedAccounts = append(stateDiff.DeletedAccounts, addrhash)
	}
	for k, v := range accounts {
		account, _ := types.FullAccount(v)

		stateDiff.NewAccounts = append(stateDiff.NewAccounts, ptypes.NewAccount{
			Address:  k,
			Balance:  uint256.MustFromBig(getAccountBalance(account)),
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
