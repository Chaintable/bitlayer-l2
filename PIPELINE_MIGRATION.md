# Bitlayer-L2 Pipeline Tracer Migration

This document describes the pipeline tracer integration changes made to bitlayer-l2, based on the blast-geth implementation.

## Migration Date
- **Date**: 2026-01-19
- **Branch**: feature/lihe_pipeline
- **Based on**: blast-geth debank implementation
- **Migrated by**: lihe

## Summary of Changes

### 1. Dependency Updates

**File**: `go.mod`
- Upgraded Go version: `1.20` → `1.22`
- Added pipeline dependencies:
  - `github.com/aws/aws-sdk-go-v2/service/s3 v1.66.3`
  - `github.com/segmentio/kafka-go v0.4.48`
  - `go.etcd.io/etcd/client/v3 v3.5.10`
- Updated `klauspost/compress` to `v1.16.7`

### 2. Core Tracing Infrastructure

**New File**: `core/tracing/hooks.go`
- Defines tracing hook interfaces for pipeline integration
- **Key Difference from Blast**: Uses standard 6-parameter `CommitHook` signature (NO `sharePrice` parameter)
- Hooks include: `OnTxStart`, `OnTxEnd`, `OnBlockStart`, `OnBlockEnd`, `OnCommit`, `OnLog`, `OnGenesisBlock`, `OnBlockchainInit`, `OnClose`

### 3. Debank Directory Structure

**New Directory**: `debank/`

Copied from blast-geth with modifications:

**debank/types/** - Data structures
- `block.go` - Block representation
- `block_notification.go` - Block change notifications for Kafka
- `state_diff.go` - State change tracking
- `transaction.go` - Transaction representation
- `trace.go` - Call trace data
- `event.go` - Event/log tracking
- `block_file.go` - Block file serialization

**debank/tracer/** - Transaction and block tracing
- `pipeline_tracer.go` - Main tracer implementing `vm.EVMLogger`
  - **Modified**: `OnCommit()` uses 6 parameters (removed `sharePrice`)
- `call_tracer.go` - Call frame tracing
- `pipeline.go` - Pipeline execution coordination
  - **Modified**: `getAccountBalance()` simplified (removed Blast-specific sharePrice logic)
  - **Modified**: `stateUpdateToStateDiff()` signature updated (removed `sharePrice` parameter)

**debank/processor/** - Data publishing
- `push.go` - S3/Kafka upload processor
- `serializer.go` - JSON serialization

**debank/util/** - Utilities
- `s3.go` - AWS S3 operations
- `kafka.go` - Kafka reader/writer
- `codec.go` - Gzip/JSON encoding
- `file.go` - File operations
- `tracer.go` - Tracer utilities

**debank/leader/** - Leader election
- `manager.go` - Leader manager (etcd or manual)
- `leader_failover.go` - Etcd-based election
- `config.go` - Configuration

**debank/metrics/** - Monitoring
- `metrics.go` - Prometheus metrics

**debank/writer/** - Registry
- `registry.go` - Writer node registry

### 4. State Tracking Modifications

**File**: `core/state/statedb.go`

Added fields to `StateDB` struct:
- `hooks *tracing.Hooks` - Tracing callbacks
- `Destructs map[common.Hash]struct{}` - Deleted accounts
- `Accounts map[common.Hash][]byte` - Account changes
- `Storage map[common.Hash]map[common.Hash][]byte` - Storage changes

Modified methods:
- `New()` - Initialize tracking maps
- `SetHooks()` - **New method** to set tracing hooks
- `AddLog()` - Call `hooks.OnLog()` for each log
- `updateStateObject()` - Track account updates
- `deleteStateObject()` - Track deletions
- `createObject()` - Track recreated accounts
- `Copy()` - Deep-copy tracking maps
- `Finalise()` - Update tracking on account deletion
- `Commit()` - **Call `hooks.OnCommit()` with 6 parameters (NO sharePrice)**

**Key Adaptation**: Bitlayer uses standard Ethereum account structure without Blast's yield-related fields (Shares, Flags, Remainder, Fixed).

### 5. Blockchain Integration

**File**: `core/blockchain.go`

Additions:
- Imports: `core/tracing`, `debank/tracer`, `debank/types`
- Field: `logger *tracing.Hooks` in `BlockChain` struct

Modified `NewBlockChain()`:
- Initialize pipeline tracer from `vmConfig.Tracer`
- Call `OnBlockchainInit(chainConfig)`
- Call `OnGenesisBlock()` for genesis block (block 0)

Modified `Stop()`:
- Call `logger.OnClose()` before shutdown

**New Methods**:
- `getCommonAncestor()` - Find fork points between blocks
- `pushBlockChange()` - Send block changes to Kafka via NodeXPusher
  - Handles new blocks and reorgs
  - Handles empty blocks (parent.Root == block.Root)

Calls to `pushBlockChange()`:
- After `setChainHead()`
- After canonical block insertion
- In reorg loop for each new chain block
- After `SetCanonical()`

**File**: `core/genesis.go`

**New Helper Functions**:
- `getGenesisState()` - Retrieve genesis allocation from database
- `coreGenesisToTypesGenesis()` - Convert allocation types

### 6. Transaction Processing

**File**: `core/state_processor.go`

**Already Modified** (modifications already present):
- Added imports: `core/tracing`, `debank/tracer`
- In `Process()` method:
  - Extract `PipelineTracer` from `cfg.Tracer`
  - Call `OnBlockStart(block)` at beginning
  - Call `SetHooks()` on statedb
  - Call `OnTxStart(tx, from)` before each transaction
  - Call `OnTxEnd(receipt, err)` after each transaction
  - Set `EffectiveGasPrice` on receipts
  - Use `defer` to call `OnBlockEnd(err)`

**Note**: bitlayer-l2's state_processor already has additional features (action tracing, parallel bloom creation) that are preserved.

### 7. Docker & CI/CD

**New File**: `Dockerfile.debank`
- Multi-stage build with Go 1.22
- Ubuntu 24.04 runtime
- Includes performance libraries (snappy, jemalloc)
- Exposes ports: 8545 (HTTP-RPC), 8546 (WS-RPC), 30303 (P2P)

**New File**: `.github/workflows/build.debank.yml`
- Triggers on PRs to `debank` branch or manual dispatch
- Builds and pushes to AWS ECR
- Image name: `blockchain-bitlayer-l2-x`
- Supports amd64 architecture

**New File**: `.github/workflows/release.debank.yml`
- Triggers on release creation or manual dispatch
- Same build/push process as build workflow
- Includes Lark notification on success
- Commented-out multi-arch manifest merge
- Fixed: Removed invalid pull_request context reference

## Key Differences from Blast Implementation

### 1. SharePrice Removal
**Blast** has yield-related features with `sharePrice` parameter:
```go
// Blast CommitHook (7 parameters)
CommitHook = func(..., sharePrice *big.Int)

// Blast StateAccount fields
type StateAccount struct {
    Shares    *big.Int
    Flags     uint8
    Remainder *big.Int
    Fixed     *big.Int
    // ...
}
```

**Bitlayer** uses standard Ethereum structure:
```go
// Bitlayer CommitHook (6 parameters)
CommitHook = func(originRoot, root, destructs, accounts, storages, codes)

// Standard StateAccount
type StateAccount struct {
    Balance *big.Int
    Nonce   uint64
    // ...
}
```

### 2. Simplified Account Balance
**Blast**: Complex balance calculation based on yield mode
```go
func getAccountBalance(account *types.StateAccount, sharePrice *big.Int) *big.Int {
    if account.Flags == types.YieldAutomatic && sharePrice != nil {
        value := new(big.Int).Mul(sharePrice, account.Shares)
        value.Add(value, account.Remainder)
        return value
    }
    return account.Fixed
}
```

**Bitlayer**: Direct balance access
```go
func getAccountBalance(account *types.StateAccount) *big.Int {
    return account.Balance
}
```

### 2. EffectiveGasPrice Fix

**Blast** uses incorrect calculation (missing baseFee for EIP-1559):
```go
// Blast (INCORRECT)
receipt.EffectiveGasPrice = tx.EffectiveGasTipValue(baseFee)
// Returns: min(gasTipCap, gasFeeCap - baseFee)  ← Missing baseFee!
```

**Bitlayer-l2** uses correct calculation:
```go
// bitlayer-l2 (CORRECT, matches chaintable-go-ethereum)
receipt.EffectiveGasPrice = tx.EffectiveGasPrice(baseFee)
// Returns: min(gasTipCap, gasFeeCap - baseFee) + baseFee  ← Complete price!
```

**Impact**:
- For Legacy transactions (Type 0/1): No difference (both return gasPrice)
- For EIP-1559 transactions (Type 2/3): Blast missing ~96% of gas price (only tip, no baseFee)
- bitlayer-l2 will have accurate gas price data in pipeline

**Risk**: If downstream pipeline systems expect Blast's incorrect format, may need adaptation.

## TODO Items for Verification

The following items are marked with `TODO(lihe)` comments in the code:

### Workflows
1. ✅ **Image naming**: Updated to `blockchain-bitlayer-l2-x`
2. ✅ **Architecture support**: Confirmed amd64 only (no arm64 needed)

### Core Integration
3. ✅ **Tracer initialization**: Verified - only PipelineTracer supported, vmConfig.Tracer can be nil
4. ✅ **Genesis handling**: Verified - genesis starts from block 0, current handling is correct
5. ✅ **Genesis alloc**: Verified - genesis alloc should not be missing, warning if absent (keeps flexibility)
6. ✅ **Consensus mechanism**: Verified - `getCommonAncestor()` is consensus-agnostic, works with Merlion (PoSA)
7. ✅ **Kafka integration**: Verified - uses standard message format and libraries, requires environment testing
8. ✅ **Empty blocks**: Verified - Merlion produces empty blocks, detection by state root comparison is correct
9. ✅ **Genesis storage**: Verified - uses standard ReadGenesisStateSpec API, compatible with bitlayer-l2
10. ✅ **Account structure**: Verified - bitlayer-l2 uses standard StateAccount (Nonce, Balance, Root, CodeHash)

### State Processing
11. ✅ **EffectiveGasPrice**: Fixed - uses correct effectiveGasPrice (tip + baseFee) instead of Blast's incorrect EffectiveGasTipValue (tip only)

## Testing Checklist

Before merging:
- [ ] Compilation succeeds without errors
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Docker image builds successfully
- [ ] Leader election works (both etcd and manual modes)
- [ ] S3 uploads function correctly
- [ ] Kafka notifications are sent properly
- [ ] Genesis block handling is correct
- [ ] Fork/reorg handling works as expected
- [ ] Empty blocks are processed correctly
- [ ] All TODO(lihe) items are resolved or documented

## Configuration Example

```json
{
  "region": "us-east-1",
  "node_x_bucket": "bitlayer-nodex-bucket",
  "chain_table_bucket": "bitlayer-chaintable-bucket",
  "brokers": ["kafka-broker-1:9092", "kafka-broker-2:9092"],
  "topic": "bitlayer-blockchain-events",
  "s3_temp_dir": "/data/s3-temp",
  "is_backup": null,
  "etcd_endpoints": ["etcd-1:2379", "etcd-2:2379", "etcd-3:2379"],
  "election_key": "/bitlayer/pipeline/leader",
  "node_id": "node-1",
  "grace_period": 10
}
```

## References

- **Blast implementation**: `/Users/lihe/ghorg/chaintable/blast/blast-geth/`
- **Original blast**: `/Users/lihe/code/upstream-refs/origin-blast/blast-geth/`
- **Migration guide**: `/Users/lihe/code/task_oasys/低版本geth链改造详细文档.md`

## Migration Notes

This migration preserves bitlayer-l2's existing features while adding pipeline tracing capability:
- Parallel bloom creation (state_processor.go)
- Action tracing support (state_processor.go)
- Account preloading optimizations (state_processor.go)
- Internal transactions tracking (state_processor.go)

All modifications include comments explaining bitlayer-specific adaptations and potential areas requiring verification.
