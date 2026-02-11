# Multi-Log Parallel Replication Feature - PR Description

## Overview

This PR introduces **multi-log based Append-Only File (AOF)** support to Garnet, enhancing write throughput and enabling optimized parallel replication replay. The feature leverages multiple physical `TsavoriteLog` instances to shard write operations and parallelize log scanning, shipping, and replay across multiple connections and iterators. While designed primarily for cluster mode replication, this feature can also be used in standalone mode to improve performance when AOF is enabled.

## Feature Requirements

### 1. **Sharded AOF Architecture**
- Improves AOF write-throughput through key-based sharding across distinct physical `TsavoriteLog` instances.
- Accelerates replica synchronization through parallel log scanning and shipping across the network.
- Full backward compatibility with existing single-log deployments

### 2. **Flexible Parallel Replay with Tunable Task Granularity**
- Introduces virtual sublog abstraction to allow for parallel replay within a given physical sublog.
- Minimizes inter-task coordination to maximize parallel execution efficiency

### 3. **Read Consistency Protocol**
- Per session prefix consistency through the use of timestamp-based sequence numbers.
- Sketch based key-level replay status tracking for efficient and lightweight freshness validation.
- Version-based prefix-consistency across replica reconfiguration operations.
- Ensures monotonically increasing sequence numbers across failovers through offset tracking during replica promotion.

### 4. **Transaction Support**
- Coordinates multi-exec transactions across sublogs to maintain ACID properties during parallel replay.
- Preserves consistent commit ordering per session through timestamp-based sequence numbers.

### 5. **Fast Prefix-Consistent Recovery**
- Multi-sublog prefix-consistent recovery within the persisted commit boundaries.
- Intra-page parallelism during recovery using multiple replay tasks.

## Newly Introduced Configuration Parameters

| Parameter | Purpose | 
|-----------|---------|
| `AofPhysicalSublogCount` | Number of physical `TsavoriteLog` instances |
| `AofReplayTaskCount` | Replay tasks per physical sublog at replica |
| `AofRefreshPhysicalSublogTailFrequencyMs` | Background task frequency for advancing idle sublog timestamps |

## Implementation Plan

### Phase 1: Core Infrastructure
- [ ] **1.1** Implement `AofHeader` extensions to eliminate single log overhead.
  - `ShardedHeader` for standalone operations.
  - `TransactionHeader` for coordinated batches.

- [ ] **1.2** Implement `GarnetLog` abstraction layer.
  - `SingleLog` wrapper for legacy single log.
  - `ShardedLog` implementation for multi-log.

- [ ] **1.3** `SequenceNumberGenerator` class.
  - Generate monotonically increasing sequence number using timestamps.
  - Ensure monotonicity at failover and recovery by using starting offset.

### Phase 2: Primary Replication Stream
- [ ] **2.1** `AofSyncDriver` class.
  - Single instance `AofSyncDriver` per attached replica.
  - Multiple instances of `AofSyncTask` per physical sublog.
  - Use dedicated `AdvanceTime` background task per attached replica.

- [ ] **2.2** `AofSyncTask` class.
  - Independent log iterators per sublog
  - Network page shipping per sublog
  - Error handling and connection teardown

- [ ] **2.3** `AdvanceTime` background task.
  - Primary monitors log changes by comparing last know tail address to the current tail address.
  - Primary associates the current tail address snapshot with a sequence number (timestamp) that is strictly larger than all sequence numbers assigned until that moment and notifies the replica.
  - Replica maintains an advance time background task that updates sublog time using the information from the primary's signal.
  - Primary advances last known tail address to the observed tail address.
  - The system reaches equilibrium when writes are quiesced and not more signals are send unless a new change is detected.

### Phase 3: Replica Replay Stream
- [ ] **3.1** `ReplicaReplayDriver` class.
  - Per-physical-sublog enqueue, scan and replay coordination
  - Manages `ReplicaReplayTask` for parallel replay within a single physical sublog.

- [ ] **3.2** `ReplicaReplayTask` class.
  - Record filtering by task affinity.
  - Coordinated update of virtual sublog replay state to enable read prefix consistency.

- [ ] **3.3** Standalone operation replay
  - Each operation executes within its appropriate context (`BasicContext` or `TransactionalContext`).
  - The virtual sublog replay state is updated prior to replay to maintain prefix consistency for read operations.

- [ ] **3.4** Multi-exec transaction replay
  - Transaction operations are distributed across replay tasks based on key affinity.
  - Upon encountering the TxnCommit marker, each participating task acquires exclusive locks for its assigned keys.
  - The associated virtual sublog replay state gets updated following the standalone operation replay.
  - All participating tasks synchronize at a barrier before commit, which releases locks and makes results visible.
  - The commit marker advances time prior to execution, ensuring timestamp consistency while locks are still held.

- [ ] **3.5** Custom transaction procedure replay
  - Similar to multi-exec transaction with the exception of having a single thread execute the custom procedure.
  - Virtual sublog replay state gets updated prior to lock acquisition.
  - Exclusive lock acquisition ensures that transaction partial results are not exposed to readers.

### Phase 4: Read Consistency Protocol
- [ ] **4.1** `ReadConsistencyManager` class
  - `VirtualSublogReplayState` struct using sketch arrays for key freshness tracking and sequence number frontier computation.
  - Provides APIs for updating sequence numbers at key or virtual sublog granularity.
  - Tracks version to maintain prefix consistency during replica reconfiguration events.

- [ ] **4.2** Session based prefix consistency enforcement
  - Implement `ConsistentReadGarnetApi` and `TransactionalConsistentReadGarnetApi` to allow the jitter to optimize operational calls.
  - Define callbacks to enforce consistent read protocol (e.g. `ValidateKeySequenceNumber`, `UpdateKeySequenceNumber`).
  - Session level `ReplicaReadSessionContext` struct used to `maximumSessionSequenceNumber` metadata (i.e. `sessionVersion`, `lastHash`, `lastVirtualSublogIdx`) to enforce prefix consistency when is stable or during recovery

### Phase 6: Prefix consistent recovery
- [ ] **5.1** Commit operation
  - Occurs in unison across alls sublogs. AutoCommit disabled and triggered at the `GarnetLog` layer instead of within `TsavoriteLog` to control across sublogs commit.
  - Commit adds cookie tracking the timestamp value of when commit occurred to enforce prefix consistent recovery.

- [ ] **5.2** `RecoverLogDriver` implementation
  - Independent iterators with shared bounds.
  - Record filtering by `sequenceNumber < untilSequenceNumber`.
  - Build `ReadConsistencyManager` state at recovery to initialize `SequenceNumberGenerator`.
  - Allow intra-page parallel recovery using scan, BulkConsume interface.

### Phase 6: Testing & Validation
- [ ] **6.1** Replication base tests passing with multi-log enabled
- [ ] **6.2** Replication diskless sync tests passing with multi-log enabled

TODO:
- [ ] Ensure transaction replay releases locks in the event of an exception
- [X] Add timestamp tracking at primary per physical sublog.
- [X] Ensure timestamp tracking is consistent with recovery.
- [ ] Ensure commit recovery does not recover on boundaries. 