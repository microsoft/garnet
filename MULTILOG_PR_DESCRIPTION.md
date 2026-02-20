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


NOTES:
## Prefix Consistent Single Key Read Protocol
The read protocol leverages timestamp-based sequence numbers associated with every write operation to delay reads as necessary to ensure prefix consistency per session.
Every session maintains a read session state which contains the maximum sequence number ($T_ms$) it observed across all previous reads.
The read occurs through the following steps
1. (Key Freshness Verification) Determine the frontier sequence number T_k (max of (key sequence number, sublog max sequence number)) for a given key K and wait until T_ms < T_k.
This ensures that any updates that have happened to K until T_ms are visible.
2. Perform actual read
3. (Read Session Time Advance) Update T_ms by retrieving the latest key sequence number for K. This happens after the actual read because verification happens outside epoch protection and the value read might be associated with a later timestamp.
Note that this is still safe since the timestamp cannot be less that T_k since the timestamps are monotonically generated.
In addition, note that the key freshness check allows reads to proceed only after T_ms has passed (no reads of keys at the boundary timestamp) since we cannot order writes with the same sequence number.


## Prefix Consistent Batch Read
Reading a batch of keys relies on the same principles as reading a single key.
It is basically an extension of the single key read case, with the addition of performing a re-read operation if the state of the world has changed after the freshness verification step.
The algorithm goes as follows:

1. For every key K_i in a given batch, ensure their associated frontier timestamp  T_ms < T_ki before proceeding to the next step. Calculate T_max = max (T_k1, ... Tkn)
2. Perform the actual batch read.
3. For every key read, check that their timestamp after the read has not moved beyond T_max ((a.) i.e. T_ki <= T_max).
This works because freshness verification does not allow reads to proceed at the boundary.
Hence, if any key gets updated its timestamp will be greater than T_max.
For this reason we can detect updates by only caching the T_max value.