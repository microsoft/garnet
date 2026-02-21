
# Multi-Log Architecture Overview

This PR introduces multi-log based AppendOnlyFile (AOF) support to enhance write throughput and optimize replication replay performance. While designed primarily for Garnet's cluster mode, this feature can also be used with standalone mode.

The multi-log model uses a `GarnetAppendOnlyFile` class that manages multiple `TsavoriteLog` instances (physical log instances) to:
- Shard write operations across multiple logs.
- Parallelize log scanning and shipping with multiple connections and iterators.
- Enable parallel replay at replicas through one or more tasks-per-physical-log architecture.

# Configuration Parameters

| Parameter | Purpose |
|-----------|---------|
| `AofPhysicalSublogCount` | Number of physical `TsavoriteLog` instances that back the `GarnetAppendOnlyFile` |
| `AofReplayTaskCount` | Number of virtual replay tasks per physical sublog at the replica |
| `AofTailWitnessFreq` | Background task frequency at the primary for advancing timestamps across multiple physical sublogs |

# Log Record Headers

To support multi-log functionality, two new AOF record header types are introduced in `AofHeader.cs`. These are designed as extensions to the basic header to preserve backward compatibility with single-log implementations.

## ShardedHeader
Records standalone upsert and RMW operations with an additional timestamp/sequence number field (long) indicating when the operation occurred.

## TransactionHeader
Used for coordinated operations including transactions, custom transaction procedures, and checkpointing. Contains metadata describing which physical sublogs and virtual replay tasks participate in the operation.

# Log Shipping

For each attached replica, the primary maintains an `AofSyncDriver` instance responsible for coordinating log shipping. When configured with `k` physical logs, the driver manages:

- **k AofSyncTasks**: Each task maintains an independent log iterator and ships log pages for one physical sublog across the network
- **AdvancePhysicalSublogTime**: A background task that advances timestamps for idle sublogs, ensuring time progresses uniformly across all physical logs

If any task fails, the replication connection drops and replay stops. Recovery is triggered either manually via the `REPLICAOF` command or automatically after the `ClusterReplicationReestablishmentTimeout` expires, matching single-log behavior.

# Log Replay
The replica receives log pages from the primary and replays them.
The order in which the replay operation executes is as follows:
1. When configured with k-sublogs the replica will establish k-(replay) resp server sessions.
2. Each replay session receives an initialization message and establishes a ReplicaReplayDriver instance which maintains metadata associated with the physical sublog it owns and is responsible for managing and replaying.
3. When configured with multiple replay tasks, the corresponding ReplicaReplayDriver instance spawns an equivalent number of background ReplicaReplayTasks.
4. Every ReplicaReplayTask will scan each log page and identify the record it is responsible of replaying by inspecting the corresponding AofHeader metadata.

With `k` physical sublogs and `m` replay tasks per sublog configured at the replica, the system effectively operates with `k × m` virtual sublogs.
In that case, every replayed operation moves time ahead for the associated key and corresponding virtual sublog.

NOTE: If any of the ReplicaReplayDriver instances encounters an error during replay it will signal the shared cancellation token in order to cancel the other ReplicaReplayDrivers associated with the same AOF replay unit.

## Standalone Write Operation Replay

Standalone write operations can be replayed in parallel with minimal coordination. Unlike single-log replay, each operation updates the timestamp tracker sketch in `ReadConsistencyManager` for the affected key. Operations execute using `BasicContext` instances from the Tsavorite store.

## Transaction Operation Replay
There are two types of transactions supported by Garnet and which need to be .
Mainly multi-exec transactions and custom transaction procedures.

Each replay instance maintains its own `ReplayContext` within the `AofProcessor` to coordinate transaction replay. The workflow proceeds as follows:

1. Upon encountering a `TxnStart` record, each participating replay task creates an entry in the `activeTxns` dictionary (indexed by session ID) to collect operations for that transaction.
2. Operations are gathered and mapped to their respective replay tasks based on the key hash.
3. When a `TxnCommit` record is encountered, all participating tasks acquire exclusive locks on their respective keys.
4. Tasks execute their transaction operations in parallel while holding locks.
5. Once all tasks complete, the transaction is committed, locks are released, and updates become visible to read requests.

**Custom Transaction Procedures**: A single replay task (from the set of participants) coordinates execution. Operations execute after lock acquisition, and all participants wait for completion before proceeding with subsequent operations.
This ensures atomicity and consistency across distributed replay tasks.

# Consistent Read Protocol
Parallel replay leverages a timestamp-based read protocol to ensure prefix consistency at replicas. The protocol uses sequence numbers generated at the primary for every AOF record, tracked via a `ReadConsistencyManager` on each replica.

Each replica session maintains a `MaximumSessionTimestamp` (MST) to control read operations. The consistent read protocol executes three phases:

1. **Prepare Phase**: Verify the key's timestamp (Ts) against the current MST. If Ts ≤ MST, proceed; otherwise, wait until Ts catches up.
2. **Read Phase**: Execute the read operation through Tsavorite to retrieve the value.
3. **Update Phase**: Update the MST with the key's timestamp to ensure all subsequent reads reflect prefix consistency.

# GarnetAppendOnlyFile Overview
`GarnetAppendOnlyFile` is the primary class orchestrating multi-log AOF functionality. It manages three core components: `GarnetLog` for coordinating operations across physical sublogs, `ReadConsistencyManager` for tracking and enforcing read consistency across replicas, and `SequenceNumberGenerator` for assigning monotonic timestamps to write operations. The class also exposes methods for creating/updating consistency managers and resetting sequence number generators during failovers.

## GarnetLog
`GarnetLog` provides a unified interface for managing append-only logs in both single-log and multi-log modes. It abstracts over `SingleLog` and `ShardedLog` implementations, exposing operations for recovery, commits, truncation, and scanning. Multi-log mode maintains independent log iterators per physical sublog and coordinates commits across all sublogs. A hash function maps write keys to physical sublog indices for sharding.

## Read Consistency Manager
The `ReadConsistencyManager` maintains prefix consistency by tracking key timestamps across virtual sublogs. It holds an array of `VirtualSublogReplayState` instances (one per virtual sublog), each storing a sketch of key sequence numbers.

### Data Structure
- **Sketch**: A 32K-slot array (2^15) per virtual sublog that maps key hashes to their sequence numbers.
- **Max Sequence Number**: The global maximum sequence number observed across all keys in a virtual sublog.
- **Wait Queue**: Readers waiting for replay to progress are enqueued and signaled when conditions are met.

### Key Operations

**Tracking Updates**: When a replay operation updates a key, the corresponding virtual sublog's sketch is updated with the new sequence number, and the global max is advanced if needed.

**Frontier Sequence Number**: Rather than tracking just per-key timestamps, the manager computes a "frontier" that combines:
- The key's specific sequence number (from the sketch)
- The virtual sublog's global maximum (ensuring time always moves forward)

This handles cases where a key hasn't been updated in a batch but time has advanced on the sublog.

**Consistent Read Coordination**: 
- **Prepare Phase**: `ConsistentReadKeyPrepare()` checks if the key's frontier sequence number is ahead of the session's maximum sequence number. If not, the reader is enqueued in a wait queue until replay catches up.
- **Update Phase**: `ConsistentReadSequenceNumberUpdate()` updates the session's maximum sequence number after the actual read to prevent underestimating the read timestamp.

**Waiter Signaling**: When timestamps progress, the manager processes the wait queue, comparing each waiter's required timestamp against the current frontier. Satisfied waiters are signaled to proceed; unsatisfied ones are re-enqueued.

**Versioning On Attach**: A replica may attach to a new primary at runtime. Each time this happens, `ReadConsistencyManager` increments its version, causing active read sessions to reset their metadata so subsequent reads remain prefix-consistent with the new replication stream.

# Prefix-Consistent Recovery

In multi-log mode, recovery replays each physical sublog in parallel, but only up to a prefix that is known to be committed across *all* sublogs.

- **Safe replay bound (`untilSequenceNumber`)**: `GarnetLog.RecoverLatestSequenceNumber()` reads the recovered commit cookie (`RecoveredCookie`) from every physical sublog. Each commit marker encodes the latest committed sequence number, and recovery uses the **minimum** sequence number across sublogs as `untilSequenceNumber`. If any sublog is missing a recovered cookie, the system cannot compute a prefix-consistent bound.

- **Per-sublog replay drivers**: `AofProcessor.Recover()` creates a `RecoverLogDriver` per physical sublog. Each driver scans `[BeginAddress, untilAddress]` via `ScanSingle(..., scanUncommitted: true, recover: false)` and replays records while skipping any entry with `sequenceNumber > untilSequenceNumber`.

- **Intra-page parallelism**: When `AofReplayTaskCount > 1`, `RecoverLogDriver` spawns multiple background replay tasks that cooperatively process each scanned page. Tasks only replay records assigned to them (`AofProcessor.CanReplay(...)`) and still enforce the `untilSequenceNumber` threshold. Commit metadata entries are applied by a single task. After a page is processed, each task updates `ReadConsistencyManager` with the maximum sequence number observed for its virtual sublog to keep read-consistency state aligned with recovery progress.


TODO:
- [ ] Role command does not work as expected with SE Redis.
<ul><ul>
  Message: 
    Unexpected response to ROLE: Array: 3 items

  Stack Trace: 
    ClusterTestUtils.RoleCommand(IPEndPoint endPoint, ILogger logger) line 2623
    ClusterReplicationBaseTests.ReplicasRestartAsReplicasAsync(CancellationToken cancellation) line 1554
    GenericAdapter`1.GetResult()
    AsyncToSyncAdapter.Await[TResult](Func`1 invoke)
    AsyncToSyncAdapter.Await(Func`1 invoke)
    TestMethodCommand.RunTestMethod(TestExecutionContext context)
    TestMethodCommand.Execute(TestExecutionContext context)
    <>c__DisplayClass1_0.<Execute>b__0()
    DelegatingTestCommand.RunTestMethodInThreadAbortSafeZone(TestExecutionContext context, Action action)
    1)    at Garnet.test.cluster.ClusterTestUtils.RoleCommand(IPEndPoint endPoint, ILogger logger) in C:\Dev\Github\vazois\test\Garnet.test.cluster\ClusterTestUtils.cs:line 2618
    ClusterReplicationBaseTests.ReplicasRestartAsReplicasAsync(CancellationToken cancellation) line 1554
    AsyncMethodBuilderCore.Start[TStateMachine](TStateMachine& stateMachine)
    ClusterReplicationBaseTests.ReplicasRestartAsReplicasAsync(CancellationToken cancellation)
</ul></ul>

- [ ] KEYS will return out of order result as observed by the PRIMARY vs the REPLICA since REPLICA replays SET in parallel.


<ul><ul>
Assume the following writes; a1,t1 and a2,t2
If the granularity of the clock is coarse (i.e. 10 sec) then it is possible for t1 == t2. In that case, we cannot differentiate on the ordering of a1 and a2 despite the fact that they might have happened one after the other.
In that situation, possible solutions include
1. Higher clock granularity/ unique sequence number generation. What is the scalability of this approach.
2. Include sessionId and dedicated counter per session to resolve ordering at the replica (i.e. consistent read) (ts, sid, ctr)
3. Use the concept of closing down timestamp. We only read at t-1 and read at t only when all sublogs have replayed t. Is that correct what are th caveats
</ul></ul>