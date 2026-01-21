---
id: replication-dev
sidebar_label: Replication
title: Replication Overview
---

# Garnet Replication Overview

Garnet cluster mode allows users to setup a replication stream by assingning certain nodes of the cluster to be replicas/secondaries of another primary.
The replicas are configured by default to serve only reads, redirecting any write request to their primary.
Replicas server only a single primary but any primary can have multiple replicas.
Replicas aim to be an exact copy of the primary by receiving and replaying individual operations through log shipping.
For this reason, the nodes excercising replication need to be setup with the AOF feature enabled.


# Garnet Replication Attach/Sync Workflow
Every node in the cluster starts as a primary node and it can be assigned to be a replica by using either CLUSTER REPLICATE or REPLICAOF commands.
When this command is issued to a specific node, depending on how the node is configured (i.e. using diskless or disk-based replication), it will follow
a synchronization workflow according to the following steps:

1. Replica initiates attach and sends its local latest checkpoint and AOF information to the primary
2. The primary will decide if the a checkpoint needs to be shipped to the replica. For diskless replication, the primary will execute a streaming checkpoint only if the replica does not have enough data to partially synchronize using its local AOF.
3. The primary signals to the replica to recover from its latest checkpoint and replay its local AOF.
4. After recovery the replica, signals back to the primary to let it know that it is ready to start receiving the AOF pages not contained in the checkpoit that it recovered from.
5. A background AofSyncTask is spawned to start iterating from the address beyond the one that is covered from the replicas recovered checkpoint.

The primary maintains one distinct AofSyncTask per replica, keeping track the pages it has send.

## Replication Options
Users can configure Garnet replication by adjusting the AOF parameters in garnet options as well as a number of other options.
The most important options are shown below:
- EnableAOF: Enables AOF
- AofMemorySize: Sets the total size of the AOF memory buffer after which any newly append records will spill to disk
- AofPageSize: Determines the AOF page size
- FastAofTruncate: This option is used to aggressively shift the AOF address begin address in order to prevent the AOF from spilling to disk which could result in performance degradation. This happens in safe manner and is a best effort approach, which means data will be not lost and may spill to disk if there is delay is shipping the log pages to the replica (which will prevent the begin address from shifting ahead of the sync iterator for that replica). U
- UseAofNullDevice: Treat the AOF memory buffer a circular buffer for writing AOF records, ensuring no disk IO. This can be used in combination with fast-aof-truncate but could lead to potential data loss. One should adjust the AofMemorySize to ensure there is enough space for incoming AOF records to be written and shipped towards the replica before truncating.
- CommitFrequencyMs: Write ahead logging (append-only file) commit issue frequency in milliseconds. 0 = issue an immediate commit per operation, -1 = manually issue commits using COMMITAOF command. To avoid performance degradation when running with FastAofTruncate and/or UseAofNullDevice, one can set the CommitFrequencyMs to -1.
- AofReplicationRefreshFrequencyMs: "AOF replication (safe tail address) refresh frequency in milliseconds. 0 = auto refresh after every enqueue.
- ReplicaDisklessSync: Enable diskless sync to avoid disk write amplification when performing full synchronization when a new replica attaches. This options uses the streaming checkpoint feature to ship the store keys to the replica when full synchronization is required.
- ReplicaDisklessSyncDelay: How long to wait between sync requests to allow for other replicas to attach in parallel hence amortize the cost of the streaming checkpoint.
- OnDemandCheckpoint: This options enables taking an on demand checkpoint when a replica attaches. It used to improve the synchronization performance in the event the AOF log has grown too large or when the AOF has been truncated and synchronization requires a fresh checkpoint to correctly synchronize with the replica. If this option is disabled and the AOF log gets truncated, not having enabled this option could break the attach process unless UseAofNullDevice or FastAofTruncate is enabled since in that case we expect data loss.
- AllowDataLoss (UseAofNullDevice || (FastAofTruncate && !OnDemandCheckpoint): This is an internal flag computed by base flags as indicated. It is used to skip AOF integrity check when an AOF sync task is started. Using this combination of options should be used with caution

## Diskbased Attach/Sync Details

The diskbased attach sync workflow is implemented in ```ReplicaDiskBasedSync.cs``` and is used both for attaching through CLUSTER REPLICATE, REPLICAOF and on startup recovery of a given node.
Before signaling the primary to start the attach/sync workflow, the node resets its state to prepare for synchronization.
This includes the following
1. Reset replay tasks if the node was already a replica and now is being assigned to a new primary
2. Reset replication offset value
3. Reset any active AOF sync tasks, if the node was a primary before being turned to a replica.
4. Reset/flush database to empty to avoid any synchronization conflicts
Every replica transmits its persistence data to the corresponding primary and waits for the attach process to complete.
If the connection breaks or an error is returned from the primary, the process releases any locks, resets the replica back to being a primary and responds to the caller with an error message.

When using diskbased replication, every replica attaching to a given primary transmits its latest checkpoint information and the begin and end addresses of its own AOF log.
A checkpoint is identified by its version number, a replication-id and the minimum AOF address it covers.
The primary uses that information to decide if the replica requires partial or full synchronization by comparing it to the latest available valid checkpoint.
From the primary's perspective a valid checkpoint is one for which the current begin AOF address is less or equal to the checkpoint covered AOF address.
In addition to that, the checkpoint must be of the same version and history for the nodes involved.
When the OnDemandCheckpoint flag is used the primary might initiate the process of taking a new checkpoint.
Any on-demand checkpoint can be shared across attaching replicas if it is still valid at the moment those replicas attach.
When a new checkpoint is created, we make a best effort approach to delete older checkpoints at the primary.
This approach requires a locking mechanism to ensure that actively read checkpoints (those part of the full synchronization) will not be deleted
The locking logic is for checkpoints is implemented in ```CheckpointStore.cs```.
The attaching replica communicates to the corresponding primary which creates a ```ReplicaSyncSession.cs``` for every attaching replica.
By examining the metadata of the replica the primary decides if a full or partial synchronization is needed
If full synchronization is necessary the primary will send the latest checkpoint files to the replica in chunks.
It will then signal the replica to recover its latest checkpoint and replay the AOF log if necessary.
For partial synchronization, the primary signals the replica to recover from its local checkpoint skipping the step for sending the latest checkpoint
When the recovery is complete, the primary will initiate a permanent background AofSyncTask by establishing an iterator over its own AOF, starting from the checkpoint covered AOF address.
At startup of the AOF sync task, we validate the AOF integrity unless the nodes are configured in such a way where data loss is inevitable (see AllowDataLoss).
Integrity validation is required to ensure that the start address requested by the replica has not been truncated and the AOF sync task can start streaming the AOF records to the replica from.

## Diskless Attach/Sync Details
Diskless synchronization works similar to the diskbased approach.
Its major difference is that it does not require a disk checkpoint.
It leverages the Streaming Checkpoint primitive to scan and transmit the kv pairs from the underlying Tsavorite store.
The diskless attach/sync workflow is implemented at the primary within ```ReplicaSyncManager.cs```.
The replica side implementation is implemented within ```ReplicaDisklessSync.cs```
The attaching replica transmit their persistence information (i.e. AOF start and tail address and store version).
As opposed to the disk-based approach, the attaching replicas are grouped and synced together.
There is no limit on the number of replicas that can be synced in parallel.
The only parameter that controls how many replicas are synced in parallel is ```ReplicaDisklessSyncDelay``` which delays replication sync to allow more replicas to sync together (i.e. at startup where a primary might need to be configured with few replicas).
Once the specified delay period passes the primary will examine all the metadata transmitted by the associated replicas and decide which ones require full vs partial synchronization.
Those requiring partial synchronization will be released immediately and a new AOF sync task will be created for them to start receiving the associated AOF records
The rest will be fully synchronized using the StreamingCheckpoint primitive.
Before starting the full synchronization, the primary broadcasts FLUSHDB command to cleanup the replica store so there is no conflicts with the primary store.
This streaming checkpoint primitive utilizes and iterator over the TsavoriteStore and it broadcasts batches of kv pairs to the replica.
The replica will receive those pairs and insert them into its store.
At completion the replica sets its version number to be equal the the version number of primary.
Finally, the primary executes the partial synchronization workflow which includes steps to validate the integrity of the AOF and the creation of the AOF sync tasks to start streaming the corresponding AOF records to each replica.

# Sharded Append-Only-File Feature
Garnet replication leverages the Append-Only-File (AOF) implementation to stream update operations to the corresponding replica.
The Garnet's AOF implementation uses a single instance of TsavoriteLog to record update operations as they occur at the primary.
Writing, streaming and replaying the AOF in order to support replication is single threaded operation
This is in contrast to Garnet's native multi-threaded architecture and does not scale well.

This motivated the development of a sharded AOF implementation that leverages multiple physical sublogs (i.e. separate TsavoriteLog instances) to scale writes at the primary and parallel replay at the replica.
This implementation works alongside a read consistency protocol running at the replica, which is required to guarantees prefix consistent reads because sublog replay happen asynchronously potentially exposing non-prefix consistent content.
The read consistency protocol relies on virtual timestamps (i.e. sequence numbers) to indicate write order across sublogs.
These timestamps are used to ensure prefix consistency per Garnet session.

## Sharded AOF architecture
Garnet can be configured to use the sharded AOF implementation by adjusting the following configuration parameters;
1. AofPhysicalSublogCount:
    This parameter controls the number of TsavoriteLog instances used by the GarnetAppendOnlyFile implementation.
    Its value ranges between 1 and 64, with 1 being the default value which maps to the legacy single log implementation    
2. AofRefreshPhysicalSublogTailFrequencyMs:
    This parameter control the background refresh tail task that spawns only when the Garnet instance is configured to use more than physical sublogs.
    This task is required to keep moving time forward for sublogs that are not being actively written, in order to ensure the consistency protocol works correctly.
3. AofReplayTaskCount:
    This parameter controls the number of replay tasks that can used per physical sublog.
    Its value ranges between 1 and 256, with the default value being 1. The combination of this default value and the AofPhysicalSublogCount default maps to the legacy single log implementation.

### GarnetAppendOnlyFile

This class implements Garnet's AOF offering an API to interact with the physical
sublog instances and ensure read prefix consistency.
Its most important members are
1. SequenceNumberGenerator
   This class implements the API used to generate sequence numbers when the 
2. ReadConsistencyManager: 
   Responsible for tracking the replayed key sequence numbers per virtual sublog and coordinating read operation to ensure read prefix consistency
3. GarnetLog
   This class implements the API associated with operating and managing a TsavoriteLog instance but extends it to seamlessly use either a single or multiple TsavoriteLog instances depending on how the Garnet instance is configured.

#### SequenceNumberGenerator

The `SequenceNumberGenerator` class is implemented by using a `baseTimestamp` and a startingOffset.
Sequence numbers are generated using the difference of the baseTimestamp from the current timestamp offseted by the `startingOffset`.
The starting offset is used to eliminate clock divergence between nodes and on recovery it is initialized as the maximum sequence number calculated from the records of recovered AOF.
Note recovery can happen on startup or when a failover occurs where a replica takes over as primary making it so it needs to generate consistent sequence numbers for writes that is going to serve in the future.

#### ReadConsistencyManager
This `ReadConsistencyManager` class is instantiated when a node becomes a replica.
It is used to track the key sequence numbers of the replayed records.
This happens because replicas needs to ensure read prefix-consistency through tracking the maximum session sequence number seen across reads
and waiting for keys that are behind to become current through the progression of background replay functionality.
This protocol is triggered only when the Garnet cluster node are configured to use the sharded AOF (i.e. AofPhysicalSublogCount > 1 || AofReplayTaskCount > 1).
The `ReadConsistencyManager` uses the `VirtualSublogReplayState` struct to track the key sequence numbers seen for all replay records at a specific point in time.
Since it not efficient to track all keys, it uses a sketch tracking a limited amount of slots to which keys being replayed are matched through hashing.
This an approximation of the actual sequence number per key due to collisions.
However, it does not affect correctness it only incurs additional read latency when key moves ahead in time as a side-effect of overlapping key mappings to the same slot.
In addition, to tracking key sequence number per fixed number of slots, each `VirtualSublogReplayState` instance tracks the maximum sequence number across slots and contains a `ConcurrentQueue` used to inform subscribers when the sublog has replayed beyond a specific sequence number.

When a `RespServerSession` processes a read command, it utilizes the `ConsistentReadGarnetApi` (through the `ConsistentReadContext` and `TransactionalConsistentReadContext` for the string and object data types respectively) to call into the `ReadConsistencyManager` and validate that it can serve the read under the prefix consistency constrains.
This happens into two phases per key

1. ConsistentReadKeyPrepare Phase
    This phase occurs before the actual processing of the corresponding read operation in Tsavorite.
    Its goal is to validate the key's freshness compared to `maximumSessionSequenceNumber` as determined by any of the previous read operations.
    Specifically, the frontier((max of(sequence number for key slot, max sublog sequence))) sequence number  of a key is established and compared against `maximumSessionSequenceNumber`.    
    Otherwise, a waiter instance is established and added to the specific `VirtualSublogReplayState` instance.
    The waiter is released by the associated background replay task when the sequence number progresses beyond the minimum threshold as established by the waiter instance
2. ConsistentReadSequenceNumberUpdate step
    This phase occurs after the read has been processed.
    Its goal is to update the `maximumSessionSequenceNumber` by taking the maximum of the current `maximumSessionSequenceNumber` and the corresponding key's sequence number.
    This update happens after read to ensure that we associate the read with a pessimistic replayed sequence number.


The `ReadConsistencyManager` maintains version number that gets incremented every time a new instance is created.
Creation of a new instance happens at the startup of the attach/sync workflow to ensure that subsequent reads start
with a clean slate.
The read protocol checks maintains the current version number per session and performs the appropriate checks to ensure that every read is consistent in terms of the last seen version number of the `ReadConsistencyManager`.
This happens at `ConsistentReadKeyPrepare` phase before validating the freshness of the key being read.
Read prefix consistency is guaranteed for a given batch of reads because reading a batch happens under epoch protection.
Across requests, the version of the `ReadConsistencyManager` may change with the database version change, so prefix read consistency follows the same rules as if a new database was recovered while a client was connected.

### GarnetLog
The `GarnetLog` instance implements an API that offers the same functionality as `TsavoriteLog` extended to support operations across multiple instances (`ShardedLog`).
The functionality that had to be extended to support multiple `TsavoriteLog` instances is as follows:
1. Metadata Operations
    These include operations that retrieve `TsavoriteLog` metadata such as `BeginAddress`, `TailAddress` etc properties. For `ShardedLog`, these operations return a vector of address in a form of a struct defined as `AofAddress`.
2. Initialize Operations
    These operations require initializing the state of the log and often require an input int the form of an address. For `ShardedLog`, the parameter passed is a value of `AofAddress`.
3. Commit Operations
    These operations happens in unison across each sublog instance. For variants that require awaiting on a Task, the `ShardedLog` implementation uses the WheAll primitive for all tasks associated with each sublog.
4. Truncate Operations
    For `ShardedLog`, truncation happens by passing an `AofAddress` parameter and applying the truncation based on the index of the sublog being truncated.
5. Enqueue Operations
    Enqueue operations are performed either by providing the index of the log to enqueue to or through computation of the index by hashing the key associated with that record.
    The first approach is used for coordinated operations that are applied and replayed across multiple physical logs such as transactions or checkpoint.
    The second approach is used to record individual operations to the store's key value pair, specifically Upsert, RMW and Delete operations are recorded through this API for both string and object data.

The `GarnetLog` instance offers also a locking mechanism to atomically insert record headers for coordinated operations. This locking mechanism works by preventing enqueue for coordinated operations that run in parallel.


### AOFSyncDriver

As mentioned previously, at completion of the attach workflow, the primary will spawn background tasks responsible for shipping the AOF pages to the replica.
For every connected replica, the primary creates an instance of an `AofSyncDriver`, which manages the `AofSyncTask` responsible for shipping each physical sublog's pages.
The `ReplicationManager` manages the `AofSynDriverStore` containing all the `AofSyncDriver` instances.
The number of `AofSyncTask` instance spawned is equal to the number of physical sublogs configured on the corresponding Garnet instance.
When a Garnet instance is configured with more than one physical sublog, a refresh tail task is also created.

#### Refresh Sublog Tail
This task is used to refresh the sublog tail by enqueuing a record with most recent sequence number to ensure that time moves forward for sublogs that have not received
any enqueues.
In fact, this task is making an effort to converge the tail sequence number for each physical sublog to ensure that the read protocol does not get stuck waiting falsely for
future updates.
For example, given the key-sequence number pairs (A,t1), (B,t2) where each one is mapped to a different physical log, if t1 < t2 and we first read B, then trying to read A
will result in the read session waiting for A to reach a sequence number t3 > t2
This could happen due to updating key A or another key, or should be forced by the refresh task which will create a record to indicate that time has moved forward.
