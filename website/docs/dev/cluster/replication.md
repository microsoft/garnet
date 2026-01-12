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
When this command is issued to a specific node, depending on how the node is configured (i.e. using diskless or diskbased replication), it will follow
a synchronization workflow according to the following steps:

1. Replica initiates attach and sends its local latest checkpoint and AOF information to the primary
2. The primary will decide if the a checkpoint needs to be shipped to the replica. For diskless replication, the primary will execute a streaming checkpoint only if the replica does not have enough data to partially sychronize using its local AOF.
3. The primary signals to the replica to recover from its latest checkpoint and replay its local AOF.
4. After recovery the replica, signals back to the primary to let it know that it is ready to start receiving the AOF pages not contained in the checkpoin that it recovered from.
5. A background AofSyncTask is spawned to start iterating from the address beyond the one that is covered from the replicas recovered checkpoint.

The primary maintains one distinct AofSyncTask per replica, keeping track the pages it has send.

## Replication Options
Users can configure Garnet replication by adjusting the AOF parameters in garnet options as well as a number of other options.
The most important options are shown below:
- EnableAOF: Enables AOF
- AofMemorySize: Sets the total size of the AOF memory buffer after which any newly append records will spill to disk
- AofPageSize: Determines the AOF page size
- FastAofTruncate: This option is used to aggresively shift the AOF address begin address in order to prevent the AOF from spilling to disk which could result in performance degradation. This happens in safe manner and is a best effort approach, which means data will be not lost and may spill to disk if there is delay is shipping the log pages to the replica (which will prevent the begin address from shifting ahead of the sync iterator for that replica). U
- UseAofNullDevice: Treat the AOF memory buffer a circular buffer for writing AOF records, ensuring no disk IO. This can be used in combination with fast-aof-truncate but could lead to potential data loss. One should adjust the AofMemorySize to ensure there is enough space for incoming AOF records to be written and shipped towards the replica before truncating.
- CommitFrequencyMs: Write ahead logging (append-only file) commit issue frequency in milliseconds. 0 = issue an immediate commit per operation, -1 = manually issue commits using COMMITAOF command. To avoid performance degradatation when running with FastAofTruncate and/or UseAofNullDevice, one can set the CommitFrequencyMs to -1.
- AofReplicationRefreshFrequencyMs: "AOF replication (safe tail address) refresh frequency in milliseconds. 0 = auto refresh after every enqueue.
- ReplicaDisklessSync: Enable diskless sync to avoid disk write amplification when performing full synchronization when a new replica attaches. This options uses the streaming checkpoint feature to ship the store keys to the replica when full synchronization is required.
- ReplicaDisklessSyncDelay: How long to wait between sync requests to allow for other replicas to attach in parallel hence amortize the cost of the streaming checkpoint.
- OnDemandCheckpoint: This options enables taking an on demand checkpoint when a replica attaches. It used to improve the sychronization performance in the event the AOF log has grown too large or when the AOF has been truncated and synchronization requires a fresh checkpoint to correctly synchronize with the replica. If this option is disabled and the AOF log gets truncated, not having enabled this option could break the attach process unless UseAofNullDevice or FastAofTruncate is enabled since in that case we expect data loss.
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
This approach requires a locking mechanism to esnure that actively read checkpoints (those part of the full sychronization) will not be deleted
The locking logic is for checkponts is implemented in ```CheckpointStore.cs```.
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
The only parameter that controls how many replicas are synced in parallel is ```ReplicaDisklessSyncDelay``` which delays replication sync to allow more replicas to sync together (i.e. at startup where a primary migth need to be configured with few replicas).
Once the specified delay period passes the primary will examine all the metadata trasmitted by the associated replicas and decide which ones require full vs partial synchronization.
Those requiring partial synchronization will be released immediately and a new AOF sync task will be created for them to start receiving the associated AOF records
The rest will be fully synchronized using the StreamingCheckpoint primitive.
Before starting the full sychronization, the primary broadcasts FLUSHDB command to cleanup the replica store so there is no conflicts with the primary store.
This streaming checkpoint primitive utilizes and iterator over the TsavoriteStore and it broadcasts batches of kv pairs to the replica.
The replica will receive those pairs and insert them into its store.
At completion the replica sets its version number to be equal the the version number of primary.
Finally, the primary executes the partial synchronization workflow which includes steps to validate the integrity of the AOF and the creation of the AOF sync tasks to start streaming the corresponding AOF records to each replica.

# Sharded Log Feature
Garnet replication leverages the AppendOnlyFile (AOF) implementation to stream update operations to the corresponding replica.
The Garnet's AOF implementation uses a single instance of TsavoriteLog to record update operations as they occur at the primary.
Writing, streaming and replaying the AOF in order to support replication is single threaded operation
This is in contrast to Garnet's native multi-threaded architecture and does not scale well.

This motivated the development of a sharded AOF implementation that leverages multiple physical sublogs (i.e. separate TsavoriteLog instances) to scale writes at the primary and parallel replay at the replica.
This implementation works alongside a read consistency protocol running at the replica, which is required to guarantees prefix consistent reads because sublog replay happen asynchronously potentially exposing non-prefix consistent content.
The read consistency protocol relies on virtual timestamps (i.e. sequence numbers) to order


## Sharded AOF architecture
NOTES:
- Varying number of tsavoritelog instances
- Writes are recorded to the log based on hashing the key associated to that write
- Records keep track of a sequence number
- There exist a class of coordinated operations that require writing to a subset of all physical logs
- Transactions write to logs associated with the keys involve in the corresponding transaction
- 


## Read Consistency Protocol

MaxSessionSequenceNumber (MSSN)
var sublogIdx = HASH(k1)
var k1sn = GetKeySequenceNumber(k1)

do
{
    var k1sn = GetKeySequenceNumber(k1)
}while(k1sn < mssn)

v1 = Read k1

k1sn = GetFrontierSequenceNumber(k1, sublogIdx)
    => Max(GetKeySequenceNumber(k1), GetMaxSublogSequenceNumber(sublogIdx))

mssn = MAX(k1sn, v1)

