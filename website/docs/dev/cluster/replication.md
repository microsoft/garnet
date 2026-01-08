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
When the OnDemandCheckpoint flag is used the primary might initiate the process of taking a new checkpoint.
Any checkpoint can be shared across attaching replicas if it is still valid at the moment those replicas attach.
However, because when a new checkpoint is taken we make a best effort to delete older checkpoint files, at replica synchronization we implement a mechanism to lock checkpoints that are actively being used for synchronization
The locking logic is for checkponts is implemented in ```CheckpointStore.cs```.
If full synchronization is necessary the primary will send the latest checkpoint files to the replica in chunks.
It will then signal the replica to recover its latest checkpoint and replay the AOF log if necessary.
When this process is complete, the primary will initiate a permanent background AofSyncTask by establishing an iterator over its own AOF, starting from the checkpoint covered AOF address.




## Diskless Full Synchronization Details

