---
id: replication
sidebar_label: Replication
title: Replication
slug: replication
---

# Garnet Cluster Replication

Garnet cluster support asynchronous replication following a simple leader-follower (i.e. primary-replica) model.
This allows the replica to be exact copies of the primary.
Replication can be leveraged to scale reads and mitigate the effects of failures primary failures by promoting replicas to primary.
In this page, we present an overview of the replication protocol and discuss the different persistent/consistency options available to the cluster operator.

# Garnet Replication Properties

Garnet cluster implements replication through log shipping.
Primaries utilize the Append-Only-File (AOF) to record insert/update operations.
A dedicated background task iterates through the log pages and transmits them in bulk to the corresponding replica.
Replicas will iterate through the received log pages, replay all insert/update operations in order, and generate their own AOF.
When the cluster operator issues a checkpoint 

The cluster operator can issue a checkpoint to any primary.
The act of taking a checkpoint 

Primaries are free to take arbitrary checkpoints.
The act of taking a checkpoint is propagated through the AOF log to the corresponding replica.
As a side effect a given replica will also take and maintain its own checkpoints.
For this reason, an exact version of the data is identified from the following triplet:

``` bash
[replication-id, checkpoint-version, replication-offset]
```

Allowing replicas to take their own checkpoints speeds up the recovery process because in the common case replicas can recover from their own checkpoint and replay their AOF before connecting to their primary and resuming synchronization. 
On rare occasions, the replica must perform a full synchronization by retrieving the latest checkpoint from the primary.
Partial synchronization involves recovering from a local checkpoint (if any) and replaying only the primary's AOF log after the corresponding Checkpoint Covered Replication Offset (CCRO).
Full synchronization involves retrieving the latest primary checkpoint and recovering from it before starting to accept any new AOF log records.

The replica decides if it will perform either type of synchronization as follows:

1. If the primary replication-id differs from the replica replication-id, a full synchronization must occur.
2. If the primary checkpoint version differs from the replica checkpoint, a full synchronization must occur.
3. If none of the above conditions are true, replica can recover from local checkpoint and then continue replaying from the AOF offsets that is missing.

## Replication Performance vs Durability Options

Under normal workload the AOF log will keep growing with every insert/update operation until a checkpoint is taken.
Newly added AOF log records will initially reside in memory and then flushed to disk as new records are being added.
For write heavye workloads, this could severely impact query performance at the primary.
In addition, newly configured replicas, added to the cluster, could face longer synchronization times since they have to replay the AOF log records starting from the corresponding CCRO.

The cluster operator can choose between various replication options to achieve a trade-off between performance and durability.
A summary of these options is shown below:

- Main Memory Replication (MMR)
	This option forces the primary to aggressively truncate the AOF so it does not spill into disk. It can be used in combination with aof-memory option which determines the maximum AOF memory buffer size.
	When a replica attaches to a primary with MMR turned on, the AOF is not guaranteed to be truncated which may result in writes being lost.
	To overcome this issue MMR should be used with ODC.
- On Demand Checkpoint (ODC)
	This option forces the primary to take a checkpoint if no checkpoint is available when replica tries to attach and recover. If a checkpoint becomes or was availalbe and the CCRO has not been truncated, then
	the primary will lock it to prevent truncation while a replica is recovering. In this case, they AOF log could spill to disk as the AOF in memory buffer becomes full.
	Under normal operation, the AOF is truncated and no spilling to disk will happen.

Using the aforementioned options, we guarantee that no writes are lost while a replica is attaching and replication remains highly performant.
However, due to the asynchronous nature of replication, if a primary goes down before unexpectedly, the replicas might have not received the latest AOF log records, resulting in lost writes.

# Creating a Garnet Cluster with Replication

The process of setting up a cluster with replication is similar to setting up a regular cluster while also enabling the aof option.
This should done for every instance in the cluster.

```bash
GarnetServer.exe --cluster --checkpointdir='data/' --aof
```

The AOF log file is stored within the checkpointdir folder which by default is the working folder.
By default every Garnet instance will take the role of an empty primary.
The cluster operator has to initially assign the slots to a subset of those primaries.
The rest of the instances can be coverted to replicas by using the ```CLUSTER REPLICATE``` command.
An example of setting up a cluster with a single primary and 2 replicas is shown below:

```bash
PS C:\Dev> redis-cli -h 192.168.1.26 -p 7000 cluster myid
"15373cf386c5090a5f8a25f819c96b04a756381c"
PS C:\Dev> redis-cli -h 192.168.1.26 -p 7000 cluster addslotsrange 0 16383
OK
PS C:\Dev> redis-cli -h 192.168.1.26 -p 7000 cluster set-config-epoch 1
OK
PS C:\Dev> redis-cli -h 192.168.1.26 -p 7001 cluster set-config-epoch 2
OK
PS C:\Dev> redis-cli -h 192.168.1.26 -p 7002 cluster set-config-epoch 3
OK
PS C:\Dev> redis-cli -h 192.168.1.26 -p 7000 cluster meet 192.168.1.26 7001
OK
PS C:\Dev> redis-cli -h 192.168.1.26 -p 7000 cluster meet 192.168.1.26 7002
OK
PS C:\Dev> redis-cli -h 192.168.1.26 -p 7001 cluster replicate 15373cf386c5090a5f8a25f819c96b04a756381c
OK
PS C:\Dev> redis-cli -h 192.168.1.26 -p 7002 cluster replicate 15373cf386c5090a5f8a25f819c96b04a756381c
OK
PS C:\Dev> redis-cli -h 192.168.1.26 -p 7000 cluster nodes
15373cf386c5090a5f8a25f819c96b04a756381c 192.168.1.26:7000@17000,DESKTOP-BT8KHK1 myself,master - 0 0 1 connected 0-16383
5868649e4205bf6ea97e4b7a12f101c3aed96b71 192.168.1.26:7001@17001,DESKTOP-BT8KHK1 slave 15373cf386c5090a5f8a25f819c96b04a756381c 0 0 4 connected
947692ec7fb4a919ae7d0d7e314e9dcbcbd99774 192.168.1.26:7002@17002,DESKTOP-BT8KHK1 slave 15373cf386c5090a5f8a25f819c96b04a756381c 0 0 5 connected
PS C:\Dev> redis-cli -h 192.168.1.26 -p 7001 cluster nodes
5868649e4205bf6ea97e4b7a12f101c3aed96b71 192.168.1.26:7001@17001,DESKTOP-BT8KHK1 myself,slave 15373cf386c5090a5f8a25f819c96b04a756381c 0 0 4 connected
15373cf386c5090a5f8a25f819c96b04a756381c 192.168.1.26:7000@17000,DESKTOP-BT8KHK1 master - 0 0 1 connected 0-16383
947692ec7fb4a919ae7d0d7e314e9dcbcbd99774 192.168.1.26:7002@17002,DESKTOP-BT8KHK1 slave 15373cf386c5090a5f8a25f819c96b04a756381c 0 0 5 connected
PS C:\Dev> redis-cli -h 192.168.1.26 -p 7002 cluster nodes
947692ec7fb4a919ae7d0d7e314e9dcbcbd99774 192.168.1.26:7002@17002,DESKTOP-BT8KHK1 myself,slave 15373cf386c5090a5f8a25f819c96b04a756381c 0 0 5 connected
15373cf386c5090a5f8a25f819c96b04a756381c 192.168.1.26:7000@17000,DESKTOP-BT8KHK1 master - 0 0 1 connected 0-16383
5868649e4205bf6ea97e4b7a12f101c3aed96b71 192.168.1.26:7001@17001,DESKTOP-BT8KHK1 slave 15373cf386c5090a5f8a25f819c96b04a756381c 0 0 4 connected
```

There is not limit on the number of replicas attached to a given primary.
Currently, we do not support chained replication.

## Querying a Replica

By default replicas only serve read queries but can also be configure to process write requests.
This option is available by issuing once ```READWRITE``` command just before executing any write operation in a single client session.
Issuing ```READONLY``` will toggle back to serving read queries.

If a replica is set to process read-only queries, it will respond with *-MOVED* to any write requests, redirecting them to the primary that is replicating.

```bash
PS C:\Dev> redis-cli -h 192.168.1.26 -p 7001 -c
192.168.1.26:7001> set x 1234
-> Redirected to slot [16287] located at 192.168.1.26:7000
OK
192.168.1.26:7000> set x 1234
OK
192.168.1.26:7000> get x
"1234"
192.168.1.26:7000> exit
PS C:\Dev> redis-cli -h 192.168.1.26 -p 7001 -c
192.168.1.26:7001> get x
"1234"
192.168.1.26:7001> exit
PS C:\Dev> redis-cli -h 192.168.1.26 -p 7002
192.168.1.26:7002> get x
"1234"
```

## Checkpointing & Recovery

As noted above primaries can take arbirtrary checkpoints and propagate that information to the replicas.
Replicas will identify the checkpoint record in the AOF file and initiate a checkpoint on its own.
Taking a checkpoint at the primary is as easy as calling ```SAVE``` or ```BGSAVE```command.
When the checkpoint operation completes, it is recorded into the AOF log.
The replicas will receive that record and initiate its own checkpoint.

An example of checkpoint propagation is shown below:

```bash
PS C:\Dev> redis-cli -h 192.168.1.26 -p 7000
192.168.1.26:7000> lastsave
(integer) 0
192.168.1.26:7000> bgsave
Background saving started
192.168.1.26:7000> lastsave
(integer) 1706555027
192.168.1.26:7000> exit
PS C:\Dev> redis-cli -h 192.168.1.26 -p 7001
192.168.1.26:7001> lastsave
(integer) 1706555029
192.168.1.26:7001> exit
PS C:\Dev> redis-cli -h 192.168.1.26 -p 7002
192.168.1.26:7002> lastsave
(integer) 1706555028
192.168.1.26:7002>
```


## Replication Information
The system admin can track the progress of replication by retrieving the replication information section using info replication command. 
A summary of the available fields returned is shown below:

role: role of the instance being queried
connected_slaves: number of replicas connected to the instance.
master_failover_state:  The state of an ongoing failover, if any.
master_replid: replication id of this Garnet server instance.
master_replid2: secondary replication id (currently not being used).
master_repl_offset: current primary AOF offset.
store_current_safe_aof_address: AOF address covered by latest checkpoint
store_recovered_safe_aof_address:  AOF address covered by the recovered checkpoint
object_store_current_safe_aof_address: AOF address covered by latest checkpoint of object store
object_store_recovered_safe_aof_address: AOF address covered by recovered checkpoint of object store

```bash
192.168.1.26:7000> info replication
# Replication
role:master
connected_slaves:2
master_failover_state:no-failover
master_replid:dd194cb04b18b31373157e22d0ab36df9cc6a04a
master_replid2:
master_repl_offset:136
second_repl_offset:136
store_current_safe_aof_address:0
store_recovered_safe_aof_address:0
object_store_current_safe_aof_address:0
object_store_recovered_safe_aof_address:0
slave0:ip=192.168.1.26,port=7001,state=online,offset=136,lag=0
slave1:ip=192.168.1.26,port=7002,state=online,offset=136,lag=0
192.168.1.26:7000> exit
PS C:\Dev> redis-cli -h 192.168.1.26 -p 7001
192.168.1.26:7001> info replication
# Replication
role:slave
connected_slaves:0
master_failover_state:no-failover
master_replid:dd194cb04b18b31373157e22d0ab36df9cc6a04a
master_replid2:
master_repl_offset:136
second_repl_offset:136
store_current_safe_aof_address:0
store_recovered_safe_aof_address:0
object_store_current_safe_aof_address:0
object_store_recovered_safe_aof_address:0
master_host:192.168.1.26
master_port:7000
master_link_status:up
master_last_io_seconds_ago:1
master_sync_in_progress:False
slave_read_repl_offset:136
slave_priority:100
slave_read_only:1
replica_announced:1
192.168.1.26:7001>
```




