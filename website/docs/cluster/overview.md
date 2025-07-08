---
id: overview
sidebar_label: Overview
title: Overview
slug: overview
---

# Garnet Cluster Overview

Garnet cluster provides an easy and scalable way to operate Garnet across multiple nodes.
It supports multiple features including **scaling up/down**, **data migration**, and **data replication**.
This document presents an overview of the sharding mechanics and cluster configuration.

## Garnet Cluster Sharding

The cluster's key space is split into 16384 slots.
Any given slot is owned by a single primary Garnet instance, and any given key maps to only a single slot.
If chosen to setup Garnet with replication, a Garnet instance can operate as a replica and serve read-only requests 
for the keys hashing to slots owned by their corresponding primaries.
All single key operations are supported in Garnet cluster.
However, multikey operations are processed only if all keys involved are mapped to a single slot.
Users can overcome this restriction by using hashtags to map differents keys to the same slot.
If a key contains \{...\}, only the string enclosed is used for the hash function computation.
For example, keys \{abc\}xyz and xyz\{abc\} will hash in the same hashlot.

### Client Redirection

Clients can connect to any node in the cluster and issues single/multi-key operations or any type of
cluster management operations.
The receiving node processes single/multi-key operations by calculating the hashlot value(s) for the key(s) associated with the corresponding operation 
and responds in one of the following ways:

- If the slot is owned by the receiving node, it performs the actual operation as expected from standalone Garnet.
- If the slot is owned by the another node, it responds with -MOVED \<slot\> \<address\> \<port\>
- If the receiving node is a replica, it will serve only read requests to the slots owned by its primary and redirect any write requests to the primary using -MOVED message.
- If the slot is owned by the receiving node and that slot is migrating then:
	- If the key exists, then read requests are served as normal while write requests return -MIGRATING.
	- If the key does not exist, then read and write requests return -ASK \<slot\> \<address\> \<port\>.
- If the slot is owned by the another node and the receiving node is the target of the migration operation then:
	- Read and write requests are served only if ASKING issued before hand. Note that write safety is not ensured if ASKING is used, so clients should take extra care when using it.

## Garnet Cluster Configuration

Every Garnet cluster instance retains a persistent local copy of the cluster configuration.
Configuration updates are either directly applied through cluster commands to a specific node
or propagated through the gossip protocol.

The cluster configuration contain slot assignment information and information about every known node
in the cluster.

For more information about the cluster configuration please see the description of *CLUSTER NODES* command.

## Control Plane

It is important to keep in mind that Garnet's cluster mode design is currently _passive_: this means that it does not implement leader election, and simply responds to cluster 
commands issued by a separate _control plane_. Users have to deploy a control plane (such as an operator in Kubernetes 
[[example](https://github.com/OT-CONTAINER-KIT/redis-operator)]) to detect failures and request failover. Garnet does not itself yet provide a control plane implementation. 
All commands necessary for building a robust control plane are included in Garnet, and our partners have built and deployed 
production-quality control plane implementations for internal use with Garnet.

This separation of concerns allows users the flexibility of deploying Garnet over a variety of fabrics such as [Kubernetes](https://kubernetes.io/), 
[Virtual Machine Scale Sets](https://learn.microsoft.com/en-us/azure/virtual-machine-scale-sets/overview), and [Service Fabric](https://azure.microsoft.com/en-us/products/service-fabric). 
This allows users to leverage the production-grade leader election features available in these systems, as well as components such as reliable cloud databases for metadata storage, 
for managing the Garnet cluster.

## Creating a Garnet Cluster

Before showing how to create a Garnet cluster, we present below a brief overview of the most important parameters associated
with running a basic Garnet cluster deployment.

- **--port**: Port to run server on. Same port is used for serving queries and internode communication.
- **--bind**: IP address to bind server
- **--checkpointdir**: Used to specify the path to checkpoint location and cluster config when --cluster option is enabled.
- **--cluster**: Enable cluster mode
- **--cluster-timeout**: Internode communication timeout.
- **--gossip-delay**: Gossip protocol delay to broadcast send updated configuration or ping known nodes.
- **--gossip-sp**: Percent of cluster nodes to gossip with at each gossip iteration

To create a Garnet cluster you need first to run Garnet instances using the `--cluster` option as shown below.
Using the `--checkpointdir` option is optional. It is include in this example to avoid any conflicts between the configuration
files.
If you don't specify the `--checkpointdir` option Garnet will you the startup folder to save any configuration associated

```bash
	GarnetServer --cluster --checkpointdir clusterData/7000 --port 7000
	GarnetServer --cluster --checkpointdir clusterData/7001 --port 7001
	GarnetServer --cluster --checkpointdir clusterData/7002 --port 7002
```

Once the instance are up and running, you can use any kind of redis compatible client to initialize
the cluster to assign slots.

For the above example, we use redis-cli to demonstrate how a cluster is initialized

```bash
	redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 --cluster-yes
```

Once the above initialization completes, the cluster is ready to process client queries.
An example of how one may use the initialized cluster is shown below:

```bash
PS C:\Dev> redis-cli -p 7000
127.0.0.1:7000> cluster nodes
ee337ebd15255c163b0d6faa4d055cdb26215938 127.0.0.1:7000@17000,hostname01 myself,master - 0 0 1 connected 0-5460
4f86082c3d3250c0dba0f925e71963d46974fbca 127.0.0.1:7002@17002,hostname02 master - 0 0 3 connected 10923-16383
cf333332b44a32fa70c30862b6d9535e9bac19f9 127.0.0.1:7001@17001,hostname03 master - 0 0 2 connected 5461-10922
127.0.0.1:7000> cluster keyslot x
(integer) 16287
127.0.0.1:7000> get x
(error) MOVED 16287 10.159.2.73:7002
127.0.0.1:7000> set x 1234
(error) MOVED 16287 10.159.2.73:7002
127.0.0.1:7000> cluster keyslot wxz
(integer) 949
127.0.0.1:7000> set wxz 1234
OK
127.0.0.1:7000> get wxz
"1234"
127.0.0.1:7000>
```

Alternatively, it is possible to configure the cluster manually or programmatically using a combination of the CLUSTER MEET, CLUSTER SET-CONFIG-EPOCH, CLUSTER ADDSLOTS or CLUSTER ADDSLOTSRANGE, and CLUSTER REPLICATE commands.
The following example demonstrates how to initialize two nodes, assign all slots to one of them, introduce the nodes to each other, and configure the second node as a replica of the first.

```bash
PS C:\Dev> redis-cli -p 7000 cluster nodes
d6c7f0c2a2839efad9a6578647d6456f90845836 127.0.0.1:7000@17000,localhost myself,master - 0 0 0 connected
PS C:\Dev> redis-cli -p 7001 cluster nodes
c65d29d420960fe5213ccb87ce1f74d7ee6ca4f1 127.0.0.1:7001@17001,localhost myself,master - 0 0 0 connected
PS C:\Dev> redis-cli -p 7000 cluster set-config-epoch 1
OK
PS C:\Dev> redis-cli -p 7001 cluster set-config-epoch 2
OK
PS C:\Dev> redis-cli -p 7000 cluster addslotsrange 0 16383
OK
PS C:\Dev> redis-cli -p 7000 cluster nodes
d6c7f0c2a2839efad9a6578647d6456f90845836 127.0.0.1:7000@17000,localhost myself,master - 0 0 1 connected 0-16383
PS C:\Dev> redis-cli -p 7001 cluster nodes
c65d29d420960fe5213ccb87ce1f74d7ee6ca4f1 127.0.0.1:7001@17001,localhost myself,master - 0 0 2 connected
PS C:\Dev> redis-cli -p 7000 cluster meet 127.0.0.1 7001
OK
PS C:\Dev> redis-cli -p 7000 cluster nodes
d6c7f0c2a2839efad9a6578647d6456f90845836 127.0.0.1:7000@17000,localhost myself,master - 0 0 1 connected 0-16383
c65d29d420960fe5213ccb87ce1f74d7ee6ca4f1 127.0.0.1:7001@17001,localhost master - 638875895159094739 0 2 connected
PS C:\Dev> redis-cli -p 7001 cluster nodes
c65d29d420960fe5213ccb87ce1f74d7ee6ca4f1 127.0.0.1:7001@17001,localhost myself,master - 0 0 2 connected
d6c7f0c2a2839efad9a6578647d6456f90845836 127.0.0.1:7000@17000,localhost master - 638875895161944429 0 1 connected 0-16383
PS C:\Dev> redis-cli -p 7001 cluster replicate $(redis-cli -p 7000 cluster myid)
OK
PS C:\Dev> redis-cli -p 7000 cluster nodes
d6c7f0c2a2839efad9a6578647d6456f90845836 127.0.0.1:7000@17000,localhost myself,master - 0 0 1 connected 0-16383
c65d29d420960fe5213ccb87ce1f74d7ee6ca4f1 127.0.0.1:7001@17001,localhost slave d6c7f0c2a2839efad9a6578647d6456f90845836 638875895309129088 638875895309125478 3 connected
PS C:\Dev> redis-cli -p 7001 cluster nodes
c65d29d420960fe5213ccb87ce1f74d7ee6ca4f1 127.0.0.1:7001@17001,localhost myself,slave d6c7f0c2a2839efad9a6578647d6456f90845836 0 0 3 connected
d6c7f0c2a2839efad9a6578647d6456f90845836 127.0.0.1:7000@17000,localhost master - 638875895311833070 638875895311830232 1 connected 0-16383
PS C:\Dev>
```

Note that the use of redis-cli is not required; any client compatible with the RESP protocol may be used to execute the aforementioned commands.

