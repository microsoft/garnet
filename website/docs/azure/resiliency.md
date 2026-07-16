---
id: resiliency
sidebar_label: Resiliency
title: Resiliency in Azure Cosmos DB Garnet Cache
---

# Resiliency in Azure Cosmos DB Garnet Cache

Azure Cosmos DB Garnet Cache is designed for high availability and resilience, providing enterprise-grade uptime, data persistence, and automatic recovery capabilities. The service offers multiple layers of protection to ensure your cache remains available and your data is preserved even during infrastructure failures, maintenance events, or unexpected outages.

## Architecture Overview

Azure Cosmos DB Garnet Cache implements a distributed architecture with built-in redundancy and data persistence, deployed in a single Azure region.

- **Primary-Replica Architecture**: Each cache cluster consists of one or more shards with primary node and replica nodes
- **Asynchronous Replication**: Data is asynchronously replicated from primaries
- **Persistence (optional)**: When enabled, append-only file (AOF) logging combined with Redis Database (RDB) snapshots provides durable storage to disk, performed asynchronously without blocking
- **Automatic Failover**: Fast failover for minimal service interruption
- **Automatic Recovery**: Failed nodes recover from the latest RDB snapshot and AOF (when persistence is enabled) and from replication

![Architecture](../../static/img/azure/architecture.png)

### Data Distribution and Sharding

Azure Cosmos DB Garnet Cache uses a consistent hashing approach to distribute data across the cluster for optimal availability and load distribution. The cluster's key space is divided into 16,384 hash slots, with each slot owned by a single primary node. Any given key maps to exactly one slot, ensuring predictable data placement and efficient retrieval.

When you have multiple shards in your cluster, hash slots are evenly distributed across all primary nodes. This distribution ensures that data and load are balanced across the cluster, preventing hotspots and maximizing resource utilization. Replica nodes serve read-only requests for the keys hashing to slots owned by their corresponding primary nodes, enabling read scaling while maintaining data consistency.

This sharding strategy provides several resiliency benefits: if a primary node fails, only the keys in its assigned slots are affected, while the rest of the cluster continues operating normally. Automatic slot reassignment ensures that when nodes become unavailable, their hash slots can be redistributed to healthy nodes, maintaining cluster availability even during node failures.


## High Availability

### Replication

Azure Cosmos DB Garnet Cache supports configurable replication factors to balance availability requirements with cost. You can configure replication during cluster provisioning and it can't be changed on existing clusters.

| Configuration | Replication Factor | Total Nodes Per Shard | Use Cases |
|---------------|--------------------|-----------------------|-----------|
| **No Replication** | 1x | 1, primary only | Development, testing, cost-optimized production |
| **High Availability** | 2x | 2, one replica per primary | Mission-critical applications |
| **Optimized Read Performance** | 3x-6x | 3-6, two to five replicas per primary | Mission-critical applications requiring high read throughput |

The maximum replication factor is 6x (one primary and five replicas). Reach out to [CosmosGarnetCache@service.microsoft.com](mailto:cosmosgarnetcache@service.microsoft.com) if you need a higher limit.

The replication process is as follows

1. **Write Operations**: All writes go to the primary node first
2. **Asynchronous Replication**: The write is acknowledged once written in the primary and is asynchronously replicated to all replicas in the shard
3. **Consistency**: Eventual consistency between replicas
4. **Read Distribution**: Read operations can be distributed across primary and replica nodes

### Automatic Failovers

A failover can be either planned, such as system updates or management operations, or it can be unplanned, such as hardware failure or unexpected outages. The Azure Cosmos DB Garnet Cache automatically handles failovers for you and will promote a replica to primary after detecting one of these events.

### Multi Availability Zone Deployment

Azure Cosmos DB Garnet Cache can be configured with availability zones during provisioning in [supported Azure regions](./cluster-configuration.md#regional-availability) where there is capacity for your chosen SKU. See the list of [Azure regions with availability zone support](https://learn.microsoft.com/azure/reliability/regions-list).

If enabled, nodes are automatically distributed across multiple availability zones within the region. Primary and replica nodes are not guaranteed to be in different availability zones. If a zone goes down and all replicas for a given shard are unhealthy, the hash slots assigned to it will automatically be reassigned to healthy shards.

## Data Persistence

Data persistence is optional and is configured when you provision a cluster. A cluster runs in one of two modes — **No Persistence** (in-memory only) or **Append Only File (AOF) and Redis Database (RDB)** — and this can't be changed after provisioning.

When persistence is enabled, Garnet durably stores your data on an attached locally redundant [Premium Managed Disk](https://learn.microsoft.com/azure/virtual-machines/disks-types#premium-ssds) using a combination of two complementary mechanisms:

- **Redis Database (RDB) snapshots**: Point-in-time snapshots of the full dataset saved to disk.
- **Append-only file (AOF)**: A continuous log of every write operation, committed frequently (roughly every second).

Combining both mechanisms provides fast recovery and fine-grained durability: RDB snapshots serve as compact, fast-to-restore recovery points while the AOF captures recent writes since the last snapshot. Both are performed asynchronously and are non-blocking, so persistence keeps latency consistent whether or not it's enabled.

The disk size is not configurable and is provisioned to scale with each SKU so that persistence keeps pace with the node's write throughput. See the [disks provisioned for each SKU](./cluster-configuration.md#available-tiers).

On restart, including after an automatic failover, each node recovers from its latest RDB snapshot and replays its AOF. It is possible to experience data loss for the portion of data that hasn't been replicated or wasn't captured in the latest snapshot or AOF.

## Learn More

- [Getting Started](./quickstart.md)
- [Cluster Configuration](./cluster-configuration.md)
- [Security](./security.md)
- [Monitoring](./monitoring.md)
