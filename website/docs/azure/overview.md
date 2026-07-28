---
id: overview
sidebar_label: Overview
title: Azure Cosmos DB Garnet Cache Overview
---

# What is Azure Cosmos DB Garnet Cache (preview)?

Azure Cosmos DB Garnet Cache is a fully managed, high-performance caching service built on the Garnet remote cache-store from Microsoft Research. It provides enterprise-grade reliability, security, and scalability without the operational overhead of managing your own cache infrastructure. With consistent low latency and high throughput even with many client connections, Azure Cosmos DB Garnet Cache accelerates data access and leads to cost savings for large apps and services.

The Azure Cosmos DB Garnet Cache is currently in an expanded Private Preview. Please [register your subscription](#register-your-subscription). For feedback or questions, reach out to [CosmosGarnetCache@service.microsoft.com](mailto:cosmosgarnetcache@service.microsoft.com) or fill out our [feedback form](https://aka.ms/garnet-feedback).

## Key Benefits

Azure Cosmos DB Garnet Cache is a cloud-native caching service that combines the performance advantages of Garnet with Azure's managed service capabilities. Unlike traditional single-threaded caches or caches that are hash-partitioned within each node, Garnet uses a shared-everything architecture within nodes where all threads directly access the single shared memory space. This architectural advantage allows Garnet to maintain sub-millisecond latencies even under heavy load with thousands of concurrent client connections and translates to cost savings for applications with many simultaneous users. It offers:

- **Ultra-Low Latency**: Median (P50) latency under 1 ms, with 3 ms at the 99th percentile (P99).
- **Throughput**: Supports millions of operations per second with linear scalability across nodes. Performance scales efficiently even with thousands of concurrent connections.
- **Scalability**: Optimized performance with vertical and horizontal scaling as well as configurable replication for increased read throughput.
- **Data Persistence**: Optional durability that combines non-blocking append-only file (AOF) logging with Redis Database (RDB) snapshots. Durably cache critical data with consistent low latency.
- **Cost Optimization**: [Per node pricing](https://azure.microsoft.com/en-us/pricing/details/cosmos-db/garnet-cache/) with multiple performance tiers and no licensing fees. Cost-effective at scale for workloads where cache size is driven by throughput requirements (as opposed to memory size).
- **Fully Managed**: No infrastructure to manage, patch, or maintain while delivering enterprise features for high availability, security and more.

## Common Use Cases

Azure Cosmos DB Garnet Cache supports distributed caching across multiple application instances and is designed for workloads where throughput efficiency and data durability matter. Here are scenarios where Garnet's unique advantages deliver the most value:

### High-Concurrency Applications
Live trading and financial tick systems, multiplayer and gaming services, and IoT platforms with thousands of simultaneous connections. Garnet's multi-threaded architecture maintains sub-millisecond latency and consistent throughput even under heavy concurrent load, eliminating the performance degradation single-threaded caches experience with many parallel requests.

### AI-Powered Applications
Vector search for recommendation engines, semantic search, retrieval-augmented generation (RAG), and other AI applications. Store and query high-dimensional vectors using VectorSet data structures with DiskANN indexing for single-digit millisecond similarity search.

Garnet is especially well suited to scale-up vector search. Because a node's threads share a single memory space, you can hold a large VectorSet entirely on one large SKU and drive high query throughput against it without partitioning the index across nodes. Keeping the whole vector set on a single node avoids the cross-node fan-out, added latency, and recall tradeoffs of a sharded index, delivering consistent, high-throughput similarity search even as the dataset grows into millions of vectors. When you need more capacity or throughput, scale up to a larger SKU rather than out across shards.

### Mission-Critical Caching with Durability
Applications where losing cached data is not acceptable like user sessions, payment information, and transaction data. Garnet's optional AOF + RDB persistence combines non-blocking append-only file (AOF) logging with Redis Database (RDB) snapshots. Both run asynchronously, so you get durable caching with consistent low latency.

### Cost-Optimized At-Scale Infrastructure
Applications where high throughput requirements drive infrastructure costs. Garnet's multi-threaded efficiency means you can handle the same throughput with smaller SKUs or fewer cache nodes than other solutions, directly reducing operational costs and complexity. 

## Features

| Feature               | Support              |
|-----------------------|----------------------|
| **Latency**           | 3ms P99, < 1ms P50 |
| **Size**              | 5TB+ with clustering |
| [**Scaling**](./cluster-configuration.md#scaling-options) | Horizontal scaling with sharding and replication or scale up SKU size |
| [**Availability**](./resiliency.md#high-availability) | 99.99%* |
| [**Data persistence**](./resiliency.md#data-persistence) | Optional AOF + RDB persistence |
| [**Advanced data structures**](./api-compatibility.md) | Support for Hash, Set, Sorted Set in addition to String |
| [**Vector search**](./api-compatibility.md#vector-set)     | VectorSet support with DiskANN indexing |
| [**Scripting**](./api-compatibility.md#scripting)          | Lua scripting disabled by default and available by request |
| **Pub/Sub**           | Publish/subscribe messaging |
| [**Authentication**](./security.md#authentication-and-access-control) | Microsoft Entra ID RBAC |
| [**Network isolation**](./security.md#network-security) | Virtual network support with no public internet access |
| [**Encryption**](./security.md#data-encryption) | At rest and in transit with TLS |
| [**Monitoring**](./monitoring.md)        | Azure Monitor Metrics |
| **Updates**           | Automatic updates with zero downtime when replication is enabled |

*This is an estimated value. Actual availability varies depending on configuration. See [high availability](./resiliency.md#high-availability) for more information.

### Compatible with Redis clients

Just like self-hosted Garnet, Azure Cosmos DB Garnet Cache uses the Redis RESP protocol, making it compatible with existing Redis clients and tools. You can migrate from Redis or other cache solutions with minimal code changes. Azure Cosmos DB Garnet Cache supports a subset of the self-hosted Garnet commands including Strings, Hashes, Sets, Sorted Sets, Pub/Sub, Lua scripting, and more. See the full list of [supported commands](./api-compatibility.md).

### Available Tiers

Azure Cosmos DB Garnet Cache offers three performance tiers to match your workload requirements. The specs below are **per node**, based on the [available SKUs](./cluster-configuration.md#available-tiers). A cluster is made up of one or more shards — each shard is a node of your chosen SKU, plus any replicas — so total cluster capacity scales well beyond a single node. The [shard count and replication factor](./cluster-configuration.md#scaling-options) determine the total node count, enabling custom cache sizes up to 5TB+.

#### General Purpose
Recommended for balanced workloads, general caching, development and testing. Per-node specs range from:
- **Memory**: 4 GB - 768 GB
- **vCPUs**: 2-192 cores

#### Memory Optimized  
Recommended for in-memory databases, large datasets, gaming leaderboards, vector search workloads. Per-node specs range from:
- **Memory**: 16 GB - 1,024 GB
- **vCPUs**: 2-128 cores

#### Compute Optimized
Recommended for CPU-intensive operations, complex computations, high-throughput scenarios. Per-node specs range from:
- **Memory**: 8 GB - 256 GB
- **vCPUs**: 2-64 cores

## Getting Started
Ready to get started? First, register your subscription for access to the preview. Then, check out our [quick start guide](./quickstart.md) to create your first Azure Cosmos DB Garnet Cache instance in minutes.

### Register your Subscription
Register your subscription to provision Azure Cosmos DB Garnet Caches. Here are the steps to request access. Please note that registration is not automatic and can take up to one week to process.
1. Go to the **Preview Features** page in your Azure subscription.
2. Filter by **Garnet** in the feature name search.
3. Select the **Register** button.

![Register Subscription for Azure Cosmos DB Garnet Cache](../../static/img/azure/register-subscription.png)

## Learn More

- [Quickstart](./quickstart.md)
- [Cluster Configuration](./cluster-configuration.md)
- [Resiliency](./resiliency.md)
- [Security](./security.md)
- [Monitoring](./monitoring.md)
