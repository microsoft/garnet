---
id: overview
sidebar_label: Overview
title: Azure Cosmos DB Garnet Cache Overview
---

# What is Azure Cosmos DB Garnet Cache (preview)?

Azure Cosmos DB Garnet Cache is a fully managed, high-performance caching service built on the Garnet remote cache-store from Microsoft Research. It provides enterprise-grade reliability, security, and scalability without the operational overhead of managing your own cache infrastructure. With consistent low latency and high throughput even with many client connections, Azure Cosmos DB Garnet Cache accelerates data access and leads to cost savings for large apps and services.

The Azure Cosmos DB Garnet Cache is currently in an expanded Private Preview. Please [sign up here](https://aka.ms/cosmos-db-garnet-preview).

## Key Benefits

Azure Cosmos DB Garnet Cache is a cloud-native caching service that combines the performance advantages of Garnet with Azure's managed service capabilities. Unlike traditional single-threaded Redis, Garnet is designed to handle high concurrency efficiently, maintaining sub-millisecond latencies even with thousands of concurrent client connections. This architectural advantage translates to cost savings for applications with many concurrent users. It offers:

- **Ultra-Low Latency**: Sub-millisecond latency with 3ms at the 99th percentile.
- **Throughput**: Supports millions of operations per second with linear scalability across nodes. Performance scales efficiently even with thousands of concurrent connections.
- **Data Persistence**: Data durability with non-blocking Append Only File (AOF) checkpoints. Maintains full throughput with no performance penalty like traditional Redis.
- **Cost Optimization**: Per node pricing with multiple performance tiers and no licensing fees. Cost effective at scale for workloads where cache size is driven by throughput requirements (as opposed to memory size).
- **Fully Managed**: No infrastructure to manage, patch, or maintain while delivering enterprise features for high availability, security and more.

## Common Use Cases

Azure Cosmos DB Garnet Cache supports distributed caching across multiple application instances and is designed for workloads where throughput efficiency and data durability matter. Here are scenarios where Garnet's unique advantages deliver the most value:

### High-Concurrency Applications
Live trading systems, multiplayer services and IoT platforms with thousands of simultaneous connections. Garnet's multi-threaded architecture maintains sub-millisecond latency and consistent throughput even under heavy concurrent load, eliminating the bottlenecks of single-threaded Redis. Examples include:
- Live trading and financial tick data
- Gaming leaderboards and player state
- Real-time IoT telemetry and sensor data

### Mission-Critical Caching with Durability
Applications where losing cached data is not acceptable. Garnet's non-blocking AOF persistence eliminates the traditional tradeoff between persistence and throughput. It lets you durably cache critical data without sacrificing performance. Common scenarios that benefit include user sessions, payment information, and transaction data.

### AI-Powered Applications  
Vector search for recommendation engines, semantic search, and AI applications. Store and query high-dimensional vectors using VectorSet data structures with DiskANN indexing for single digit millisecond vector search at scale.

### Cost-Optimized At-Scale Infrastructure
Applications where high throughput requirements would otherwise lead to infrastructure decisions like upgrading cache size. Garnet's multi-threaded efficiency means you can handle the same throughput with smaller SKUs or fewer cache nodes than other solutions, directly reducing operational costs and complexity. 

## Features

| Feature               | Support              |
|-----------------------|----------------------|
| **Latency**           | 3ms P99, < 1ms P50 |
| **Size**              | 5TB+ with clustering |
| [**Scaling**](./cluster-configuration.md#scaling-options) | Horizontal scaling with sharding and replication or scale up SKU size |
| [**Availability**](./resiliency.md#high-availability) | 99.99%* |
| [**Data persistence**](./resiliency.md#data-persistence) | Append only file (AOF) checkpointing |
| [**Advanced data structures**](./api-compatibility.md) | Support for Hash, Set, Sorted Set in addition to String |
| **Vector search**     | VectorSet support with DiskANN indexing |
| **Scripting**         | Lua scripting |
| **Pub/Sub**           | Publish/subscribe messaging |
| [**Authentication**](./security.md#authentication-and-access-control) | Microsoft Entra ID RBAC |
| [**Network isolation**](./security.md#network-security) | Virtual network support with no public internet access |
| [**Encryption**](./security.md#encryption-at-rest) | At rest and in transit with TLS |
| [**Monitoring**](./monitoring.md)        | Azure Monitor Metrics |
| **Updates**           | Automatic updates with zero downtime |

*This is an estimated value. Actual availability varies depending on configuration. See [high availability](./resiliency.md#high-availability) for more information.

### Compatible with Redis clients

Just like self-hosted Garnet, Azure Cosmos DB Garnet Cache uses the Redis RESP protocol, making it compatible with existing Redis clients and tools. You can migrate from Redis or other cache solutions with minimal code changes. Azure Cosmos DB Garnet Cache supports a subset of the self-hosted Garnet commands including Strings, Hashes, Sets, Sorted Sets, Pub/Sub, Lua scripting and more. See the full list of [supported commands](./api-compatibility.md).

### Available Tiers

Azure Cosmos DB Garnet Cache offers two performance tiers to match your workload requirements. An overview of single-node specs is below for each tier based on the [available SKUs](./cluster-configuration.md#available-tiers). Each cluster can be scaled to have one or more nodes of the same SKU determined by the shard count and replication factor configured. This allows for custom cache sizes up to 5TB+ that match your specific workload needs.

#### General Purpose
Recommended for balanced workloads, general caching, development and testing. Single-node specs ranging from:
- **Memory**: 4 GB - 128 GB
- **vCPUs**: 2-32 cores
- **Network**: Up to 16 Gbps

#### Memory Optimized  
Recommended for in-memory databases, large datasets, gaming leaderboards, vector search workloads. Single-node specs ranging from:
- **Memory**: 16 GB - 256 GB
- **vCPUs**: 2-32 cores
- **Network**: Up to 16 Gbps

## Getting Started

Ready to get started? Check out our [quick start guide](./quickstart.md) to create your first Azure Cosmos DB Garnet Cache instance in minutes.

## Learn More

- [Cluster Configuration](./cluster-configuration.md)
- [Resiliency](./resiliency.md)
- [Security](./security.md)
- [Monitoring](./monitoring.md)
