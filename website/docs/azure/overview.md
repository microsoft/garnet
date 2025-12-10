---
id: overview
sidebar_label: Overview
title: Azure Cosmos DB Garnet Cache Overview
---

# What is Azure Cosmos DB Garnet Cache (preview)?

Azure Cosmos DB Garnet Cache is a fully managed, high-performance caching service built on the Garnet remote cache-store from Microsoft Research. It provides enterprise-grade reliability, security, and scalability without the operational overhead of managing your own cache infrastructure. With consistent low latency and high throughput even with many client connections, Azure Cosmos DB Garnet Cache accelerates data access and leads to cost savings for large apps and services.

The Azure Cosmos DB Garnet Cache is currently in an expanded Private Preview. Please [sign up here](https://aka.ms/cosmos-db-garnet-preview).

## Key Benefits

Azure Cosmos DB Garnet Cache is a cloud-native caching service that combines the performance advantages of Garnet with Azure's managed service capabilities. It offers:

- **Ultra-Low Latency**: Sub-millisecond latency with 3ms at the 99th percentile.
- **Performance**: Supports millions of operations per second with linear scalability across multiple nodes.
- **Cost Optimization**: Per node pricing with multiple performance tiers and no licensing fees.
- **Fully Managed**: No infrastructure to manage, patch, or maintain while delivering enterprise features like high availability and data persistence.

## Common Use Cases

Azure Cosmos DB Garnet Cache is ideal for distributed caching across multiple application instances and a wide range of caching scenarios:

### Application Cache
Cache frequently accessed database queries, API responses, and computed results to reduce backend load and improve response times.

### Session Store
Store user session data, shopping carts, and user preferences.

### Gaming & Leaderboards
Leverage sorted sets for leaderboards, player rankings, and game state management.

### Vector Search & AI Applications
Store and search high-dimensional vectors for recommendation engines, similarity search, and AI-powered features using VectorSet data structures with DiskANN indexing.

### Content Delivery
Cache static content, configuration data, and frequently accessed information close to your users for faster delivery.

### Rate Limiting & Counters
Implement distributed rate limiting, usage quotas, and real-time counters across multiple application instances.

### Pub/Sub Messaging
Enable real-time communication between applications with Redis-compatible publish/subscribe messaging patterns for event-driven architectures, notifications, and live updates.

## Features

| Feature               | Support              |
|-----------------------|----------------------|
| **Latency**           | 3ms P99, < 1ms P50 |
| **Size**              | 5TB+ with clustering |
| [**Scaling**](./cluster-configuration.md#scaling-options) | Horizontal scaling with sharding and replication or scale up SKU size |
| [**Availability**](./resiliency.md#high-availability) | 99.99%* |
| [**Data persistence**](./resiliency.md#data-persistence) | Append only file (AOF) checkpointing |
| **Vector search**     | VectorSet support with DiskANN indexing |
| [**Authentication**](./security.md#authentication-and-access-control) | Microsoft Entra ID RBAC |
| [**Network isolation**](./security.md#network-security) | Virtual network support with no public internet access |
| [**Encryption**](./security.md#encryption-at-rest) | At rest and in transit with TLS |
| [**Monitoring**](./monitoring.md)        | Azure Monitor Metrics |
| **Updates**           | Automatic updates with zero downtime |

*This is an estimated value. Actual availability varies depending on configuration. See [high availability](./resiliency.md#high-availability) for more information.

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


## Compatible with Redis clients

Just like self-hosted Garnet, Azure Cosmos DB Garnet Cache uses the Redis RESP protocol, making it compatible with existing Redis clients and tools. You can migrate from Redis or other cache solutions with minimal code changes. Azure Cosmos DB Garnet Cache supports a subset of the self-hosted Garnet commands. See the full list of [supported commands](./api-compatibility.md).

## Getting Started

Ready to get started? Check out our [quick start guide](./quickstart.md) to create your first Azure Cosmos DB Garnet Cache instance in minutes.

## Learn More

- [Cluster Configuration](./cluster-configuration.md)
- [Resiliency](./resiliency.md)
- [Security](./security.md)
- [Monitoring](./monitoring.md)
