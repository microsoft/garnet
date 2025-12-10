---
id: faq
sidebar_label: FAQ
title: Frequently Asked Questions
---

# Frequently Asked Questions

## General Questions

### What is Azure Cosmos DB Garnet Cache?
Azure Cosmos DB Garnet Cache is a fully managed, high-performance caching service built on the Garnet remote cache-store from Microsoft Research. It provides Redis protocol compatibility with ultra-low latency and enterprise-grade security, scalability, and reliability. 

### How can I access the Preview?
Azure Cosmos DB Garnet Cache is currently in an expanded Private Preview. Please [sign up](https://aka.ms/cosmos-db-garnet-preview) to join the preview.

### How is it different from self-hosted Garnet?
As a fully managed service, Azure Cosmos DB Garnet Cache handles infrastructure provisioning, scaling, patching, and monitoring automatically. Self-hosted Garnet requires you to manage the infrastructure, updates, and operations yourself.

### Does it only work with Azure Cosmos DB?
No, Azure Cosmos DB Garnet Cache can be used to accelerate data access for any application, including but not limited to use with Azure Cosmos DB. It doesn't use the Azure Cosmos DB SDKs or have any automatic syncing.

### Is it compatible with Redis clients?
Yes, Azure Cosmos DB Garnet Cache uses the Redis RESP protocol, making it compatible with existing Redis clients in all major programming languages without code changes. 

### What Redis version is supported?
Azure Cosmos DB Garnet Cache supports the RESP protocol and doesn't have full support for any specific Redis version. Visit the list of [supported Redis commands](./api-compatibility.md).

### How is Azure Cosmos DB Garnet Cache priced?
Azure Cosmos DB Garnet Cache clusters are billed per instance per hour with no licensing fees. Each node will be billed for the chosen SKU plus an attached disk, used for [data persistence](./resiliency.md#data-persistence), with 4x the storage available for the SKU. Pricing per SKU is set at different rates than the underlying Azure VM and is subject to change between our extended Private Preview and Public Preview. 

For information about pricing for specific SKUs, reach out to [ManagedGarnet@service.microsoft.com](mailto:ManagedGarnet@service.microsoft.com).


## Performance and Scalability

### What performance can I expect?
Latency is typically sub-millisecond and is around 3ms the 99th percentile. Performance and throughput varies by tier, key/ value size, and number of concurrent requests, among other factors.

### Can I scale my cache?
Yes, you can [scale out](./cluster-configuration.md#horizontal-scaling-scale-outin) by adding shards, or [scale up](./cluster-configuration.md#vertical-scaling-scale-updown) by changing SKU size within a VM family and generation with no downtime.

### How many connections are supported?
Garnet doesn't limit the number of client connections that can be made on any node for any SKU. In practice, connection limits vary by SKU. See the [virtual machine limits](https://learn.microsoft.com/azure/virtual-machines/sizes/overview#list-of-vm-size-families-by-type) corresponding to the [Azure Cosmos DB Garnet Cache SKU](./api-compatibility.md) you choose.


## Development and Integration

### Which client libraries are supported?
All Redis client libraries are supported. Ensure you visit the list of [supported commands](./api-compatibility.md). Popular libraries by language include:
- **C#**: [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis)
- **Java**: [Jedis](https://github.com/redis/jedis), [Redisson](https://github.com/redisson/redisson)
- **Python**: [redis-py](https://github.com/redis/redis-py)
- **Node.js**: [node_redis](https://github.com/redis/node-redis)
- **Go**: [go-redis](https://github.com/redis/go-redis)

### Can I test it locally?
For local development, you can use the self-hosted Garnet server.


## Regional Availability

### Which regions is it available in?
Azure Cosmos DB Garnet Cache is available in many Azure regions. Check our [supported regions list](./cluster-configuration.md#regional-availability).

### Which regions support Availability Zones?
Azure Cosmos DB Garnet Cache can be configured with availability zones during provisioning in supported Azure regions where there is capacity for your chosen SKU. See the list of [Azure regions with availability zone support](https://learn.microsoft.com/azure/reliability/regions-list).


## Troubleshooting

### My application can't connect to the cache
Check these common issues:
1. **VNet configuration**: Verify your client application is in the [same virtual network](./security.md#network-security) as your cache
2. **Authentication**: Verify you've configured [role based access control](./security.md#authentication-and-access-control), which is required for data plane access
3. **SSL/TLS**: Ensure your client supports [TLS](./security.md#data-encryption)

### Cache performance is slower than expected
Common causes and solutions:
1. **Connection pooling**: Ensure proper connection pool configuration
2. **Hot keys**: Check for key access patterns causing bottlenecks
3. **Memory pressure**: [Monitor memory usage](./monitoring.md#high-memory-usage) and consider scaling up
4. **Network latency**: Test from the same region as your cache

### I'm getting timeout errors
Troubleshooting steps:
1. **Check timeout settings**: Verify client timeout configuration
2. **Monitor CPU/memory**: [High resource usage](./monitoring.md#high-cpu-usage) can cause timeouts
4. **Network issues**: Test network connectivity between client and cache


## Getting Help

### Where can I get technical support?
- [**Documentation**](./overview.md): For conceptual guidance and tutorials
- **Azure Support**: Create support tickets for technical issues
- **Community**: Stack Overflow and Azure forums

### How do I report bugs or request features?
- **Azure Support** for bugs and critical issues
- [**Feedback Form**](https://aka.ms/garnet-feedback) for feature requests or suggestions


## Next Steps

- [Garnet Overview](./overview.md)
- [Getting Started](./quickstart.md)
