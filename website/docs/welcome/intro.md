---
id: intro
sidebar_label: Intro
title: Welcome to Garnet
slug: /
---

# Welcome to Garnet

Garnet is a new remote cache-store from Microsoft Research, that is designed to be extremely fast, extensible, 
and low latency. Garnet is thread-scalable within a single node. It also supports sharded cluster execution, 
with replication, checkpointing, failover, and transactions. It can operate over main memory as well as 
tiered storage (such as SSD and Azure Storage). Garnet supports a rich API surface and a powerful extensibility 
model.

Garnet uses Redis' RESP as its primary wire protocol. Thus, one can use Garnet with unmodified Redis clients 
[available](https://redis.io/docs/latest/develop/connect/clients/) in most programming languages, for example, with [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis)
in C#. Compared to other open-source cache-stores, you get much better performance, latency, extensibility, and durability features.

Note that Garnet is a research project from Microsoft Research, and the project should be treated as such. That said, we are a bunch 
of highly passionate researchers and developers working on it full-time at the moment to make it as stable and efficient as we 
can. Our goal is to create a vibrant community around Garnet. In fact, Garnet has been of sufficiently high quality that several 
first-party and platform teams at Microsoft have deployed versions of Garnet internally for many months now.

Garnet offers the following key advantages:
* Orders-of-magnitude better server throughput (ops/sec) with small batches and many client sessions, relative to
  comparable open-source cache-stores.
* Extremely low single operation latency (often less than 300 microseconds at the 99.9th percentile) on commodity cloud
  (Azure) machines with accelerated TCP enabled, on both Windows and Linux.
* Better scalability as we increase the number of clients, with or without client-side batching.
* The ability to use all CPU/memory resources of a server machine with a single shared-memory server instance 
(no intra-node cluster needed).
* Support for larger-than-memory datasets, spilling to local and cloud storage devices.
* Database features such as fast checkpointing and recovery, and publish/subscribe.
* Support for multi-node sharded hash partitioning (Redis "cluster" mode), state migration, and replication.
* Well tested with a comprehensive test suite (thousands of unit tests across Garnet and its storage layer Tsavorite).
* A C# codebase that is easy to evolve and extend.

If you need a cache-store for your application or service, with lots of practical features, high performance, and a
modern design based on state-of-the-art Microsoft Research technology, then Garnet is the system for you. Check out
more details on Garnet's performance advantages [here](../benchmarking/overview.md).

## API Coverage

Garnet supports a large (and growing) [API surface](../commands/api-compatibility.md), including:

* Raw string operations such as GET, SET, MGET, MSET, GETSET, SETEX, DEL, EXISTS, RENAME, EXPIRE, SET variants (set if exists, set if not exists).
* Numeric operations such as INCR, INCRBY, DECR, DECRBY.
* Remote data structures such as List, Hash, Set, Sorted Set, and Geo.
* Analytics APIs such as Hyperloglog and Bitmap.
* Checkpoint/recovery ops such as SAVE, LASTSAVE, BGSAVE.
* Admin ops such as PING, QUIT, CONFIG, RESET, TIME.
* ACL support.
* Publish/subscribe, transactions, and Lua scripting support.

The list is growing over time, and we would love to hear from you on what APIs you want the most!

Further, Garnet supports a powerful custom operator framework whereby you can register custom  C# data structures and read-modify-write operations on the server, and access them 
using the same wire protocol. Thus, you can invoke these commands using clients' ability to execute new commands, e.g., the `Execute` and `ExecuteAsync` calls in the 
StackExchange.Redis client library.

## Platforms Supported

Garnet server is based on high-performance .NET technology written from the ground up with performance 
in mind. Garnet has been extensively tested to work equally efficiently on both Linux and Windows, 
and on commodity Azure hardware as well as edge devices.

One can also view Garnet as an incredibly fast remote .NET data structure server that can be extended
by leveraging the rich ecosystem of C# libraries, so we can expand beyond the core API. Garnet's 
storage layer is called Tsavorite, which supports for various backing storage devices such as fast 
local SSD drives and Azure Storage. It has devices optimized for Windows and Linux as well. Finally, 
Garnet supports TLS for secure connections.

## Deployment Options

### Azure Cosmos DB Garnet Cache (preview)
For production workloads, **Azure Cosmos DB Garnet Cache** provides a fully managed experience with enterprise-grade security, 
automatic scaling, global distribution, and comprehensive monitoring. This managed service eliminates infrastructure management 
overhead while delivering the performance advantages of Garnet. It's currently in an expanded Private Preview.

**Key benefits:**
- Fully managed infrastructure and operations
- High availability and seamless scaling
- Enterprise security and compliance
- Integration with Azure monitoring and diagnostics

[Get started with Azure Cosmos DB Garnet Cache →](../azure/overview.md)

### Self-Hosted Garnet
For development, research, or custom deployments, the open-source Garnet server provides complete control over your 
infrastructure. Garnet is a research project from Microsoft Research that has been deployed by several 
first-party and platform teams at Microsoft for many months.

**Key benefits:**
- Complete control over infrastructure
- Customizable configurations and extensions
- Cost-effective for development and testing
- Direct access to latest research features

[Learn about self-hosted Garnet →](../getting-started/build.md)

<sub>
Redis is a registered trademark of Redis Ltd. Any rights therein are reserved to Redis Ltd. Any use by Microsoft is for referential purposes only and does not indicate any sponsorship, endorsement or affiliation between Redis and Microsoft.
</sub>

