---
id: features
sidebar_label: Features
title: Summary of Garnet Features
---

# Summary of Features

## Core API

* Raw strings (Get and Set variants, key expiration)
* Data structures (List, Hash, Set, Sorted Set, Geo)
* Analytics APIs such as Hyperloglog and Bitmap
* Client-side transaction API (MULTI/EXEC)
* Publish/Subscribe
* Admin operations
* Access control (ACL) features

## Logging & Diagnostics

* Metrics - client side
* Metrics - in-process queryable
* Diagnostic logger via `ILogger`
* JSON config file support, with basic support for redis.conf format as well

## Networking

* Pluggable network layer
* Full support for TLS via SslStream

## Extensibility

* Fast C# based extensibility (raw string and object operations)
* Dynamic and static registration
* Transactional multi-key stored procedures

## Memory

* Tsavorite storage engine optimized for scalable memory access
* Space reuse for memory tier to prevent fragmentation
* Hybrid log-structured storage design with in-place updates in memory
* Configurable memory size control (index, log, objects)

## Tiered Storage

* Three uses of storage: larger-than-memory cache, AOF (append-only file), checkpoints
* Extensible `IDevice` abstraction to support different devices
* Specializations for SSD/HDD device (Windows and Linux Native, as well as generic device based on .NET FileStream)
* Specialization for Azure Storage device
* Automatic log compaction

## Durability

* Fast non-blocking checkpoint-recovery
* Append-only-file (write-ahead log)

## Cluster Mode

* Sharding
* Replication
* Failover
* Key migration for dynamic scale-out

## Multi-Platform

* Any platform supported by .NET
* Windows
* Linux

