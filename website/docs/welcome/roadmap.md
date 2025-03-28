---
id: roadmap
sidebar_label: Roadmap
title: Garnet Roadmap
---

We would love to get feedback on what features are most important to add to Garnet. Currently. the areas we would like to
investigate include the following.

## Short Term

* See open pull requests for work in progress.
* Multi-database support.

## Medium Term (~1-4 months)

* Tsavorite v2
  * Migrate to `Span<byte>` keys for main and object stores, and `Span<byte>` values for the main store.
  * Use `Span<byte>` instead of SpanByte in API and `IFunctions` callbacks.
  * Introduce the `LogRecord` abstraction for records in the `IFunctions` callbacks.
  * Implement Object Allocator to replace Generic Allocator.
    * Store keys and values inline or on the heap.
    * Flush pages to the same log (no separate object log).
    * Fine-grained eviction of parts of page for better memory management.
  * Unify the main and object stores based on Object Allocator.
* Providing in-process access to the Garnet API for embedded use cases.
* STREAM feature.
* JSON module.

## Long Term (~1 year, depends on user contributions)

* Text indexing and vector search modules, other popular or new modules.
* Keyspace notifications.
* Optimize replication for update-intensive workloads.

If you would like to learn and contribute to Garnet, we would absolutely welcome it! Start from the developer section [here](../dev/onboarding.md).
