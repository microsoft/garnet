---
id: roadmap
sidebar_label: Roadmap
title: Garnet Roadmap
---

We would love to get feedback on what features are most important to add to Garnet. Currently. the areas we would like to
investigate include:

* Providing in-process access to the Garnet API for embedded use cases.
* Optimizing Tsavorite (storage layer) to take advantage of inlining and better IO opportunities.
* Making it easy to create extensions (e.g., modules and functions) and hooks from various languages.
* Supporting for more of the missing core APIs.
* Adding extensions to support popular non-core features such as text indexing, vector search, and JSON.
* Unifying the main and object stores to use a common hash index and backing record log.
* Improving the way heap data structures are stored in memory and on disk.
* Optimizing the performance of replication and update-intensive workloads that are bottlenecked by the append-only-file.
* Cleaning up and improving the way command parsing is done today.

If you would like to learn and contribute to Garnet, we would absolutely welcome it too! Start from the developer section [here](../dev/onboarding.md).
