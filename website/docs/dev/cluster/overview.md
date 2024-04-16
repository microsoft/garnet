---
id: overview
sidebar_label: Cluster Overview
title: Cluster Overview
---

# Cluster Implementation Overview

<!---
	Cluster is implemented as a separate component consisting of two pieces.
	Cluster session and cluster provider.
	
	Cluster session implements the parsing of related cluster commands and operations.
	Cluster provider implements the primitives to support execution of the associated cluster commands and ops.
	
	dev/cluster/intro.md
	dev/cluster/sharding.md
	dev/cluster/replication-manager.md
	dev/cluster/migration-manager.md
	dev/cluster/failover-manager.md
-->

The cluster is designed and implemented as an independent plugin component within the standalone Garnet server.
It consists of the following two subcomponents:

1. Cluster session (```IClusterSession```) subcomponent
	This component implements the RESP parsing logic of the related cluster commands and the associated primitives
	for performing slot checks and generating the appropriate redirection messages (i.e. -MOVED, -ASK etc.) if need be.

2. Cluster provider (```IClusterProvider```) subcomponent
	This components implements the core cluster functionality and features such as gossip, key migration, replication, and sharding.

The decision to partition the cluster into distinct components, which are separate from the standalone Garnet server, serves a dual purpose.
First it allows for an initial implementation supporting essential cluster features while also enabling developers to contribute new functionality as needed.
Second, developers may opt-in using their own cluster implementation that best suits their needs.