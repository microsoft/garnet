---
id: overview
sidebar_label: Cluster Overview
title: Cluster Overview
---

# Cluster Implementation Overview

The cluster is designed and implemented as an independent plugin component within the standalone Garnet server.
It consists of the following two subcomponents:

1. Cluster session (```IClusterSession```) subcomponent
	This component implements the RESP parsing logic for the relevant cluster commands and the associated primitives
	which are used to perform slot verification checks and generate the appropriate redirection messages (i.e. ```-MOVED```, ```-ASK``` etc.).

2. Cluster provider (```IClusterProvider```) subcomponent
	This components implements the core cluster functionality and features such as gossip, key migration, replication, and sharding.

The decision to partition the cluster into distinct components which are separate from the standalone Garnet server serves a dual purpose.
First, it allows for an initial implementation supporting essential cluster features while also enabling developers to contribute new functionality as needed.
Second, developers may opt-in to use their own cluster implementation if they find it necessary or our default implementation does not fit their needs.