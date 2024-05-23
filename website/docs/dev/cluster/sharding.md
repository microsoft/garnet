---
id: sharding
sidebar_label: Sharding
title: Sharding Overview
---

# Sharding Overview

## Cluster Configuration

Every running cluster instance maintains a local copy of the cluster configuration.
This copy maintains information about the known cluster workers and the related slot assignment.
Both pieces of information are represented using an array of structs as shown below.
Changes to the local copy are communicated to the rest of the cluster nodes through gossiping.

Note that information related to the node characteristics can be updated only by the node itself by issuing the relevant cluster commands.
For example, a node cannot become a **REPLICA** by receiving a gossip message.
It can only change its current role only after receiving a ```CLUSTER REPLICATE``` request.
We follow this constrain to avoid having to deal with cluster misconfiguration in the event of network partitions.
This convention also extends to slot assignment, which is managed through direct requests to cluster instances made using the ```CLUSTER [ADDSLOTS|DELSLOTS]``` and ```CLUSTER [ADDSLOTSRANGE|DELSLOTSRANGE]``` commands.

```csharp reference title="Hashlot & Worker Array Declaration"
https://github.com/microsoft/garnet/blob/8856dc3990fb0863141cb902bbf64c13202d5f85/libs/cluster/Server/ClusterConfig.cs#L16-L42
```

Initially, the cluster nodes are empty, taking the role of a **PRIMARY**, having no assigned slots and with no knowledge of any other node in the cluster.
The local node contains information only about itself stored at workers[1] while workers[0] is reserved for special use to indicate unassigned slots.
Garnet cluster nodes are connected to each other through the ```CLUSTER MEET```  command which generates a special kind of gossip message.
This message forces a remote node to add the sender to its list of trusted nodes.
Remote nodes are stored in any order starting from workers[2].

```csharp reference title="Worker Definition"
https://github.com/microsoft/garnet/blob/951cf82c120d4069e940e832db03bfa018c688ea/libs/cluster/Server/Worker.cs#L28-L85
```

Information about the individual slot assignment is captured within the configuration object using an array of HashSlot struct type.
It maintains information about the slot state and corresponding owner.
The slot owner is represented using the offset in the local copy of the workers array.
The slot state is used to determine how to serve requests for keys that map to the relevant slot.

```csharp reference title="HashSlot Definition"
https://github.com/microsoft/garnet/blob/951cf82c120d4069e940e832db03bfa018c688ea/libs/cluster/Server/HashSlot.cs#L43-L61
```

At cluster startup slots are are unassigned, hence their initial state is set to **OFFLINE** and workerId to 0.
When a slot is assigned to a specific node, its state is set to **STABLE** and workerId (from the perspective of the local configuration copy) to the corresponding offset of the owner node in workers array.
Owners of a slot can perform read/write and migrate operations on the data associated with that specific slot.
Replicas can serve read requests only for keys mapped to slots owned by their primary.

```csharp reference title="SlotState Definition"
https://github.com/microsoft/garnet/blob/951cf82c120d4069e940e832db03bfa018c688ea/libs/cluster/Server/HashSlot.cs#L11-L37
```

### Configuration Update Propagation

A given node will accept gossip messages from trusted nodes.
The gossip message will contain a serialized byte array representation which represents a snapshot of the remote node's local configuration.
The receiving node will try to atomically merge the incoming configuration to its local copy by comparing the relevant workers' configuration epoch.
Hence any changes to the cluster's configuration can happen at the granularity of a single worker.
We leverage this mechanism to control when local updates become visiable to the rest of the cluster.
This is extremely useful for operations that are extended over a long duration and consist of several phases (e.g. [data migration](slot-migration)).
Such operations are susceptible to interruptions and require protective measures to prevent any compromise of data integrity.

As mentioned previously, local updates are propagated through gossiping which can operate in broadcast mode or gossip sampling mode.
In the former case, we broadcast the configuration to all nodes in the cluster periodically, while in the latter case we pick randomly a subset of nodes to gossip with.
This can be configured at server start-up by using ***--gossip-sp*** flag.

## Slot Verification

RESP data commands operate either on a single key or a collection of keys.
In addition, they can be classified either as readonly (e.g. *GET* mykey) or read-write (e.g. *SET* mykey foo).
When operating in cluster mode and before processing any command Garnet performs an extra slot verification step.
Slot verification involves inspecting the key or keys associated with a given command and validating that it maps to a slot that can be served by the node receiving the associated request.
Garnet primary nodes can serve *read* and *read-write* requests for slots that they own, while Garnet replica nodes can only serve read requests for slots that their primary owns.
On failure of the slot verification step, the corresponding command will not be processed and the slot verification method will write a redirection message directly to the network buffer.

```csharp reference title="Slot Verification Methods"
https://github.com/microsoft/garnet/blob/951cf82c120d4069e940e832db03bfa018c688ea/libs/server/Cluster/IClusterSession.cs#L47-L67
```
##  Redirection Messages

From the perspective of a single node, any requests for keys mapping towards an unassigned slot will result in ```-CLUSTERDOWN Hashlot not served``` message.
For a single key request an assigned slot is considered ***LOCAL*** if the receiving node owns that slot, otherwise it is classified as a ***REMOTE*** slot since it is owned by a remote node.
In the table below, we provide a summary of the different redirection messages that are generated depending on the slot state and the type of operation being performed.
Read-only and read-write requests for a specific key mapping to a ***REMOTE*** slot will result in ```-MOVED <slot> <address>:<port>``` redirection message, pointing to the endpoint that claims ownership of the associated slot.
A slot can also be in a special state such as ```IMPORTING``` or ```MIGRATING```.
These states are used primarily during slot migration, with the ```IMPORTING``` state assigned to the slot map of the target node and the ```MIGRATING``` state to the slot map of the source node.
Should a slot be in the ```MIGRATING``` state and the key is present (meaning it has not yet been migrated), the read requests can continue to be processed as usual.
Otherwise the receiving node (for both read-only and read-write requests) will return ```-ASK <slot> <address>:<port>``` redirection message pointing to the target node.
Note read-write key requests on existing keys are not allowed in order to ensure data integrity during migration.

| Operation/State |  ASSIGNED LOCAL  | ASSIGNED REMOTE  | MIGRATING EXISTS | MIGRATING ~EXISTS | IMPORTING ASKING | IMPORTING ~ASKING |
| --------------- | ---------------- | ---------------- | ---------------- | ----------------- | ---------------- | ----------------- |
|    Read-Only    |        OK        |     -MOVED       |       OK         |      -ASK         |        OK        |      -MOVED       |
|    Read-Write   |        OK        |     -MOVED       |    -MIGRATING    |      -ASK         |        OK        |      -MOVED       |

