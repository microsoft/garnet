---
id: sharding
sidebar_label: Sharding
title: Sharding Overview
---

# Sharding Overview

## Cluster Configuration

Every running instance in the cluster maintains a local copy of the cluster configuration.
This copy maintains information about the known cluster workers (a.k.a nodes) and the corresponding slot assignment.
Both pieces of information are represented using an array of structs as shown below.
Changes to the local copy are communicated to the rest of the cluster nodes through gossiping.

Note that information related to the node characteristics can be updated only by the node itself by issuing the related RESP commands.
For example, a node cannot become a **REPLICA** by receiving a gossip message.
It can only change its role itself by receiving a ```CLUSTER REPLICATE``` message.
We follow this constrain to avoid having to deal with cluster misconfiguration in the event of network partitions.
This does not apply towards slot assignment since slots are a shared resource across the cluster, thus care must be taken to avoid a split brain scenario.

<details>
    <summary>ClusterConfig Definition</summary>
    ```bash
        /// <summary>
        /// Cluster configuration
        /// </summary>
        internal sealed partial class ClusterConfig
        {
            ...
            readonly HashSlot[] slotMap;
            readonly Worker[] workers;
            ...
        }
    ```
</details>

Initially, the cluster nodes are empty, taking the role of a **PRIMARY**, having no assigned slots, and with no knowledge of any other node in the cluster.
The local node contains information only about itself stored at workers[1], while workers[0] is reserved for special use with slot assignment to indicate unassigned slots.
Garnet cluster nodes are connected to each other through ```CLUSTER MEET```  command which generates a special kind of gossip message.
This message forces a remote node to add the sender to its list of trusted nodes.
Remote nodes are stored in any order starting from workers[2].

<details>
    <summary>Worker Definition</summary>
    ```bash
        /// <summary>
        /// Cluster worker definition
        /// </summary>
        public struct Worker
        {
            /// <summary>
            /// Unique node ID
            /// </summary>
            public string nodeid;

            /// <summary>
            /// IP address
            /// </summary>
            public string address;

            /// <summary>
            /// Port
            /// </summary>
            public int port;

            /// <summary>
            /// Configuration epoch.
            /// </summary>
            public long configEpoch;

            /// <summary>
            /// Current config epoch used for voting.
            /// </summary>
            public long currentConfigEpoch;

            /// <summary>
            /// Last config epoch this worker has voted for.
            /// </summary>
            public long lastVotedConfigEpoch;

            /// <summary>
            /// Role of node (i.e 0: primary 1: replica).
            /// </summary>
            public NodeRole role;

            /// <summary>
            /// Node ID that this node is replicating (i.e. primary id).
            /// </summary>
            public string replicaOfNodeId;

            /// <summary>
            /// Replication offset (readonly value for information only)
            /// </summary>
            public long replicationOffset;

            /// <summary>
            /// Hostname of this instance
            /// </summary>
            public string hostname;

            /// <summary>
            /// ToString
            /// </summary>
            /// <returns></returns>
            public override string ToString() => $"{nodeid} {address} {port} {configEpoch} {role} {replicaOfNodeId}";
        }
    ```
</details>

Information about the individual slot assignment is captured within the configuration object using an array of HashSlot struct type.
It maintains information about the slot state and corresponding owner.
The slot owner is represented using the offset in the local copy of the workers array.
The slot state is used to decide how requests for specific keys mapping to that slot are served.

<details>
    <summary>HashSlot Definition</summary>
    ```bash
        /// <summary>
        /// Hashslot info
        /// </summary>
        [StructLayout(LayoutKind.Explicit)]
        public struct HashSlot
        {
            /// <summary>
            /// WorkerId of slot owner.
            /// </summary>
            [FieldOffset(0)]
            public ushort _workerId;

            /// <summary>
            /// State of this slot.
            /// </summary>
            [FieldOffset(2)]
            public SlotState _state;

            /// <summary>
            /// Slot in migrating state points to target node though still owned by local node until migration completes.
            /// </summary>
            public ushort workerId => _state == SlotState.MIGRATING ? (ushort)1 : _workerId;
        }
    ```
</details>

Initially, slots are unassigned hence their initial state is set to **OFFLINE** and workerId to 0.
When a slot is assigned to a specific node, its state is set to **STABLE** and workerId to the corresponding offset of the owner node in workers array (from the perspective of the local configuration copy).
Owners of a slot can perform read/write and migrate operations on the data associated with that specific slot.
Replicas can serve read requests for keys mapped to slots owned by their primary.

<details>    
    <summary>SlotState Definition</summary>
    ```bash
        /// <summary>
        /// NodeRole identifier
        /// </summary>
        public enum SlotState : byte
        {   
            /// <summary>
            /// Slot not assigned
            /// </summary>
            OFFLINE = 0x0,
            /// <summary>
            /// Slot assigned and ready to be used.
            /// </summary>
            STABLE,
            /// <summary>
            /// Slot is being moved to another node.
            /// </summary>
            MIGRATING,
            /// <summary>
            /// Reverse of migrating, preparing node to receive commands for that slot.
            /// </summary>
            IMPORTING,
            /// <summary>
            /// Slot in FAIL state.
            /// </summary>
            FAIL,
            /// <summary>
            /// 
            /// </summary>
            NODE,
        }
    ```
</details>

### Configuration Update Propagation

A given node will accept gossip messages from trusted nodes and merge the corresponding configuration atomically.
The process of merging relies on comparing the receiving and local configurationEpochs per worker.
If the former is greater than the latter updates to the local copy are accepted.
We leverage this mechanism to control when local updates become visiable to the rest of the cluster.
This is extremely useful for long running operations which involve multiple steps (changing the state of cluster), are prone to failures and necessitate guarding against data corruption.
Migration is such an example, since it requires transitioning the node into a state where writes are paused while the data is being trasferred accross nodes.
For more information please refer to the slot migration [page](slot-migration).

As mentioned previously, local updates are propagated through gossiping which can operate in broadcast mode or gossip sampling mode.
In the former case, we broadcast the config to all nodes in the cluster periodically, while the latter case we pick randomly a subset of nodes to gossip with.
This can be configured at start-up by using ***--gossip-sp*** flag.

## Slot Verification

Resp data commands operate either on a single key or a collection of keys.
In addition, they can be classified either as readonly (e.g. *GET* mykey) or read-write (e.g. SET mykey foo).
When operating in cluster mode and before processing any command Garnet performs an extra slot verification step.
Slot verification involves inspecting the key or keys associated with a given command and validating that it maps to a slot that can be served by the node receiving the associated request.
Garnet primary nodes can serve read and read-write requests for slots that they own, while Garnet replica nodes can only serve read requests for slots that their primary owns.
In addition, multi-key commands require all keys involved to map to the same slot.
On failure of the slot verification step, the corresponding command will be skipped and unless otherwise noted the corresponding slot verification method will write a redirection directly to the network buffer.

<details>
        <summary>Slot Verification Methods</summary>
        ```bash
        /// <summary>
        /// Single key slot verify (check only, do not write result to network)
        /// </summary>
        unsafe bool CheckSingleKeySlotVerify(ArgSlice keySlice, bool readOnly, byte SessionAsking);

        /// <summary>
        /// Key array slot verify (write result to network)
        /// </summary>
        unsafe bool NetworkKeyArraySlotVerify(int keyCount, ref byte* ptr, byte* endPtr, bool interleavedKeys, bool readOnly, byte SessionAsking, ref byte* dcurr, ref byte* dend, out bool retVal);

        /// <summary>
        /// Key array slot verify (write result to network)
        /// </summary>
        unsafe bool NetworkKeyArraySlotVerify(ref ArgSlice[] keys, bool readOnly, byte SessionAsking, ref byte* dcurr, ref byte* dend, int count = -1);

        /// <summary>
        /// Single key slot verify (write result to network)
        /// </summary>
        unsafe bool NetworkSingleKeySlotVerify(byte[] key, bool readOnly, byte SessionAsking, ref byte* dcurr, ref byte* dend);

        /// <summary>
        /// Single key slot verify (write result to network)
        /// </summary>
        unsafe bool NetworkSingleKeySlotVerify(ArgSlice keySlice, bool readOnly, byte SessionAsking, ref byte* dcurr, ref byte* dend);
        ```
</details>

##  Redirection Messages

From the perspective of a given node, any requests for keys mapping towards an unassigned slot will result in ```-CLUSTERDOWN Hashlot not served``` message.
For a given key request, an assigned slot is considered ***LOCAL*** if the receiving node owns that slot, otherwise it is classified as a ***REMOTE*** slot since it is owned by a remote node.
In the table below, we provide a summary of the different redirection messages that are generated depending on the slot state and the type of operation being performed.
Read-only and read-write requests for a specific key mapping to a ***REMOTE*** slot will result in ```-MOVED <slot> <address>:<port>``` redirection message, pointing to the endpoint that claims ownership the associated slot.
A slot can also be in a special state such as ```IMPORTING``` or ```MIGRATING```.
These states are used primarily during slot migration, with the ```IMPORTING``` state assigned to the slot map of the target node and the ```MIGRATING``` to the slot map of the source node.
If a slot is in ```MIGRATING``` state the and the key exists (i.e. not migrated yet) the read requests can be served as alwasy, otherwise the receiving node (for both read-only and read-write) will return ```-ASK <slot> <address>:<port>``` redirection message pointing to the target node.
Read-write key requests on existing keys are not allowed in order to ensure data integrity during migration.


| Operation/State |  ASSIGNED LOCAL  | ASSIGNED REMOTE  | MIGRATING EXISTS | MIGRATING ~EXISTS | IMPORTING ASKING | IMPORTING ~ASKING |
| --------------- | ---------------- | ---------------- | ---------------- | ----------------- | ---------------- | ----------------- |
|    Read-Only    |        OK        |     -MOVED       |       OK         |      -ASK         |        OK        |      -MOVED       |
|    Read-Write   |        OK        |     -MOVED       |    -MIGRATING    |      -ASK         |        OK        |      -MOVED       |

