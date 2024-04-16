---
id: sharding
sidebar_label: Sharding
title: Sharding Overview
---
<!---
    Sharding components
    - Cluster configuration
        1. Maintained information
        2. Configuration update
        3. Configuration propagation
    - Slot redirection


    Cluster configuration is a data structure containing information about slot assignmnet and known nodes in the cluster.

    HashSlot

    A single instances maintains a local copy of co
    Key space is sharded across instances at any given
-->

# Sharding Overview

## Cluster Configuration

Every running instance in the cluster maintains a local copy of the cluster configuration.
This local copy contains information related to the known workers in the cluster and the corresponding slot assignment, which are stored inside an array of structs respectively.
Nodes exchange information about their local configuration through gossiping as they configuration evolves over time.
Incoming configuration from a remote node is merged atomically with local configuration.
This is done per worker and if and only if that worker's incoming configurationEpoch is greater than the configurationEpoch of that worker in the local copy.
Will leverage this mechanism to timely propagate updates in configuration as described at the next section. 

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

At initialization the workers array contains only information about the local node.
Clusters nodes are introduced to each other through cluster meet at which point they will exchange cluster config information through gossiping.
When that happens nodes will add each other to their own configuration.
Information about the local worker is always stored at workers[1], while workers[0] is reserved for used with slot assignment as described later.
Remote node configuration information is stored in any order starting at workers[2].

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

From the perspective of a newly started cluster node, slots are unassigned hence their initial state is set to OFFLINE and workerId to 0.
WorkerId represents an offset in the workers array which corresponds to the slot owner.
SlotState is used to serve requests on keys mapping to that specific slot.
Owners of a slot can perform read/write and migrate operations on the associated data.
During migration, the slot state is transient, and the owner node can serve only read requests.
The target of migration can potentially serve write requests to that specific slot using the appropriate RESP command.
In this case, the original owner maintains ownership but needs to redirect clients to target node for write requests.
To achieve this, we use _workerId when implementing the redirection logic and workerId property for everything else.

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

### Update propagation
Any updates to the working copy of the cluster config is performed atomically.
Direct updates to the local copy are not visible to remote nodes until bumpepoch is called.
We leverage this to avoid propagating configuration changes for situations where the associated operation has not completed in full (i.e. slot migration).
for transient states (i.e. slot MIGRATING state).


## Redirection Messages

<!---
    TODO: Talk about redirection
-->
