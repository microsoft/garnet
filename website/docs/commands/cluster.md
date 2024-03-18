---
id: cluster
sidebar_label: Cluster Commands
title: Cluster Commands
slug: cluster
---


# Cluster Commands

## CLUSTER ADDSLOTS

#### Syntax

```bash
	CLUSTER ADDSLOTS slot [slot ...]
```

This command is used to assign specific slots to a given node.
The node receiving the command will perform the following checks:

1. A node will refuse to acquire ownership of a slot that is already assigned to another node, from its point of view.
2. The command will fail if a node is specified multiple times.
3. If the slot value specified is out of the designated range (0-16383), the command will return out of range error.

This command is used initially to setup a redis cluster or fix a broken cluster where slots ranges might be unassigned due to failures.

#### RESP Reply
Returns +OK on success, otherwise --ERR message

----

## CLUSTER ADDSLOTSRANGE

#### Syntax

```bash
	CLUSTER ADDSLOTSRANGE start-slot end-slot [start-slot end-slot ...]
```

This command is similar to addslots and only differs in that it allows for specifying slot ranges assigned to the receiving node.

#### RESP Reply
Returns +OK on success, otherwise --ERR message

----

## CLUSTER BUMPEPOCH

#### Syntax

```bash
	CLUSTER BUMPEPOCH
```

Advances the config epoch of the receiving node.
The config epoch is used internally to apply configuration changes across the cluster.
Migration and failover operations automatically bump the configuration epoch when necessary.
It should be used with caution by the cluster orchestrator as needed to adjust cluster configuration.

#### RESP Reply
Returns +OK on success, otherwise --ERR message if any

----

## CLUSTER BANLIST

#### Syntax

```bash
	CLUSTER BANLIST
```

Return the list of nodes currently banned from being added to the cluster along with expiry time in seconds.
This command is used in combination with ```CLUSTER FORGET``` to monitor which nodes are banned from being added in the cluster.

#### RESP Reply
Returns array list of strings "nodeid : expiry" or empty array.

```bash
127.0.0.1:7000> cluster banlist
1) "ad9e5b8bde5ffb0cf7a3372fe0345f765186983f : 57"
```
----

## CLUSTER COUNTKEYSINSLOT

#### Syntax

```bash
	CLUSTER COUNTKEYSINSLOT slot
```

Returns the number of existing keys hashing to the corresponding slot.
The command queries only the data of the receiving node.
For slots not owned by the receiving node, the result returned is always zero.

#### RESP Reply
Returns integer value.

----

## CLUSTER DELKEYSINSLOT

#### Syntax

```bash
	CLUSTER DELKEYSINSLOT slot [slot ...]
```

This command asks the receiving node to delete all keys mapping to the provided nodes.
The cluster operator can use this command to cleanup any orphaned keys which are no longer served by the receiving node.

#### RESP Reply
Returns +OK on success, otherwise --ERR message if any

----

## CLUSTER DELKEYSINSLOTRANGE

#### Syntax

```bash
	CLUSTER DELKEYSINSLOTRANGE start-slot end-slot [start-slot end-slot ...]
```

This command is similar to delkeysinslot and only differs in that it allows for specifying slot ranges.

#### RESP Reply
Returns +OK on success, otherwise --ERR message if any

----

## CLUSTER DELSLOTS

#### Syntax

```bash
	CLUSTER DELSLOTS slot [slot ...]
```

This command asks the receiving node to forget which primary serves the corresponding slots.
Note that this command will work only for the receiving node.
Forgeting slots will not propagate to other nodes.
The receiving node will not stop accepting gossip messages.
In that case, if a gossip message is received from a remote node containing assignment of these nodes, that
node will accept the assignment if the config epoch of the slot owner is greater than the local copy.

#### RESP Reply
Returns +OK on success, otherwise --ERR message if any

----

## CLUSTER DELSLOTSRANGE

#### Syntax

```bash
	CLUSTER DELSLOTSRANGE start-slot end-slot [start-slot end-slot ...]
```

This command is similar to delslots and only differs in that it allows for specifying slot ranges to be forgotten at the the receiving node.

#### RESP Reply
Returns +OK on success, otherwise --ERR message if any

----

## CLUSTER ENDPOINT

#### Syntax

```bash
	CLUSTER ENDPOINT node-id
```

This command returns the endpoint associated with the given node-id.
If the receiving node does not have any information about the provided node-id it will return "unassigned:0".

#### RESP Reply
Bulk string of endpoint "address:port" or "unassigned:0"

----

## CLUSTER FAILOVER

#### Syntax

```bash
	CLUSTER FAILOVER [FORCE | ABORT]
```

This command is sent to a replica to start a manual failover and take the role of its primary.
It can be called to safely swap the current primary to a replica node without any data loss, if the primary is reachable.
When a replica receives the above command it executes the following steps

1. Issue stop writes to primary. Behavior can be overriden using FORCE option.
2. Primary replies with current replication offset after it has blocked any writes. Behavior can be overriden using FORCE option.
3. Replica awaits for its local replication offset to match that of the primary. Behavior can be overriden using FORCE option.
4. Replica takes over as new primary and bumps its local config epoch.
5. New primary propagates configuration change through gossip.

The cluster operator can also use the ABORT option to abort an ongoing failover where appropriate.

#### RESP Reply
Returns +OK on success, otherwise --ERR message if any.
This does not mean that FAILOVER has succeeded, only that the failover background task has started.

----

## CLUSTER FORGET

#### Syntax

```bash
	CLUSTER FORGET node-id [expiry-in-seconds]
```

This command is used to forget a node from the perspective of the receiving node.
Because other nodes in the cluster might know about the node that is being forgotten, it is possible that the forgotten node will be re-added through gossip.
For this reason, every forgotten node is associated with an expiry that prevents, that node to be re-added for the expiration period.
The cluster operator can provide a custom expiration period. The default value is 60 seconds.
In order to forget the node across the cluster, the operator has to issue forget to all nodes in the cluster individually.
The command will fail if any of the following occurs:

1. A node cannot forget itself.
2. An unknown node cannot be forgotten.
3. The receiving node is a replica and the node to be forgotten is its primary.

#### RESP Reply
Returns +OK on success, otherwise --ERR message if any.

----

## CLUSTER GETKEYSINSLOT

#### Syntax

```bash
	CLUSTER GETKEYSINSLOT slot count
```

This command returns an array of existing keys that map to the corresponding slot.
It is used during manual re-sharding along with other related commands (i.e. countkeysinslot).

#### RESP Reply
An array of keys or empty.

----

## CLUSTER KEYSLOT

#### Syntax

```bash
	CLUSTER KEYSLOT <key>
```

The command returns an integer value identifying the slot the corresponding key hashes to.

#### RESP Reply
Returns integer value in range between 0 to 16383

----

## CLUSTER MEET

#### Syntax

```bash
	CLUSTER MEET address port
```

This command is used to connect Garnet cluster instances to each other.
By default the nodes do not trust its other and only accept nodes that have been introduced through cluster meet or through gossip messages from another trusted node.
The cluster operator is supposed to connect nodes by issuing meet when setting up the cluster.
However, as indicated above the meet issued does not need to be reciprocal.

#### RESP Reply
Returns +OK on success, otherwise --ERR message if any.

----

## CLUSTER MYID

#### Syntax

```bash
	CLUSTER MYID
```

This command returns the unique node-id of the receiving node

----

#### RESP Reply
Returns a bulk string of the node-id

## CLUSTER MYPARENTID

#### Syntax

```bash
	CLUSTER MYPARENTID
```

This command returns the node-id of its parent-node (i.e. primary) if the node is a replica.

#### RESP Reply
Returns a bulk string of the parent-node else it returns its own id.

----

## CLUSTER NODES

#### Syntax

```bash
	CLUSTER NODES
```

This command returns the cluster configuration from the perspective of the receiving node.
The cluster operator should use this command to retrieve cluster information for administrative tasks, debugging and configuration inspections.

The output of the command is space separated csv string which contains information about all known nodes in the cluster.

```bash
PS C:\Dev> redis-cli -p 7000
127.0.0.1:7000> cluster nodes
e39d271c7c4a4afca0e3d97154d2788502af12e3 192.168.1.26:7000@17000,test-host1 myself,master - 0 0 1 connected 0-5461
e0a69e89458c078d61a7a38f8e5f191522dcb1a7 192.168.1.26:7001@17001,test-host2 master - 0 0 2 connected 5462-10922
6fad21a7b28f1f2f05324257abf5e5e3b54e3286 192.168.1.26:7002@17002,test-host3 master - 0 0 3 connected 10923-16383
93a06cbed623ab044f809e2c67f3c3607ec0cc43 192.168.1.26:7003@17003,test-host4 slave e0a69e89458c078d61a7a38f8e5f191522dcb1a7 0 0 7 connected
03742a8ce43a911a81562e4947194bf54a8da2c6 192.168.1.26:7004@17004,test-host5 slave 6fad21a7b28f1f2f05324257abf5e5e3b54e3286 0 0 8 connected
e5f1c7ec263e5b9133d88535572901af881ab644 192.168.1.26:7005@17005,test-host6 slave e39d271c7c4a4afca0e3d97154d2788502af12e3 0 0 9 connected
127.0.0.1:7000>
```

Each line is contains the following fields:

```bash
<id> <ip:port@cport[,hostname]> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> <slot> ... <slot>
```

To be compatible with the resp protocol, we emit the above information when responding to cluster nodes command.
However, not all information is actively used or updated.
For each, we actively use in Garnet, we present a short description below:

1.	id: The node-id, a 40-character globally unique string generated for each node at start-up.
2.	ip:port@cport: The node's endpoint that clients should connect to issue queries. The second port is not being used actively by the Garnet instances nor it is open for communication.
3.	hostname: A human readable string that is always available automatically from the system. It is not configurable at this point.
4.	flags: Comma separated flags myself,master,slave
5. 	master: If the node is a replica and the primary is known, this would be the node-id of the master otherwise -
6. 	ping-sent: N/A
7. 	pong-recv: N/A
8. 	config-epoch: local configuration epoch for receiving node
9. 	slot: slots served by associated node. If node is a replica, it implicitly serves reads for that slot

Note: Unfortunately for this command, the word slave is part of the protocol and cannot be removed, until that part of the API becomes deprecated.

#### RESP Reply
Returns a bulk string of serialized cluster configuration.

----

## CLUSTER REPLICAS

#### Syntax

```bash
	CLUSTER REPLICAS node-id
```

This command returns an array of nodes, replicating the primary specified by the provided node-id.
The returned information use the same format as in cluster nodes.

#### RESP Reply
Array list of nodes replicating the corresponding node.

----

## CLUSTER REPLICATE

#### Syntax

```bash
	CLUSTER REPLICATE node-id
```

This command configures the receiving node to replicate the primary indicated by the corresponding node-id.
Once the command succeeds, the rest of the nodes will be informed about the configuration change through gossip.

A node that receives the above command will accept it assuming the following conditions are met:

1. Receiving node is a primary
2. Provided node-id refers to a known primary.
3. Provided node-id differs from the node-id of the receiving node.
4. The receiving node does not serve any slots.

Upon receipt of the command, the instance will immediately try to attach to the primary and retrieve the latest primary checkpoint.
After receiving it, it will initiate aof sync from primary and start responding to read requests.
The request to replicate is executed async, so the response to the client will be immediate.
This does not mean replica has attached succesfully, only that replicate request was initiated succesfully.

#### RESP Reply
Returns +OK on success, otherwise --ERR message if any.

----

## CLUSTER SET-CONFIG-EPOCH

#### Syntax

```bash
	CLUSTER SET-CONFIG-EPOCH config-epoch
```

This command sets a specific config epoch in a fresh node. It will succeed only if:

1.	The nodes table is empty
2.	The config epoch of the receiving node is zero.

This command is usually called when setting up a new cluster.

#### RESP Reply
Returns +OK on success, otherwise --ERR message if any.

----

## CLUSTER SETSLOT

#### Syntax

```bash
	CLUSTER SETSLOT slot <IMPORTING source-node-id | MIGRATING target-node-id | NODE node-id | STABLE>
```

This command is used to alter the state of a slot in the receiving node during migration.
The cluster operator can use this command in combination with MIGRATE, COUNTKEYSINSLOT, and GETKEYSINSLOT to transfer slot ownership between nodes in the cluster.

The following options are available:

#### MIGRATING

The slot state is set to MIGRATING, if and only if
1. the receiving node is the primary owner of the slot
2. the slot is not already in migrating state
3. the specifief node-id is known and it corresponds to a primary node.

Due to the aforementioned conditions, slots can transition to MIGRATING state only at the source node.
Therefore, any queries on keys associated to a slot that is in MIGRATING state, will be processed at the source node as follows:
- Read commands are processed as usual if key exists otherwise they are redirected to target node using *-MOVED* response.
- Write requests for existing keys are declined using *-MIGRATING* response otherwise they are redirected to the target node using *-ASK* response.

**NOTE:**
This state change is transient so it will not be propagated through gossip to rest of cluster nodes.
However, *-MOVED* redirection will still be emitted pointing to the source node of the slot.

#### IMPORTING

The slot state is set to IMPORTING, if and only if

1. The receiving node is a primary that does not already own the slot.
2. The corresponding source-node-id is a known primary node that owns the slot.

Any queries referring to keys related to the importing slot are processed only if precedeed by ASKING, otherwise all requests emit *-MOVED* response.

**NOTE:**
This state change is transient so it will not be propagated through gossip to rest of cluster nodes.
However, *-MOVED* redirection will still be emitted pointing to the source node of the slot.

##### STABLE

This option is used to clear MIGRATING or IMPORTING state.
It is used mainly to fix the cluster that has stuck in a bad state due to possible failures.

#### NODE

This option is used to transition ownership of the migrating slot to the target node.

If the node receiving the command is in importing state (i.e. the target node), it will set itself at the owner of the node and bump config epoch.
If the node receiving the comamdn is in migrating state (i.e. the source node), it will assign ownernship of the node to the target node but will not bump the epoch.

**NOTE** For more info about the command checkout the key migration page.

#### RESP Reply
Returns +OK on success, otherwise --ERR message if any.

----

## CLUSTER SETSLOTRANGE

#### Syntax

```bash
	CLUSTER SETSLOTRANGE <IMPORTING source-node-id | MIGRATING target-node-id | NODE node-id | STABLE> slot-start slot-end [slot-start slot-end ...]
```

This command is similar to setslot and only differs in that it allows for specifying slot ranges.

#### RESP Reply
Returns +OK on success, otherwise --ERR message if any.

----

## CLUSTER SHARDS

#### Syntax

```bash
	CLUSTER SETSLOTRANGE <IMPORTING source-node-id | MIGRATING target-node-id | NODE node-id | STABLE> slot-start slot-end [slot-start slot-end ...]
```

This commands returns details about the shards in the cluster.
Shards have a single primary node and multiple replicas serving a specific range of slots.
The information returned are generated from the local configuration view of the receiving node.

An example of the command output is shown below:

```bash
PS C:\Dev> redis-cli -p 7001
127.0.0.1:7001> cluster shards
1) 1) "slots"
   2) 1) (integer) 5462
      2) (integer) 10922
   3) "nodes"
   4) 1)  1) "id"
          2) "e0f69d9f8d4d2b2fa100d1dd1fbd7bc2cf4e9396"
          3) "port"
          4) (integer) 7001
          5) "address"
          6) "10.159.2.73"
          7) "role"
          8) "PRIMARY"
          9) "replication-offset"
         10) (integer) 64
         11) "health"
         12) "online"
      2)  1) "id"
          2) "4e79261982fe0162262da2f912f6dd4bc9161099"
          3) "port"
          4) (integer) 7003
          5) "address"
          6) "10.159.2.73"
          7) "role"
          8) "REPLICA"
          9) "replication-offset"
         10) (integer) 0
         11) "health"
         12) "online"
2) 1) "slots"
   2) 1) (integer) 0
      2) (integer) 5461
   3) "nodes"
   4) 1)  1) "id"
          2) "228133c63d1151f784c9404241c8b06afbc83117"
          3) "port"
          4) (integer) 7000
          5) "address"
          6) "10.159.2.73"
          7) "role"
          8) "PRIMARY"
          9) "replication-offset"
         10) (integer) 0
         11) "health"
         12) "online"
      2)  1) "id"
          2) "96cd8f6d4b57cea02deb9147cdcfac79a4641d0c"
          3) "port"
          4) (integer) 7005
          5) "address"
          6) "10.159.2.73"
          7) "role"
          8) "REPLICA"
          9) "replication-offset"
         10) (integer) 0
         11) "health"
         12) "online"
3) 1) "slots"
   2) 1) (integer) 10923
      2) (integer) 16383
   3) "nodes"
   4) 1)  1) "id"
          2) "810544afb338b6b217a6e169b0115d70337bf557"
          3) "port"
          4) (integer) 7002
          5) "address"
          6) "10.159.2.73"
          7) "role"
          8) "PRIMARY"
          9) "replication-offset"
         10) (integer) 0
         11) "health"
         12) "online"
      2)  1) "id"
          2) "2035250845f809ab23a2f4c51e73b1c4541c0092"
          3) "port"
          4) (integer) 7004
          5) "address"
          6) "10.159.2.73"
          7) "role"
          8) "REPLICA"
          9) "replication-offset"
         10) (integer) 0
         11) "health"
         12) "online"
```

#### RESP Reply
Array reply: a nested list of hash ranges and node information grouped by shard.

----




