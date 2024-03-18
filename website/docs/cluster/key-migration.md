---
id: key-migration
sidebar_label: Key Migration
title: Key Migration
slug: key-migration
---

# Resharding Overview

Garnet cluster supports scale up/down operations as a way to mitigate demand variability or failovers.
The cluster operator can add/remove nodes and perform online resharding without any down time (i.e. clients are still able to perform queries while the associated data migrate between nodes).
Under the covers, resharding is achieved by utilizing a combination of CLUSTER SETSLOT and MIGRATE commands.
This page presents an overview of the options and features supported by these commands and some examples on how to use them to reshard slots among available primary nodes.
For more details about the commands mentioned herewith, please consult the cluster commands doc.

# Slot Migration Overview

Migrating a slot from a (source) primary node to another (target) primary node involves the following steps

1. Change slot state at target node to IMPORTING state.
2. Change slot state at source node to MIGRATING state.
3. Migrate actual keys from source to target node
4. Inform target and source nodes of ownership change by changing the slot owner and state.

The cluster operator can either manually perform the above steps using a combination of CLUSTER SETSLOT, MIGRATE, CLUSTER COUNTKEYSINSLOT and CLUSTER GETKEYSINSLOT commands
or utilize a variant of MIGRATE command to automatically perform all steps of the process.

## Manual slot migration

The first step of migrating a slot between two nodes involves setting slot to IMPORTING and MIGRATING respectively.
This is achieve using either of the following commands:

```
CLUSTER SETSLOT <slot> <IMPORTING node-id | MIGRATING node-id | NODE node-id | STABLE>
```
```
CLUSTER SETSLOTRANGE <IMPORTING node-id | MIGRATING node-id | NODE node-id | STABLE> start-slot end-slot [start-slot end-slot ...]
```

Changing the slot state prevents clients for performing write operations at the source node until migration completes.
This ensures write safety, however it does not block reads for existing keys.
The source node will redirect reads to non-existent keys and writes to target node.
Clients can use ASKING command in front of every write to override write restrictions.
However, this should be used with care because there are no safeguards implemented to prevent from loosing writes while migration is happening.

The next step after changing the state of the slot that is being migrated, is two find all keys hashing to the corresponding migrate slot and transfer them to the target node.
This is achieved using the following commands:

```
CLUSTER COUNTKEYSINSLOT slot
```

```
CLUSTER GETKEYSINSLOT slot
```

```
migrate <host> <port> [KEY] | [destination-db] [COPY] [REPLACE] [AUTH password] [AUTH2 username password] [KEYS keys]
```

The operator can utilize the above commands to iterate through existing keys and transfer them to the target node.
When data migration completes the operator should issue ```CLUSTER SETSLOT NODE <node-id>``` to source and target node to complete slot change of ownernship.
If migration fails the cluster operator is responsible for resolving any possible errors to bring the cluster in a stable state.
This can be done by using a combination of the following commands as needed

```CLUSTER SETSLOT STABLE``` 

```CLUSTER DELKEYSINSLOT <slot>```

```CLUSTER DELKEYSINSLOTRANGE start-slot end-slot [start-slot end-slot ...]```.



## Automatic slot migration

Manually migrating keys is a cumbersome operation and prone to errors.
The cluster operator can overcome this by utilizing a variant of migrate command that migrates entire slots or ranges of slots at a time.

```
migrate <host> <port> [KEY] | [destination-db] [COPY] [REPLACE] [AUTH password] [AUTH2 username password] [[SLOTS slot] [SLOTSRANGE start-slot end-slot [start-slot end-slot ...]]
```

The slots variant of migrate command will perform all the aforementioned operations automatically.
It will spawn a background that performs all the necessary operations.
The cluster operator can monitor the progress of migration by utilizing the following commands:

```
CLUSTER MTASKS
```

```
CLUSTER SLOTSTATE <slot>
```

# Migrate slots example

This section presents and example on how to use migrate slots option to migrate multiple slots at once.
This example assumes the existence of cluster deployment with 2 nodes, as shown below:

```bash
127.0.0.1:7000> cluster nodes
7e59ecfef6cae5e22b47d8f63c789a1e66031a60 192.168.1.26:7000@17000,test-host1 myself,master - 0 0 1 connected 0-8191
4cb7e9cb54684d20f777c2916145acfd6be48efe 192.168.1.26:7001@17001,test-host2 master - 0 0 2 connected 8192-16383
```

After adding data to node 2, we can identify which slots contain the corresponding keys.
For the sake of this test case, we will be moving only the associated slots to node 1.
Alternatively, we can move entire slot ranges or individual keys manually.

```bash
PS C:\Dev> redis-cli -p 7001
127.0.0.1:7001> set x 12
OK
127.0.0.1:7001> set y 22
OK
127.0.0.1:7001> set a 33
OK
127.0.0.1:7001> set d 44
OK
127.0.0.1:7001> get x
"12"
127.0.0.1:7001> get y
"22"
127.0.0.1:7001> get a
"33"
127.0.0.1:7001> get d
"44"
127.0.0.1:7001> cluster nodes
4cb7e9cb54684d20f777c2916145acfd6be48efe 192.168.1.26:7001@17001,test-host2 myself,master - 0 0 2 connected 8192-16383
7e59ecfef6cae5e22b47d8f63c789a1e66031a60 192.168.1.26:7000@17000,test-host1 master - 0 0 1 connected 0-8191
127.0.0.1:7001> cluster keyslot x
(integer) 16287
127.0.0.1:7001> cluster keyslot y
(integer) 12222
127.0.0.1:7001> cluster keyslot a
(integer) 15495
127.0.0.1:7001> cluster keyslot d
(integer) 11298
```

To move individual slots, we utilize the SLOTS option as shown below:

```bash
127.0.0.1:7001> migrate 192.168.1.26 7000 "" 0 -1 SLOTS 16287 12222 15495 11298
OK
127.0.0.1:7001> cluster nodes
4cb7e9cb54684d20f777c2916145acfd6be48efe 192.168.1.26:7001@17001,test-host2 myself,master - 0 0 2 connected 8192-11297 11299-12221 12223-15494 15496-16286 16288-16383
7e59ecfef6cae5e22b47d8f63c789a1e66031a60 192.168.1.26:7000@17000,test-host1 master - 0 0 3 connected 0-8191 11298 12222 15495 16287
```

After migration completes, we can see the updated configuration by calling ```cluster nodes``` at each node.
At this point if we issue a read request towards node 2, we receive a redirection message to node 1.
To access the migrated keys, we have connect to node 1 since it is the owner of the slots associated with corresponding keys.

```bash
127.0.0.1:7001> get x
(error) MOVED 16287 192.168.1.26:7000
127.0.0.1:7001> get y
(error) MOVED 12222 192.168.1.26:7000
127.0.0.1:7001> get a
(error) MOVED 15495 192.168.1.26:7000
127.0.0.1:7001> get d
(error) MOVED 11298 192.168.1.26:7000
127.0.0.1:7001> exit
PS C:\Dev> redis-cli -p 7000
127.0.0.1:7000> get x
"12"
127.0.0.1:7000> get y
"22"
127.0.0.1:7000> get a
"33"
127.0.0.1:7000> get d
"44"
127.0.0.1:7000> cluster nodes
7e59ecfef6cae5e22b47d8f63c789a1e66031a60 192.168.1.26:7000@17000,test-host1 myself,master - 0 0 3 connected 0-8191 11298 12222 15495 16287
4cb7e9cb54684d20f777c2916145acfd6be48efe 192.168.1.26:7001@17001,test-host2 master - 0 0 2 connected 8192-11297 11299-12221 12223-15494 15496-16286 16288-16383
127.0.0.1:7000>
```



