---
id: azure-api-compatibility
sidebar_label: API Compatibility
title: API compatibility for Azure Cosmos DB Garnet Cache
---

# API compatibility for Azure Cosmos DB Garnet Cache

Below is the full list of API commands and their implementation status in Azure Cosmos DB Garnet Cache. There is compatibility with Redis clients and support for a subset of Redis data types and commands. The maximum size for a key value pair is 32MB. For the lowest latency, it’s recommended to keep the size of a key value pair to around 1KB. 

## Command Categories

The Azure Cosmos DB Garnet Cache implements a growing subset of the [open-source Garnet commands](../commands/api-compatibility.md). Categories with at least one supported command are listed here.

1. [CLIENT](#client)
2. [CLUSTER](#cluster)
3. [COMMAND](#command)
4. [CONNECTION](#connection)
5. [GENERIC](#generic)
6. [HASH](#hash)
7. [KEYS](#keys)
8. [LATENCY](#latency)
9. [PUB/SUB](#pubsub)
10. [SCRIPTING](#scripting)
11. [SERVER](#server)
12. [SET](#set)
13. [SORTED SET](#sorted-set)
14. [STRING](#string)

## Full Commands List

| Category | Command | Implemented in Azure Cosmos DB Garnet Cache | Notes |
| ------------- | ------------- | ------------- | ------------- | 
| <span id="client">**CLIENT**</span> | CACHING | ➖ |  |
|  | [GETNAME](../commands/client.md#client-getname) | ➖ |  |
|  | GETREDIR | ➖ |  |
|  | HELP | ➖ |  |
|  | [ID](../commands/client.md#client-id) | ➕ |  |
|  | [INFO](../commands/client.md#client-info) | ➕ |  |
|  | [KILL](../commands/client.md#client-kill) | ➖ |  |
|  | [LIST](../commands/client.md#client-list) | ➖ |  |
|  | NO-EVICT | ➖ |  |
|  | NO-TOUCH | ➖ |  |
|  | PAUSE | ➖ |  |
|  | REPLY | ➖ |  |
|  | [SETINFO](../commands/client.md#client-setinfo) | ➖ |  |
|  | [SETNAME](../commands/client.md#client-setname) | ➖ |  |
|  | TRACKING | ➖ |  |
|  | TRACKINGINFO | ➖ |  |
|  | [UNBLOCK](../commands/client.md#client-unblock) | ➖ |  |
|  | UNPAUSE | ➖ |  |
| <span id="cluster">**CLUSTER**</span> | [ADDSLOTS](../commands/cluster.md#cluster-addslots) | ➖ |  |
|  | [ADDSLOTSRANGE](../commands/cluster.md#cluster-addslotsrange) | ➖ |  |
|  | [ASKING](../commands/cluster.md#asking) | ➖ |  |
|  | [BUMPEPOCH](../commands/cluster.md#cluster-bumpepoch) | ➖ |  |
|  | COUNT-FAILURE-REPORTS | ➖ |  |
|  | [COUNTKEYSINSLOT](../commands/cluster.md#cluster-countkeysinslot) | ➖ |  |
|  | [DELSLOTS](../commands/cluster.md#cluster-delslots) | ➖ |  |
|  | [DELSLOTSRANGE](../commands/cluster.md#cluster-delslotsrange) | ➖ |  |
|  | [FAILOVER](../commands/cluster.md#cluster-failover) | ➖ |  |
|  | FLUSHSLOTS | ➖ |  |
|  | [FORGET](../commands/cluster.md#cluster-forget) | ➖ |  |
|  | [GETKEYINSLOT](../commands/cluster.md#cluster-getkeysinslot) | ➖ |  |
|  | [INFO](../commands/cluster.md#cluster-info) | ➖ |  |
|  | [KEYSLOT](../commands/cluster.md#cluster-keyslot) | ➕ |  |
|  | LINKS | ➖ |  |
|  | [MEET](../commands/cluster.md#cluster-meet) | ➖ |  |
|  | [MYID](../commands/cluster.md#cluster-myid) | ➖ |  |
|  | MYSHARDID | ➖ |  |
|  | [NODES](../commands/cluster.md#cluster-nodes) | ➕ |  |
|  | [READONLY](../commands/cluster.md#readonly) | ➕ |  |
|  | [READWRITE](../commands/cluster.md#readwrite) | ➕ |  |
|  | [REPLICAS](../commands/cluster.md#cluster-replicas) | ➖ |  |
|  | [REPLICATE](../commands/cluster.md#cluster-replicate) | ➖ |  |
|  | [RESET](../commands/cluster.md#reset) | ➖ |  |
|  | SAVECONFIG | ➖ |  |
|  | [SET-CONFIG-EPOCH](../commands/cluster.md#cluster-set-config-epoch) | ➖ |  |
|  | [SETSLOT](../commands/cluster.md#cluster-setslot) | ➖ |  |
|  | SHARDS | ➖ |  |
|  | [SLAVES](../commands/cluster.md#slaves) | ➖ | (Deprecated) |
|  | [SLOTS](../commands/cluster.md#cluster-slots) | ➕ | (Deprecated) |
| <span id="command">**COMMAND**</span> | [COMMAND](../commands/server.md#command) | ➖ |  |
|  | [COUNT](../commands/server.md#command-count) | ➖ |  |
|  | [DOCS](../commands/server.md#command-docs) | ➖ |  |
|  | [GETKEYS](../commands/server.md#command-getkeys) | ➖ |  |
|  | [GETKEYSANDFLAGS](../commands/server.md#command-getkeysandflags) | ➖ |  |
|  | HELP | ➖ |  | 
|  | [INFO](../commands/server.md#command-info) | ➕ |  | 
|  | LIST | ➖ |  | 
| <span id="connection">**CONNECTION**</span> | [AUTH](../commands/generic-commands.md#auth) | ➖ |  |
|  | [ECHO](../commands/generic-commands.md#echo) | ➕ |  |
|  | [HELLO](../commands/generic-commands.md#hello) | ➖ |  |
|  | [PING](../commands/generic-commands.md#ping) | ➕ |  |
|  | [QUIT](../commands/generic-commands.md#quit) | ➖ | (Deprecated) |
|  | [SELECT](../commands/generic-commands.md#select) | ➖ |  |
| <span id="generic">**GENERIC**</span> | [PERSIST](generic-commands.md#persist) | ➖ |  |
|  | [PEXPIRE](../commands/generic-commands.md#pexpire) | ➖ |  |
|  | [PEXPIREAT](../commands/generic-commands.md#pexpireat) | ➖ |  |
|  | [PEXPIRETIME](../commands/generic-commands.md#pexpiretime) | ➖ |  |
|  | [PTTL](../commands/generic-commands.md#pttl) | ➖ |  |
|  | RANDOMKEY | ➖ |  |
|  | [RENAME](../commands/generic-commands.md#rename) | ➖ |  |
|  | [RENAMENX](../commands/generic-commands.md#renamenx) | ➖ |  |
|  | [RESTORE](../commands/generic-commands.md#restore) | ➖ |
|  | [SCAN](../commands/generic-commands.md#scan) | ➖ |  |
|  | SORT | ➖ |  |
|  | SORT_RO | ➖ |  |
|  | TOUCH | ➖ |  |
|  | [TTL](../commands/generic-commands.md#ttl) | ➖ |  |
|  | [TYPE](../commands/generic-commands.md#type) | ➖ |  |
|  | [UNLINK](../commands/generic-commands.md#unlink) | ➕ |  |
|  | WAIT | ➖ |  |
|  | WAITAOF | ➖ |  |
| <span id="hash">**HASH**</span> | [HDEL](../commands/data-structures.md#hdel) | ➕ |  |
|  | [HEXISTS](../commands/data-structures.md#hexists) | ➕ |  |
|  | [HEXPIRE](../commands/data-structures.md#hexpire) | ➕ |  |
|  | [HEXPIREAT](../commands/data-structures.md#hexpireat) | ➕ |  |
|  | [HEXPIRETIME](../commands/data-structures.md#hexpiretime) | ➕ |  |
|  | [HGET](../commands/data-structures.md#hget) | ➕ |  |
|  | [HGETALL](../commands/data-structures.md#hgetall) | ➕ |  |
|  | [HINCRBY](../commands/data-structures.md#hincrby) | ➕ |  |
|  | [HINCRBYFLOAT](../commands/data-structures.md#hincrbyfloat) | ➕ |  |
|  | [HKEYS](../commands/data-structures.md#hkeys) | ➕ |  |
|  | [HLEN](../commands/data-structures.md#hlen) | ➕ |  |
|  | [HMGET](../commands/data-structures.md#hmget) | ➕ |  |
|  | [HMSET](../commands/data-structures.md#hmset) | ➕ | (Deprecated) |
|  | [HPERSIST](../commands/data-structures.md#hpersist) | ➕ |  |
|  | [HPEXPIRE](../commands/data-structures.md#hpexpire) | ➕ |  |
|  | [HPEXPIREAT](../commands/data-structures.md#hpexpireat) | ➕ |  |
|  | [HPEXPIRETIME](../commands/data-structures.md#hepxpiretime) | ➕ |  |
|  | [HPTTL](../commands/data-structures.md#hpttl) | ➕ |  |
|  | [HRANDFIELD](../commands/data-structures.md#hrandfield) | ➕ |  |
|  | [HSCAN](../commands/data-structures.md#hscan) | ➕ |  |
|  | [HSET](../commands/data-structures.md#hset) | ➕ |  |
|  | [HSETNX](../commands/data-structures.md#hsetnx) | ➕ |  |
|  | [HSTRLEN](../commands/data-structures.md#hstrlen) | ➕ |  |
|  | [HTTL](../commands/data-structures.md#httl) | ➕ |  |
|  | [HVALS](../commands/data-structures.md#hvals) | ➕ |  |
| <span id="keys">**KEYS**</span> | COPY | ➖ |  |
|  | [DEL](../commands/generic-commands.md#del) | ➕ |  |
|  | [DUMP](../commands/generic-commands.md#dump) | ➖ |
|  | [EXISTS](../commands/generic-commands.md#exists) | ➕ |  |
|  | [EXPIRE](../commands/generic-commands.md#expire) | ➕ |  |
|  | [EXPIREAT](../commands/generic-commands.md#expireat) | ➖ |  |
|  | [EXPIRETIME](../commands/generic-commands.md#expiretime) | ➖ |  |
|  | [KEYS](../commands/generic-commands.md#keys) | ➖ |  |
|  | [MIGRATE](../commands/generic-commands.md#migrate) | ➖ |  |
|  | MOVE | ➖ |  |
| <span id="latency">**LATENCY**</span> | DOCTOR | ➖ |  |
|  | GRAPH | ➖ |  |
|  | HELP | ➖ |  |
|  | [HISTOGRAM](../commands/server.md#latency-histogram) | ➕ |  |
|  | HISTORY | ➖ |  |
|  | LATEST | ➖ |  |
|  | [RESET](../commands/server.md#latency-reset) | ➕ |  |
| <span id="pubsub">**PUB/SUB**</span> | [PSUBSCRIBE](../commands/analytics.md#psubscribe) | ➕ |  |
|  | [PUBLISH](../commands/analytics.md#publish) | ➕ |  |
|  | [PUBSUB CHANNELS](../commands/analytics.md#pubsub-channels) | ➕ |  |
|  | PUBSUB HELP | ➖ |  |
|  | [PUBSUB NUMPAT](../commands/analytics.md#pubsub-numpat) | ➕ |  |
|  | [PUBSUB NUMSUB](../commands/analytics.md#pubsub-numsub) | ➕ |  |
|  | PUBSUB SHARDCHANNELS | ➖ |  |
|  | PUBSUB SHARDNUMSUB | ➖ |  |
|  | [PUNSUBSCRIBE](../commands/analytics.md#punsubscribe) | ➕ |  |
|  | [SUBSCRIBE](../commands/analytics.md#subscribe) | ➕ |  |
|  | [UNSUBSCRIBE](../commands/analytics.md#unsubscribe) | ➕ |  |
| <span id="scripting">**SCRIPTING**</span> | [EVAL](scripting-and-functions.md#eval) | ➕ |  |
|  | EVAL_RO | ➖ |  |
|  | [EVALSHA](scripting-and-functions.md#evalsha) | ➕ |  |
|  | EVALSHA_RO | ➖ |  |
|  | SCRIPT DEBUG | ➖ |  |
|  | [SCRIPT EXISTS](scripting-and-functions.md#script-exists) | ➕ |  |
|  | [SCRIPT FLUSH](scripting-and-functions.md#script-flush) | ➕ |  |
|  | SCRIPT HELP | ➖ |  |
|  | SCRIPT KILL | ➖ |  |
|  | [SCRIPT LOAD](scripting-and-functions.md#script-load) | ➕ |  |
| <span id="server">**SERVER**</span> | ACL | ➖ |  |
|  | BGREWRITEAOF | ➖ |  |
|  | [BGSAVE](checkpoint.md#bgsave) | ➖ |  |
|  | [COMMITAOF](server.md#commitaof) | ➖ |  |
|  | [CONFIG GET](server.md#config-get) | ➕ |  |
|  | CONFIG HELP | ➖ |  |
|  | CONFIG RESETSTAT | ➖ |  |
|  | CONFIG REWRITE | ➖ |  |
|  | [CONFIG SET](server.md#config-set) | ➖ |  |
|  | [DBSIZE](server.md#dbsize) | ➖ |  |
|  | [DEBUG](server.md#debug) | ➖ | Internal command |
|  | [FLUSHALL](server.md#flushall) | ➖ |  |
|  | [FLUSHDB](server.md#flushdb) | ➕ |  |
|  | [LASTSAVE](checkpoint.md#lastsave) | ➖ |  |
|  | LOLWUT | ➖ |  |
|  | [MONITOR](server.md#monitor) | ➖ |  |
|  | PSYNC | ➖ |  |
|  | REPLCONF | ➖ |  |
|  | [REPLICAOF](server.md#replicaof) | ➖ |  |
|  | RESTORE-ASKING | ➖ |  |
|  | [ROLE](server.md#role) | ➖ |  |
|  | [SAVE](checkpoint.md#save) | ➖ |  |
|  | SHUTDOWN | ➖ |  |
|  | [SLAVEOF](server.md#slaveof) | ➖ | (Deprecated) |
|  | [SWAPDB](server.md#swapdb) | ➖ |  |
|  | SYNC | ➖ |  |
|  | [TIME](server.md#time) | ➖ |  |
| <span id="set">**SET**</span> | [SADD](data-structures.md#sadd) | ➕ |  |
|  | [SCARD](data-structures.md#scard) | ➕ |  |
|  | [SDIFF](data-structures.md#sdiff) | ➕ |  |
|  | [SDIFFSTORE](data-structures.md#sdiffstore) | ➕ |  |
|  | [SINTER](data-structures.md#sinter) | ➕ |  |
|  | [SINTERSTORE](data-structures.md#sinterstore) | ➕ |  |
|  | [SINTERCARD](data-structures.md#sintercard) | ➕ |  |
|  | [SISMEMBER](data-structures.md#sismember) | ➕ |  |
|  | [SMEMBERS](data-structures.md#smembers) | ➕ |  |
|  | [SMISMEMBER](data-structures.md#smismember) | ➕ |  |
|  | [SMOVE](data-structures.md#smove) | ➕ |  |
|  | [SPOP](data-structures.md#spop) | ➕ |  |
|  | SPUBLISH | ➖ |  |
|  | [SRANDMEMBER](data-structures.md#srandmember) | ➕ |  |
|  | [SREM](data-structures.md#srem) | ➕ |  |
|  | [SSCAN](data-structures.md#sscan) | ➕ |  |
|  | SSUBSCRIBE | ➖ |  |
|  | [SUNION](data-structures.md#sunion) | ➕ |  |
|  | [SUNIONSTORE](data-structures.md#sunionstore) | ➕ |  |
|  | SUNSUBSCRIBE | ➖ |  |
| <span id="sorted-set">**SORTED SET**</span> | [BZMPOP](data-structures.md#bzmpop) | ➕ |  |
|  | [BZPOPMAX](data-structures.md#bzpopmax) | ➕ |  |
|  | [BZPOPMIN](data-structures.md#bzpopmin) | ➕ |  |
|  | [ZADD](data-structures.md#zadd) | ➕ |  |
|  | [ZCARD](data-structures.md#zcard) | ➕ |  |
|  | [ZCOUNT](data-structures.md#zcount) | ➕ |  |
|  | [ZDIFF](data-structures.md#zdiff) | ➕ |  |
|  | [ZDIFFSTORE](data-structures.md#zdiffstore) | ➕ |  |
|  | [ZINCRBY](data-structures.md#zincrby) | ➕ |  |
|  | [ZINTER](data-structures.md#zinter) | ➕ |  |
|  | [ZINTERCARD](data-structures.md#zintercard) | ➕ |  |
|  | [ZINTERSTORE](data-structures.md#zinterstore) | ➕ |  |
|  | [ZLEXCOUNT](data-structures.md#zlexcount) | ➕ |  |
|  | [ZMPOP](data-structures.md#zmpop) | ➕ |  |
|  | [ZMSCORE](data-structures.md#zmscore) | ➕ |  |
|  | [ZPOPMAX](data-structures.md#zpopmax) | ➕ |  |
|  | [ZPOPMIN](data-structures.md#zpopmin) | ➕ |  |
|  | [ZRANDMEMBER](data-structures.md#zrandmember) | ➕ |  |
|  | [ZRANGE](data-structures.md#zrange) | ➕ |  |
|  | [ZRANGEBYLEX](data-structures.md#zrangebylex) | ➕ | (Deprecated) |
|  | [ZRANGEBYSCORE](data-structures.md#zrangebyscore) | ➕ | (Deprecated) |
|  | [ZRANGESTORE](data-structures.md#zrangestore) | ➕ |  |
|  | [ZRANK](data-structures.md#zrank) | ➕ |  |
|  | [ZREM](data-structures.md#zrem) | ➕ |  |
|  | [ZREMRANGEBYLEX](data-structures.md#zremrangebylex) | ➕ |  |
|  | [ZREMRANGEBYRANK](data-structures.md#zremrangebyrank) | ➕ |  |
|  | [ZREMRANGEBYSCORE](data-structures.md#zremrangebyscore) | ➕ |  |
|  | [ZREVRANGE](data-structures.md#zrevrange) | ➕ | (Deprecated) |
|  | [ZREVRANGEBYLEX](data-structures.md#zrevrangebylex) | ➕ | (Deprecated) |
|  | [ZREVRANGEBYSCORE](data-structures.md#zrevrangebyscore) | ➕ | (Deprecated) |
|  | [ZREVRANK](data-structures.md#zrevrank) | ➕ |  |
|  | [ZSCAN](data-structures.md#zscan) | ➕ |  |
|  | [ZSCORE](data-structures.md#zscore) | ➕ |  |
|  | [ZUNION](data-structures.md#zunion) | ➕ |  |
|  | [ZUNIONSTORE](data-structures.md#zunionstore) | ➕ |  |
| <span id="string">**STRING**</span> | [APPEND](raw-string.md#append) | ➕ |  |
|  | [DECR](raw-string.md#decr) | ➕ |  |
|  | [DECRBY](raw-string.md#decrby) | ➕ |  |
|  | [GET](raw-string.md#get) | ➕ |  |
|  | [GETDEL](raw-string.md#getdel) | ➕ |  |
|  | [GETEX](raw-string.md#getex) | ➕ |  |
|  | [GETRANGE](raw-string.md#getrange) | ➕ |  |
|  | [GETSET](raw-string.md#getset) | ➕ |  |
|  | [INCR](raw-string.md#incr) | ➕ |  |
|  | [INCRBY](raw-string.md#incrby) | ➕ |  |
|  | [INCRBYFLOAT](raw-string.md#incrbyfloat) | ➕ |  |
|  | [LCS](raw-string.md#lcs) | ➕ |  |
|  | [MGET](raw-string.md#mget) | ➕ |  |
|  | [MSET](raw-string.md#mset) | ➕ |  |
|  | [MSETNX](raw-string.md#msetnx) | ➕ |  |
|  | [PSETEX](raw-string.md#psetex) | ➕ | (Deprecated) |
|  | [SET](raw-string.md#set) | ➕ |  |
|  | [SETEX](raw-string.md#setex) | ➕ | (Deprecated) |
|  | [SETNX](raw-string.md#setnx) | ➕ |  |
|  | [SETRANGE](raw-string.md#setrange) | ➕ |  |
|  | [STRLEN](raw-string.md#strlen) | ➕ |  |
|  | [SUBSTR](raw-string.md#substr) | ➕ | (Deprecated) |


## Learn More

- [Getting Started](./quickstart.md)
- [Cluster Configuration](./cluster-configuration.md)
