---
id: api-compatibility
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
| <span id="generic">**GENERIC**</span> | [PERSIST](../commands/generic-commands.md#persist) | ➖ |  |
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
| <span id="scripting">**SCRIPTING**</span> | [EVAL](../commands/scripting-and-functions.md#eval) | ➕ |  |
|  | EVAL_RO | ➖ |  |
|  | [EVALSHA](../commands/scripting-and-functions.md#evalsha) | ➕ |  |
|  | EVALSHA_RO | ➖ |  |
|  | SCRIPT DEBUG | ➖ |  |
|  | [SCRIPT EXISTS](../commands/scripting-and-functions.md#script-exists) | ➕ |  |
|  | [SCRIPT FLUSH](../commands/scripting-and-functions.md#script-flush) | ➕ |  |
|  | SCRIPT HELP | ➖ |  |
|  | SCRIPT KILL | ➖ |  |
|  | [SCRIPT LOAD](../commands/scripting-and-functions.md#script-load) | ➕ |  |
| <span id="server">**SERVER**</span> | ACL | ➖ |  |
|  | BGREWRITEAOF | ➖ |  |
|  | [BGSAVE](../commands/checkpoint.md#bgsave) | ➖ |  |
|  | [COMMITAOF](../commands/server.md#commitaof) | ➖ |  |
|  | [CONFIG GET](../commands/server.md#config-get) | ➕ |  |
|  | CONFIG HELP | ➖ |  |
|  | CONFIG RESETSTAT | ➖ |  |
|  | CONFIG REWRITE | ➖ |  |
|  | [CONFIG SET](../commands/server.md#config-set) | ➖ |  |
|  | [DBSIZE](../commands/server.md#dbsize) | ➖ |  |
|  | [DEBUG](../commands/server.md#debug) | ➖ | Internal command |
|  | [FLUSHALL](../commands/server.md#flushall) | ➖ |  |
|  | [FLUSHDB](../commands/server.md#flushdb) | ➕ |  |
|  | [LASTSAVE](../commands/checkpoint.md#lastsave) | ➖ |  |
|  | LOLWUT | ➖ |  |
|  | [MONITOR](../commands/server.md#monitor) | ➖ |  |
|  | PSYNC | ➖ |  |
|  | REPLCONF | ➖ |  |
|  | [REPLICAOF](../commands/server.md#replicaof) | ➖ |  |
|  | RESTORE-ASKING | ➖ |  |
|  | [ROLE](../commands/server.md#role) | ➖ |  |
|  | [SAVE](../commands/checkpoint.md#save) | ➖ |  |
|  | SHUTDOWN | ➖ |  |
|  | [SLAVEOF](../commands/server.md#slaveof) | ➖ | (Deprecated) |
|  | [SWAPDB](../commands/server.md#swapdb) | ➖ |  |
|  | SYNC | ➖ |  |
|  | [TIME](../commands/server.md#time) | ➖ |  |
| <span id="set">**SET**</span> | [SADD](../commands/data-structures.md#sadd) | ➕ |  |
|  | [SCARD](../commands/data-structures.md#scard) | ➕ |  |
|  | [SDIFF](../commands/data-structures.md#sdiff) | ➕ |  |
|  | [SDIFFSTORE](../commands/data-structures.md#sdiffstore) | ➕ |  |
|  | [SINTER](../commands/data-structures.md#sinter) | ➕ |  |
|  | [SINTERSTORE](../commands/data-structures.md#sinterstore) | ➕ |  |
|  | [SINTERCARD](../commands/data-structures.md#sintercard) | ➕ |  |
|  | [SISMEMBER](../commands/data-structures.md#sismember) | ➕ |  |
|  | [SMEMBERS](../commands/data-structures.md#smembers) | ➕ |  |
|  | [SMISMEMBER](../commands/data-structures.md#smismember) | ➕ |  |
|  | [SMOVE](../commands/data-structures.md#smove) | ➕ |  |
|  | [SPOP](../commands/data-structures.md#spop) | ➕ |  |
|  | SPUBLISH | ➖ |  |
|  | [SRANDMEMBER](../commands/data-structures.md#srandmember) | ➕ |  |
|  | [SREM](../commands/data-structures.md#srem) | ➕ |  |
|  | [SSCAN](../commands/data-structures.md#sscan) | ➕ |  |
|  | SSUBSCRIBE | ➖ |  |
|  | [SUNION](../commands/data-structures.md#sunion) | ➕ |  |
|  | [SUNIONSTORE](../commands/data-structures.md#sunionstore) | ➕ |  |
|  | SUNSUBSCRIBE | ➖ |  |
| <span id="sorted-set">**SORTED SET**</span> | [BZMPOP](../commands/data-structures.md#bzmpop) | ➕ |  |
|  | [BZPOPMAX](../commands/data-structures.md#bzpopmax) | ➕ |  |
|  | [BZPOPMIN](../commands/data-structures.md#bzpopmin) | ➕ |  |
|  | [ZADD](../commands/data-structures.md#zadd) | ➕ |  |
|  | [ZCARD](../commands/data-structures.md#zcard) | ➕ |  |
|  | [ZCOUNT](../commands/data-structures.md#zcount) | ➕ |  |
|  | [ZDIFF](../commands/data-structures.md#zdiff) | ➕ |  |
|  | [ZDIFFSTORE](../commands/data-structures.md#zdiffstore) | ➕ |  |
|  | [ZINCRBY](../commands/data-structures.md#zincrby) | ➕ |  |
|  | [ZINTER](../commands/data-structures.md#zinter) | ➕ |  |
|  | [ZINTERCARD](../commands/data-structures.md#zintercard) | ➕ |  |
|  | [ZINTERSTORE](../commands/data-structures.md#zinterstore) | ➕ |  |
|  | [ZLEXCOUNT](../commands/data-structures.md#zlexcount) | ➕ |  |
|  | [ZMPOP](../commands/data-structures.md#zmpop) | ➕ |  |
|  | [ZMSCORE](../commands/data-structures.md#zmscore) | ➕ |  |
|  | [ZPOPMAX](../commands/data-structures.md#zpopmax) | ➕ |  |
|  | [ZPOPMIN](../commands/data-structures.md#zpopmin) | ➕ |  |
|  | [ZRANDMEMBER](../commands/data-structures.md#zrandmember) | ➕ |  |
|  | [ZRANGE](../commands/data-structures.md#zrange) | ➕ |  |
|  | [ZRANGEBYLEX](../commands/data-structures.md#zrangebylex) | ➕ | (Deprecated) |
|  | [ZRANGEBYSCORE](../commands/data-structures.md#zrangebyscore) | ➕ | (Deprecated) |
|  | [ZRANGESTORE](../commands/data-structures.md#zrangestore) | ➕ |  |
|  | [ZRANK](../commands/data-structures.md#zrank) | ➕ |  |
|  | [ZREM](../commands/data-structures.md#zrem) | ➕ |  |
|  | [ZREMRANGEBYLEX](../commands/data-structures.md#zremrangebylex) | ➕ |  |
|  | [ZREMRANGEBYRANK](../commands/data-structures.md#zremrangebyrank) | ➕ |  |
|  | [ZREMRANGEBYSCORE](../commands/data-structures.md#zremrangebyscore) | ➕ |  |
|  | [ZREVRANGE](../commands/data-structures.md#zrevrange) | ➕ | (Deprecated) |
|  | [ZREVRANGEBYLEX](../commands/data-structures.md#zrevrangebylex) | ➕ | (Deprecated) |
|  | [ZREVRANGEBYSCORE](../commands/data-structures.md#zrevrangebyscore) | ➕ | (Deprecated) |
|  | [ZREVRANK](../commands/data-structures.md#zrevrank) | ➕ |  |
|  | [ZSCAN](../commands/data-structures.md#zscan) | ➕ |  |
|  | [ZSCORE](../commands/data-structures.md#zscore) | ➕ |  |
|  | [ZUNION](../commands/data-structures.md#zunion) | ➕ |  |
|  | [ZUNIONSTORE](../commands/data-structures.md#zunionstore) | ➕ |  |
| <span id="string">**STRING**</span> | [APPEND](../commands/raw-string.md#append) | ➕ |  |
|  | [DECR](../commands/raw-string.md#decr) | ➕ |  |
|  | [DECRBY](../commands/raw-string.md#decrby) | ➕ |  |
|  | [GET](../commands/raw-string.md#get) | ➕ |  |
|  | [GETDEL](../commands/raw-string.md#getdel) | ➕ |  |
|  | [GETEX](../commands/raw-string.md#getex) | ➕ |  |
|  | [GETRANGE](../commands/raw-string.md#getrange) | ➕ |  |
|  | [GETSET](../commands/raw-string.md#getset) | ➕ |  |
|  | [INCR](../commands/raw-string.md#incr) | ➕ |  |
|  | [INCRBY](../commands/raw-string.md#incrby) | ➕ |  |
|  | [INCRBYFLOAT](../commands/raw-string.md#incrbyfloat) | ➕ |  |
|  | [LCS](../commands/raw-string.md#lcs) | ➕ |  |
|  | [MGET](../commands/raw-string.md#mget) | ➕ |  |
|  | [MSET](../commands/raw-string.md#mset) | ➕ |  |
|  | [MSETNX](../commands/raw-string.md#msetnx) | ➕ |  |
|  | [PSETEX](../commands/raw-string.md#psetex) | ➕ | (Deprecated) |
|  | [SET](../commands/raw-string.md#set) | ➕ |  |
|  | [SETEX](../commands/raw-string.md#setex) | ➕ | (Deprecated) |
|  | [SETNX](../commands/raw-string.md#setnx) | ➕ |  |
|  | [SETRANGE](../commands/raw-string.md#setrange) | ➕ |  |
|  | [STRLEN](../commands/raw-string.md#strlen) | ➕ |  |
|  | [SUBSTR](../commands/raw-string.md#substr) | ➕ | (Deprecated) |


## Learn More

- [Getting Started](./quickstart.md)
- [Cluster Configuration](./cluster-configuration.md)
