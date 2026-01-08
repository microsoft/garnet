---
id: api-compatibility
sidebar_label: API Compatibility
title: API compatibility
slug: api-compatibility
---

Below is the full list of API commands and their implementation status in Garnet.<br/>
Note that this list is subject to change as we continue to expand our API command support with the help of our growing community.

### Command Categories
1. [ACL](#acl)
2. [BITMAP](#bitmap)
3. [CLIENT](#client)
4. [CLUSTER](#cluster)
5. [COMMAND](#command)
6. [CONNECTION](#connection)
7. [FUNCTIONS](#functions)
8. [GENERIC](#generic)
9. [GEO](#geo)
10. [HASH](#hash)
11. [HYPERLOGLOG](#hyperloglog)
12. [KEYS](#keys)
13. [LATENCY](#latency)
14. [LIST](#list)
15. [MEMORY](#memory)
16. [MODULE](#module)
17. [OBJECT](#object)
18. [PUB/SUB](#pubsub)
19. [SCRIPTING](#scripting)
20. [SERVER](#server)
21. [SET](#set)
22. [SORTED SET](#sorted-set)
23. [STREAM](#stream)
24. [STRING](#string)
25. [TRANSACTIONS](#transactions)

### Full Commands List

| Category | Command | Implemented in Garnet | Notes |
| ------------- | ------------- | ------------- | ------------- | 
| <span id="acl">**ACL**</span> | [CAT](acl.md#acl-cat) | ➕ |  |
|  | [DELUSER](acl.md#acl-deluser) | ➕ |  |
|  | DRYRUN | ➖ |  |
|  | [GENPASS](acl.md#acl-genpass) | ➕ |  |
|  | [GETUSER](acl.md#acl-getuser) | ➕ |  |
|  | [LIST](acl.md#acl-list) | ➕ |  |
|  | [LOAD](acl.md#acl-load) | ➕ |  |
|  | HELP | ➖ |  |
|  | LOG | ➖ |  |
|  | [SAVE](acl.md#acl-save) | ➕ |  |
|  | [SETUSER](acl.md#acl-setuser) | ➕ |  |
|  | [USERS](acl.md#acl-users) | ➕ |  |
|  | [WHOAMI](acl.md#acl-whoami) | ➕ |  |
| <span id="bitmap">**BITMAP**</span> | [BITCOUNT](analytics.md#bitcount) | ➕ |  |
|  | [BITFIELD](analytics.md#bitfield) | ➕ |  |
|  | [BITFIELD_RO](analytics.md#bitfield_ro) | ➕ |  |
|  | [BITOP AND](analytics.md#bitop-and) | ➕ |  |
|  | [BITOP NOT](analytics.md#bitop-not) | ➕ |  |
|  | [BITPOS](analytics.md#bitpos) | ➕ |  |
|  | [GETBIT](analytics.md#getbit) | ➕ |  |
|  | [SETBIT](analytics.md#setbit) | ➕ |  |
| <span id="client">**CLIENT**</span> | CACHING | ➖ |  |
|  | [GETNAME](client.md#client-getname) | ➕ |  |
|  | GETREDIR | ➖ |  |
|  | HELP | ➖ |  |
|  | [ID](client.md#client-id) | ➕ |  |
|  | [INFO](client.md#client-info) | ➕ |  |
|  | [KILL](client.md#client-kill) | ➕ |  |
|  | [LIST](client.md#client-list) | ➕ |  |
|  | NO-EVICT | ➖ |  |
|  | NO-TOUCH | ➖ |  |
|  | PAUSE | ➖ |  |
|  | REPLY | ➖ |  |
|  | [SETINFO](client.md#client-setinfo) | ➕ |  |
|  | [SETNAME](client.md#client-setname) | ➕ |  |
|  | TRACKING | ➖ |  |
|  | TRACKINGINFO | ➖ |  |
|  | [UNBLOCK](client.md#client-unblock) | ➕ |  |
|  | UNPAUSE | ➖ |  |
| <span id="cluster">**CLUSTER**</span> | [ADDSLOTS](cluster.md#cluster-addslots) | ➕ |  |
|  | [ADDSLOTSRANGE](cluster.md#cluster-addslotsrange) | ➕ |  |
|  | [ASKING](cluster.md#asking) | ➕ |  |
|  | [BUMPEPOCH](cluster.md#cluster-bumpepoch) | ➕ |  |
|  | COUNT-FAILURE-REPORTS | ➖ |  |
|  | [COUNTKEYSINSLOT](cluster.md#cluster-countkeysinslot) | ➕ |  |
|  | [DELSLOTS](cluster.md#cluster-delslots) | ➕ |  |
|  | [DELSLOTSRANGE](cluster.md#cluster-delslotsrange) | ➕ |  |
|  | [FAILOVER](cluster.md#cluster-failover) | ➕ |  |
|  | FLUSHSLOTS | ➖ |  |
|  | [FORGET](cluster.md#cluster-forget) | ➕ |  |
|  | [GETKEYINSLOT](cluster.md#cluster-getkeysinslot) | ➕ |  |
|  | [INFO](cluster.md#cluster-info) | ➕ |  |
|  | [KEYSLOT](cluster.md#cluster-keyslot) | ➕ |  |
|  | LINKS | ➖ |  |
|  | [MEET](cluster.md#cluster-meet) | ➕ |  |
|  | [MYID](cluster.md#cluster-myid) | ➕ |  |
|  | MYSHARDID | ➖ |  |
|  | [NODES](cluster.md#cluster-nodes) | ➕ |  |
|  | [READONLY](cluster.md#readonly) | ➕ |  |
|  | [READWRITE](cluster.md#readwrite) | ➕ |  |
|  | [REPLICAS](cluster.md#cluster-replicas) | ➕ |  |
|  | [REPLICATE](cluster.md#cluster-replicate) | ➕ |  |
|  | [RESET](cluster.md#reset) | ➕ |  |
|  | SAVECONFIG | ➖ |  |
|  | [SET-CONFIG-EPOCH](cluster.md#cluster-set-config-epoch) | ➕ |  |
|  | [SETSLOT](cluster.md#cluster-setslot) | ➕ |  |
|  | SHARDS | ➖ |  |
|  | [SLAVES](cluster.md#slaves) | ➕ | (Deprecated) |
|  | [SLOTS](cluster.md#cluster-slots) | ➕ | (deprecated) |
| <span id="command">**COMMAND**</span> | [COMMAND](server.md#command) | ➕ |  |
|  | [COUNT](server.md#command-count) | ➕ |  |
|  | [DOCS](server.md#command-docs) | ➕ |  |
|  | [GETKEYS](server.md#command-getkeys) | ➕ |  |
|  | [GETKEYSANDFLAGS](server.md#command-getkeysandflags) | ➕ |  |
|  | HELP | ➖ |  | 
|  | [INFO](server.md#command-info) | ➕ |  | 
|  | LIST | ➖ |  | 
| <span id="connection">**CONNECTION**</span> | [AUTH](generic-commands.md#auth) | ➕ |  |
|  | [ECHO](generic-commands.md#echo) | ➕ |  |
|  | [HELLO](generic-commands.md#hello) | ➕ |  |
|  | [PING](generic-commands.md#ping) | ➕ |  |
|  | [QUIT](generic-commands.md#quit) | ➕ | (Deprecated) |
|  | [SELECT](generic-commands.md#select) | ➕ |  |
| <span id="functions">**FUNCTIONS**</span> | FCALL | ➖ |  |
|  | FCALL_RO | ➖ |  |
|  | DELETE | ➖ |
|  | DUMP | ➖ |  |
|  | FLUSH | ➖ |
|  | HELP | ➖ |
|  | KILL | ➖ |
|  | LIST | ➖ |
|  | LOAD | ➖ |
|  | RESTORE | ➖ |  |
|  | STATS | ➖ |
| <span id="generic">**GENERIC**</span> | [PERSIST](generic-commands.md#persist) | ➕ |  |
|  | [PEXPIRE](generic-commands.md#pexpire) | ➕ |  |
|  | [PEXPIREAT](generic-commands.md#pexpireat) | ➕ |  |
|  | [PEXPIRETIME](generic-commands.md#pexpiretime) | ➕ |  |
|  | [PTTL](generic-commands.md#pttl) | ➕ |  |
|  | RANDOMKEY | ➖ |  |
|  | [RENAME](generic-commands.md#rename) | ➕ |  |
|  | [RENAMENX](generic-commands.md#renamenx) | ➕ |  |
|  | [RESTORE](generic-commands.md#restore) | ➕ |
|  | [SCAN](generic-commands.md#scan) | ➕ |  |
|  | SORT | ➖ |  |
|  | SORT_RO | ➖ |  |
|  | TOUCH | ➖ |  |
|  | [TTL](generic-commands.md#ttl) | ➕ |  |
|  | [TYPE](generic-commands.md#type) | ➕ |  |
|  | [UNLINK](generic-commands.md#unlink) | ➕ |  |
|  | WAIT | ➖ |  |
|  | WAITAOF | ➖ |  |
| <span id="geo">**GEO**</span> | [GEOADD](data-structures.md#geoadd) | ➕ |  |
|  | [GEODIST](data-structures.md#geodist) | ➕ |  |
|  | [GEOHASH](data-structures.md#geohash) | ➕ |  |
|  | [GEOPOS](data-structures.md#geopos) | ➕ |  |
|  | [GEORADIUS](data-structures.md#georadius) | ➕ | (Deprecated) |
|  | [GEORADIUS_RO](data-structures.md#georadius_ro) | ➕ | (Deprecated) |
|  | [GEORADIUSBYMEMBER](data-structures.md#georadiusbymember) | ➕ | (Deprecated) |
|  | [GEORADIUSBYMEMBER_RO](data-structures.md#georadiusbymember_ro) | ➕ | (Deprecated) |
|  | [GEOSEARCH](data-structures.md#geosearch) | ➕ | |
|  | [GEOSEARCHSTORE](data-structures.md#geosearchstore) | ➕ | |
| <span id="hash">**HASH**</span> | [HDEL](data-structures.md#hdel) | ➕ |  |
|  | [HEXISTS](data-structures.md#hexists) | ➕ |  |
|  | [HEXPIRE](data-structures.md#hexpire) | ➕ |  |
|  | [HEXPIREAT](data-structures.md#hexpireat) | ➕ |  |
|  | [HEXPIRETIME](data-structures.md#hexpiretime) | ➕ |  |
|  | [HGET](data-structures.md#hget) | ➕ |  |
|  | [HGETALL](data-structures.md#hgetall) | ➕ |  |
|  | [HINCRBY](data-structures.md#hincrby) | ➕ |  |
|  | [HINCRBYFLOAT](data-structures.md#hincrbyfloat) | ➕ |  |
|  | [HKEYS](data-structures.md#hkeys) | ➕ |  |
|  | [HLEN](data-structures.md#hlen) | ➕ |  |
|  | [HMGET](data-structures.md#hmget) | ➕ |  |
|  | [HMSET](data-structures.md#hmset) | ➕ | (Deprecated) |
|  | [HPERSIST](data-structures.md#hpersist) | ➕ |  |
|  | [HPEXPIRE](data-structures.md#hpexpire) | ➕ |  |
|  | [HPEXPIREAT](data-structures.md#hpexpireat) | ➕ |  |
|  | [HPEXPIRETIME](data-structures.md#hepxpiretime) | ➕ |  |
|  | [HPTTL](data-structures.md#hpttl) | ➕ |  |
|  | [HRANDFIELD](data-structures.md#hrandfield) | ➕ |  |
|  | [HSCAN](data-structures.md#hscan) | ➕ |  |
|  | [HSET](data-structures.md#hset) | ➕ |  |
|  | [HSETNX](data-structures.md#hsetnx) | ➕ |  |
|  | [HSTRLEN](data-structures.md#hstrlen) | ➕ |  |
|  | [HTTL](data-structures.md#httl) | ➕ |  |
|  | [HVALS](data-structures.md#hvals) | ➕ |  |
| <span id="hyperloglog">**HYPERLOGLOG**</span> | [PFADD](analytics.md#pfadd) | ➕ |  |
|  | [PFCOUNT](analytics.md#pfcount) | ➕ |  |
|  | PFDEBUG | ➖ | Internal command |
|  | [PFMERGE](analytics.md#pfmerge) | ➕ |  |
|  | PFSELFTEST | ➖ | Internal command |
| <span id="keys">**KEYS**</span> | COPY | ➖ |  |
|  | [DEL](generic-commands.md#del) | ➕ |  |
|  | [DUMP](generic-commands.md#dump) | ➕ |
|  | [EXISTS](generic-commands.md#exists) | ➕ |  |
|  | [EXPIRE](generic-commands.md#expire) | ➕ |  |
|  | [EXPIREAT](generic-commands.md#expireat) | ➕ |  |
|  | [EXPIRETIME](generic-commands.md#expiretime) | ➕ |  |
|  | [KEYS](generic-commands.md#keys) | ➕ |  |
|  | [MIGRATE](generic-commands.md#migrate) | ➕ |  |
|  | MOVE | ➖ |  |
| <span id="latency">**LATENCY**</span> | DOCTOR | ➖ |  |
|  | GRAPH | ➖ |  |
|  | HELP | ➖ |  |
|  | [HISTOGRAM](server.md#latency-histogram) | ➕ |  |
|  | HISTORY | ➖ |  |
|  | LATEST | ➖ |  |
|  | [RESET](server.md#latency-reset) | ➕ |  |
| <span id="list">**LIST**</span> | [BLMOVE](data-structures.md#blmove) | ➕ |  |
|  | [BLMPOP](data-structures.md#blmpop) | ➕ |  |
|  | [BLPOP](data-structures.md#blpop) | ➕ |  |
|  | [BRPOP](data-structures.md#brpop) | ➕ |  |
|  | [BRPOPLPUSH](data-structures.md#brpoplpush) | ➕ | (Deprecated) |
|  | [LINDEX](data-structures.md#lindex) | ➕ |  |
|  | [LINSERT](data-structures.md#linsert) | ➕ |  |
|  | [LLEN](data-structures.md#llen) | ➕ |  |
|  | [LMOVE](data-structures.md#lmove) | ➕ |  |
|  | [LMPOP](data-structures.md#lmpop) | ➕ |  |
|  | [LPOP](data-structures.md#lpop) | ➕ |  |
|  | [LPOS](data-structures.md#lpos) | ➕ |  |
|  | [LPUSH](data-structures.md#lpush) | ➕ |  |
|  | [LPUSHX](data-structures.md#lpushx) | ➕ |  |
|  | [LRANGE](data-structures.md#lrange) | ➕ |  |
|  | [LREM](data-structures.md#lrem) | ➕ |  |
|  | [LSET](data-structures.md#lset) | ➕ |  |
|  | [LTRIM](data-structures.md#ltrim) | ➕ |  |
|  | [RPOP](data-structures.md#rpop) | ➕ |  |
|  | [RPOPLPUSH](data-structures.md#rpoplpush) | ➕ | (Deprecated) |
|  | [RPUSH](data-structures.md#rpush) | ➕ |  |
|  | [RPUSHX](data-structures.md#rpushx) | ➕ |  |
| <span id="memory">**MEMORY**</span> | DOCTOR | ➖ |  |
|  | HELP | ➖ |  |
|  | MALLOC-STATS | ➖ |  |
|  | PURGE | ➖ |  |
|  | STATS | ➖ |  |
|  | [USAGE](server.md#memory-usage) | ➕ |  |
| <span id="module">**MODULE**</span> | HELP | ➖ |  |
|  | LIST | ➖ |  |
|  | LOAD | ➖ |  |
|  | LOADEX | ➖ |  |
|  | UNLOAD | ➖ |  |
| <span id="object">**OBJECT**</span> | ENCODING | ➖ |  |
|  | FREQ | ➖ |  |
|  | HELP | ➖ |  |
|  | IDLETIME | ➖ |  |
|  | REFCOUNT | ➖ |  |
| <span id="pubsub">**PUB/SUB**</span> | [PSUBSCRIBE](analytics.md#psubscribe) | ➕ |  |
|  | [PUBLISH](analytics.md#publish) | ➕ |  |
|  | [PUBSUB CHANNELS](analytics.md#pubsub-channels) | ➕ |  |
|  | PUBSUB HELP | ➖ |  |
|  | [PUBSUB NUMPAT](analytics.md#pubsub-numpat) | ➕ |  |
|  | [PUBSUB NUMSUB](analytics.md#pubsub-numsub) | ➕ |  |
|  | PUBSUB SHARDCHANNELS | ➖ |  |
|  | PUBSUB SHARDNUMSUB | ➖ |  |
|  | [PUNSUBSCRIBE](analytics.md#punsubscribe) | ➕ |  |
|  | [SUBSCRIBE](analytics.md#subscribe) | ➕ |  |
|  | [UNSUBSCRIBE](analytics.md#unsubscribe) | ➕ |  |
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
|  | [BGSAVE](checkpoint.md#bgsave) | ➕ |  |
|  | [COMMITAOF](server.md#commitaof) | ➕ |  |
|  | [CONFIG GET](server.md#config-get) | ➕ |  |
|  | CONFIG HELP | ➖ |  |
|  | CONFIG RESETSTAT | ➖ |  |
|  | CONFIG REWRITE | ➖ |  |
|  | [CONFIG SET](server.md#config-set) | ➕ |  |
|  | [DBSIZE](server.md#dbsize) | ➕ |  |
|  | [DEBUG](server.md#debug) | ➕ | Internal command |
|  | [FLUSHALL](server.md#flushall) | ➕ |  |
|  | [FLUSHDB](server.md#flushdb) | ➕ |  |
|  | [LASTSAVE](checkpoint.md#lastsave) | ➕ |  |
|  | LOLWUT | ➖ |  |
|  | [MONITOR](server.md#monitor) | ➕ |  |
|  | PSYNC | ➖ |  |
|  | REPLCONF | ➖ |  |
|  | [REPLICAOF](server.md#replicaof) | ➕ |  |
|  | RESTORE-ASKING | ➖ |  |
|  | [ROLE](server.md#role) | ➕ |  |
|  | [SAVE](checkpoint.md#save) | ➕ |  |
|  | SHUTDOWN | ➖ |  |
|  | [SLAVEOF](server.md#slaveof) | ➕ | (Deprecated) |
|  | [SWAPDB](server.md#swapdb) | ➕ |  |
|  | SYNC | ➖ |  |
|  | [TIME](server.md#time) | ➕ |  |
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
| <span id="sorted-set">**SLOWLOG**</span> | GET | ➕ |  |
|  | HELP | ➕ |  |
|  | LEN | ➕ |  |
|  | RESET | ➕ |  |
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
| <span id="stream">**STREAM**</span> | XACK | ➖ |  |
|  | XADD | ➕ | (Does not support Capped Streams) |
|  | XAUTOCLAIM | ➖ |  |
|  | XCLAIM | ➖ |  |
|  | XDEL | ➕ |  |
|  | XGROUP CREATE | ➖ |  |
|  | XGROUP CREATECONSUMER | ➖ |  |
|  | XGROUP DELCONSUMER | ➖ |  |
|  | XGROUP DESTROY | ➖ |  |
|  | XGROUP HELP | ➖ |  |
|  | XGROUP SETID | ➖ |  |
|  | XINFO CONSUMERS | ➖ |  |
|  | XINFO GROUPS | ➖ |  |
|  | XINFO HELP | ➖ |  |
|  | XINFO STREAM | ➖ |  |
|  | XLEN | ➕ |  |
|  | XPENDING | ➖ |  |
|  | XRANGE | ➕ |  |
|  | XREAD | ➖ |  |
|  | XREADGROUP | ➖ |  |
|  | XREVRANGE | + |  |
|  | XSETID | ➖ |  |
|  | XTRIM | ➕ | Does not support near-exact trimming |
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
| <span id="transactions">**TRANSACTIONS**</span> | [DISCARD](transactions.md#discard) | ➕ |  |
|  | [EXEC](transactions.md#exec) | ➕ |  |
|  | [MULTI](transactions.md#multi) | ➕ |  |
|  | [UNWATCH](transactions.md#unwatch) | ➕ |  |
|  | [WATCH](transactions.md#watch) | ➕ |  |
| <span id="json">**JSON Module**</span> | [JSON Module](json.md) | ➕ | Partially Implemented |
