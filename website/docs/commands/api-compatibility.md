---
id: api-compatibility
sidebar_label: API Compatibility
title: API compatibility
slug: api-compatibility
---

Below is the full list of API commands and their implementation status in Garnet.<br/>
Note that this list is subject to change as we continue to expand our API command support with the help of our growing community.

### Command Categories
1. [BITMAP](#bitmap)
2. [CLUSTER](#cluster)
3. [CONNECTION](#connection)
4. [FUNCTIONS](#functions)
5. [GENERIC](#generic)
6. [GEO](#geo)
7. [HASH](#hash)
8. [HYPERLOGLOG](#hyperloglog)
9. [KEYS](#keys)
10. [LIST](#list)
11. [PUB/SUB](#pubsub)
12. [SCRIPTING](#scripting)
13. [SERVER](#server)
14. [SET](#set)
15. [SORTED SET](#sorted-set)
16. [STREAM](#stream)
17. [STRING](#string)
18. [TRANSACTIONS](#transactions)

### Full Commands List

| Category | Command | Implemented in Garnet | Notes |
| ------------- | ------------- | ------------- | ------------- | 
| <span id="bitmap">**BITMAP**</span> | [BITCOUNT](analytics.md#bitcount) | ➕ |  |
|  | [BITFIELD](analytics.md#bitfield) | ➕ |  |
|  | BITFIELD_RO | ➖ |  |
|  | [BITOP AND](analytics.md#bitop-and) | ➕ |  |
|  | [BITOP NOT](analytics.md#bitop-not) | ➕ |  |
|  | [BITPOS](analytics.md#bitpos) | ➕ |  |
|  | [GETBIT](analytics.md#getbit) | ➕ |  |
|  | [SETBIT](analytics.md#setbit) | ➕ |  |
| <span id="cluster">**CLUSTER**</span> | [ADDSLOTS](cluster.md#cluster-addslots) | ➕ |  |
|  | [ADDSLOTSRANGE](cluster.md#cluster-addslotsrange) | ➕ |  |
|  | ASKING | ➕ |  |
|  | [BUMPEPOCH](cluster.md#cluster-bumpepoch) | ➕ |  |
|  | COUNT-FAILURE-REPORTS | ➖ |  |
|  | [COUNTKEYSINSLOT](cluster.md#cluster-countkeysinslot) | ➕ |  |
|  | [DELSLOTS](cluster.md#cluster-delslots) | ➕ |  |
|  | [DELSLOTSRANGE](cluster.md#cluster-delslotsrange) | ➕ |  |
|  | [FAILOVER](cluster.md#cluster-failover) | ➕ |  |
|  | FLUSHSLOTS | ➖ |  |
|  | [FORGET](cluster.md#cluster-forget) | ➕ |  |
|  | [GETKEYINSLOT](cluster.md#cluster-getkeysinslot) | ➕ |  |
|  | INFO | ➕ |  |
|  | [KEYSLOT](cluster.md#cluster-keyslot) | ➕ |  |
|  | LINKS | ➖ |  |
|  | [MEET](cluster.md#cluster-meet) | ➕ |  |
|  | [MYID](cluster.md#cluster-myid) | ➕ |  |
|  | [NODES](cluster.md#cluster-nodes) | ➕ |  |
|  | READONLY | ➕ |  |
|  | READWRITE | ➕ |  |
|  | [REPLICAS](cluster.md#cluster-replicas) | ➕ |  |
|  | [REPLICATE](cluster.md#cluster-replicate) | ➕ |  |
|  | RESET | ➕ |  |
|  | SAVECONFIG | ➖ |  |
|  | [SET-CONFIG-EPOCH](cluster.md#cluster-set-config-epoch) | ➕ |  |
|  | [SETSLOT](cluster.md#cluster-setslot) | ➕ |  |
|  | [SLAVES](cluster.md#slaves) | ➕ |  |
|  | SLOTS | ➕ |  |
| <span id="connection">**CONNECTION**</span> | [AUTH](generic-commands.md#auth) | ➕ |  |
|  | CLIENT CACHING | ➖ |  |
|  | [ECHO](generic-commands.md#echo) | ➕ |  |
|  | [PING](generic-commands.md#ping) | ➕ |  |
|  | [QUIT](generic-commands.md#quit) | ➕ |  |
|  | SELECT | ➕ |  |
| <span id="functions">**FUNCTIONS**</span> | FCALL | ➖ |  |
|  | FCALL_RO | ➖ |  |
|  | FUNCTION | ➖ |
| <span id="generic">**GENERIC**</span> | OBJECT | ➖ |  |
|  | [PERSIST](generic-commands.md#persist) | ➕ |  |
|  | [PEXPIRE](generic-commands.md#pexpire) | ➕ |  |
|  | PEXPIREAT | ➖ |  |
|  | PEXPIRETIME | ➖ |  |
|  | [PTTL](generic-commands.md#pttl) | ➕ |  |
|  | RANDOMKEY | ➖ |  |
|  | [RENAME](generic-commands.md#rename) | ➕ |  |
|  | RENAMENX | ➖ |  |
|  | RESTORE | ➖ |  |
|  | [SCAN](generic-commands.md#scan) | ➕ |  |
|  | SORT | ➖ |  |
|  | SORT_RO | ➖ |  |
|  | TOUCH | ➖ |  |
|  | [TTL](generic-commands.md#ttl) | ➕ |  |
|  | [TYPE](generic-commands.md#type) | ➕ |  |
|  | [UNLINK](generic-commands.md#unlink) | ➕ |  |
|  | WAIT | ➖ |  |
| <span id="geo">**GEO**</span> | [GEOADD](data-structures.md#geoadd) | ➕ |  |
|  | [GEODIST](data-structures.md#geodist) | ➕ |  |
|  | [GEOHASH](data-structures.md#geohash) | ➕ |  |
|  | [GEOPOS](data-structures.md#geopos) | ➕ |  |
|  | GEORADIUS | ➖ |  |
|  | GEORADIUS_RO | ➖ |  |
|  | GEORADIUSBYMEMBER | ➖ |  |
|  | [GEOSEARCH](data-structures.md#geosearch) | ➕ | Partially Implemented |
|  | GEOSEARCHSTORE | ➖ |  |
| <span id="hash">**HASH**</span> | [HDEL](data-structures.md#hdel) | ➕ |  |
|  | [HEXISTS](data-structures.md#hexists) | ➕ |  |
|  | [HGET](data-structures.md#hget) | ➕ |  |
|  | [HGETALL](data-structures.md#hgetall) | ➕ |  |
|  | [HINCRBY](data-structures.md#hincrby) | ➕ |  |
|  | [HINCRBYFLOAT](data-structures.md#hincrbyfloat) | ➕ |  |
|  | [HKEYS](data-structures.md#hkeys) | ➕ |  |
|  | [HLEN](data-structures.md#hlen) | ➕ |  |
|  | [HMGET](data-structures.md#hmget) | ➕ |  |
|  | [HMSET](data-structures.md#hmset) | ➕ |  |
|  | [HRANDFIELD](data-structures.md#hrandfield) | ➕ |  |
|  | [HSCAN](data-structures.md#hscan) | ➕ |  |
|  | [HSET](data-structures.md#hset) | ➕ |  |
|  | [HSETNX](data-structures.md#hsetnx) | ➕ |  |
|  | HSTRLEN | ➖ |  |
|  | [HVALS](data-structures.md#hvals) | ➕ |  |
| <span id="hyperloglog">**HYPERLOGLOG**</span> | [PFADD](analytics.md#pfadd) | ➕ |  |
|  | [PFCOUNT](analytics.md#pfcount) | ➕ |  |
|  | PFDEBUG | ➖ |  |
|  | [PFMERGE](analytics.md#pfmerge) | ➕ |  |
|  | PFSELFTEST | ➖ |  |
| <span id="keys">**KEYS**</span> | COPY | ➖ |  |
|  | [DEL](generic-commands.md#del)  | ➕ |  |
|  | DUMP | ➖ |  |
|  | [EXISTS](generic-commands.md#exists) | ➕ |  |
|  | [EXPIRE](generic-commands.md#expire) | ➕ |  |
|  | EXPIREAT | ➖ |  |
|  | EXPIRETIME | ➖ |  |
|  | [KEYS](generic-commands.md#keys) | ➕ |  |
|  | [MIGRATE](generic-commands.md#migrate) | ➕ |  |
|  | MOVE | ➖ |  |
| <span id="list">**LIST**</span> | BLMOVE | ➖ |  |
|  | BLMPOP | ➖ |  |
|  | BLPOP | ➖ |  |
|  | BRPOP | ➖ |  |
|  | BRPOPLPUSH | ➖ |  |
|  | [LINDEX](data-structures.md#lindex) | ➕ |  |
|  | [LINSERT](data-structures.md#linsert) | ➕ |  |
|  | [LLEN](data-structures.md#llen) | ➕ |  |
|  | [LMOVE](data-structures.md#lmove) | ➕ |  |
|  | LMPOP | ➖ |  |
|  | [LPOP](data-structures.md#lpop) | ➕ |  |
|  | LPOS | ➖ |  |
|  | [LPUSH](data-structures.md#lpush) | ➕ |  |
|  | [LPUSHX](data-structures.md#lpushx) | ➕ |  |
|  | [LRANGE](data-structures.md#lrange) | ➕ |  |
|  | [LREM](data-structures.md#lrem) | ➕ |  |
|  | LSET | ➖ |  |
|  | [LTRIM](data-structures.md#ltrim) | ➕ |  |
|  | [RPOP](data-structures.md#rpop) | ➕ |  |
|  | [RPOPLPUSH](data-structures.md#rpoplpush) | ➕ |  |
|  | [RPUSH](data-structures.md#rpush) | ➕ |  |
|  | [RPUSHX](data-structures.md#rpushx) | ➕ |  |
| <span id="pubsub">**PUB/SUB**</span> | [PSUBSCRIBE](analytics.md#psubscribe) | ➕ |  |
|  | [PUBLISH](analytics.md#publish) | ➕ |  |
|  | PUBSUB CHANNELS | ➖ |  |
|  | PUBSUB NUMPAT | ➖ |  |
|  | PUBSUB NUMSUB | ➖ |  |
|  | [PUNSUBSCRIBE](analytics.md#punsubscribe) | ➕ |  |
|  | [SUBSCRIBE](analytics.md#subscribe) | ➕ |  |
|  | [UNSUBSCRIBE](analytics.md#unsubscribe) | ➕ |  |
| <span id="scripting">**SCRIPTING**</span> | EVAL | ➖ |  |
|  | EVAL_RO | ➖ |  |
|  | EVALSHA | ➖ |  |
|  | EVALSHA_RO | ➖ |  |
|  | SCRIPT DEBUG | ➖ |  |
|  | SCRIPT EXISTS | ➖ |  |
|  | SCRIPT FLUSH | ➖ |  |
|  | SCRIPT KILL | ➖ |  |
|  | SCRIPT LOAD | ➖ |  |
| <span id="server">**SERVER**</span> | ACL | ➖ |  |
|  | BGREWRITEAOF | ➖ |  |
|  | [BGSAVE](checkpoint.md#bgsave) | ➕ |  |
|  | [COMMAND](server.md#command) | ➕ |  |
|  | [COMMITAOF](server.md#commitaof) | ➕ |  |
|  | [CONFIG GET](server.md#config-get) | ➕ |  |
|  | [CONFIG SET](server.md#config-set) | ➕ |  |
|  | [DBSIZE](server.md#dbsize) | ➕ |  |
|  | FLUSHALL | ➖ |  |
|  | [FLUSHDB](server.md#flushdb) | ➕ |  |
|  | [LASTSAVE](checkpoint.md#lastsave) | ➕ |  |
|  | LATENCY DOCTOR | ➖ |  |
|  | LATENCY GRAPH | ➖ |  |
|  | [LATENCY HISTOGRAM](server.md#latency-histogram) | ➕ |  |
|  | LATENCY HISTORY | ➖ |  |
|  | LATENCY LATEST | ➖ |  |
|  | [LATENCY RESET](server.md#latency-reset) | ➕ |  |
|  | LOLWUT | ➕ |  |
|  | MEMORY DOCTOR | ➖ |  |
|  | MEMORY MALLOC-STATS | ➖ |  |
|  | MEMORY PURGE | ➖ |  |
|  | MEMORY STATS | ➖ |  |
|  | [MEMORY USAGE](server.md#memory-usage) | ➕ |  |
|  | MODULE | ➖ |  |
|  | MONITOR | ➖ |  |
|  | PSYNC | ➖ |  |
|  | REPLCONF | ➖ |  |
|  | [REPLICAOF](server.md#replicaof) | ➕ |  |
|  | RESTORE-ASKING | ➖ |  |
|  | ROLE | ➖ |  |
|  | [SAVE](checkpoint.md#save) | ➕ |  |
|  | SHUTDOWN | ➖ |  |
|  | SLAVEOF | ➖ |  |
|  | SLOWLOG | ➖ |  |
|  | SWAPDB | ➖ |  |
|  | SYNC | ➖ |  |
|  | [TIME](server.md#time) | ➕ |  |
| <span id="set">**SET**</span> | [SADD](data-structures.md#sadd) | ➕ |  |
|  | [SCARD](data-structures.md#scard) | ➕ |  |
|  | SDIFF | ➖ |  |
|  | SDIFFSTORE | ➖ |  |
|  | SINTER | ➖ |  |
|  | SINTERCARD | ➖ |  |
|  | SINTERSTORE | ➖ |  |
|  | SISMEMBER | ➖ |  |
|  | [SMEMBERS](data-structures.md#smembers) | ➕ |  |
|  | SMISMEMBER | ➖ |  |
|  | SMOVE | ➖ |  |
|  | [SPOP](data-structures.md#spop) | ➕ |  |
|  | SRANDMEMBER | ➖ |  |
|  | [SREM](data-structures.md#srem) | ➕ |  |
|  | [SSCAN](data-structures.md#sscan) | ➕ |  |
|  | SUNION | ➖ |  |
|  | SUNIONSTORE | ➖ |  |
| <span id="sorted-set">**SORTED SET**</span> | BZPOP | ➖ |  |
|  | BZPOPMAX | ➖ |  |
|  | BZPOPMIN | ➖ |  |
|  | [ZADD](data-structures.md#zadd) | ➕ |  |
|  | [ZCARD](data-structures.md#zcard) | ➕ |  |
|  | [ZCOUNT](data-structures.md#zcount) | ➕ |  |
|  | [ZDIFF](data-structures.md#zdiff) | ➕ |  |
|  | ZDIFFSTORE | ➖ |  |
|  | [ZINCRBY](data-structures.md#zincrby) | ➕ |  |
|  | ZINTER | ➖ |  |
|  | ZINTERCARD | ➖ |  |
|  | ZINTERSTORE | ➖ |  |
|  | [ZLEXCOUNT](data-structures.md#zlexcount) | ➕ |  |
|  | ZMPOP | ➖ |  |
|  | ZMSCORE | ➖ |  |
|  | [ZPOPMAX](data-structures.md#zpopmax) | ➕ |  |
|  | [ZPOPMIN](data-structures.md#zpopmin) | ➕ |  |
|  | [ZRANDMEMBER](data-structures.md#zrandmember) | ➕ |  |
|  | [ZRANGE](data-structures.md#zrange) | ➕ |  |
|  | [ZRANGEBYLEX](data-structures.md#zrangebylex) | ➕ |  |
|  | [ZRANGEBYSCORE](data-structures.md#zrangebyscore) | ➕ |  |
|  | ZRANGESTORE | ➖ |  |
|  | [ZRANK](data-structures.md#zrank) | ➕ |  |
|  | [ZREM](data-structures.md#zrem) | ➕ |  |
|  | [ZREMRANGEBYLEX](data-structures.md#zremrangebylex) | ➕ |  |
|  | [ZREMRANGEBYRANK](data-structures.md#zremrangebyrank) | ➕ |  |
|  | [ZREMRANGEBYSCORE](data-structures.md#zremrangebyscore) | ➕ |  |
|  | [ZREVRANGE](data-structures.md#zrevrange) | ➕ |  |
|  | ZREVRANGEBYLEX | ➖ |  |
|  | ZREVRANGEBYSCORE | ➖ |  |
|  | [ZREVRANK](data-structures.md#zrevrank) | ➕ |  |
|  | [ZSCAN](data-structures.md#zscan) | ➕ |  |
|  | [ZSCORE](data-structures.md#zscore) | ➕ |  |
|  | ZUNION | ➖ |  |
|  | ZUNIONSTORE | ➖ |  |
| <span id="stream">**STREAM**</span> | XACK | ➖ |  |
|  | XADD | ➖ |  |
|  | XAUTOCLAIM | ➖ |  |
|  | XCLAIM | ➖ |  |
|  | XDEL | ➖ |  |
|  | XGROUP | ➖ |  |
|  | XINFO | ➖ |  |
|  | XLEN | ➖ |  |
|  | XPENDING | ➖ |  |
|  | XRANGE | ➖ |  |
|  | XREAD | ➖ |  |
|  | XREADGROUP | ➖ |  |
|  | XREVRANGE | ➖ |  |
|  | XSETID | ➖ |  |
|  | XTRIM | ➖ |  |
| <span id="string">**STRING**</span> | APPEND | ➕ |  |
|  | [DECR](raw-string.md#decr) | ➕ |  |
|  | [DECRBY](raw-string.md#decrby) | ➕ |  |
|  | [GET](raw-string.md#get) | ➕ |  |
|  | [GETDEL](raw-string.md#getdel) | ➕ |  |
|  | GETEX | ➖ |  |
|  | [GETRANGE](raw-string.md#getrange) | ➕ |  |
|  | GETSET | ➖ |  |
|  | [INCR](raw-string.md#incr) | ➕ |  |
|  | [INCRBY](raw-string.md#incrby) | ➕ |  |
|  | INCRBYFLOAT | ➖ |  |
|  | LCS | ➖ |  |
|  | [MGET](raw-string.md#mget) | ➕ |  |
|  | [MSET](raw-string.md#mset) | ➕ |  |
|  | [MSETNX](raw-string.md#msetnx) | ➕ |  |
|  | [PSETEX](raw-string.md#psetex) | ➕ |  |
|  | [SET](raw-string.md#set) | ➕ |  |
|  | [SET ... NX](raw-string.md#set) | ➕ |  |
|  | [SETEX](raw-string.md#setex) | ➕ |  |
|  | [SETNX](raw-string.md#setnx) | ➕ |  |
|  | SETRANGE | ➕ |  |
|  | [STRLEN](raw-string.md#strlen) | ➕ |  |
|  | SUBSTR | ➖ |  |
| <span id="transactions">**TRANSACTIONS**</span> | [DISCARD](transactions.md#discard) | ➕ |  |
|  | [EXEC](transactions.md#exec) | ➕ |  |
|  | [MULTI](transactions.md#multi) | ➕ |  |
|  | [UNWATCH](transactions.md#unwatch) | ➕ |  |
|  | [WATCH](transactions.md#watch) | ➕ |  |