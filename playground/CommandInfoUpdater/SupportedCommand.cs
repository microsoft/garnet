// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.ObjectModel;
using Garnet.server;

namespace CommandInfoUpdater
{
    /// <summary>
    /// Defines a command supported by Garnet
    /// </summary>
    public class SupportedCommand
    {
        private static readonly SupportedCommand[] AllSupportedCommands = [
            new("ACL", RespCommand.ACL, StoreType.None,
            [
                new("ACL|CAT", RespCommand.ACL_CAT),
                new("ACL|DELUSER", RespCommand.ACL_DELUSER),
                new("ACL|GENPASS", RespCommand.ACL_GENPASS),
                new("ACL|GETUSER", RespCommand.ACL_GETUSER),
                new("ACL|LIST", RespCommand.ACL_LIST),
                new("ACL|LOAD", RespCommand.ACL_LOAD),
                new("ACL|SAVE", RespCommand.ACL_SAVE),
                new("ACL|SETUSER", RespCommand.ACL_SETUSER),
                new("ACL|USERS", RespCommand.ACL_USERS),
                new("ACL|WHOAMI", RespCommand.ACL_WHOAMI),
            ]),
            new("EXPDELSCAN", RespCommand.EXPDELSCAN),
            new("APPEND", RespCommand.APPEND, StoreType.Main),
            new("ASKING", RespCommand.ASKING),
            new("ASYNC", RespCommand.ASYNC),
            new("AUTH", RespCommand.AUTH),
            new("BGSAVE", RespCommand.BGSAVE),
            new("BITCOUNT", RespCommand.BITCOUNT, StoreType.Main),
            new("BITFIELD", RespCommand.BITFIELD, StoreType.Main),
            new("BITFIELD_RO", RespCommand.BITFIELD_RO, StoreType.Main),
            new("BITOP", RespCommand.BITOP, StoreType.Main),
            new("BITPOS", RespCommand.BITPOS, StoreType.Main),
            new("BLPOP", RespCommand.BLPOP, StoreType.Object),
            new("BRPOP", RespCommand.BRPOP, StoreType.Object),
            new("BLMOVE", RespCommand.BLMOVE, StoreType.Object),
            new("BRPOPLPUSH", RespCommand.BRPOPLPUSH, StoreType.Object),
            new("BZMPOP", RespCommand.BZMPOP, StoreType.Object),
            new("BZPOPMAX", RespCommand.BZPOPMAX, StoreType.Object),
            new("BZPOPMIN", RespCommand.BZPOPMIN, StoreType.Object),
            new("BLMPOP", RespCommand.BLMPOP, StoreType.Object),
            new("CLIENT", RespCommand.CLIENT, StoreType.None,
            [
                new("CLIENT|ID", RespCommand.CLIENT_ID),
                new("CLIENT|INFO", RespCommand.CLIENT_INFO),
                new("CLIENT|LIST", RespCommand.CLIENT_LIST),
                new("CLIENT|KILL", RespCommand.CLIENT_KILL),
                new("CLIENT|GETNAME", RespCommand.CLIENT_GETNAME),
                new("CLIENT|SETNAME", RespCommand.CLIENT_SETNAME),
                new("CLIENT|SETINFO", RespCommand.CLIENT_SETINFO),
                new("CLIENT|UNBLOCK", RespCommand.CLIENT_UNBLOCK),
            ]),
            new("CLUSTER", RespCommand.CLUSTER, StoreType.None,
            [
                new("CLUSTER|ADDSLOTS", RespCommand.CLUSTER_ADDSLOTS),
                new("CLUSTER|ADDSLOTSRANGE", RespCommand.CLUSTER_ADDSLOTSRANGE),
                new("CLUSTER|AOFSYNC", RespCommand.CLUSTER_AOFSYNC),
                new("CLUSTER|APPENDLOG", RespCommand.CLUSTER_APPENDLOG),
                new("CLUSTER|ATTACH_SYNC", RespCommand.CLUSTER_ATTACH_SYNC),
                new("CLUSTER|BANLIST", RespCommand.CLUSTER_BANLIST),
                new("CLUSTER|BEGIN_REPLICA_RECOVER", RespCommand.CLUSTER_BEGIN_REPLICA_RECOVER),
                new("CLUSTER|BUMPEPOCH", RespCommand.CLUSTER_BUMPEPOCH),
                new("CLUSTER|COUNTKEYSINSLOT", RespCommand.CLUSTER_COUNTKEYSINSLOT),
                new("CLUSTER|DELKEYSINSLOT", RespCommand.CLUSTER_DELKEYSINSLOT),
                new("CLUSTER|DELKEYSINSLOTRANGE", RespCommand.CLUSTER_DELKEYSINSLOTRANGE),
                new("CLUSTER|DELSLOTS", RespCommand.CLUSTER_DELSLOTS),
                new("CLUSTER|DELSLOTSRANGE", RespCommand.CLUSTER_DELSLOTSRANGE),
                new("CLUSTER|ENDPOINT", RespCommand.CLUSTER_ENDPOINT),
                new("CLUSTER|FAILOVER", RespCommand.CLUSTER_FAILOVER),
                new("CLUSTER|FAILREPLICATIONOFFSET", RespCommand.CLUSTER_FAILREPLICATIONOFFSET),
                new("CLUSTER|FAILSTOPWRITES", RespCommand.CLUSTER_FAILSTOPWRITES),
                new("CLUSTER|FLUSHALL", RespCommand.CLUSTER_FLUSHALL),
                new("CLUSTER|FORGET", RespCommand.CLUSTER_FORGET),
                new("CLUSTER|GETKEYSINSLOT", RespCommand.CLUSTER_GETKEYSINSLOT),
                new("CLUSTER|GOSSIP", RespCommand.CLUSTER_GOSSIP),
                new("CLUSTER|HELP", RespCommand.CLUSTER_HELP),
                new("CLUSTER|INFO", RespCommand.CLUSTER_INFO),
                new("CLUSTER|INITIATE_REPLICA_SYNC", RespCommand.CLUSTER_INITIATE_REPLICA_SYNC),
                new("CLUSTER|KEYSLOT", RespCommand.CLUSTER_KEYSLOT),
                new("CLUSTER|MEET", RespCommand.CLUSTER_MEET),
                new("CLUSTER|MIGRATE", RespCommand.CLUSTER_MIGRATE),
                new("CLUSTER|MTASKS", RespCommand.CLUSTER_MTASKS),
                new("CLUSTER|MYID", RespCommand.CLUSTER_MYID),
                new("CLUSTER|MYPARENTID", RespCommand.CLUSTER_MYPARENTID),
                new("CLUSTER|NODES", RespCommand.CLUSTER_NODES),
                new("CLUSTER|PUBLISH", RespCommand.CLUSTER_PUBLISH),
                new("CLUSTER|SPUBLISH", RespCommand.CLUSTER_SPUBLISH),
                new("CLUSTER|REPLICAS", RespCommand.CLUSTER_REPLICAS),
                new("CLUSTER|REPLICATE", RespCommand.CLUSTER_REPLICATE),
                new("CLUSTER|RESET", RespCommand.CLUSTER_RESET),
                new("CLUSTER|SEND_CKPT_FILE_SEGMENT", RespCommand.CLUSTER_SEND_CKPT_FILE_SEGMENT),
                new("CLUSTER|SEND_CKPT_METADATA", RespCommand.CLUSTER_SEND_CKPT_METADATA),
                new("CLUSTER|SET-CONFIG-EPOCH", RespCommand.CLUSTER_SETCONFIGEPOCH),
                new("CLUSTER|SETSLOT", RespCommand.CLUSTER_SETSLOT),
                new("CLUSTER|SETSLOTSRANGE", RespCommand.CLUSTER_SETSLOTSRANGE),
                new("CLUSTER|SHARDS", RespCommand.CLUSTER_SHARDS),
                new("CLUSTER|SLOTS", RespCommand.CLUSTER_SLOTS),
                new("CLUSTER|SLOTSTATE", RespCommand.CLUSTER_SLOTSTATE),
                new("CLUSTER|SYNC", RespCommand.CLUSTER_SYNC),
            ]),
            new("COMMAND", RespCommand.COMMAND, StoreType.None,
            [
                new("COMMAND|INFO", RespCommand.COMMAND_INFO),
                new("COMMAND|COUNT", RespCommand.COMMAND_COUNT),
                new("COMMAND|DOCS", RespCommand.COMMAND_DOCS),
                new("COMMAND|GETKEYS", RespCommand.COMMAND_GETKEYS),
                new("COMMAND|GETKEYSANDFLAGS", RespCommand.COMMAND_GETKEYSANDFLAGS),
            ]),
            new("COMMITAOF", RespCommand.COMMITAOF),
            new("CONFIG", RespCommand.CONFIG, StoreType.None,
            [
                new("CONFIG|GET", RespCommand.CONFIG_GET),
                new("CONFIG|SET", RespCommand.CONFIG_SET),
                new("CONFIG|REWRITE", RespCommand.CONFIG_REWRITE),
            ]),
            new("COSCAN", RespCommand.COSCAN, StoreType.Object),
            new("CustomRawStringCmd", RespCommand.CustomRawStringCmd, StoreType.Main),
            new("CustomObjCmd", RespCommand.CustomObjCmd, StoreType.Object),
            new("CustomTxn", RespCommand.CustomTxn),
            new("CustomProcedure", RespCommand.CustomProcedure),
            new("DBSIZE", RespCommand.DBSIZE),
            new("DEBUG", RespCommand.DEBUG),
            new("DECR", RespCommand.DECR, StoreType.Main),
            new("DECRBY", RespCommand.DECRBY, StoreType.Main),
            new("DEL", RespCommand.DEL, StoreType.All),
            new("DELIFGREATER", RespCommand.DELIFGREATER, StoreType.Main),
            new("DISCARD", RespCommand.DISCARD),
            new("DUMP", RespCommand.DUMP, StoreType.All),
            new("ECHO", RespCommand.ECHO),
            new("EXEC", RespCommand.EXEC),
            new("EXISTS", RespCommand.EXISTS, StoreType.All),
            new("EXPIRE", RespCommand.EXPIRE, StoreType.All),
            new("EXPIREAT", RespCommand.EXPIREAT, StoreType.All),
            new("EXPIRETIME", RespCommand.EXPIRETIME, StoreType.All),
            new("FAILOVER", RespCommand.FAILOVER),
            new("FLUSHALL", RespCommand.FLUSHALL),
            new("FLUSHDB", RespCommand.FLUSHDB),
            new("FORCEGC", RespCommand.FORCEGC),
            new("GEOADD", RespCommand.GEOADD, StoreType.Object),
            new("GEODIST", RespCommand.GEODIST, StoreType.Object),
            new("GEOHASH", RespCommand.GEOHASH, StoreType.Object),
            new("GEOPOS", RespCommand.GEOPOS, StoreType.Object),
            new("GEORADIUS", RespCommand.GEORADIUS, StoreType.Object),
            new("GEORADIUS_RO", RespCommand.GEORADIUS_RO, StoreType.Object),
            new("GEORADIUSBYMEMBER", RespCommand.GEORADIUSBYMEMBER, StoreType.Object),
            new("GEORADIUSBYMEMBER_RO", RespCommand.GEORADIUSBYMEMBER_RO, StoreType.Object),
            new("GEOSEARCH", RespCommand.GEOSEARCH, StoreType.Object),
            new("GEOSEARCHSTORE", RespCommand.GEOSEARCHSTORE, StoreType.Object),
            new("GET", RespCommand.GET, StoreType.Main),
            new("GETEX", RespCommand.GETEX, StoreType.Main),
            new("GETBIT", RespCommand.GETBIT, StoreType.Main),
            new("GETDEL", RespCommand.GETDEL, StoreType.Main),
            new("GETIFNOTMATCH", RespCommand.GETIFNOTMATCH, StoreType.Main),
            new("GETRANGE", RespCommand.GETRANGE, StoreType.Main),
            new("GETWITHETAG", RespCommand.GETWITHETAG, StoreType.Main),
            new("GETSET", RespCommand.GETSET, StoreType.Main),
            new("HCOLLECT", RespCommand.HCOLLECT, StoreType.Object),
            new("HDEL", RespCommand.HDEL, StoreType.Object),
            new("HELLO", RespCommand.HELLO),
            new("HEXISTS", RespCommand.HEXISTS, StoreType.Object),
            new("HEXPIRE", RespCommand.HEXPIRE, StoreType.Object),
            new("HPEXPIRE", RespCommand.HPEXPIRE, StoreType.Object),
            new("HEXPIREAT", RespCommand.HEXPIREAT, StoreType.Object),
            new("HPEXPIREAT", RespCommand.HPEXPIREAT, StoreType.Object),
            new("HTTL", RespCommand.HTTL, StoreType.Object),
            new("HPTTL", RespCommand.HPTTL, StoreType.Object),
            new("HEXPIRETIME", RespCommand.HEXPIRETIME, StoreType.Object),
            new("HPEXPIRETIME", RespCommand.HPEXPIRETIME, StoreType.Object),
            new("HPERSIST", RespCommand.HPERSIST, StoreType.Object),
            new("HGET", RespCommand.HGET, StoreType.Object),
            new("HGETALL", RespCommand.HGETALL, StoreType.Object),
            new("HINCRBY", RespCommand.HINCRBY, StoreType.Object),
            new("HINCRBYFLOAT", RespCommand.HINCRBYFLOAT, StoreType.Object),
            new("HKEYS", RespCommand.HKEYS, StoreType.Object),
            new("HLEN", RespCommand.HLEN, StoreType.Object),
            new("HMGET", RespCommand.HMGET, StoreType.Object),
            new("HMSET", RespCommand.HMSET, StoreType.Object),
            new("HRANDFIELD", RespCommand.HRANDFIELD, StoreType.Object),
            new("HSCAN", RespCommand.HSCAN, StoreType.Object),
            new("HSET", RespCommand.HSET, StoreType.Object),
            new("HSETNX", RespCommand.HSETNX, StoreType.Object),
            new("HSTRLEN", RespCommand.HSTRLEN, StoreType.Object),
            new("HVALS", RespCommand.HVALS, StoreType.Object),
            new("INCR", RespCommand.INCR, StoreType.Main),
            new("INCRBY", RespCommand.INCRBY, StoreType.Main),
            new("INCRBYFLOAT", RespCommand.INCRBYFLOAT, StoreType.Main),
            new("INFO", RespCommand.INFO),
            new("KEYS", RespCommand.KEYS),
            new("LCS", RespCommand.LCS, StoreType.Main),
            new("LASTSAVE", RespCommand.LASTSAVE),
            new("LATENCY", RespCommand.LATENCY, StoreType.None,
            [
                new("LATENCY|HELP", RespCommand.LATENCY_HELP),
                new("LATENCY|HISTOGRAM", RespCommand.LATENCY_HISTOGRAM),
                new("LATENCY|RESET", RespCommand.LATENCY_RESET),
            ]),
            new("LINDEX", RespCommand.LINDEX, StoreType.Object),
            new("LINSERT", RespCommand.LINSERT, StoreType.Object),
            new("LLEN", RespCommand.LLEN, StoreType.Object),
            new("LMOVE", RespCommand.LMOVE, StoreType.Object),
            new("LMPOP", RespCommand.LMPOP, StoreType.Object),
            new("LPOP", RespCommand.LPOP, StoreType.Object),
            new("LPOS", RespCommand.LPOS, StoreType.Object),
            new("LPUSH", RespCommand.LPUSH, StoreType.Object),
            new("LPUSHX", RespCommand.LPUSHX, StoreType.Object),
            new("LRANGE", RespCommand.LRANGE, StoreType.Object),
            new("LREM", RespCommand.LREM, StoreType.Object),
            new("LSET", RespCommand.LSET, StoreType.Object),
            new("LTRIM", RespCommand.LTRIM, StoreType.Object),
            new("MEMORY", RespCommand.MEMORY, StoreType.None,
            [
                new("MEMORY|USAGE", RespCommand.MEMORY_USAGE),
            ]),
            new("MGET", RespCommand.MGET, StoreType.Main),
            new("MIGRATE", RespCommand.MIGRATE),
            new("PURGEBP", RespCommand.PURGEBP),
            new("MODULE", RespCommand.MODULE, StoreType.None,
            [
                new("MODULE|LOADCS", RespCommand.MODULE_LOADCS),
            ]),
            new("MONITOR", RespCommand.MONITOR),
            new("MSET", RespCommand.MSET, StoreType.Main),
            new("MSETNX", RespCommand.MSETNX, StoreType.Main),
            new("MULTI", RespCommand.MULTI),
            new("PERSIST", RespCommand.PERSIST, StoreType.All),
            new("PEXPIRE", RespCommand.PEXPIRE, StoreType.All),
            new("PEXPIREAT", RespCommand.PEXPIREAT, StoreType.All),
            new("PEXPIRETIME", RespCommand.PEXPIRETIME, StoreType.All),
            new("PFADD", RespCommand.PFADD, StoreType.Main),
            new("PFCOUNT", RespCommand.PFCOUNT, StoreType.Main),
            new("PFMERGE", RespCommand.PFMERGE, StoreType.Main),
            new("PING", RespCommand.PING),
            new("PSETEX", RespCommand.PSETEX, StoreType.Main),
            new("PSUBSCRIBE", RespCommand.PSUBSCRIBE),
            new("PTTL", RespCommand.PTTL, StoreType.All),
            new("PUBLISH", RespCommand.PUBLISH),
            new("PUBSUB", RespCommand.PUBSUB, StoreType.None,
            [
                new("PUBSUB|CHANNELS", RespCommand.PUBSUB_CHANNELS),
                new("PUBSUB|NUMPAT", RespCommand.PUBSUB_NUMPAT),
                new("PUBSUB|NUMSUB", RespCommand.PUBSUB_NUMSUB),
            ]),
            new("PUNSUBSCRIBE", RespCommand.PUNSUBSCRIBE),
            new("REGISTERCS", RespCommand.REGISTERCS),
            new("QUIT", RespCommand.QUIT),
            new("READONLY", RespCommand.READONLY),
            new("READWRITE", RespCommand.READWRITE),
            new("RENAME", RespCommand.RENAME, StoreType.All),
            new("RESTORE", RespCommand.RESTORE, StoreType.All),
            new("RENAMENX", RespCommand.RENAMENX, StoreType.All),
            new("REPLICAOF", RespCommand.REPLICAOF),
            new("ROLE", RespCommand.ROLE),
            new("RPOP", RespCommand.RPOP, StoreType.Object),
            new("RPOPLPUSH", RespCommand.RPOPLPUSH, StoreType.Object),
            new("RPUSH", RespCommand.RPUSH, StoreType.Object),
            new("RPUSHX", RespCommand.RPUSHX, StoreType.Object),
            new("RUNTXP", RespCommand.RUNTXP),
            new("SADD", RespCommand.SADD, StoreType.Object),
            new("SCARD", RespCommand.SCARD, StoreType.Object),
            new("SAVE", RespCommand.SAVE),
            new("SCAN", RespCommand.SCAN, StoreType.All),
            new("SDIFF", RespCommand.SDIFF, StoreType.Object),
            new("SDIFFSTORE", RespCommand.SDIFFSTORE, StoreType.Object),
            new("SECONDARYOF", RespCommand.SECONDARYOF),
            new("SELECT", RespCommand.SELECT),
            new("SET", RespCommand.SET, StoreType.Main),
            new("SETBIT", RespCommand.SETBIT, StoreType.Main),
            new("SETEX", RespCommand.SETEX, StoreType.Main),
            new("SETIFMATCH", RespCommand.SETIFMATCH, StoreType.Main),
            new("SETIFGREATER", RespCommand.SETIFGREATER, StoreType.Main),
            new("SETNX", RespCommand.SETNX, StoreType.Main),
            new("SETRANGE", RespCommand.SETRANGE, StoreType.Main),
            new("SISMEMBER", RespCommand.SISMEMBER, StoreType.Object),
            new("SLAVEOF", RespCommand.SECONDARYOF),
            new("SLOWLOG", RespCommand.SLOWLOG, StoreType.None,
            [
                new("SLOWLOG|GET", RespCommand.SLOWLOG_GET),
                new("SLOWLOG|LEN", RespCommand.SLOWLOG_LEN),
                new("SLOWLOG|RESET", RespCommand.SLOWLOG_RESET),
                new("SLOWLOG|HELP", RespCommand.SLOWLOG_HELP),
            ]),
            new("SMEMBERS", RespCommand.SMEMBERS, StoreType.Object),
            new("SMISMEMBER", RespCommand.SMISMEMBER, StoreType.Object),
            new("SMOVE", RespCommand.SMOVE, StoreType.Object),
            new("SPOP", RespCommand.SPOP, StoreType.Object),
            new("SPUBLISH", RespCommand.SPUBLISH),
            new("SRANDMEMBER", RespCommand.SRANDMEMBER, StoreType.Object),
            new("SREM", RespCommand.SREM, StoreType.Object),
            new("SSCAN", RespCommand.SSCAN, StoreType.Object),
            new("STRLEN", RespCommand.STRLEN, StoreType.Main),
            new("SUBSCRIBE", RespCommand.SUBSCRIBE),
            new("SSUBSCRIBE", RespCommand.SSUBSCRIBE),
            new("SUBSTR", RespCommand.SUBSTR, StoreType.Main),
            new("SUNION", RespCommand.SUNION, StoreType.Object),
            new("SUNIONSTORE", RespCommand.SUNIONSTORE, StoreType.Object),
            new("SINTER", RespCommand.SINTER, StoreType.Object),
            new("SINTERCARD", RespCommand.SINTERCARD, StoreType.Object),
            new("SINTERSTORE", RespCommand.SINTERSTORE, StoreType.Object),
            new("SWAPDB", RespCommand.SWAPDB),
            new("TIME", RespCommand.TIME),
            new("TTL", RespCommand.TTL, StoreType.All),
            new("TYPE", RespCommand.TYPE, StoreType.All),
            new("UNLINK", RespCommand.UNLINK, StoreType.All),
            new("UNSUBSCRIBE", RespCommand.UNSUBSCRIBE),
            new("UNWATCH", RespCommand.UNWATCH),
            new("WATCH", RespCommand.WATCH),
            new("WATCHMS", RespCommand.WATCHMS),
            new("WATCHOS", RespCommand.WATCHOS),
            new("XADD", RespCommand.XADD),
            new("XDEL", RespCommand.XDEL),
            new("XLEN", RespCommand.XLEN),
            new("XRANGE", RespCommand.XRANGE),
            new ("XREVRANGE", RespCommand.XREVRANGE),
            new("XTRIM", RespCommand.XTRIM),
            new("ZADD", RespCommand.ZADD),
            new("ZCARD", RespCommand.ZCARD),
            new("ZCOUNT", RespCommand.ZCOUNT),
            new("ZDIFF", RespCommand.ZDIFF),
            new("ZDIFFSTORE", RespCommand.ZDIFFSTORE),
            new("ZINCRBY", RespCommand.ZINCRBY),
            new("ZINTER", RespCommand.ZINTER),
            new("ZINTERCARD", RespCommand.ZINTERCARD),
            new("ZINTERSTORE", RespCommand.ZINTERSTORE),
            new("ZLEXCOUNT", RespCommand.ZLEXCOUNT),
            new("ZMSCORE", RespCommand.ZMSCORE),
            new("ZMPOP", RespCommand.ZMPOP),
            new("ZPOPMAX", RespCommand.ZPOPMAX),
            new("ZPOPMIN", RespCommand.ZPOPMIN),
            new("ZRANDMEMBER", RespCommand.ZRANDMEMBER),
            new("ZRANGE", RespCommand.ZRANGE),
            new("ZRANGEBYLEX", RespCommand.ZRANGEBYLEX),
            new("ZRANGEBYSCORE", RespCommand.ZRANGEBYSCORE),
            new("ZRANGESTORE", RespCommand.ZRANGESTORE),
            new("ZRANK", RespCommand.ZRANK),
            new("ZREM", RespCommand.ZREM),
            new("ZREMRANGEBYLEX", RespCommand.ZREMRANGEBYLEX),
            new("ZREMRANGEBYRANK", RespCommand.ZREMRANGEBYRANK),
            new("ZREMRANGEBYSCORE", RespCommand.ZREMRANGEBYSCORE),
            new("ZREVRANGE", RespCommand.ZREVRANGE),
            new("ZREVRANGEBYLEX", RespCommand.ZREVRANGEBYLEX),
            new("ZREVRANGEBYSCORE", RespCommand.ZREVRANGEBYSCORE),
            new("ZREVRANK", RespCommand.ZREVRANK),
            new("ZSCAN", RespCommand.ZSCAN),
            new("ZSCORE", RespCommand.ZSCORE),
            new("ZEXPIRE", RespCommand.HEXPIRE),
            new("ZPEXPIRE", RespCommand.HPEXPIRE),
            new("ZEXPIREAT", RespCommand.HEXPIREAT),
            new("ZPEXPIREAT", RespCommand.HPEXPIREAT),
            new("ZTTL", RespCommand.HTTL),
            new("ZPTTL", RespCommand.HPTTL),
            new("ZEXPIRETIME", RespCommand.HEXPIRETIME),
            new("ZPEXPIRETIME", RespCommand.HPEXPIRETIME),
            new("ZPERSIST", RespCommand.HPERSIST),
            new("ZCOLLECT", RespCommand.HPERSIST),
            new("ZUNION", RespCommand.ZUNION),
            new("ZUNIONSTORE", RespCommand.ZUNIONSTORE),
            new("ZADD", RespCommand.ZADD, StoreType.Object),
            new("ZCARD", RespCommand.ZCARD, StoreType.Object),
            new("ZCOUNT", RespCommand.ZCOUNT, StoreType.Object),
            new("ZDIFF", RespCommand.ZDIFF, StoreType.Object),
            new("ZDIFFSTORE", RespCommand.ZDIFFSTORE, StoreType.Object),
            new("ZINCRBY", RespCommand.ZINCRBY, StoreType.Object),
            new("ZINTER", RespCommand.ZINTER, StoreType.Object),
            new("ZINTERCARD", RespCommand.ZINTERCARD, StoreType.Object),
            new("ZINTERSTORE", RespCommand.ZINTERSTORE, StoreType.Object),
            new("ZLEXCOUNT", RespCommand.ZLEXCOUNT, StoreType.Object),
            new("ZMSCORE", RespCommand.ZMSCORE, StoreType.Object),
            new("ZMPOP", RespCommand.ZMPOP, StoreType.Object),
            new("ZPOPMAX", RespCommand.ZPOPMAX, StoreType.Object),
            new("ZPOPMIN", RespCommand.ZPOPMIN, StoreType.Object),
            new("ZRANDMEMBER", RespCommand.ZRANDMEMBER, StoreType.Object),
            new("ZRANGE", RespCommand.ZRANGE, StoreType.Object),
            new("ZRANGEBYLEX", RespCommand.ZRANGEBYLEX, StoreType.Object),
            new("ZRANGEBYSCORE", RespCommand.ZRANGEBYSCORE, StoreType.Object),
            new("ZRANGESTORE", RespCommand.ZRANGESTORE, StoreType.Object),
            new("ZRANK", RespCommand.ZRANK, StoreType.Object),
            new("ZREM", RespCommand.ZREM, StoreType.Object),
            new("ZREMRANGEBYLEX", RespCommand.ZREMRANGEBYLEX, StoreType.Object),
            new("ZREMRANGEBYRANK", RespCommand.ZREMRANGEBYRANK, StoreType.Object),
            new("ZREMRANGEBYSCORE", RespCommand.ZREMRANGEBYSCORE, StoreType.Object),
            new("ZREVRANGE", RespCommand.ZREVRANGE, StoreType.Object),
            new("ZREVRANGEBYLEX", RespCommand.ZREVRANGEBYLEX, StoreType.Object),
            new("ZREVRANGEBYSCORE", RespCommand.ZREVRANGEBYSCORE, StoreType.Object),
            new("ZREVRANK", RespCommand.ZREVRANK, StoreType.Object),
            new("ZSCAN", RespCommand.ZSCAN, StoreType.Object),
            new("ZSCORE", RespCommand.ZSCORE, StoreType.Object),
            new("ZEXPIRE", RespCommand.ZEXPIRE, StoreType.Object),
            new("ZPEXPIRE", RespCommand.ZPEXPIRE, StoreType.Object),
            new("ZEXPIREAT", RespCommand.ZEXPIREAT, StoreType.Object),
            new("ZPEXPIREAT", RespCommand.ZPEXPIREAT, StoreType.Object),
            new("ZTTL", RespCommand.ZTTL, StoreType.Object),
            new("ZPTTL", RespCommand.ZPTTL, StoreType.Object),
            new("ZEXPIRETIME", RespCommand.ZEXPIRETIME, StoreType.Object),
            new("ZPEXPIRETIME", RespCommand.ZPEXPIRETIME, StoreType.Object),
            new("ZPERSIST", RespCommand.ZPERSIST, StoreType.Object),
            new("ZCOLLECT", RespCommand.ZCOLLECT, StoreType.Object),
            new("ZUNION", RespCommand.ZUNION, StoreType.Object),
            new("ZUNIONSTORE", RespCommand.ZUNIONSTORE, StoreType.Object),
            new("EVAL", RespCommand.EVAL),
            new("EVALSHA", RespCommand.EVALSHA),
            new("SCRIPT", RespCommand.SCRIPT, StoreType.None,
            [
                new("SCRIPT|EXISTS", RespCommand.SCRIPT_EXISTS),
                new("SCRIPT|FLUSH", RespCommand.SCRIPT_FLUSH),
                new("SCRIPT|LOAD", RespCommand.SCRIPT_LOAD),
            ])
        ];

        static readonly Lazy<IReadOnlyDictionary<string, SupportedCommand>> LazySupportedCommandsMap =
            new(() =>
            {
                var map = new Dictionary<string, SupportedCommand>(StringComparer.OrdinalIgnoreCase);
                foreach (var supportedCommand in AllSupportedCommands)
                {
                    map.Add(supportedCommand.Command, supportedCommand);
                }

                return new ReadOnlyDictionary<string, SupportedCommand>(map);
            });

        static readonly Lazy<IReadOnlyDictionary<string, SupportedCommand>> LazySupportedCommandsFlattenedMap =
            new(() =>
            {
                var map = new Dictionary<string, SupportedCommand>(SupportedCommandsMap, StringComparer.OrdinalIgnoreCase);
                foreach (var supportedCommand in SupportedCommandsMap.Values)
                {
                    if (supportedCommand.SubCommands != null)
                    {
                        foreach (var subCommand in supportedCommand.SubCommands)
                        {
                            map.Add(subCommand.Key, subCommand.Value);
                        }
                    }
                }

                return new ReadOnlyDictionary<string, SupportedCommand>(map);
            });

        /// <summary>
        /// Map between a supported command's name and its SupportedCommand object
        /// </summary>
        public static IReadOnlyDictionary<string, SupportedCommand> SupportedCommandsMap => LazySupportedCommandsMap.Value;

        /// <summary>
        /// Map between a supported command's and supported sub-command's name and its SupportedCommand object
        /// </summary>
        public static IReadOnlyDictionary<string, SupportedCommand> SupportedCommandsFlattenedMap => LazySupportedCommandsFlattenedMap.Value;

        /// <summary>
        /// Supported command's name
        /// </summary>
        public string Command { get; set; }

        /// <summary>
        /// Supported command's sub-commands' names
        /// </summary>
        public IReadOnlyDictionary<string, SupportedCommand> SubCommands { get; set; }

        /// <summary>
        /// Garnet RespCommand
        /// </summary>
        public RespCommand RespCommand { get; set; }

        /// <summary>
        /// Store type that the command operates on (None/Main/Object/All). Default: None for commands without key arguments.
        /// </summary>
        public StoreType StoreType { get; set; }

        /// <summary>
        /// Default constructor provided for JSON serialization
        /// </summary>
        public SupportedCommand()
        {

        }

        /// <summary>
        /// SupportedCommand constructor
        /// </summary>
        /// <param name="command">Supported command name</param>
        /// <param name="respCommand">RESP Command enum</param>
        /// <param name="storeType">Store type that the command operates on (None/Main/Object/All). Default: None for commands without key arguments.</param>
        /// <param name="subCommands">List of supported sub-command names (optional)</param>
        public SupportedCommand(string command, RespCommand respCommand = RespCommand.NONE, StoreType storeType = StoreType.None, IEnumerable<SupportedCommand> subCommands = null) : this()
        {
            Command = command;
            SubCommands = subCommands?.ToDictionary(sc => sc.Command, sc => sc);
            RespCommand = respCommand;
            StoreType = storeType;
        }
    }
}