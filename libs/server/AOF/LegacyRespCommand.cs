// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Backward-compatibility mapping for the pre-v4 (AOF header version 3) <see cref="RespCommand"/>
    /// numbering. In v4 the write commands were moved to a dense, explicitly-numbered block at the
    /// start of the enum (writes-first), which changed the numeric value of every write command that
    /// is persisted in the AOF (RespInputHeader.cmd). When replaying a v3 AOF (local recovery or a
    /// replica consuming an older primary's stream) the persisted v3 value must be translated to the
    /// current value before dispatch.
    ///
    /// The translation is name-based and computed once at startup, so it is robust to the numeric
    /// reshuffle. If a v3 command name no longer resolves to a current <see cref="RespCommand"/>
    /// (e.g. a write command was renamed or removed), startup fails fast — that is a deliberate
    /// forcing function: such a change breaks v3 AOF compatibility and must be handled explicitly.
    /// </summary>
    internal static class LegacyRespCommand
    {
        // DO NOT EDIT: frozen snapshot of the v3 RespCommand member order (excluding NONE and INVALID).
        // Index i corresponds to the v3 numeric value (i + 1), i.e. V3Order[0] had v3 value 1.
        // This captures the numbering that shipped with AOF header version 3 and must never change.
        private static readonly string[] V3Order =
        [
            "BITCOUNT", "BITFIELD_RO", "BITPOS", "COSCAN", "DBSIZE", "DUMP", "EXISTS", "EXPIRETIME", "GEODIST", "GEOHASH",
            "GEOPOS", "GEORADIUS_RO", "GEORADIUSBYMEMBER_RO", "GEOSEARCH", "GET", "GETBIT", "GETIFNOTMATCH", "GETRANGE", "GETWITHETAG", "HEXISTS",
            "HGET", "HGETALL", "HKEYS", "HLEN", "HMGET", "HRANDFIELD", "HSCAN", "HSTRLEN", "HVALS", "KEYS",
            "LCS", "HTTL", "HPTTL", "HEXPIRETIME", "HPEXPIRETIME", "LINDEX", "LLEN", "LPOS", "LRANGE", "MEMORY_USAGE",
            "MGET", "PEXPIRETIME", "PFCOUNT", "PTTL", "SCAN", "SCARD", "SDIFF", "SINTER", "SINTERCARD", "SISMEMBER",
            "SMEMBERS", "SMISMEMBER", "SPUBLISH", "SRANDMEMBER", "SSCAN", "SSUBSCRIBE", "STRLEN", "SUBSTR", "SUNION", "TTL",
            "TYPE", "VCARD", "VDIM", "VEMB", "VGETATTR", "VINFO", "VISMEMBER", "VLINKS", "VRANDMEMBER", "VSIM",
            "WATCH", "WATCHMS", "WATCHOS", "ZCARD", "ZCOUNT", "ZDIFF", "ZINTER", "ZINTERCARD", "ZLEXCOUNT", "ZMSCORE",
            "ZRANDMEMBER", "ZRANGE", "ZRANGEBYLEX", "ZRANGEBYSCORE", "ZRANK", "ZREVRANGE", "ZREVRANGEBYLEX", "ZREVRANGEBYSCORE", "ZREVRANK", "ZTTL",
            "ZPTTL", "ZEXPIRETIME", "ZPEXPIRETIME", "ZSCAN", "ZSCORE", "ZUNION", "RICONFIG", "RIEXISTS", "RIGET", "RIMETRICS",
            "RIRANGE", "RISCAN", "APPEND", "BITFIELD", "BZMPOP", "BZPOPMAX", "BZPOPMIN", "DECR", "DECRBY", "DEL",
            "DELIFEXPIM", "DELIFGREATER", "EXPIRE", "EXPIREAT", "FLUSHALL", "FLUSHDB", "GEOADD", "GEORADIUS", "GEORADIUSBYMEMBER", "GEOSEARCHSTORE",
            "GETDEL", "GETEX", "GETSET", "HCOLLECT", "HDEL", "HEXPIRE", "HPEXPIRE", "HEXPIREAT", "HPEXPIREAT", "HPERSIST",
            "HINCRBY", "HINCRBYFLOAT", "HMSET", "HSET", "HSETNX", "INCR", "INCRBY", "INCRBYFLOAT", "LINSERT", "LMOVE",
            "LMPOP", "LPOP", "LPUSH", "LPUSHX", "LREM", "LSET", "LTRIM", "BLPOP", "BRPOP", "BLMOVE",
            "BRPOPLPUSH", "BLMPOP", "MIGRATE", "MSET", "MSETNX", "PERSIST", "PEXPIRE", "PEXPIREAT", "PFADD", "PFMERGE",
            "PSETEX", "RENAME", "RICREATE", "RIDEL", "RIPROMOTE", "RIRESTORE", "RISET", "RESTORE", "RENAMENX", "RPOP",
            "RPOPLPUSH", "RPUSH", "RPUSHX", "SADD", "SDIFFSTORE", "SET", "SETBIT", "SETEX", "SETEXNX", "SETEXXX",
            "SETNX", "SETIFMATCH", "SETIFGREATER", "SETWITHETAG", "SETKEEPTTL", "SETKEEPTTLXX", "SETRANGE", "SINTERSTORE", "SMOVE", "SPOP",
            "SREM", "SUNIONSTORE", "SWAPDB", "UNLINK", "VADD", "VREM", "VSETATTR", "ZADD", "ZCOLLECT", "ZDIFFSTORE",
            "ZEXPIRE", "ZPEXPIRE", "ZEXPIREAT", "ZPEXPIREAT", "ZPERSIST", "ZINCRBY", "ZMPOP", "ZINTERSTORE", "ZPOPMAX", "ZPOPMIN",
            "ZRANGESTORE", "ZREM", "ZREMRANGEBYLEX", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZUNIONSTORE", "BITOP", "BITOP_AND", "BITOP_OR", "BITOP_XOR",
            "BITOP_NOT", "BITOP_DIFF", "EVAL", "EVALSHA", "ASYNC", "PING", "PUBSUB", "PUBSUB_CHANNELS", "PUBSUB_NUMPAT", "PUBSUB_NUMSUB",
            "PUBLISH", "SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE", "ASKING", "SELECT", "ECHO", "CLIENT", "CLIENT_ID",
            "CLIENT_INFO", "CLIENT_LIST", "CLIENT_KILL", "CLIENT_GETNAME", "CLIENT_SETNAME", "CLIENT_SETINFO", "CLIENT_UNBLOCK", "MONITOR", "MODULE", "MODULE_LOADCS",
            "REGISTERCS", "MULTI", "EXEC", "DISCARD", "UNWATCH", "RUNTXP", "READONLY", "READWRITE", "REPLICAOF", "SECONDARYOF",
            "INFO", "TIME", "ROLE", "SAVE", "EXPDELSCAN", "LASTSAVE", "BGSAVE", "COMMITAOF", "PURGEBP", "FAILOVER",
            "SCRIPT", "SCRIPT_EXISTS", "SCRIPT_FLUSH", "SCRIPT_LOAD", "ACL", "ACL_CAT", "ACL_DELUSER", "ACL_GENPASS", "ACL_GETUSER", "ACL_LIST",
            "ACL_LOAD", "ACL_SAVE", "ACL_SETUSER", "ACL_USERS", "ACL_WHOAMI", "COMMAND", "COMMAND_COUNT", "COMMAND_DOCS", "COMMAND_INFO", "COMMAND_GETKEYS",
            "COMMAND_GETKEYSANDFLAGS", "MEMORY", "CONFIG", "CONFIG_GET", "CONFIG_REWRITE", "CONFIG_SET", "DEBUG", "LATENCY", "LATENCY_HELP", "LATENCY_HISTOGRAM",
            "LATENCY_RESET", "SLOWLOG", "SLOWLOG_HELP", "SLOWLOG_LEN", "SLOWLOG_GET", "SLOWLOG_RESET", "CLUSTER", "CLUSTER_ADDSLOTS", "CLUSTER_ADDSLOTSRANGE", "CLUSTER_ADVANCE_TIME",
            "CLUSTER_APPENDLOG", "CLUSTER_ATTACH_SYNC", "CLUSTER_BANLIST", "CLUSTER_BEGIN_REPLICA_RECOVER", "CLUSTER_BUMPEPOCH", "CLUSTER_COUNTKEYSINSLOT", "CLUSTER_DELKEYSINSLOT", "CLUSTER_DELKEYSINSLOTRANGE", "CLUSTER_DELSLOTS", "CLUSTER_DELSLOTSRANGE",
            "CLUSTER_ENDPOINT", "CLUSTER_FAILOVER", "CLUSTER_FAILREPLICATIONOFFSET", "CLUSTER_FAILSTOPWRITES", "CLUSTER_FLUSHALL", "CLUSTER_FORGET", "CLUSTER_GETKEYSINSLOT", "CLUSTER_GOSSIP", "CLUSTER_HELP", "CLUSTER_INFO",
            "CLUSTER_INITIATE_REPLICA_SYNC", "CLUSTER_KEYSLOT", "CLUSTER_MEET", "CLUSTER_MIGRATE", "CLUSTER_MLOG_KEY_TIME", "CLUSTER_MTASKS", "CLUSTER_MYID", "CLUSTER_MYPARENTID", "CLUSTER_NODES", "CLUSTER_PUBLISH",
            "CLUSTER_SPUBLISH", "CLUSTER_REPLICAS", "CLUSTER_REPLICATE", "CLUSTER_RESERVE", "CLUSTER_RESET", "CLUSTER_SEND_CKPT_FILE_SEGMENT", "CLUSTER_SEND_CKPT_METADATA", "CLUSTER_SETCONFIGEPOCH", "CLUSTER_SETSLOT", "CLUSTER_SETSLOTSRANGE",
            "CLUSTER_SHARDS", "CLUSTER_SLOTS", "CLUSTER_SLOTSTATE", "CLUSTER_SNAPSHOT_DATA", "CLUSTER_SYNC", "AUTH", "HELLO", "QUIT",
        ];

        /// <summary>
        /// Lookup table: v3 numeric value -> current <see cref="RespCommand"/>. Index 0 (NONE) and any
        /// value outside the built-in range map to themselves.
        /// </summary>
        private static readonly RespCommand[] V3ToCurrent = BuildMap();

        private static RespCommand[] BuildMap()
        {
            // Size covers NONE (0) plus all v3 built-in members (values 1..V3Order.Length).
            var map = new RespCommand[V3Order.Length + 1];
            map[0] = RespCommand.NONE;
            for (var i = 0; i < V3Order.Length; i++)
            {
                if (!Enum.TryParse<RespCommand>(V3Order[i], out var current))
                {
                    throw new GarnetException(
                        $"v3 RespCommand '{V3Order[i]}' (legacy AOF value {i + 1}) no longer maps to a current RespCommand. " +
                        "Renaming or removing a persisted (write) command breaks v3 AOF compatibility; add an explicit mapping.");
                }

                map[i + 1] = current;
            }

            return map;
        }

        /// <summary>
        /// Translate a RespCommand value read from a v3 (pre-v4) AOF entry to the current numbering.
        /// Custom raw-string command ids (anchored to <see cref="RespCommand.INVALID"/>) and any value
        /// outside the built-in range are unchanged.
        /// </summary>
        internal static RespCommand FromV3(RespCommand v3Command)
        {
            var value = (ushort)v3Command;
            if (value < V3ToCurrent.Length)
                return V3ToCurrent[value];

            // Custom raw-string command ids and INVALID were numbered relative to INVALID in both
            // versions, so they are unchanged.
            return v3Command;
        }

        /// <summary>
        /// Force initialization of the map (so a rename regression fails fast at startup, not on first replay).
        /// </summary>
        internal static void EnsureInitialized() => _ = V3ToCurrent;
    }
}