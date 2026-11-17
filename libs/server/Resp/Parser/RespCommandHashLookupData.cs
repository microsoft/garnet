// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;

namespace Garnet.server
{
    /// <summary>
    /// Command registration data for <see cref="RespCommandHashLookup"/>.
    /// Primary command table and subcommand definitions.
    /// </summary>
    internal static unsafe partial class RespCommandHashLookup
    {
        #region Command Definitions

        private static void PopulatePrimaryTable()
        {
            // Helper to insert with optional subcommand flag and known exact arg count
            static void Add(string name, RespCommand cmd, bool hasSub = false, byte? exactArgCount = null)
            {
                byte flags = 0;
                if (hasSub)
                {
                    flags |= FlagHasSubcommands;
                }

                if (exactArgCount != null)
                {
                    ArgumentOutOfRangeException.ThrowIfGreaterThan(exactArgCount.Value, 64, nameof(exactArgCount));

                    flags |= FlagHasExactArgCount;
                    flags |= (byte)(exactArgCount.Value << 2);
                }

                InsertIntoTable(
                    primaryTable,
                    PrimaryTableMask,
                    Encoding.ASCII.GetBytes(name),
                    cmd,
                    flags
                );
            }

            // ===== Data commands (read + write) =====

            // String commands
            Add("GET", RespCommand.GET, exactArgCount: 1);
            Add("SET", RespCommand.SET);
            Add("DEL", RespCommand.DEL);
            Add("DELIFEQ", RespCommand.DELIFEQ);
            Add("DELEX", RespCommand.DELEX);
            Add("DIGEST", RespCommand.DIGEST, exactArgCount: 1);
            Add("INCR", RespCommand.INCR, exactArgCount: 1);
            Add("DECR", RespCommand.DECR, exactArgCount: 1);
            Add("INCRBY", RespCommand.INCRBY, exactArgCount: 2);
            Add("DECRBY", RespCommand.DECRBY, exactArgCount: 2);
            Add("INCRBYFLOAT", RespCommand.INCRBYFLOAT, exactArgCount: 2);
            Add("APPEND", RespCommand.APPEND, exactArgCount: 2);
            Add("GETSET", RespCommand.GETSET, exactArgCount: 2);
            Add("GETDEL", RespCommand.GETDEL, exactArgCount: 1);
            Add("INCR", RespCommand.INCR);
            Add("DECR", RespCommand.DECR);
            Add("INCRBY", RespCommand.INCRBY);
            Add("DECRBY", RespCommand.DECRBY);
            Add("INCRBYFLOAT", RespCommand.INCRBYFLOAT);
            Add("APPEND", RespCommand.APPEND);
            Add("GETSET", RespCommand.GETSET);
            Add("GETDEL", RespCommand.GETDEL);
            Add("GETEX", RespCommand.GETEX);
            Add("GETRANGE", RespCommand.GETRANGE, exactArgCount: 3);
            Add("SETRANGE", RespCommand.SETRANGE, exactArgCount: 3);
            Add("STRLEN", RespCommand.STRLEN, exactArgCount: 1);
            Add("SUBSTR", RespCommand.SUBSTR, exactArgCount: 3);
            Add("SETNX", RespCommand.SETNX);
            Add("SETEX", RespCommand.SETEX);
            Add("PSETEX", RespCommand.PSETEX);
            Add("MGET", RespCommand.MGET);
            Add("MSET", RespCommand.MSET);
            Add("MSETNX", RespCommand.MSETNX);
            Add("DUMP", RespCommand.DUMP, exactArgCount: 1);
            Add("RESTORE", RespCommand.RESTORE);
            Add("GETBIT", RespCommand.GETBIT, exactArgCount: 2);
            Add("SETBIT", RespCommand.SETBIT, exactArgCount: 3);
            Add("GETWITHETAG", RespCommand.GETWITHETAG);
            Add("GETIFNOTMATCH", RespCommand.GETIFNOTMATCH);
            Add("SETIFMATCH", RespCommand.SETIFMATCH);
            Add("SETIFGREATER", RespCommand.SETIFGREATER);
            Add("SETWITHETAG", RespCommand.SETWITHETAG);
            Add("DELIFGREATER", RespCommand.DELIFGREATER);
            Add("LCS", RespCommand.LCS);

            // Key commands
            Add("EXISTS", RespCommand.EXISTS);
            Add("TOUCH", RespCommand.TOUCH);
            Add("TTL", RespCommand.TTL, exactArgCount: 1);
            Add("PTTL", RespCommand.PTTL, exactArgCount: 1);
            Add("EXPIRE", RespCommand.EXPIRE);
            Add("PEXPIRE", RespCommand.PEXPIRE);
            Add("EXPIREAT", RespCommand.EXPIREAT);
            Add("PEXPIREAT", RespCommand.PEXPIREAT);
            Add("EXPIRETIME", RespCommand.EXPIRETIME, exactArgCount: 1);
            Add("PEXPIRETIME", RespCommand.PEXPIRETIME, exactArgCount: 1);
            Add("PERSIST", RespCommand.PERSIST, exactArgCount: 1);
            Add("TYPE", RespCommand.TYPE, exactArgCount: 1);
            Add("RENAME", RespCommand.RENAME, exactArgCount: 2);
            Add("RENAMENX", RespCommand.RENAMENX, exactArgCount: 2);
            Add("UNLINK", RespCommand.UNLINK);
            Add("KEYS", RespCommand.KEYS, exactArgCount: 1);
            Add("SCAN", RespCommand.SCAN);
            Add("DBSIZE", RespCommand.DBSIZE, exactArgCount: 0);
            Add("SELECT", RespCommand.SELECT, exactArgCount: 1);
            Add("SWAPDB", RespCommand.SWAPDB, exactArgCount: 2);
            Add("MIGRATE", RespCommand.MIGRATE);

            // Bitmap commands
            Add("BITCOUNT", RespCommand.BITCOUNT);
            Add("BITPOS", RespCommand.BITPOS);
            Add("BITFIELD", RespCommand.BITFIELD);
            Add("BITFIELD_RO", RespCommand.BITFIELD_RO);
            Add("BITOP", RespCommand.BITOP, hasSub: true);

            // HyperLogLog commands
            Add("PFADD", RespCommand.PFADD);
            Add("PFCOUNT", RespCommand.PFCOUNT);
            Add("PFMERGE", RespCommand.PFMERGE);

            // Hash commands
            Add("HSET", RespCommand.HSET);
            Add("HGET", RespCommand.HGET, exactArgCount: 2);
            Add("HDEL", RespCommand.HDEL);
            Add("HLEN", RespCommand.HLEN, exactArgCount: 1);
            Add("HEXISTS", RespCommand.HEXISTS, exactArgCount: 2);
            Add("HGETALL", RespCommand.HGETALL, exactArgCount: 1);
            Add("HKEYS", RespCommand.HKEYS, exactArgCount: 1);
            Add("HVALS", RespCommand.HVALS, exactArgCount: 1);
            Add("HMSET", RespCommand.HMSET);
            Add("HMGET", RespCommand.HMGET);
            Add("HSETNX", RespCommand.HSETNX);
            Add("HINCRBY", RespCommand.HINCRBY, exactArgCount: 3);
            Add("HINCRBYFLOAT", RespCommand.HINCRBYFLOAT, exactArgCount: 3);
            Add("HRANDFIELD", RespCommand.HRANDFIELD);
            Add("HSCAN", RespCommand.HSCAN);
            Add("HSTRLEN", RespCommand.HSTRLEN, exactArgCount: 2);
            Add("HTTL", RespCommand.HTTL);
            Add("HPTTL", RespCommand.HPTTL);
            Add("HEXPIRE", RespCommand.HEXPIRE);
            Add("HPEXPIRE", RespCommand.HPEXPIRE);
            Add("HEXPIREAT", RespCommand.HEXPIREAT);
            Add("HPEXPIREAT", RespCommand.HPEXPIREAT);
            Add("HEXPIRETIME", RespCommand.HEXPIRETIME);
            Add("HPEXPIRETIME", RespCommand.HPEXPIRETIME);
            Add("HPERSIST", RespCommand.HPERSIST);
            Add("HCOLLECT", RespCommand.HCOLLECT);

            // List commands
            Add("LPUSH", RespCommand.LPUSH);
            Add("RPUSH", RespCommand.RPUSH);
            Add("LPUSHX", RespCommand.LPUSHX);
            Add("RPUSHX", RespCommand.RPUSHX);
            Add("LPOP", RespCommand.LPOP);
            Add("RPOP", RespCommand.RPOP);
            Add("LLEN", RespCommand.LLEN, exactArgCount: 1);
            Add("LINDEX", RespCommand.LINDEX, exactArgCount: 2);
            Add("LINSERT", RespCommand.LINSERT);
            Add("LRANGE", RespCommand.LRANGE, exactArgCount: 3);
            Add("LREM", RespCommand.LREM, exactArgCount: 3);
            Add("LSET", RespCommand.LSET, exactArgCount: 3);
            Add("LTRIM", RespCommand.LTRIM, exactArgCount: 3);
            Add("LPOS", RespCommand.LPOS);
            Add("LMOVE", RespCommand.LMOVE);
            Add("LMPOP", RespCommand.LMPOP);
            Add("RPOPLPUSH", RespCommand.RPOPLPUSH);
            Add("BLPOP", RespCommand.BLPOP);
            Add("BRPOP", RespCommand.BRPOP);
            Add("BLMOVE", RespCommand.BLMOVE);
            Add("BRPOPLPUSH", RespCommand.BRPOPLPUSH);
            Add("BLMPOP", RespCommand.BLMPOP);

            // Set commands
            Add("SADD", RespCommand.SADD);
            Add("SREM", RespCommand.SREM);
            Add("SPOP", RespCommand.SPOP);
            Add("SCARD", RespCommand.SCARD, exactArgCount: 1);
            Add("SMEMBERS", RespCommand.SMEMBERS, exactArgCount: 1);
            Add("SISMEMBER", RespCommand.SISMEMBER, exactArgCount: 2);
            Add("SMISMEMBER", RespCommand.SMISMEMBER);
            Add("SRANDMEMBER", RespCommand.SRANDMEMBER);
            Add("SMOVE", RespCommand.SMOVE, exactArgCount: 3);
            Add("SSCAN", RespCommand.SSCAN);
            Add("SDIFF", RespCommand.SDIFF);
            Add("SDIFFSTORE", RespCommand.SDIFFSTORE);
            Add("SINTER", RespCommand.SINTER);
            Add("SINTERCARD", RespCommand.SINTERCARD);
            Add("SINTERSTORE", RespCommand.SINTERSTORE);
            Add("SUNION", RespCommand.SUNION);
            Add("SUNIONSTORE", RespCommand.SUNIONSTORE);

            // Sorted set commands
            Add("ZADD", RespCommand.ZADD);
            Add("ZREM", RespCommand.ZREM);
            Add("ZCARD", RespCommand.ZCARD, exactArgCount: 1);
            Add("ZSCORE", RespCommand.ZSCORE, exactArgCount: 2);
            Add("ZMSCORE", RespCommand.ZMSCORE);
            Add("ZRANK", RespCommand.ZRANK);
            Add("ZREVRANK", RespCommand.ZREVRANK);
            Add("ZCOUNT", RespCommand.ZCOUNT, exactArgCount: 3);
            Add("ZLEXCOUNT", RespCommand.ZLEXCOUNT, exactArgCount: 3);
            Add("ZRANGE", RespCommand.ZRANGE);
            Add("ZRANGEBYLEX", RespCommand.ZRANGEBYLEX);
            Add("ZRANGEBYSCORE", RespCommand.ZRANGEBYSCORE);
            Add("ZRANGESTORE", RespCommand.ZRANGESTORE);
            Add("ZREVRANGE", RespCommand.ZREVRANGE);
            Add("ZREVRANGEBYLEX", RespCommand.ZREVRANGEBYLEX);
            Add("ZREVRANGEBYSCORE", RespCommand.ZREVRANGEBYSCORE);
            Add("ZPOPMIN", RespCommand.ZPOPMIN);
            Add("ZPOPMAX", RespCommand.ZPOPMAX);
            Add("ZRANDMEMBER", RespCommand.ZRANDMEMBER);
            Add("ZSCAN", RespCommand.ZSCAN);
            Add("ZINCRBY", RespCommand.ZINCRBY, exactArgCount: 3);
            Add("ZDIFF", RespCommand.ZDIFF);
            Add("ZDIFFSTORE", RespCommand.ZDIFFSTORE);
            Add("ZINTER", RespCommand.ZINTER);
            Add("ZINTERCARD", RespCommand.ZINTERCARD);
            Add("ZINTERSTORE", RespCommand.ZINTERSTORE);
            Add("ZUNION", RespCommand.ZUNION);
            Add("ZUNIONSTORE", RespCommand.ZUNIONSTORE);
            Add("ZMPOP", RespCommand.ZMPOP);
            Add("BZMPOP", RespCommand.BZMPOP);
            Add("BZPOPMAX", RespCommand.BZPOPMAX);
            Add("BZPOPMIN", RespCommand.BZPOPMIN);
            Add("ZREMRANGEBYLEX", RespCommand.ZREMRANGEBYLEX);
            Add("ZREMRANGEBYRANK", RespCommand.ZREMRANGEBYRANK);
            Add("ZREMRANGEBYSCORE", RespCommand.ZREMRANGEBYSCORE);
            Add("ZTTL", RespCommand.ZTTL);
            Add("ZPTTL", RespCommand.ZPTTL);
            Add("ZEXPIRE", RespCommand.ZEXPIRE);
            Add("ZPEXPIRE", RespCommand.ZPEXPIRE);
            Add("ZEXPIREAT", RespCommand.ZEXPIREAT);
            Add("ZPEXPIREAT", RespCommand.ZPEXPIREAT);
            Add("ZEXPIRETIME", RespCommand.ZEXPIRETIME);
            Add("ZPEXPIRETIME", RespCommand.ZPEXPIRETIME);
            Add("ZPERSIST", RespCommand.ZPERSIST);
            Add("ZCOLLECT", RespCommand.ZCOLLECT);

            // Vector Set commands
            Add("VADD", RespCommand.VADD);
            Add("VCARD", RespCommand.VCARD, exactArgCount: 1);
            Add("VDIM", RespCommand.VDIM, exactArgCount: 1);
            Add("VEMB", RespCommand.VEMB);
            Add("VGETATTR", RespCommand.VGETATTR, exactArgCount: 2);
            Add("VINFO", RespCommand.VINFO, exactArgCount: 1);
            Add("VISMEMBER", RespCommand.VISMEMBER, exactArgCount: 2);
            Add("VLINKS", RespCommand.VLINKS);
            Add("VRANDMEMBER", RespCommand.VRANDMEMBER);
            Add("VREM", RespCommand.VREM, exactArgCount: 2);
            Add("VSETATTR", RespCommand.VSETATTR, exactArgCount: 3);
            Add("VSIM", RespCommand.VSIM);
            Add("XVCREATE", RespCommand.XVCREATE);
            Add("XVIMPORT", RespCommand.XVIMPORT);

            // Range Index commands (dot-prefixed wire names)
            Add("RI.CREATE", RespCommand.RICREATE);
            Add("RI.SET", RespCommand.RISET);
            Add("RI.GET", RespCommand.RIGET);
            Add("RI.DEL", RespCommand.RIDEL);
            Add("RI.RANGE", RespCommand.RIRANGE);
            Add("RI.SCAN", RespCommand.RISCAN);
            Add("RI.EXISTS", RespCommand.RIEXISTS);
            Add("RI.CONFIG", RespCommand.RICONFIG);
            Add("RI.METRICS", RespCommand.RIMETRICS);

            // Geo commands
            Add("GEOADD", RespCommand.GEOADD);
            Add("GEOPOS", RespCommand.GEOPOS);
            Add("GEOHASH", RespCommand.GEOHASH);
            Add("GEODIST", RespCommand.GEODIST);
            Add("GEOSEARCH", RespCommand.GEOSEARCH);
            Add("GEOSEARCHSTORE", RespCommand.GEOSEARCHSTORE);
            Add("GEORADIUS", RespCommand.GEORADIUS);
            Add("GEORADIUS_RO", RespCommand.GEORADIUS_RO);
            Add("GEORADIUSBYMEMBER", RespCommand.GEORADIUSBYMEMBER);
            Add("GEORADIUSBYMEMBER_RO", RespCommand.GEORADIUSBYMEMBER_RO);

            // Scripting
            Add("EVAL", RespCommand.EVAL);
            Add("EVALSHA", RespCommand.EVALSHA);

            // Pub/Sub
            Add("PUBLISH", RespCommand.PUBLISH, exactArgCount: 2);
            Add("SUBSCRIBE", RespCommand.SUBSCRIBE);
            Add("PSUBSCRIBE", RespCommand.PSUBSCRIBE);
            Add("UNSUBSCRIBE", RespCommand.UNSUBSCRIBE);
            Add("PUNSUBSCRIBE", RespCommand.PUNSUBSCRIBE);
            Add("SPUBLISH", RespCommand.SPUBLISH, exactArgCount: 2);
            Add("SSUBSCRIBE", RespCommand.SSUBSCRIBE);

            // Custom object scan
            Add("CUSTOMOBJECTSCAN", RespCommand.COSCAN);

            // ===== Control / admin commands =====
            Add("PING", RespCommand.PING);
            Add("ECHO", RespCommand.ECHO, exactArgCount: 1);
            Add("QUIT", RespCommand.QUIT, exactArgCount: 0);
            Add("AUTH", RespCommand.AUTH);
            Add("HELLO", RespCommand.HELLO);
            Add("INFO", RespCommand.INFO);
            Add("TIME", RespCommand.TIME, exactArgCount: 0);
            Add("ROLE", RespCommand.ROLE, exactArgCount: 0);
            Add("SAVE", RespCommand.SAVE);
            Add("LASTSAVE", RespCommand.LASTSAVE);
            Add("BGSAVE", RespCommand.BGSAVE);
            Add("COMMITAOF", RespCommand.COMMITAOF);
            Add("FLUSHALL", RespCommand.FLUSHALL);
            Add("FLUSHDB", RespCommand.FLUSHDB);
            Add("FAILOVER", RespCommand.FAILOVER);
            Add("MONITOR", RespCommand.MONITOR, exactArgCount: 0);
            Add("REGISTERCS", RespCommand.REGISTERCS);
            Add("ASYNC", RespCommand.ASYNC);
            Add("DEBUG", RespCommand.DEBUG);
            Add("EXPDELSCAN", RespCommand.EXPDELSCAN);
            Add("WATCH", RespCommand.WATCH);
            Add("WATCHMS", RespCommand.WATCHMS);
            Add("WATCHOS", RespCommand.WATCHOS);
            Add("MULTI", RespCommand.MULTI, exactArgCount: 0);
            Add("EXEC", RespCommand.EXEC, exactArgCount: 0);
            Add("DISCARD", RespCommand.DISCARD, exactArgCount: 0);
            Add("UNWATCH", RespCommand.UNWATCH);
            Add("RUNTXP", RespCommand.RUNTXP);
            Add("ASKING", RespCommand.ASKING, exactArgCount: 0);
            Add("READONLY", RespCommand.READONLY, exactArgCount: 0);
            Add("READWRITE", RespCommand.READWRITE, exactArgCount: 0);
            Add("REPLICAOF", RespCommand.REPLICAOF);
            Add("SECONDARYOF", RespCommand.SECONDARYOF);
            Add("SLAVEOF", RespCommand.SECONDARYOF);

            // Parent commands with subcommands
            Add("SCRIPT", RespCommand.SCRIPT, hasSub: true);
            Add("CONFIG", RespCommand.CONFIG, hasSub: true);
            Add("CLIENT", RespCommand.CLIENT, hasSub: true);
            Add("CLUSTER", RespCommand.CLUSTER, hasSub: true);
            Add("ACL", RespCommand.ACL, hasSub: true);
            Add("COMMAND", RespCommand.COMMAND, hasSub: true);
            Add("LATENCY", RespCommand.LATENCY, hasSub: true);
            Add("SLOWLOG", RespCommand.SLOWLOG, hasSub: true);
            Add("MODULE", RespCommand.MODULE, hasSub: true);
            Add("PUBSUB", RespCommand.PUBSUB, hasSub: true);
            Add("MEMORY", RespCommand.MEMORY, hasSub: true);
            Add("OBJECT", RespCommand.OBJECT, hasSub: true);
        }

        #endregion

        #region Subcommand Definitions

        private static readonly (string Name, RespCommand Command)[] ClusterSubcommands =
        [
            ("ADDSLOTS", RespCommand.CLUSTER_ADDSLOTS),
            ("ADDSLOTSRANGE", RespCommand.CLUSTER_ADDSLOTSRANGE),
            ("ADVANCE_TIME", RespCommand.CLUSTER_ADVANCE_TIME),
            ("APPENDLOG", RespCommand.CLUSTER_APPENDLOG),
            ("ATTACH_SYNC", RespCommand.CLUSTER_ATTACH_SYNC),
            ("BANLIST", RespCommand.CLUSTER_BANLIST),
            ("BEGIN_REPLICA_RECOVER", RespCommand.CLUSTER_BEGIN_REPLICA_RECOVER),
            ("BUMPEPOCH", RespCommand.CLUSTER_BUMPEPOCH),
            ("COUNTKEYSINSLOT", RespCommand.CLUSTER_COUNTKEYSINSLOT),
            ("DELKEYSINSLOT", RespCommand.CLUSTER_DELKEYSINSLOT),
            ("DELKEYSINSLOTRANGE", RespCommand.CLUSTER_DELKEYSINSLOTRANGE),
            ("DELSLOTS", RespCommand.CLUSTER_DELSLOTS),
            ("DELSLOTSRANGE", RespCommand.CLUSTER_DELSLOTSRANGE),
            ("ENDPOINT", RespCommand.CLUSTER_ENDPOINT),
            ("FAILOVER", RespCommand.CLUSTER_FAILOVER),
            ("FAILREPLICATIONOFFSET", RespCommand.CLUSTER_FAILREPLICATIONOFFSET),
            ("FAILSTOPWRITES", RespCommand.CLUSTER_FAILSTOPWRITES),
            ("FLUSHALL", RespCommand.CLUSTER_FLUSHALL),
            ("FORGET", RespCommand.CLUSTER_FORGET),
            ("GETKEYSINSLOT", RespCommand.CLUSTER_GETKEYSINSLOT),
            ("GOSSIP", RespCommand.CLUSTER_GOSSIP),
            ("HELP", RespCommand.CLUSTER_HELP),
            ("INFO", RespCommand.CLUSTER_INFO),
            ("INITIATE_REPLICA_SYNC", RespCommand.CLUSTER_INITIATE_REPLICA_SYNC),
            ("KEYSLOT", RespCommand.CLUSTER_KEYSLOT),
            ("MEET", RespCommand.CLUSTER_MEET),
            ("MIGRATE", RespCommand.CLUSTER_MIGRATE),
            ("MLOG_KEY_TIME", RespCommand.CLUSTER_MLOG_KEY_TIME),
            ("MTASKS", RespCommand.CLUSTER_MTASKS),
            ("MYID", RespCommand.CLUSTER_MYID),
            ("MYPARENTID", RespCommand.CLUSTER_MYPARENTID),
            ("NODES", RespCommand.CLUSTER_NODES),
            ("PUBLISH", RespCommand.CLUSTER_PUBLISH),
            ("SPUBLISH", RespCommand.CLUSTER_SPUBLISH),
            ("REPLICAS", RespCommand.CLUSTER_REPLICAS),
            ("REPLICATE", RespCommand.CLUSTER_REPLICATE),
            ("RESERVE", RespCommand.CLUSTER_RESERVE),
            ("RESET", RespCommand.CLUSTER_RESET),
            ("SEND_CKPT_FILE_SEGMENT", RespCommand.CLUSTER_SEND_CKPT_FILE_SEGMENT),
            ("SEND_CKPT_METADATA", RespCommand.CLUSTER_SEND_CKPT_METADATA),
            ("SET-CONFIG-EPOCH", RespCommand.CLUSTER_SETCONFIGEPOCH),
            ("SETSLOT", RespCommand.CLUSTER_SETSLOT),
            ("SETSLOTSRANGE", RespCommand.CLUSTER_SETSLOTSRANGE),
            ("SHARDS", RespCommand.CLUSTER_SHARDS),
            ("SLOTS", RespCommand.CLUSTER_SLOTS),
            ("SLOTSTATE", RespCommand.CLUSTER_SLOTSTATE),
            ("SNAPSHOT_DATA", RespCommand.CLUSTER_SNAPSHOT_DATA),
            ("SYNC", RespCommand.CLUSTER_SYNC),
        ];

        private static readonly (string Name, RespCommand Command)[] ClientSubcommands =
        [
            ("ID", RespCommand.CLIENT_ID),
            ("INFO", RespCommand.CLIENT_INFO),
            ("LIST", RespCommand.CLIENT_LIST),
            ("KILL", RespCommand.CLIENT_KILL),
            ("GETNAME", RespCommand.CLIENT_GETNAME),
            ("SETNAME", RespCommand.CLIENT_SETNAME),
            ("SETINFO", RespCommand.CLIENT_SETINFO),
            ("UNBLOCK", RespCommand.CLIENT_UNBLOCK),
        ];

        private static readonly (string Name, RespCommand Command)[] AclSubcommands =
        [
            ("CAT", RespCommand.ACL_CAT),
            ("DELUSER", RespCommand.ACL_DELUSER),
            ("GENPASS", RespCommand.ACL_GENPASS),
            ("GETUSER", RespCommand.ACL_GETUSER),
            ("LIST", RespCommand.ACL_LIST),
            ("LOAD", RespCommand.ACL_LOAD),
            ("SAVE", RespCommand.ACL_SAVE),
            ("SETUSER", RespCommand.ACL_SETUSER),
            ("USERS", RespCommand.ACL_USERS),
            ("WHOAMI", RespCommand.ACL_WHOAMI),
        ];

        private static readonly (string Name, RespCommand Command)[] CommandSubcommands =
        [
            ("COUNT", RespCommand.COMMAND_COUNT),
            ("DOCS", RespCommand.COMMAND_DOCS),
            ("INFO", RespCommand.COMMAND_INFO),
            ("GETKEYS", RespCommand.COMMAND_GETKEYS),
            ("GETKEYSANDFLAGS", RespCommand.COMMAND_GETKEYSANDFLAGS),
        ];

        private static readonly (string Name, RespCommand Command)[] ConfigSubcommands =
        [
            ("GET", RespCommand.CONFIG_GET),
            ("REWRITE", RespCommand.CONFIG_REWRITE),
            ("SET", RespCommand.CONFIG_SET),
        ];

        private static readonly (string Name, RespCommand Command)[] ScriptSubcommands =
        [
            ("LOAD", RespCommand.SCRIPT_LOAD),
            ("FLUSH", RespCommand.SCRIPT_FLUSH),
            ("EXISTS", RespCommand.SCRIPT_EXISTS),
        ];

        private static readonly (string Name, RespCommand Command)[] LatencySubcommands =
        [
            ("HELP", RespCommand.LATENCY_HELP),
            ("HISTOGRAM", RespCommand.LATENCY_HISTOGRAM),
            ("RESET", RespCommand.LATENCY_RESET),
        ];

        private static readonly (string Name, RespCommand Command)[] SlowlogSubcommands =
        [
            ("HELP", RespCommand.SLOWLOG_HELP),
            ("GET", RespCommand.SLOWLOG_GET),
            ("LEN", RespCommand.SLOWLOG_LEN),
            ("RESET", RespCommand.SLOWLOG_RESET),
        ];

        private static readonly (string Name, RespCommand Command)[] ModuleSubcommands =
        [
            ("LOADCS", RespCommand.MODULE_LOADCS),
        ];

        private static readonly (string Name, RespCommand Command)[] PubsubSubcommands =
        [
            ("CHANNELS", RespCommand.PUBSUB_CHANNELS),
            ("NUMSUB", RespCommand.PUBSUB_NUMSUB),
            ("NUMPAT", RespCommand.PUBSUB_NUMPAT),
        ];

        private static readonly (string Name, RespCommand Command)[] MemorySubcommands =
        [
            ("USAGE", RespCommand.MEMORY_USAGE),
        ];

        private static readonly (string Name, RespCommand Command)[] ObjectSubcommands =
        [
            ("ENCODING", RespCommand.OBJECT_ENCODING),
            ("FREQ", RespCommand.OBJECT_FREQ),
            ("HELP", RespCommand.OBJECT_HELP),
            ("IDLETIME", RespCommand.OBJECT_IDLETIME),
            ("REFCOUNT", RespCommand.OBJECT_REFCOUNT),
        ];

        private static readonly (string Name, RespCommand Command)[] BitopSubcommands =
        [
            ("AND", RespCommand.BITOP_AND),
            ("OR", RespCommand.BITOP_OR),
            ("XOR", RespCommand.BITOP_XOR),
            ("NOT", RespCommand.BITOP_NOT),
            ("DIFF", RespCommand.BITOP_DIFF),
        ];

        #endregion
    }
}