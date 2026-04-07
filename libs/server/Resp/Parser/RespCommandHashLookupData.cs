// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
            // Helper to insert with optional subcommand flag
            void Add(string name, RespCommand cmd, bool hasSub = false)
            {
                InsertIntoTable(primaryTable, PrimaryTableMask,
                    System.Text.Encoding.ASCII.GetBytes(name), cmd,
                    hasSub ? FlagHasSubcommands : (byte)0);
            }

            // ===== Data commands (read + write) =====

            // String commands
            Add("GET", RespCommand.GET);
            Add("SET", RespCommand.SET);
            Add("DEL", RespCommand.DEL);
            Add("INCR", RespCommand.INCR);
            Add("DECR", RespCommand.DECR);
            Add("INCRBY", RespCommand.INCRBY);
            Add("DECRBY", RespCommand.DECRBY);
            Add("INCRBYFLOAT", RespCommand.INCRBYFLOAT);
            Add("APPEND", RespCommand.APPEND);
            Add("GETSET", RespCommand.GETSET);
            Add("GETDEL", RespCommand.GETDEL);
            Add("GETEX", RespCommand.GETEX);
            Add("GETRANGE", RespCommand.GETRANGE);
            Add("SETRANGE", RespCommand.SETRANGE);
            Add("STRLEN", RespCommand.STRLEN);
            Add("SUBSTR", RespCommand.SUBSTR);
            Add("SETNX", RespCommand.SETNX);
            Add("SETEX", RespCommand.SETEX);
            Add("PSETEX", RespCommand.PSETEX);
            Add("MGET", RespCommand.MGET);
            Add("MSET", RespCommand.MSET);
            Add("MSETNX", RespCommand.MSETNX);
            Add("DUMP", RespCommand.DUMP);
            Add("RESTORE", RespCommand.RESTORE);
            Add("GETBIT", RespCommand.GETBIT);
            Add("SETBIT", RespCommand.SETBIT);
            Add("GETWITHETAG", RespCommand.GETWITHETAG);
            Add("GETIFNOTMATCH", RespCommand.GETIFNOTMATCH);
            Add("SETIFMATCH", RespCommand.SETIFMATCH);
            Add("SETIFGREATER", RespCommand.SETIFGREATER);
            Add("DELIFGREATER", RespCommand.DELIFGREATER);
            Add("LCS", RespCommand.LCS);

            // Key commands
            Add("EXISTS", RespCommand.EXISTS);
            Add("TTL", RespCommand.TTL);
            Add("PTTL", RespCommand.PTTL);
            Add("EXPIRE", RespCommand.EXPIRE);
            Add("PEXPIRE", RespCommand.PEXPIRE);
            Add("EXPIREAT", RespCommand.EXPIREAT);
            Add("PEXPIREAT", RespCommand.PEXPIREAT);
            Add("EXPIRETIME", RespCommand.EXPIRETIME);
            Add("PEXPIRETIME", RespCommand.PEXPIRETIME);
            Add("PERSIST", RespCommand.PERSIST);
            Add("TYPE", RespCommand.TYPE);
            Add("RENAME", RespCommand.RENAME);
            Add("RENAMENX", RespCommand.RENAMENX);
            Add("UNLINK", RespCommand.UNLINK);
            Add("KEYS", RespCommand.KEYS);
            Add("SCAN", RespCommand.SCAN);
            Add("DBSIZE", RespCommand.DBSIZE);
            Add("SELECT", RespCommand.SELECT);
            Add("SWAPDB", RespCommand.SWAPDB);
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
            Add("HGET", RespCommand.HGET);
            Add("HDEL", RespCommand.HDEL);
            Add("HLEN", RespCommand.HLEN);
            Add("HEXISTS", RespCommand.HEXISTS);
            Add("HGETALL", RespCommand.HGETALL);
            Add("HKEYS", RespCommand.HKEYS);
            Add("HVALS", RespCommand.HVALS);
            Add("HMSET", RespCommand.HMSET);
            Add("HMGET", RespCommand.HMGET);
            Add("HSETNX", RespCommand.HSETNX);
            Add("HINCRBY", RespCommand.HINCRBY);
            Add("HINCRBYFLOAT", RespCommand.HINCRBYFLOAT);
            Add("HRANDFIELD", RespCommand.HRANDFIELD);
            Add("HSCAN", RespCommand.HSCAN);
            Add("HSTRLEN", RespCommand.HSTRLEN);
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
            Add("LLEN", RespCommand.LLEN);
            Add("LINDEX", RespCommand.LINDEX);
            Add("LINSERT", RespCommand.LINSERT);
            Add("LRANGE", RespCommand.LRANGE);
            Add("LREM", RespCommand.LREM);
            Add("LSET", RespCommand.LSET);
            Add("LTRIM", RespCommand.LTRIM);
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
            Add("SCARD", RespCommand.SCARD);
            Add("SMEMBERS", RespCommand.SMEMBERS);
            Add("SISMEMBER", RespCommand.SISMEMBER);
            Add("SMISMEMBER", RespCommand.SMISMEMBER);
            Add("SRANDMEMBER", RespCommand.SRANDMEMBER);
            Add("SMOVE", RespCommand.SMOVE);
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
            Add("ZCARD", RespCommand.ZCARD);
            Add("ZSCORE", RespCommand.ZSCORE);
            Add("ZMSCORE", RespCommand.ZMSCORE);
            Add("ZRANK", RespCommand.ZRANK);
            Add("ZREVRANK", RespCommand.ZREVRANK);
            Add("ZCOUNT", RespCommand.ZCOUNT);
            Add("ZLEXCOUNT", RespCommand.ZLEXCOUNT);
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
            Add("ZINCRBY", RespCommand.ZINCRBY);
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
            Add("PUBLISH", RespCommand.PUBLISH);
            Add("SUBSCRIBE", RespCommand.SUBSCRIBE);
            Add("PSUBSCRIBE", RespCommand.PSUBSCRIBE);
            Add("UNSUBSCRIBE", RespCommand.UNSUBSCRIBE);
            Add("PUNSUBSCRIBE", RespCommand.PUNSUBSCRIBE);
            Add("SPUBLISH", RespCommand.SPUBLISH);
            Add("SSUBSCRIBE", RespCommand.SSUBSCRIBE);

            // Custom object scan
            Add("CUSTOMOBJECTSCAN", RespCommand.COSCAN);

            // ===== Control / admin commands =====
            Add("PING", RespCommand.PING);
            Add("ECHO", RespCommand.ECHO);
            Add("QUIT", RespCommand.QUIT);
            Add("AUTH", RespCommand.AUTH);
            Add("HELLO", RespCommand.HELLO);
            Add("INFO", RespCommand.INFO);
            Add("TIME", RespCommand.TIME);
            Add("ROLE", RespCommand.ROLE);
            Add("SAVE", RespCommand.SAVE);
            Add("LASTSAVE", RespCommand.LASTSAVE);
            Add("BGSAVE", RespCommand.BGSAVE);
            Add("COMMITAOF", RespCommand.COMMITAOF);
            Add("FLUSHALL", RespCommand.FLUSHALL);
            Add("FLUSHDB", RespCommand.FLUSHDB);
            Add("FORCEGC", RespCommand.FORCEGC);
            Add("PURGEBP", RespCommand.PURGEBP);
            Add("FAILOVER", RespCommand.FAILOVER);
            Add("MONITOR", RespCommand.MONITOR);
            Add("REGISTERCS", RespCommand.REGISTERCS);
            Add("ASYNC", RespCommand.ASYNC);
            Add("DEBUG", RespCommand.DEBUG);
            Add("EXPDELSCAN", RespCommand.EXPDELSCAN);
            Add("WATCH", RespCommand.WATCH);
            Add("WATCHMS", RespCommand.WATCHMS);
            Add("WATCHOS", RespCommand.WATCHOS);
            Add("MULTI", RespCommand.MULTI);
            Add("EXEC", RespCommand.EXEC);
            Add("DISCARD", RespCommand.DISCARD);
            Add("UNWATCH", RespCommand.UNWATCH);
            Add("RUNTXP", RespCommand.RUNTXP);
            Add("ASKING", RespCommand.ASKING);
            Add("READONLY", RespCommand.READONLY);
            Add("READWRITE", RespCommand.READWRITE);
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
        }

        #endregion

        #region Subcommand Definitions

        private static readonly (string Name, RespCommand Command)[] ClusterSubcommands =
        [
            ("ADDSLOTS", RespCommand.CLUSTER_ADDSLOTS),
            ("ADDSLOTSRANGE", RespCommand.CLUSTER_ADDSLOTSRANGE),
            ("AOFSYNC", RespCommand.CLUSTER_AOFSYNC),
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
            ("MTASKS", RespCommand.CLUSTER_MTASKS),
            ("MYID", RespCommand.CLUSTER_MYID),
            ("MYPARENTID", RespCommand.CLUSTER_MYPARENTID),
            ("NODES", RespCommand.CLUSTER_NODES),
            ("PUBLISH", RespCommand.CLUSTER_PUBLISH),
            ("SPUBLISH", RespCommand.CLUSTER_SPUBLISH),
            ("REPLICAS", RespCommand.CLUSTER_REPLICAS),
            ("REPLICATE", RespCommand.CLUSTER_REPLICATE),
            ("RESET", RespCommand.CLUSTER_RESET),
            ("SEND_CKPT_FILE_SEGMENT", RespCommand.CLUSTER_SEND_CKPT_FILE_SEGMENT),
            ("SEND_CKPT_METADATA", RespCommand.CLUSTER_SEND_CKPT_METADATA),
            ("SET-CONFIG-EPOCH", RespCommand.CLUSTER_SETCONFIGEPOCH),
            ("SETSLOT", RespCommand.CLUSTER_SETSLOT),
            ("SETSLOTSRANGE", RespCommand.CLUSTER_SETSLOTSRANGE),
            ("SHARDS", RespCommand.CLUSTER_SHARDS),
            ("SLOTS", RespCommand.CLUSTER_SLOTS),
            ("SLOTSTATE", RespCommand.CLUSTER_SLOTSTATE),
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