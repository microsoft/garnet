// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;

namespace CommandInfoUpdater
{
    /// <summary>
    /// Defines a command supported by Garnet
    /// </summary>
    public class SupportedCommand
    {
        private static readonly SupportedCommand[] AllSupportedCommands = {
            new("ACL", RespCommand.ACL, new[]
            {
                "ACL|CAT",
                "ACL|DELUSER",
                "ACL|LIST",
                "ACL|LOAD",
                "ACL|SAVE",
                "ACL|SETUSER",
                "ACL|USERS",
                "ACL|WHOAMI",
            }),
            new("APPEND", RespCommand.APPEND),
            new("ASKING", RespCommand.ASKING),
            new("ASYNC", RespCommand.ASYNC),
            new("AUTH", RespCommand.AUTH),
            new("BGSAVE", RespCommand.BGSAVE),
            new("BITCOUNT", RespCommand.BITCOUNT),
            new("BITFIELD", RespCommand.BITFIELD),
            new("BITFIELD_RO", RespCommand.BITFIELD_RO),
            new("BITOP", RespCommand.BITOP),
            new("BITPOS", RespCommand.BITPOS),
            new("BLPOP", RespCommand.BLPOP),
            new("BRPOP", RespCommand.BRPOP),
            new("BLMOVE", RespCommand.BLMOVE),
            new("CLIENT", RespCommand.CLIENT),
            new("CLUSTER", RespCommand.CLUSTER, new []
            {
                "CLUSTER|ADDSLOTS",
                "CLUSTER|ADDSLOTSRANGE",
                "CLUSTER|AOFSYNC",
                "CLUSTER|APPENDLOG",
                "CLUSTER|BANLIST",
                "CLUSTER|BEGIN_REPLICA_RECOVER",
                "CLUSTER|BUMPEPOCH",
                "CLUSTER|COUNTKEYSINSLOT",
                "CLUSTER|DELKEYSINSLOT",
                "CLUSTER|DELKEYSINSLOTRANGE",
                "CLUSTER|DELSLOTS",
                "CLUSTER|DELSLOTSRANGE",
                "CLUSTER|ENDPOINT",
                "CLUSTER|FAILOVER",
                "CLUSTER|FAILREPLICATIONOFFSET",
                "CLUSTER|FAILSTOPWRITES",
                "CLUSTER|FORGET",
                "CLUSTER|GETKEYSINSLOT",
                "CLUSTER|GOSSIP",
                "CLUSTER|HELP",
                "CLUSTER|INFO",
                "CLUSTER|INITIATE_REPLICA_SYNC",
                "CLUSTER|KEYSLOT",
                "CLUSTER|MEET",
                "CLUSTER|MIGRATE",
                "CLUSTER|MTASKS",
                "CLUSTER|MYID",
                "CLUSTER|MYPARENTID",
                "CLUSTER|NODES",
                "CLUSTER|REPLICAS",
                "CLUSTER|REPLICATE",
                "CLUSTER|RESET",
                "CLUSTER|SEND_CKPT_FILE_SEGMENT",
                "CLUSTER|SEND_CKPT_METADATA",
                "CLUSTER|SET-CONFIG-EPOCH",
                "CLUSTER|SETSLOT",
                "CLUSTER|SETSLOTSRANGE",
                "CLUSTER|SHARDS",
                "CLUSTER|SLOTS",
                "CLUSTER|SLOTSTATE",

            }),
            new("COMMAND", RespCommand.COMMAND, new []
            {
                "COMMAND|INFO",
                "COMMAND|COUNT",
            }),
            new("COMMITAOF", RespCommand.COMMITAOF),
            new("CONFIG", RespCommand.CONFIG, new []
            {
                "CONFIG|GET",
                "CONFIG|SET",
                "CONFIG|REWRITE"
            }),
            new("COSCAN", RespCommand.COSCAN),
            new("CustomCmd", RespCommand.CustomCmd),
            new("CustomObjCmd", RespCommand.CustomCmd),
            new("CustomTxn", RespCommand.CustomTxn),
            new("DBSIZE", RespCommand.DBSIZE),
            new("DECR", RespCommand.DECR),
            new("DECRBY", RespCommand.DECRBY),
            new("DEL", RespCommand.DEL),
            new("DISCARD", RespCommand.DISCARD),
            new("ECHO", RespCommand.ECHO),
            new("EXEC", RespCommand.EXEC),
            new("EXISTS", RespCommand.EXISTS),
            new("EXPIRE", RespCommand.EXPIRE),
            new("FAILOVER", RespCommand.FAILOVER),
            new("FLUSHDB", RespCommand.FLUSHDB),
            new("FORCEGC", RespCommand.FORCEGC),
            new("GEOADD", RespCommand.GEOADD),
            new("GEODIST", RespCommand.GEODIST),
            new("GEOHASH", RespCommand.GEOHASH),
            new("GEOPOS", RespCommand.GEOPOS),
            new("GEOSEARCH", RespCommand.GEOSEARCH),
            new("GET", RespCommand.GET),
            new("GETBIT", RespCommand.GETBIT),
            new("GETDEL", RespCommand.GETDEL),
            new("GETRANGE", RespCommand.GETRANGE),
            new("HDEL", RespCommand.HDEL),
            new("HELLO", RespCommand.HELLO),
            new("HEXISTS", RespCommand.HEXISTS),
            new("HGET", RespCommand.HGET),
            new("HGETALL", RespCommand.HGETALL),
            new("HINCRBY", RespCommand.HINCRBY),
            new("HINCRBYFLOAT", RespCommand.HINCRBYFLOAT),
            new("HKEYS", RespCommand.HKEYS),
            new("HLEN", RespCommand.HLEN),
            new("HMGET", RespCommand.HMGET),
            new("HMSET", RespCommand.HMSET),
            new("HRANDFIELD", RespCommand.HRANDFIELD),
            new("HSCAN", RespCommand.HSCAN),
            new("HSET", RespCommand.HSET),
            new("HSETNX", RespCommand.HSETNX),
            new("HSTRLEN", RespCommand.HSTRLEN),
            new("HVALS", RespCommand.HVALS),
            new("INCR", RespCommand.INCR),
            new("INCRBY", RespCommand.INCRBY),
            new("INFO", RespCommand.INFO),
            new("KEYS", RespCommand.KEYS),
            new("LASTSAVE", RespCommand.LASTSAVE),
            new("LATENCY", RespCommand.LATENCY, new []
            {
                "LATENCY|HELP",
                "LATENCY|HISTOGRAM",
                "LATENCY|RESET"
            }),
            new("LINDEX", RespCommand.LINDEX),
            new("LINSERT", RespCommand.LINSERT),
            new("LLEN", RespCommand.LLEN),
            new("LMOVE", RespCommand.LMOVE),
            new("LMPOP", RespCommand.LMPOP),
            new("LPOP", RespCommand.LPOP),
            new("LPUSH", RespCommand.LPUSH),
            new("LPUSHX", RespCommand.LPUSHX),
            new("LRANGE", RespCommand.LRANGE),
            new("LREM", RespCommand.LREM),
            new("LSET", RespCommand.LSET),
            new("LTRIM", RespCommand.LTRIM),
            new("MEMORY", RespCommand.MEMORY, new []
            {
                "MEMORY|USAGE"
            }),
            new("MGET", RespCommand.MGET),
            new("MIGRATE", RespCommand.MIGRATE),
            new("MODULE", RespCommand.MODULE,
            [
                 "MODULE|LOADCS",
            ]),
            new ("MONITOR", RespCommand.MONITOR),
            new ("MSET", RespCommand.MSET),
            new ("MSETNX", RespCommand.MSETNX),
            new ("MULTI", RespCommand.MULTI),
            new ("PERSIST", RespCommand.PERSIST),
            new ("PEXPIRE", RespCommand.PEXPIRE),
            new ("PFADD", RespCommand.PFADD),
            new ("PFCOUNT", RespCommand.PFCOUNT),
            new ("PFMERGE", RespCommand.PFMERGE),
            new ("PING", RespCommand.PING),
            new ("PSETEX", RespCommand.PSETEX),
            new ("PSUBSCRIBE", RespCommand.PSUBSCRIBE),
            new ("PTTL", RespCommand.PTTL),
            new ("PUBLISH", RespCommand.PUBLISH),
            new ("PUNSUBSCRIBE", RespCommand.PUNSUBSCRIBE),
            new ("REGISTERCS", RespCommand.REGISTERCS),
            new ("QUIT", RespCommand.QUIT),
            new ("READONLY", RespCommand.READONLY),
            new ("READWRITE", RespCommand.READWRITE),
            new ("RENAME", RespCommand.RENAME),
            new ("REPLICAOF", RespCommand.REPLICAOF),
            new ("RPOP", RespCommand.RPOP),
            new ("RPOPLPUSH", RespCommand.RPOPLPUSH),
            new ("RPUSH", RespCommand.RPUSH),
            new ("RPUSHX", RespCommand.RPUSHX),
            new ("RUNTXP", RespCommand.RUNTXP),
            new ("SADD", RespCommand.SADD),
            new ("SCARD", RespCommand.SCARD),
            new ("SAVE", RespCommand.SAVE),
            new ("SCAN", RespCommand.SCAN),
            new ("SDIFF", RespCommand.SDIFF),
            new ("SDIFFSTORE", RespCommand.SDIFFSTORE),
            new ("SECONDARYOF", RespCommand.SECONDARYOF),
            new ("SELECT", RespCommand.SELECT),
            new ("SET", RespCommand.SET),
            new ("SETBIT", RespCommand.SETBIT),
            new ("SETEX", RespCommand.SETEX),
            new ("SETRANGE", RespCommand.SETRANGE),
            new ("SISMEMBER", RespCommand.SISMEMBER),
            new ("SLAVEOF", RespCommand.SECONDARYOF),
            new ("SMEMBERS", RespCommand.SMEMBERS),
            new ("SMOVE", RespCommand.SMOVE),
            new ("SPOP", RespCommand.SPOP),
            new ("SRANDMEMBER", RespCommand.SRANDMEMBER),
            new ("SREM", RespCommand.SREM),
            new ("SSCAN", RespCommand.SSCAN),
            new ("STRLEN", RespCommand.STRLEN),
            new ("SUBSCRIBE", RespCommand.SUBSCRIBE),
            new ("SUNION", RespCommand.SUNION),
            new ("SUNIONSTORE", RespCommand.SUNIONSTORE),
            new ("SINTER", RespCommand.SINTER),
            new ("SINTERSTORE", RespCommand.SINTERSTORE),
            new ("TIME", RespCommand.TIME),
            new ("TTL", RespCommand.TTL),
            new ("TYPE", RespCommand.TYPE),
            new ("UNLINK", RespCommand.UNLINK),
            new ("UNSUBSCRIBE", RespCommand.UNSUBSCRIBE),
            new ("UNWATCH", RespCommand.UNWATCH),
            new ("WATCH", RespCommand.WATCH, new []
            {
                "WATCH|MS",
                "WATCH|OS",
            }),
            new ("ZADD", RespCommand.ZADD),
            new ("ZCARD", RespCommand.ZCARD),
            new ("ZCOUNT", RespCommand.ZCOUNT),
            new ("ZDIFF", RespCommand.ZDIFF),
            new ("ZINCRBY", RespCommand.ZINCRBY),
            new ("ZLEXCOUNT", RespCommand.ZLEXCOUNT),
            new ("ZMSCORE", RespCommand.ZMSCORE),
            new ("ZPOPMAX", RespCommand.ZPOPMAX),
            new ("ZPOPMIN", RespCommand.ZPOPMIN),
            new ("ZRANDMEMBER", RespCommand.ZRANDMEMBER),
            new ("ZRANGE", RespCommand.ZRANGE),
            new ("ZRANGEBYSCORE", RespCommand.ZRANGEBYSCORE),
            new ("ZRANK", RespCommand.ZRANK),
            new ("ZREM", RespCommand.ZREM),
            new ("ZREMRANGEBYLEX", RespCommand.ZREMRANGEBYLEX),
            new ("ZREMRANGEBYRANK", RespCommand.ZREMRANGEBYRANK),
            new ("ZREMRANGEBYSCORE", RespCommand.ZREMRANGEBYSCORE),
            new ("ZREVRANGE", RespCommand.ZREVRANGE),
            new ("ZREVRANGEBYSCORE", RespCommand.ZREVRANGEBYSCORE),
            new ("ZREVRANK", RespCommand.ZREVRANK),
            new ("ZSCAN", RespCommand.ZSCAN),
            new ("ZSCORE", RespCommand.ZSCORE),
        };

        private static readonly Lazy<IReadOnlyDictionary<string, SupportedCommand>> LazySupportedCommandsMap =
            new(() =>
            {
                return AllSupportedCommands.ToDictionary(sc => sc.Command, sc => sc);
            });

        /// <summary>
        /// Map between a supported command's name and its SupportedCommand object
        /// </summary>
        public static IReadOnlyDictionary<string, SupportedCommand> SupportedCommandsMap => LazySupportedCommandsMap.Value;

        /// <summary>
        /// Supported command's name
        /// </summary>
        public string Command { get; set; }

        /// <summary>
        /// Supported command's sub-commands' names
        /// </summary>
        public HashSet<string> SubCommands { get; set; }

        /// <summary>
        /// Garnet RespCommand
        /// </summary>
        public RespCommand RespCommand { get; set; }

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
        /// <param name="subCommands">List of supported sub-command names (optional)</param>
        public SupportedCommand(string command, RespCommand respCommand = RespCommand.NONE, IEnumerable<string> subCommands = null) : this()
        {
            Command = command;
            SubCommands = subCommands == null ? null : new HashSet<string>(subCommands);
            RespCommand = respCommand;
        }
    }
}