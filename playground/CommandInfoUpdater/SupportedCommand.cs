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
            new("ACL", RespCommand.ACL,
            [
                new("ACL|CAT", RespCommand.ACL_CAT),
                new("ACL|DELUSER", RespCommand.ACL_DELUSER),
                new("ACL|LIST", RespCommand.ACL_LIST),
                new("ACL|LOAD", RespCommand.ACL_LOAD),
                new("ACL|SAVE", RespCommand.ACL_SAVE),
                new("ACL|SETUSER", RespCommand.ACL_SETUSER),
                new("ACL|USERS", RespCommand.ACL_USERS),
                new("ACL|WHOAMI", RespCommand.ACL_WHOAMI),
            ]),
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
            new("CLIENT", RespCommand.CLIENT,
            [
                new("CLIENT|ID", RespCommand.CLIENT_ID),
                new("CLIENT|INFO", RespCommand.CLIENT_INFO),
                new("CLIENT|LIST", RespCommand.CLIENT_LIST),
                new("CLIENT|KILL", RespCommand.CLIENT_KILL),
            ]),
            new("CLUSTER", RespCommand.CLUSTER,
            [
                new("CLUSTER|ADDSLOTS", RespCommand.CLUSTER_ADDSLOTS),
                new("CLUSTER|ADDSLOTSRANGE", RespCommand.CLUSTER_ADDSLOTSRANGE),
                new("CLUSTER|AOFSYNC", RespCommand.CLUSTER_AOFSYNC),
                new("CLUSTER|APPENDLOG", RespCommand.CLUSTER_APPENDLOG),
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
            ]),
            new("COMMAND", RespCommand.COMMAND,
            [
                new("COMMAND|INFO", RespCommand.COMMAND_INFO),
                new("COMMAND|COUNT", RespCommand.COMMAND_COUNT),
                new("COMMAND|DOCS", RespCommand.COMMAND_DOCS),
            ]),
            new("COMMITAOF", RespCommand.COMMITAOF),
            new("CONFIG", RespCommand.CONFIG,
            [
                new("CONFIG|GET", RespCommand.CONFIG_GET),
                new("CONFIG|SET", RespCommand.CONFIG_SET),
                new("CONFIG|REWRITE", RespCommand.CONFIG_REWRITE),
            ]),
            new("COSCAN", RespCommand.COSCAN),
            new("CustomRawStringCmd", RespCommand.CustomRawStringCmd),
            new("CustomObjCmd", RespCommand.CustomObjCmd),
            new("CustomTxn", RespCommand.CustomTxn),
            new("CustomProcedure", RespCommand.CustomProcedure),
            new("DBSIZE", RespCommand.DBSIZE),
            new("DECR", RespCommand.DECR),
            new("DECRBY", RespCommand.DECRBY),
            new("DEL", RespCommand.DEL),
            new("DISCARD", RespCommand.DISCARD),
            new("ECHO", RespCommand.ECHO),
            new("EXEC", RespCommand.EXEC),
            new("EXISTS", RespCommand.EXISTS),
            new("EXPIRE", RespCommand.EXPIRE),
            new("EXPIREAT", RespCommand.EXPIREAT),
            new("EXPIRETIME", RespCommand.EXPIRETIME),
            new("FAILOVER", RespCommand.FAILOVER),
            new("FLUSHALL", RespCommand.FLUSHALL),
            new("FLUSHDB", RespCommand.FLUSHDB),
            new("FORCEGC", RespCommand.FORCEGC),
            new("GEOADD", RespCommand.GEOADD),
            new("GEODIST", RespCommand.GEODIST),
            new("GEOHASH", RespCommand.GEOHASH),
            new("GEOPOS", RespCommand.GEOPOS),
            new("GEOSEARCH", RespCommand.GEOSEARCH),
            new("GET", RespCommand.GET),
            new("GETEX", RespCommand.GETEX),
            new("GETBIT", RespCommand.GETBIT),
            new("GETDEL", RespCommand.GETDEL),
            new("GETRANGE", RespCommand.GETRANGE),
            new("GETSET", RespCommand.GETSET),
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
            new("INCRBYFLOAT", RespCommand.INCRBYFLOAT),
            new("INFO", RespCommand.INFO),
            new("KEYS", RespCommand.KEYS),
            new("LASTSAVE", RespCommand.LASTSAVE),
            new("LATENCY", RespCommand.LATENCY,
            [
                new("LATENCY|HELP", RespCommand.LATENCY_HELP),
                new("LATENCY|HISTOGRAM", RespCommand.LATENCY_HISTOGRAM),
                new("LATENCY|RESET", RespCommand.LATENCY_RESET),
            ]),
            new("LINDEX", RespCommand.LINDEX),
            new("LINSERT", RespCommand.LINSERT),
            new("LLEN", RespCommand.LLEN),
            new("LMOVE", RespCommand.LMOVE),
            new("LMPOP", RespCommand.LMPOP),
            new("LPOP", RespCommand.LPOP),
            new("LPOS", RespCommand.LPOS),
            new("LPUSH", RespCommand.LPUSH),
            new("LPUSHX", RespCommand.LPUSHX),
            new("LRANGE", RespCommand.LRANGE),
            new("LREM", RespCommand.LREM),
            new("LSET", RespCommand.LSET),
            new("LTRIM", RespCommand.LTRIM),
            new("MEMORY", RespCommand.MEMORY,
            [
                new("MEMORY|USAGE", RespCommand.MEMORY_USAGE),
            ]),
            new("MGET", RespCommand.MGET),
            new("MIGRATE", RespCommand.MIGRATE),
            new("PURGEBP", RespCommand.PURGEBP),
            new("MODULE", RespCommand.MODULE,
            [
                new("MODULE|LOADCS", RespCommand.MODULE_LOADCS),
            ]),
            new("MONITOR", RespCommand.MONITOR),
            new("MSET", RespCommand.MSET),
            new("MSETNX", RespCommand.MSETNX),
            new("MULTI", RespCommand.MULTI),
            new("PERSIST", RespCommand.PERSIST),
            new("PEXPIRE", RespCommand.PEXPIRE),
            new("PEXPIREAT", RespCommand.PEXPIREAT),
            new("PEXPIRETIME", RespCommand.PEXPIRETIME),
            new("PFADD", RespCommand.PFADD),
            new("PFCOUNT", RespCommand.PFCOUNT),
            new("PFMERGE", RespCommand.PFMERGE),
            new("PING", RespCommand.PING),
            new("PSETEX", RespCommand.PSETEX),
            new("PSUBSCRIBE", RespCommand.PSUBSCRIBE),
            new("PTTL", RespCommand.PTTL),
            new("PUBLISH", RespCommand.PUBLISH),
            new("PUBSUB", RespCommand.PUBSUB,
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
            new("RENAME", RespCommand.RENAME),
            new("RENAMENX", RespCommand.RENAMENX),
            new("REPLICAOF", RespCommand.REPLICAOF),
            new("RPOP", RespCommand.RPOP),
            new("RPOPLPUSH", RespCommand.RPOPLPUSH),
            new("RPUSH", RespCommand.RPUSH),
            new("RPUSHX", RespCommand.RPUSHX),
            new("RUNTXP", RespCommand.RUNTXP),
            new("SADD", RespCommand.SADD),
            new("SCARD", RespCommand.SCARD),
            new("SAVE", RespCommand.SAVE),
            new("SCAN", RespCommand.SCAN),
            new("SDIFF", RespCommand.SDIFF),
            new("SDIFFSTORE", RespCommand.SDIFFSTORE),
            new("SECONDARYOF", RespCommand.SECONDARYOF),
            new("SELECT", RespCommand.SELECT),
            new("SET", RespCommand.SET),
            new("SETBIT", RespCommand.SETBIT),
            new("SETEX", RespCommand.SETEX),
            new("SETNX", RespCommand.SETNX),
            new("SETRANGE", RespCommand.SETRANGE),
            new("SISMEMBER", RespCommand.SISMEMBER),
            new("SLAVEOF", RespCommand.SECONDARYOF),
            new("SMEMBERS", RespCommand.SMEMBERS),
            new("SMISMEMBER", RespCommand.SMISMEMBER),
            new("SMOVE", RespCommand.SMOVE),
            new("SPOP", RespCommand.SPOP),
            new("SRANDMEMBER", RespCommand.SRANDMEMBER),
            new("SREM", RespCommand.SREM),
            new("SSCAN", RespCommand.SSCAN),
            new("STRLEN", RespCommand.STRLEN),
            new("SUBSCRIBE", RespCommand.SUBSCRIBE),
            new("SUBSTR", RespCommand.SUBSTR),
            new("SUNION", RespCommand.SUNION),
            new("SUNIONSTORE", RespCommand.SUNIONSTORE),
            new("SINTER", RespCommand.SINTER),
            new("SINTERSTORE", RespCommand.SINTERSTORE),
            new("TIME", RespCommand.TIME),
            new("TTL", RespCommand.TTL),
            new("TYPE", RespCommand.TYPE),
            new("UNLINK", RespCommand.UNLINK),
            new("UNSUBSCRIBE", RespCommand.UNSUBSCRIBE),
            new("UNWATCH", RespCommand.UNWATCH),
            new("WATCH", RespCommand.WATCH),
            new("WATCHMS", RespCommand.WATCHMS),
            new("WATCHOS", RespCommand.WATCHOS),
            new("ZADD", RespCommand.ZADD),
            new("ZCARD", RespCommand.ZCARD),
            new("ZCOUNT", RespCommand.ZCOUNT),
            new("ZDIFF", RespCommand.ZDIFF),
            new("ZDIFFSTORE", RespCommand.ZDIFFSTORE),
            new("ZINCRBY", RespCommand.ZINCRBY),
            new("ZLEXCOUNT", RespCommand.ZLEXCOUNT),
            new("ZMSCORE", RespCommand.ZMSCORE),
            new("ZPOPMAX", RespCommand.ZPOPMAX),
            new("ZPOPMIN", RespCommand.ZPOPMIN),
            new("ZRANDMEMBER", RespCommand.ZRANDMEMBER),
            new("ZRANGE", RespCommand.ZRANGE),
            new("ZRANGEBYSCORE", RespCommand.ZRANGEBYSCORE),
            new("ZRANK", RespCommand.ZRANK),
            new("ZREM", RespCommand.ZREM),
            new("ZREMRANGEBYLEX", RespCommand.ZREMRANGEBYLEX),
            new("ZREMRANGEBYRANK", RespCommand.ZREMRANGEBYRANK),
            new("ZREMRANGEBYSCORE", RespCommand.ZREMRANGEBYSCORE),
            new("ZREVRANGE", RespCommand.ZREVRANGE),
            new("ZREVRANGEBYSCORE", RespCommand.ZREVRANGEBYSCORE),
            new("ZREVRANK", RespCommand.ZREVRANK),
            new("ZSCAN", RespCommand.ZSCAN),
            new("ZSCORE", RespCommand.ZSCORE),
            new("EVAL", RespCommand.EVAL),
            new("EVALSHA", RespCommand.EVALSHA),
            new("SCRIPT", RespCommand.SCRIPT),
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
        public SupportedCommand(string command, RespCommand respCommand = RespCommand.NONE, IEnumerable<SupportedCommand> subCommands = null) : this()
        {
            Command = command;
            SubCommands = subCommands?.ToDictionary(sc => sc.Command, sc => sc);
            RespCommand = respCommand;
        }
    }
}