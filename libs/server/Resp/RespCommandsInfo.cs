// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace Garnet.server
{
    /// <summary>
    /// Container for command information
    /// </summary>
    class RespCommandsInfo
    {
        public readonly string nameStr;
        public readonly int arity;
        public readonly byte[] name;
        public readonly byte arrayCommand;
        public readonly RespCommand command;
        public readonly HashSet<RespCommandOption> options;

        public RespCommandsInfo(string name, RespCommand command, int arity, HashSet<RespCommandOption> options)
        {
            nameStr = name.ToUpper();
            this.name = System.Text.Encoding.ASCII.GetBytes(nameStr);
            this.command = command;
            this.arity = arity;
            this.options = options;
            this.arrayCommand = 255;
        }
        public RespCommandsInfo(string name, RespCommand command, int arity, HashSet<RespCommandOption> options, byte arrayCommand) : this(name, command, arity, options)
        {
            this.arrayCommand = arrayCommand;
        }

        /// <summary>
        /// Check wether the option is for this command or not and returns RespCommandsOptionInfo
        /// </summary>
        public bool MatchOptions(ReadOnlySpan<byte> command, out RespCommandsOptionInfo optionOutput)
        {
            for (int i = 0; i < RespCommandsOptionInfo.optionMap.Length; i++)
            {
                optionOutput = RespCommandsOptionInfo.optionMap[i];
                if (command.SequenceEqual(new ReadOnlySpan<byte>(optionOutput.name)) && this.options.Contains(optionOutput.option))
                    return true;
            }
            optionOutput = null;
            return false;
        }

        public static RespCommandsInfo findCommand(RespCommand cmd, byte subCmd = 0)
        {

            RespCommandsInfo result = cmd switch
            {
                RespCommand.SortedSet => sortedSetCommandsInfoMap.GetValueOrDefault(subCmd),
                RespCommand.List => listCommandsInfoMap.GetValueOrDefault(subCmd),
                RespCommand.Hash => hashCommandsInfoMap.GetValueOrDefault(subCmd),
                RespCommand.Set => setCommandsInfoMap.GetValueOrDefault(subCmd),
                RespCommand.All => customCommandsInfoMap.GetValueOrDefault(cmd),
                _ => basicCommandsInfoMap.GetValueOrDefault(cmd)
            };
            return result;
        }

        private static readonly Dictionary<RespCommand, RespCommandsInfo> basicCommandsInfoMap = new Dictionary<RespCommand, RespCommandsInfo>
        {
            {RespCommand.GET,       new RespCommandsInfo("GET", RespCommand.GET,             2, null)},
            {RespCommand.SET,       new RespCommandsInfo("SET", RespCommand.SET,            -3, new HashSet<RespCommandOption>{
                RespCommandOption.EX,
                RespCommandOption.NX,
                RespCommandOption.XX,
                RespCommandOption.GET,
                RespCommandOption.PX,
                RespCommandOption.EXAT,
                RespCommandOption.PXAT,
            })},
            {RespCommand.GETRANGE,  new RespCommandsInfo("GET", RespCommand.GETRANGE,        4, null)},
            {RespCommand.SETRANGE,  new RespCommandsInfo("SETRANGE", RespCommand.SETRANGE,        4, null)},
            // PUBLISH
            {RespCommand.PFADD,     new RespCommandsInfo("PFADD", RespCommand.PFADD,        -3, null)},
            {RespCommand.PFCOUNT,   new RespCommandsInfo("PFCOUNT", RespCommand.PFCOUNT,    -2, null)},
            {RespCommand.PFMERGE,   new RespCommandsInfo("PFMERGE", RespCommand.PFMERGE,    -3, null)},

            {RespCommand.SETEX,     new RespCommandsInfo("SETEX", RespCommand.SETEX,        -4, null)},
            {RespCommand.PSETEX,    new RespCommandsInfo("PSETEX", RespCommand.PSETEX,       4, null)},
            {RespCommand.SETEXNX,   new RespCommandsInfo("SETEXNX", RespCommand.SETEXNX,    -4, null)},
            {RespCommand.SETEXXX,   new RespCommandsInfo("SETEXXX", RespCommand.SETEXXX,    -4, null)},
            {RespCommand.DEL,       new RespCommandsInfo("DEL", RespCommand.DEL,            -2, null)},
            {RespCommand.EXISTS,    new RespCommandsInfo("EXISTS", RespCommand.EXISTS,       2, null)},
            {RespCommand.RENAME,    new RespCommandsInfo("RENAME", RespCommand.RENAME,       3, null)},
            {RespCommand.INCR,      new RespCommandsInfo("INCR", RespCommand.INCR,           2, null)},
            {RespCommand.INCRBY,    new RespCommandsInfo("INCRBY", RespCommand.INCRBY,       3, null)},
            {RespCommand.DECR,      new RespCommandsInfo("DECR", RespCommand.DECR,           2, null)},
            {RespCommand.DECRBY,    new RespCommandsInfo("DECRBY", RespCommand.DECRBY,       3, null)},
            {RespCommand.EXPIRE,    new RespCommandsInfo("EXPIRE", RespCommand.EXPIRE,      -3, new HashSet<RespCommandOption>{
                RespCommandOption.NX,
                RespCommandOption.XX,
                RespCommandOption.GT,
                RespCommandOption.LT,
            })},
            {RespCommand.PEXPIRE,    new RespCommandsInfo("PEXPIRE", RespCommand.PEXPIRE,      -3, new HashSet<RespCommandOption>{
                RespCommandOption.NX,
                RespCommandOption.XX,
                RespCommandOption.GT,
                RespCommandOption.LT,
            })},
            {RespCommand.PERSIST,   new RespCommandsInfo("PERSIST", RespCommand.PERSIST,     2, null)},
            {RespCommand.TTL,       new RespCommandsInfo("TTL", RespCommand.TTL,     2, null)},
            {RespCommand.PTTL,       new RespCommandsInfo("PTTL", RespCommand.PTTL,     2, null)},
            {RespCommand.SETBIT,    new RespCommandsInfo("SETBIT", RespCommand.SETBIT,       4, null)},
            {RespCommand.GETBIT,    new RespCommandsInfo("GETBIT", RespCommand.GETBIT,       3, null)},
            {RespCommand.BITCOUNT,  new RespCommandsInfo("BITCOUNT", RespCommand.BITCOUNT,  -2, null)},
            {RespCommand.BITPOS,    new RespCommandsInfo("BITPOS", RespCommand.BITPOS,      -3, null)},
            {RespCommand.BITFIELD,  new RespCommandsInfo("BITFIELD", RespCommand.BITFIELD,  -2, null)},

            {RespCommand.MSET,      new RespCommandsInfo("MSET", RespCommand.MSET,          -3, null)},
            {RespCommand.MSETNX,      new RespCommandsInfo("MSETNX", RespCommand.MSETNX,          -3, null)},
            {RespCommand.MGET,      new RespCommandsInfo("MGET", RespCommand.MGET,          -3, null)},
            {RespCommand.UNLINK,    new RespCommandsInfo("UNLINK", RespCommand.UNLINK,      -2, null)},

            {RespCommand.MULTI,     new RespCommandsInfo("MULTI", RespCommand.MULTI,         1,  null)},
            {RespCommand.EXEC,      new RespCommandsInfo("EXEC", RespCommand.EXEC,           1,  null)},
            {RespCommand.WATCH,     new RespCommandsInfo("WATCH", RespCommand.WATCH,        -2, null)},
            {RespCommand.UNWATCH,   new RespCommandsInfo("WATCH", RespCommand.UNWATCH,       1, null)},
            {RespCommand.DISCARD,   new RespCommandsInfo("DISCARD", RespCommand.DISCARD,     1,  null)},
            {RespCommand.GETDEL,    new RespCommandsInfo("GETDEL", RespCommand.GETDEL,       2, null)},
            {RespCommand.APPEND,    new RespCommandsInfo("APPEND", RespCommand.APPEND,       3,  null)},

            //Admin Commands
            {RespCommand.ECHO, new RespCommandsInfo("ECHO", RespCommand.ECHO, 2, null)},
            {RespCommand.REPLICAOF, new RespCommandsInfo("REPLICAOF", RespCommand.REPLICAOF, 3, null)},
            {RespCommand.SECONDARYOF, new RespCommandsInfo("SLAVEOF", RespCommand.SECONDARYOF, 3, null)},
            {RespCommand.CONFIG, new RespCommandsInfo("CONFIG", RespCommand.CONFIG, 2, null)},
            {RespCommand.CLIENT, new RespCommandsInfo("CLIENT", RespCommand.CLIENT, 4, null)},
            {RespCommand.REGISTERCS, new RespCommandsInfo("REGISTERCS", RespCommand.REGISTERCS, -5, null)},
        };

        private static readonly Dictionary<byte, RespCommandsInfo> sortedSetCommandsInfoMap = new Dictionary<byte, RespCommandsInfo>
        {
            {(byte)SortedSetOperation.ZADD,             new RespCommandsInfo("ZADD", RespCommand.SortedSet,             -4,null, (byte)SortedSetOperation.ZADD)},
            {(byte)SortedSetOperation.ZREM,             new RespCommandsInfo("ZREM", RespCommand.SortedSet,             -3,null, (byte)SortedSetOperation.ZREM)},
            {(byte)SortedSetOperation.ZCARD,            new RespCommandsInfo("ZCARD", RespCommand.SortedSet,             2,null, (byte)SortedSetOperation.ZCARD)},
            {(byte)SortedSetOperation.ZPOPMAX,          new RespCommandsInfo("ZPOPMAX", RespCommand.SortedSet,          -2,null, (byte)SortedSetOperation.ZPOPMAX)},
            {(byte)SortedSetOperation.ZSCORE,           new RespCommandsInfo("ZSCORE", RespCommand.SortedSet,            3,null, (byte)SortedSetOperation.ZSCORE)},
            {(byte)SortedSetOperation.ZCOUNT,           new RespCommandsInfo("ZCOUNT", RespCommand.SortedSet,            4,null, (byte)SortedSetOperation.ZCOUNT)},
            {(byte)SortedSetOperation.ZINCRBY,          new RespCommandsInfo("ZINCRBY", RespCommand.SortedSet,           4,null, (byte)SortedSetOperation.ZINCRBY)},
            {(byte)SortedSetOperation.ZRANK,            new RespCommandsInfo("ZRANK", RespCommand.SortedSet,             3,null, (byte)SortedSetOperation.ZRANK)},
            {(byte)SortedSetOperation.ZRANGE,           new RespCommandsInfo("ZRANGE", RespCommand.SortedSet,           -4,null, (byte)SortedSetOperation.ZRANGE)},
            {(byte)SortedSetOperation.ZRANGEBYSCORE,    new RespCommandsInfo("ZRANGEBYSCORE", RespCommand.SortedSet,    -4,null, (byte)SortedSetOperation.ZRANGEBYSCORE)},
            {(byte)SortedSetOperation.ZREVRANK,         new RespCommandsInfo("ZREVRANK", RespCommand.SortedSet,          3,null, (byte)SortedSetOperation.ZREVRANK)},
            {(byte)SortedSetOperation.ZREMRANGEBYLEX,   new RespCommandsInfo("ZREMRANGEBYLEX", RespCommand.SortedSet,    4,null, (byte)SortedSetOperation.ZREMRANGEBYLEX)},
            {(byte)SortedSetOperation.ZREMRANGEBYRANK,  new RespCommandsInfo("ZREMRANGEBYRANK", RespCommand.SortedSet,   4,null, (byte)SortedSetOperation.ZREMRANGEBYRANK)},
            {(byte)SortedSetOperation.ZREMRANGEBYSCORE, new RespCommandsInfo("ZREMRANGEBYSCORE", RespCommand.SortedSet,  4,null, (byte)SortedSetOperation.ZREMRANGEBYSCORE)},
            {(byte)SortedSetOperation.ZLEXCOUNT,        new RespCommandsInfo("ZLEXCOUNT", RespCommand.SortedSet,         4,null, (byte)SortedSetOperation.ZLEXCOUNT)},
            {(byte)SortedSetOperation.ZPOPMIN,          new RespCommandsInfo("ZPOPMIN", RespCommand.SortedSet,          -2,null, (byte)SortedSetOperation.ZPOPMIN)},
            {(byte)SortedSetOperation.ZRANDMEMBER,      new RespCommandsInfo("ZRANDMEMBER", RespCommand.SortedSet,      -2,null, (byte)SortedSetOperation.ZRANDMEMBER)},
            {(byte)SortedSetOperation.GEOADD,           new RespCommandsInfo("GEOADD", RespCommand.SortedSet,           -5,null, (byte)SortedSetOperation.GEOADD)},
            {(byte)SortedSetOperation.GEOHASH,          new RespCommandsInfo("GEOHASH", RespCommand.SortedSet,          -2,null, (byte)SortedSetOperation.GEOHASH)},
            {(byte)SortedSetOperation.GEODIST,          new RespCommandsInfo("GEODIST", RespCommand.SortedSet,          -4,null, (byte)SortedSetOperation.GEODIST)},
            {(byte)SortedSetOperation.GEOPOS,           new RespCommandsInfo("GEOPOS", RespCommand.SortedSet,           -2,null, (byte)SortedSetOperation.GEOPOS)},
            {(byte)SortedSetOperation.GEOSEARCH,        new RespCommandsInfo("GEOSEARCH", RespCommand.SortedSet,        -7,null, (byte)SortedSetOperation.GEOSEARCH)},
            {(byte)SortedSetOperation.ZREVRANGE,        new RespCommandsInfo("ZREVRANGE", RespCommand.SortedSet,        -4,null, (byte)SortedSetOperation.ZREVRANGE)},
            {(byte)SortedSetOperation.ZSCAN,            new RespCommandsInfo("ZSCAN", RespCommand.SortedSet,            -3,null, (byte)SortedSetOperation.ZSCAN)},
        };

        private static readonly Dictionary<byte, RespCommandsInfo> listCommandsInfoMap = new Dictionary<byte, RespCommandsInfo>
        {
            {(byte)ListOperation.LPUSH,     new RespCommandsInfo("LPUSH",   RespCommand.List,   -3, null, (byte)ListOperation.LPUSH)},
            {(byte)ListOperation.LPOP,      new RespCommandsInfo("LPOP",    RespCommand.List,   -2, null, (byte)ListOperation.LPOP)},
            {(byte)ListOperation.RPUSH,     new RespCommandsInfo("RPUSH",   RespCommand.List,   -3, null, (byte)ListOperation.RPUSH)},
            {(byte)ListOperation.RPOP,      new RespCommandsInfo("RPOP",    RespCommand.List,   -2, null, (byte)ListOperation.RPOP)},
            {(byte)ListOperation.LLEN,      new RespCommandsInfo("LLEN",    RespCommand.List,    2, null, (byte)ListOperation.LLEN)},
            {(byte)ListOperation.LTRIM,     new RespCommandsInfo("LTRIM",   RespCommand.List,    4, null, (byte)ListOperation.LTRIM)},
            {(byte)ListOperation.LRANGE,    new RespCommandsInfo("LRANGE",  RespCommand.List,    4, null, (byte)ListOperation.LRANGE)},
            {(byte)ListOperation.LINDEX,    new RespCommandsInfo("LINDEX",  RespCommand.List,    3, null, (byte)ListOperation.LINDEX)},
            {(byte)ListOperation.LINSERT,   new RespCommandsInfo("LINSERT", RespCommand.List,    5, null, (byte)ListOperation.LINSERT)},
            {(byte)ListOperation.LREM,      new RespCommandsInfo("LREM",    RespCommand.List,    4, null, (byte)ListOperation.LREM) },
        };

        private static readonly Dictionary<byte, RespCommandsInfo> hashCommandsInfoMap = new Dictionary<byte, RespCommandsInfo>
        {
            {(byte)HashOperation.HSET,          new RespCommandsInfo("HSET",            RespCommand.Hash,   -4,  null,   (byte)HashOperation.HSET) },
            {(byte)HashOperation.HMSET,         new RespCommandsInfo("HMSET",           RespCommand.Hash,   -4,  null,   (byte)HashOperation.HMSET)},
            {(byte)HashOperation.HGET,          new RespCommandsInfo("HGET",            RespCommand.Hash,    3,  null,   (byte)HashOperation.HGET)},
            {(byte)HashOperation.HMGET,         new RespCommandsInfo("HMGET",           RespCommand.Hash,   -3,  null,   (byte)HashOperation.HMGET)},
            {(byte)HashOperation.HGETALL,       new RespCommandsInfo("HGETALL",         RespCommand.Hash,    2,  null,   (byte)HashOperation.HGETALL)},
            {(byte)HashOperation.HDEL,          new RespCommandsInfo("HDEL",            RespCommand.Hash,   -3,  null,   (byte)HashOperation.HDEL)},
            {(byte)HashOperation.HLEN,          new RespCommandsInfo("HLEN",            RespCommand.Hash,    2,  null,   (byte)HashOperation.HLEN)},
            {(byte)HashOperation.HEXISTS,       new RespCommandsInfo("HEXISTS",         RespCommand.Hash,    3,  null,   (byte)HashOperation.HEXISTS)},
            {(byte)HashOperation.HKEYS,         new RespCommandsInfo("HKEYS",           RespCommand.Hash,    2,  null,   (byte)HashOperation.HKEYS)},
            {(byte)HashOperation.HVALS,         new RespCommandsInfo("HVALS",           RespCommand.Hash,    2,  null,   (byte)HashOperation.HVALS)},
            {(byte)HashOperation.HINCRBY,       new RespCommandsInfo("HINCRBY",         RespCommand.Hash,    4,  null,   (byte)HashOperation.HINCRBY)},
            {(byte)HashOperation.HINCRBYFLOAT,  new RespCommandsInfo("HINCRBYFLOAT",    RespCommand.Hash,    4,  null,   (byte)HashOperation.HINCRBYFLOAT)},
            {(byte)HashOperation.HSETNX,        new RespCommandsInfo("HSETNX",          RespCommand.Hash,    4,  null,   (byte)HashOperation.HSETNX)},
            {(byte)HashOperation.HRANDFIELD,    new RespCommandsInfo("HRANDFIELD",      RespCommand.Hash,   -2,  null,   (byte)HashOperation.HRANDFIELD)},
            {(byte)HashOperation.HSCAN,         new RespCommandsInfo("HSCAN",           RespCommand.Hash,   -3,  null,   (byte)HashOperation.HSCAN)},
        };

        private static readonly Dictionary<byte, RespCommandsInfo> setCommandsInfoMap = new Dictionary<byte, RespCommandsInfo>
        {
            {(byte)SetOperation.SADD,       new RespCommandsInfo("SADD",     RespCommand.Set,   -3, null, (byte)SetOperation.SADD)},
            {(byte)SetOperation.SMEMBERS,   new RespCommandsInfo("SMEMBERS", RespCommand.Set,    2, null, (byte)SetOperation.SMEMBERS)},
            {(byte)SetOperation.SREM,       new RespCommandsInfo("SREM",     RespCommand.Set,   -3, null, (byte)SetOperation.SREM)},
            {(byte)SetOperation.SCARD,      new RespCommandsInfo("SCARD",    RespCommand.Set,    2, null, (byte)SetOperation.SCARD)},
            {(byte)SetOperation.SPOP,       new RespCommandsInfo("SPOP",     RespCommand.Set,   -2, null, (byte)SetOperation.SPOP) },
            {(byte)SetOperation.SSCAN,      new RespCommandsInfo("SSCAN",    RespCommand.Set,   -3, null, (byte)SetOperation.SSCAN) },
        };

        private static readonly Dictionary<RespCommand, RespCommandsInfo> customCommandsInfoMap = new Dictionary<RespCommand, RespCommandsInfo>
        {
            {RespCommand.COSCAN,    new RespCommandsInfo("COSCAN",   RespCommand.All,   -3, null, (byte)RespCommand.COSCAN) },
        };
    }

    /// <summary>
    /// Container for commands option information
    /// </summary>
    class RespCommandsOptionInfo
    {
        public readonly string nameStr;
        public readonly int arity;
        public readonly byte[] name;
        public readonly RespCommandOption option;


        public RespCommandsOptionInfo(string name, RespCommandOption opt, int ariry)
        {
            nameStr = name.ToUpper();
            this.name = System.Text.Encoding.ASCII.GetBytes(nameStr);
            this.option = opt;
            this.arity = ariry;
        }

        public static readonly RespCommandsOptionInfo[] optionMap = new RespCommandsOptionInfo[]
        {
            new RespCommandsOptionInfo("EX" ,RespCommandOption.EX, 2),
            new RespCommandsOptionInfo("NX" ,RespCommandOption.NX, 1),
            new RespCommandsOptionInfo("XX" ,RespCommandOption.XX, 1),
            new RespCommandsOptionInfo("GET" ,RespCommandOption.GET, 1),
            new RespCommandsOptionInfo("PX" ,RespCommandOption.PX, 2),
            new RespCommandsOptionInfo("EXAT" ,RespCommandOption.EXAT, 2),
            new RespCommandsOptionInfo("PXAT" ,RespCommandOption.PXAT, 2),
            new RespCommandsOptionInfo("PERSIST" ,RespCommandOption.PERSIST, 1),
            new RespCommandsOptionInfo("GT" ,RespCommandOption.GT, 1),
            new RespCommandsOptionInfo("LT" ,RespCommandOption.LT, 1),
        };
    }
}