// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json.Serialization;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    public class RespCommandsInfo : IRespSerializable
    {
        [JsonIgnore]
        public RespCommand Command { get; private init; }

        [JsonIgnore]
        public byte? ArrayCommand { get; private init; }

        public string Name
        {
            get => name;
            init
            {
                name = value;

                if (RespCommandMapping.ContainsKey(name))
                {
                    var respCommand = RespCommandMapping[name];
                    this.Command = respCommand.Item1;
                    this.ArrayCommand = respCommand.Item2;
                }
            }
        }

        public int Arity { get; init; }

        public RespCommandFlags Flags
        {
            get => this.flags;
            init
            {
                this.flags = value;
                this.respFormatFlags = EnumUtils.GetEnumDescriptions(this.flags);
            }
        }

        public int FirstKey { get; init; }

        public int LastKey { get; init; }

        public int Step { get; init; }

        public RespAclCategories AclCategories
        {
            get => this.aclCategories;
            init
            {
                this.aclCategories = value;
                this.respFormatAclCategories = EnumUtils.GetEnumDescriptions(this.aclCategories);
            }
        }

        public string[]? Tips { get; init; }

        public RespCommandKeySpecification[] KeySpecifications { get; init; }

        public RespCommandsInfo[] SubCommands { get; init; }

        [JsonIgnore]
        public string RespFormat => respFormat ??= ToRespFormat();

        private string respFormat;

        private static bool isInitialized = false;
        private static IReadOnlyDictionary<string, RespCommandsInfo> allRespCommandsInfo = null;
        private static IReadOnlyDictionary<RespCommand, RespCommandsInfo> basicRespCommandsInfo = null;

        private static IReadOnlyDictionary<RespCommand, IReadOnlyDictionary<byte, RespCommandsInfo>>
            arrayRespCommandsInfo = null;

        private const string RespCommandsEmbeddedFileName = @"RespCommandsInfo.json";

        private readonly string name;
        private readonly RespCommandFlags flags;
        private readonly RespAclCategories aclCategories;

        private readonly string[] respFormatFlags;
        private readonly string[] respFormatAclCategories;

        private static bool TryInitializeRespCommandsInfo(ILogger logger)
        {
            var streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.EmbeddedResource, null,
                Assembly.GetExecutingAssembly());
            var commandsInfoProvider = RespCommandsInfoProviderFactory.GetRespCommandsInfoProvider();

            var importSucceeded = commandsInfoProvider.TryImportRespCommandsInfo(RespCommandsEmbeddedFileName,
                streamProvider, logger, out var tmpAllRespCommandsInfo);

            if (!importSucceeded) return false;

            var tmpBasicRespCommandsInfo = new Dictionary<RespCommand, RespCommandsInfo>();
            var tmpArrayRespCommandsInfo = new Dictionary<RespCommand, Dictionary<byte, RespCommandsInfo>>();
            foreach (var respCommandInfo in tmpAllRespCommandsInfo.Values)
            {
                if (respCommandInfo.Command == RespCommand.NONE) continue;

                if (respCommandInfo.ArrayCommand.HasValue)
                {
                    if (!tmpArrayRespCommandsInfo.ContainsKey(respCommandInfo.Command))
                        tmpArrayRespCommandsInfo.Add(respCommandInfo.Command, new Dictionary<byte, RespCommandsInfo>());
                    tmpArrayRespCommandsInfo[respCommandInfo.Command]
                        .Add(respCommandInfo.ArrayCommand.Value, respCommandInfo);
                }
                else
                {
                    tmpBasicRespCommandsInfo.Add(respCommandInfo.Command, respCommandInfo);
                }
            }

            allRespCommandsInfo = tmpAllRespCommandsInfo;
            basicRespCommandsInfo = new ReadOnlyDictionary<RespCommand, RespCommandsInfo>(tmpBasicRespCommandsInfo);
            arrayRespCommandsInfo = new ReadOnlyDictionary<RespCommand, IReadOnlyDictionary<byte, RespCommandsInfo>>(
                tmpArrayRespCommandsInfo
                    .ToDictionary(kvp => kvp.Key,
                        kvp =>
                            (IReadOnlyDictionary<byte, RespCommandsInfo>)new ReadOnlyDictionary<byte, RespCommandsInfo>(
                                kvp.Value)));

            return true;
        }

        internal static bool TryGetRespCommandsInfoCount(ILogger logger, out int count)
        {
            count = -1;
            if (!isInitialized && !TryInitializeRespCommandsInfo(logger)) return false;

            count = allRespCommandsInfo!.Count;
            return true;
        }

        internal static bool TryGetRespCommandsInfo(ILogger logger, out IEnumerable<RespCommandsInfo> respCommandsInfo)
        {
            respCommandsInfo = default;
            if (!isInitialized && !TryInitializeRespCommandsInfo(logger)) return false;

            respCommandsInfo = allRespCommandsInfo!.Values;
            return true;
        }

        internal static bool TryGetRespCommandInfo(string cmdName, ILogger logger,
            out RespCommandsInfo respCommandsInfo)
        {
            respCommandsInfo = default;
            if ((!isInitialized && !TryInitializeRespCommandsInfo(logger)) ||
                !allRespCommandsInfo.ContainsKey(cmdName)) return false;

            respCommandsInfo = allRespCommandsInfo[cmdName];
            return true;
        }

        internal static bool TryGetRespCommandInfo(RespCommand cmd, ILogger logger,
            out RespCommandsInfo respCommandsInfo, byte subCmd = 0, bool txnOnly = false)
        {
            respCommandsInfo = default;
            if ((!isInitialized && !TryInitializeRespCommandsInfo(logger))) return false;

            RespCommandsInfo tmpRespCommandInfo = default;
            if (arrayRespCommandsInfo.ContainsKey(cmd) && arrayRespCommandsInfo[cmd].ContainsKey(subCmd))
                tmpRespCommandInfo = arrayRespCommandsInfo[cmd][subCmd];
            else if (basicRespCommandsInfo.ContainsKey(cmd))
                tmpRespCommandInfo = basicRespCommandsInfo[cmd];

            if (tmpRespCommandInfo == default ||
                (txnOnly && tmpRespCommandInfo.Flags.HasFlag(RespCommandFlags.NoMulti))) return false;

            respCommandsInfo = tmpRespCommandInfo;
            return true;
        }

        public string ToRespFormat()
        {
            var sb = new StringBuilder();

            sb.Append("*10\r\n");
            // 1) Name
            sb.Append($"${this.Name.Length}\r\n{this.Name}\r\n");
            // 2) Arity
            sb.Append($":{this.Arity}\r\n");
            // 3) Flags
            sb.Append($"*{this.respFormatFlags.Length}\r\n");
            foreach (var flag in this.respFormatFlags)
                sb.Append($"+{flag}\r\n");
            // 4) First key
            sb.Append($":{this.FirstKey}\r\n");
            // 5) Last key
            sb.Append($":{this.LastKey}\r\n");
            // 6) Step
            sb.Append($":{this.Step}\r\n");
            // 7) ACL categories
            sb.Append($"*{this.respFormatAclCategories.Length}\r\n");
            foreach (var aclCat in this.respFormatAclCategories)
                sb.Append($"+@{aclCat}\r\n");
            // 8) Tips
            var tipCount = this.Tips?.Length ?? 0;
            sb.Append($"*{tipCount}\r\n");
            if (this.Tips != null && tipCount > 0)
            {
                foreach (var tip in this.Tips)
                    sb.Append($"${tip.Length}\r\n{tip}\r\n");
            }

            // 9) Key specifications
            var ksCount = this.KeySpecifications?.Length ?? 0;
            sb.Append($"*{ksCount}\r\n");
            if (this.KeySpecifications != null && ksCount > 0)
            {
                foreach (var ks in this.KeySpecifications)
                    sb.Append(ks.RespFormat);
            }

            // 10) SubCommands
            var subCommandCount = this.SubCommands?.Length ?? 0;
            sb.Append($"*{subCommandCount}\r\n");
            if (this.SubCommands != null && subCommandCount > 0)
            {
                foreach (var subCommand in SubCommands)
                    sb.Append(subCommand.RespFormat);
            }

            return sb.ToString();
        }

        private static readonly IDictionary<string, (RespCommand, byte)> RespCommandMapping =
            new Dictionary<string, (RespCommand, byte)>(StringComparer.OrdinalIgnoreCase)
            {
                { "GET", (RespCommand.GET, 0) },
                { "SET", (RespCommand.SET, 0) },
                { "GETRANGE", (RespCommand.GETRANGE, 0) },
                { "SETRANGE", (RespCommand.SETRANGE, 0) },
                { "PFADD", (RespCommand.PFADD, 0) },
                { "PFCOUNT", (RespCommand.PFCOUNT, 0) },
                { "PFMERGE", (RespCommand.PFMERGE, 0) },
                { "SETEX", (RespCommand.SETEX, 0) },
                { "PSETEX", (RespCommand.PSETEX, 0) },
                { "SETEXNX", (RespCommand.SETEXNX, 0) },
                { "SETEXXX", (RespCommand.SETEXXX, 0) },
                { "DEL", (RespCommand.DEL, 0) },
                { "EXISTS", (RespCommand.EXISTS, 0) },
                { "RENAME", (RespCommand.RENAME, 0) },
                { "INCR", (RespCommand.INCR, 0) },
                { "INCRBY", (RespCommand.INCRBY, 0) },
                { "DECR", (RespCommand.DECR, 0) },
                { "DECRBY", (RespCommand.DECRBY, 0) },
                { "EXPIRE", (RespCommand.EXPIRE, 0) },
                { "PEXPIRE", (RespCommand.PEXPIRE, 0) },
                { "PERSIST", (RespCommand.PERSIST, 0) },
                { "TTL", (RespCommand.TTL, 0) },
                { "PTTL", (RespCommand.PTTL, 0) },
                { "SETBIT", (RespCommand.SETBIT, 0) },
                { "GETBIT", (RespCommand.GETBIT, 0) },
                { "BITCOUNT", (RespCommand.BITCOUNT, 0) },
                { "BITPOS", (RespCommand.BITPOS, 0) },
                { "BITFIELD", (RespCommand.BITFIELD, 0) },
                { "MSET", (RespCommand.MSET, 0) },
                { "MSETNX", (RespCommand.MSETNX, 0) },
                { "MGET", (RespCommand.MGET, 0) },
                { "UNLINK", (RespCommand.UNLINK, 0) },
                { "MULTI", (RespCommand.MULTI, 0) },
                { "EXEC", (RespCommand.EXEC, 0) },
                { "WATCH", (RespCommand.WATCH, 0) },
                { "UNWATCH", (RespCommand.UNWATCH, 0) },
                { "DISCARD", (RespCommand.DISCARD, 0) },
                { "GETDEL", (RespCommand.GETDEL, 0) },
                { "APPEND", (RespCommand.APPEND, 0) },
                { "ECHO", (RespCommand.ECHO, 0) },
                { "REPLICAOF", (RespCommand.REPLICAOF, 0) },
                { "SLAVEOF", (RespCommand.SECONDARYOF, 0) },
                { "CONFIG", (RespCommand.CONFIG, 0) },
                { "CLIENT", (RespCommand.CLIENT, 0) },
                { "REGISTERCS", (RespCommand.REGISTERCS, 0) },
                { "ZADD", (RespCommand.SortedSet, (byte)SortedSetOperation.ZADD) },
                { "ZMSCORE", (RespCommand.SortedSet, (byte)SortedSetOperation.ZMSCORE) },
                { "ZREM", (RespCommand.SortedSet, (byte)SortedSetOperation.ZREM) },
                { "ZCARD", (RespCommand.SortedSet, (byte)SortedSetOperation.ZCARD) },
                { "ZPOPMAX", (RespCommand.SortedSet, (byte)SortedSetOperation.ZPOPMAX) },
                { "ZSCORE", (RespCommand.SortedSet, (byte)SortedSetOperation.ZSCORE) },
                { "ZCOUNT", (RespCommand.SortedSet, (byte)SortedSetOperation.ZCOUNT) },
                { "ZINCRBY", (RespCommand.SortedSet, (byte)SortedSetOperation.ZINCRBY) },
                { "ZRANK", (RespCommand.SortedSet, (byte)SortedSetOperation.ZRANK) },
                { "ZRANGE", (RespCommand.SortedSet, (byte)SortedSetOperation.ZRANGE) },
                { "ZRANGEBYSCORE", (RespCommand.SortedSet, (byte)SortedSetOperation.ZRANGEBYSCORE) },
                { "ZREVRANK", (RespCommand.SortedSet, (byte)SortedSetOperation.ZREVRANK) },
                { "ZREMRANGEBYLEX", (RespCommand.SortedSet, (byte)SortedSetOperation.ZREMRANGEBYLEX) },
                { "ZREMRANGEBYRANK", (RespCommand.SortedSet, (byte)SortedSetOperation.ZREMRANGEBYRANK) },
                { "ZREMRANGEBYSCORE", (RespCommand.SortedSet, (byte)SortedSetOperation.ZREMRANGEBYSCORE) },
                { "ZLEXCOUNT", (RespCommand.SortedSet, (byte)SortedSetOperation.ZLEXCOUNT) },
                { "ZPOPMIN", (RespCommand.SortedSet, (byte)SortedSetOperation.ZPOPMIN) },
                { "ZRANDMEMBER", (RespCommand.SortedSet, (byte)SortedSetOperation.ZRANDMEMBER) },
                { "GEOADD", (RespCommand.SortedSet, (byte)SortedSetOperation.GEOADD) },
                { "GEOHASH", (RespCommand.SortedSet, (byte)SortedSetOperation.GEOHASH) },
                { "GEODIST", (RespCommand.SortedSet, (byte)SortedSetOperation.GEODIST) },
                { "GEOPOS", (RespCommand.SortedSet, (byte)SortedSetOperation.GEOPOS) },
                { "GEOSEARCH", (RespCommand.SortedSet, (byte)SortedSetOperation.GEOSEARCH) },
                { "ZREVRANGE", (RespCommand.SortedSet, (byte)SortedSetOperation.ZREVRANGE) },
                { "ZSCAN", (RespCommand.SortedSet, (byte)SortedSetOperation.ZSCAN) },
                { "LPUSH", (RespCommand.List, (byte)ListOperation.LPUSH) },
                { "LPOP", (RespCommand.List, (byte)ListOperation.LPOP) },
                { "RPUSH", (RespCommand.List, (byte)ListOperation.RPUSH) },
                { "RPOP", (RespCommand.List, (byte)ListOperation.RPOP) },
                { "LLEN", (RespCommand.List, (byte)ListOperation.LLEN) },
                { "LTRIM", (RespCommand.List, (byte)ListOperation.LTRIM) },
                { "LRANGE", (RespCommand.List, (byte)ListOperation.LRANGE) },
                { "LINDEX", (RespCommand.List, (byte)ListOperation.LINDEX) },
                { "LINSERT", (RespCommand.List, (byte)ListOperation.LINSERT) },
                { "LREM", (RespCommand.List, (byte)ListOperation.LREM) },
                { "HSET", (RespCommand.Hash, (byte)HashOperation.HSET) },
                { "HMSET", (RespCommand.Hash, (byte)HashOperation.HMSET) },
                { "HGET", (RespCommand.Hash, (byte)HashOperation.HGET) },
                { "HMGET", (RespCommand.Hash, (byte)HashOperation.HMGET) },
                { "HGETALL", (RespCommand.Hash, (byte)HashOperation.HGETALL) },
                { "HDEL", (RespCommand.Hash, (byte)HashOperation.HDEL) },
                { "HLEN", (RespCommand.Hash, (byte)HashOperation.HLEN) },
                { "HEXISTS", (RespCommand.Hash, (byte)HashOperation.HEXISTS) },
                { "HKEYS", (RespCommand.Hash, (byte)HashOperation.HKEYS) },
                { "HVALS", (RespCommand.Hash, (byte)HashOperation.HVALS) },
                { "HINCRBY", (RespCommand.Hash, (byte)HashOperation.HINCRBY) },
                { "HINCRBYFLOAT", (RespCommand.Hash, (byte)HashOperation.HINCRBYFLOAT) },
                { "HSETNX", (RespCommand.Hash, (byte)HashOperation.HSETNX) },
                { "HRANDFIELD", (RespCommand.Hash, (byte)HashOperation.HRANDFIELD) },
                { "HSCAN", (RespCommand.Hash, (byte)HashOperation.HSCAN) },
                { "HSTRLEN", (RespCommand.Hash, (byte)HashOperation.HSTRLEN) },
                { "SADD", (RespCommand.Set, (byte)SetOperation.SADD) },
                { "SMEMBERS", (RespCommand.Set, (byte)SetOperation.SMEMBERS) },
                { "SREM", (RespCommand.Set, (byte)SetOperation.SREM) },
                { "SCARD", (RespCommand.Set, (byte)SetOperation.SCARD) },
                { "SPOP", (RespCommand.Set, (byte)SetOperation.SPOP) },
                { "SSCAN", (RespCommand.Set, (byte)SetOperation.SSCAN) },
                { "COSCAN", (RespCommand.All, (byte)RespCommand.COSCAN) }
            };
    }
}
