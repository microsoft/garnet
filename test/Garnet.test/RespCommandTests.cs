// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json.Serialization;
using System.Text.Json;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;

namespace Garnet.test
{
    [TestFixture]
    public class RespCommandTests
    {
        private static readonly Dictionary<string, RespCommand> SupportedCommands =
            new(StringComparer.OrdinalIgnoreCase)
            {
                { "ACL", RespCommand.ACL },
                { "CLUSTER", RespCommand.CLUSTER },
                { "MEMORY", RespCommand.MEMORY },
                { "LATENCY", RespCommand.LATENCY },
                { "GET", RespCommand.GET },
                { "SET", RespCommand.SET },
                { "GETRANGE", RespCommand.GETRANGE },
                { "SETRANGE", RespCommand.SETRANGE },
                { "PFADD", RespCommand.PFADD },
                { "PFCOUNT", RespCommand.PFCOUNT },
                { "PFMERGE", RespCommand.PFMERGE },
                { "SETEX", RespCommand.SETEX },
                { "PSETEX", RespCommand.PSETEX },
                { "DEL", RespCommand.DEL },
                { "EXISTS", RespCommand.EXISTS },
                { "RENAME", RespCommand.RENAME },
                { "INCR", RespCommand.INCR },
                { "INCRBY", RespCommand.INCRBY },
                { "DECR", RespCommand.DECR },
                { "DECRBY", RespCommand.DECRBY },
                { "EXPIRE", RespCommand.EXPIRE },
                { "PEXPIRE", RespCommand.PEXPIRE },
                { "PERSIST", RespCommand.PERSIST },
                { "TTL", RespCommand.TTL },
                { "PTTL", RespCommand.PTTL },
                { "SETBIT", RespCommand.SETBIT },
                { "GETBIT", RespCommand.GETBIT },
                { "BITCOUNT", RespCommand.BITCOUNT },
                { "BITPOS", RespCommand.BITPOS },
                { "BITFIELD", RespCommand.BITFIELD },
                { "MSET", RespCommand.MSET },
                { "MSETNX", RespCommand.MSETNX },
                { "MGET", RespCommand.MGET },
                { "UNLINK", RespCommand.UNLINK },
                { "MULTI", RespCommand.MULTI },
                { "EXEC", RespCommand.EXEC },
                { "WATCH", RespCommand.WATCH },
                { "UNWATCH", RespCommand.UNWATCH },
                { "DISCARD", RespCommand.DISCARD },
                { "GETDEL", RespCommand.GETDEL },
                { "APPEND", RespCommand.APPEND },
                { "ECHO", RespCommand.ECHO },
                { "REPLICAOF", RespCommand.REPLICAOF },
                { "SLAVEOF", RespCommand.SECONDARYOF },
                { "CONFIG", RespCommand.CONFIG },
                { "CLIENT", RespCommand.CLIENT },

                { "ZADD", RespCommand.SortedSet },
                { "ZMSCORE", RespCommand.SortedSet },
                { "ZREM", RespCommand.SortedSet },
                { "ZCARD", RespCommand.SortedSet },
                { "ZPOPMAX", RespCommand.SortedSet },
                { "ZSCORE", RespCommand.SortedSet },
                { "ZCOUNT", RespCommand.SortedSet },
                { "ZINCRBY", RespCommand.SortedSet },
                { "ZRANK", RespCommand.SortedSet },
                { "ZRANGE", RespCommand.SortedSet },
                { "ZRANGEBYSCORE", RespCommand.SortedSet },
                { "ZREVRANK", RespCommand.SortedSet },
                { "ZREMRANGEBYLEX", RespCommand.SortedSet },
                { "ZREMRANGEBYRANK", RespCommand.SortedSet },
                { "ZREMRANGEBYSCORE", RespCommand.SortedSet },
                { "ZLEXCOUNT", RespCommand.SortedSet },
                { "ZPOPMIN", RespCommand.SortedSet },
                { "ZRANDMEMBER", RespCommand.SortedSet },
                { "GEOADD", RespCommand.SortedSet },
                { "GEOHASH", RespCommand.SortedSet },
                { "GEODIST", RespCommand.SortedSet },
                { "GEOPOS", RespCommand.SortedSet },
                { "GEOSEARCH", RespCommand.SortedSet },
                { "ZREVRANGE", RespCommand.SortedSet },
                { "ZSCAN", RespCommand.SortedSet },

                { "LPUSH", RespCommand.List },
                { "LPOP", RespCommand.List },
                { "RPUSH", RespCommand.List },
                { "RPOP", RespCommand.List },
                { "LLEN", RespCommand.List },
                { "LTRIM", RespCommand.List },
                { "LRANGE", RespCommand.List },
                { "LINDEX", RespCommand.List },
                { "LINSERT", RespCommand.List },
                { "LREM", RespCommand.List },

                { "HSET", RespCommand.Hash },
                { "HMSET", RespCommand.Hash },
                { "HGET", RespCommand.Hash },
                { "HMGET", RespCommand.Hash },
                { "HGETALL", RespCommand.Hash },
                { "HDEL", RespCommand.Hash },
                { "HLEN", RespCommand.Hash },
                { "HEXISTS", RespCommand.Hash },
                { "HKEYS", RespCommand.Hash },
                { "HVALS", RespCommand.Hash },
                { "HINCRBY", RespCommand.Hash },
                { "HINCRBYFLOAT", RespCommand.Hash },
                { "HSETNX", RespCommand.Hash },
                { "HRANDFIELD", RespCommand.Hash },
                { "HSCAN", RespCommand.Hash },
                { "HSTRLEN", RespCommand.Hash },

                { "SADD", RespCommand.Set },
                { "SMEMBERS", RespCommand.Set },
                { "SREM", RespCommand.Set },
                { "SCARD", RespCommand.Set },
                { "SPOP", RespCommand.Set },
                { "SSCAN", RespCommand.Set }
            };

        private static readonly Dictionary<string, RespCommand> SupportedSubCommands =
            new(StringComparer.OrdinalIgnoreCase)
            {
                { "ACL|CAT", RespCommand.ACL },
                { "ACL|DELUSER", RespCommand.ACL },
                { "ACL|LIST", RespCommand.ACL },
                { "ACL|LOAD", RespCommand.ACL },
                { "ACL|SETUSER", RespCommand.ACL },
                { "ACL|USERS", RespCommand.ACL },
                { "ACL|WHOAMI", RespCommand.ACL },

                { "CLUSTER|ADDSLOTS", RespCommand.CLUSTER },
                { "CLUSTER|ADDSLOTSRANGE", RespCommand.CLUSTER },
                { "CLUSTER|BUMPEPOCH", RespCommand.CLUSTER },
                { "CLUSTER|COUNTKEYSINSLOT", RespCommand.CLUSTER },
                { "CLUSTER|DELSLOTS", RespCommand.CLUSTER },
                { "CLUSTER|DELSLOTSRANGE", RespCommand.CLUSTER },
                { "CLUSTER|FAILOVER", RespCommand.CLUSTER },
                { "CLUSTER|FORGET", RespCommand.CLUSTER },
                { "CLUSTER|GETKEYSINSLOT", RespCommand.CLUSTER },
                { "CLUSTER|INFO", RespCommand.CLUSTER },
                { "CLUSTER|KEYSLOT", RespCommand.CLUSTER },
                { "CLUSTER|MEET", RespCommand.CLUSTER },
                { "CLUSTER|MYID", RespCommand.CLUSTER },
                { "CLUSTER|NODES", RespCommand.CLUSTER },
                { "CLUSTER|REPLICAS", RespCommand.CLUSTER },
                { "CLUSTER|REPLICATE", RespCommand.CLUSTER },
                { "CLUSTER|RESET", RespCommand.CLUSTER },
                { "CLUSTER|SET-CONFIG-EPOCH", RespCommand.CLUSTER },
                { "CLUSTER|SETSLOT", RespCommand.CLUSTER },
                { "CLUSTER|SLOTS", RespCommand.CLUSTER },

                { "CONFIG|GET", RespCommand.CONFIG },
                { "CONFIG|SET", RespCommand.CONFIG },
                { "CONFIG|REWRITE", RespCommand.CONFIG },

                { "MEMORY|USAGE", RespCommand.MEMORY },

                { "LATENCY|HISTOGRAM", RespCommand.LATENCY },
                { "LATENCY|RESET", RespCommand.LATENCY },
            };

        private unsafe bool TryParseRespCommandInfo(ref byte* ptr, byte* end, bool isSubCommand, out RespCommandsInfoNew command)
        {
            command = default;

            Assert.AreNotEqual("$-1\r\n", Encoding.ASCII.GetString(ptr, 5));

            RespReadUtils.ReadArrayLength(out var infoElemCount, ref ptr, end);
            Assert.AreEqual(10, infoElemCount);

            // 1) Name
            RespReadUtils.ReadStringWithLengthHeader(out var name, ref ptr, end);

            // 2) Arity
            RespReadUtils.ReadIntegerAsString(out var strArity, ref ptr, end);
            Assert.IsTrue(int.TryParse(strArity, out var arity));

            // 3) Flags
            var flags = RespCommandFlags.None;
            RespReadUtils.ReadArrayLength(out var flagCount, ref ptr, end);
            for (var flagIdx = 0; flagIdx < flagCount; flagIdx++)
            {
                RespReadUtils.ReadSimpleString(out var strFlag, ref ptr, end);
                Assert.IsTrue(EnumUtils.TryParseEnumFromDescription<RespCommandFlags>(strFlag, out var flag));
                flags |= flag;
            }

            // 4) First key
            RespReadUtils.ReadIntegerAsString(out var strFirstKey, ref ptr, end);
            Assert.IsTrue(int.TryParse(strFirstKey, out var firstKey));

            // 5) Last key
            RespReadUtils.ReadIntegerAsString(out var strLastKey, ref ptr, end);
            Assert.IsTrue(int.TryParse(strLastKey, out var lastKey));

            // 6) Step
            RespReadUtils.ReadIntegerAsString(out var strStep, ref ptr, end);
            Assert.IsTrue(int.TryParse(strStep, out var step));

            // 7) ACL categories
            var aclCategories = RespAclCategories.None;
            RespReadUtils.ReadArrayLength(out var aclCatCount, ref ptr, end);
            for (var aclCatIdx = 0; aclCatIdx < aclCatCount; aclCatIdx++)
            {
                RespReadUtils.ReadSimpleString(out var strAclCat, ref ptr, end);
                Assert.IsTrue(EnumUtils.TryParseEnumFromDescription<RespAclCategories>(strAclCat.TrimStart('@'), out var aclCat));
                aclCategories |= aclCat;
            }

            // 8) Tips
            RespReadUtils.ReadStringArrayWithLengthHeader(out var tips, ref ptr, end);

            // 9) Key specifications
            RespReadUtils.ReadArrayLength(out var ksCount, ref ptr, end);
            var keySpecifications = new RespCommandKeySpecifications[ksCount];
            for (var ksIdx = 0; ksIdx < ksCount; ksIdx++)
            {
                string notes = null;
                KeySpecificationFlags ksFlags = KeySpecificationFlags.None;
                IBeginSearchKeySpec beginSearchKeySpec = null;
                IFindKeysKeySpec findKeysKeySpec = null;

                RespReadUtils.ReadArrayLength(out var ksElemCount, ref ptr, end);
                for (var ksElemIdx = 0; ksElemIdx < ksElemCount; ksElemIdx += 2)
                {
                    RespReadUtils.ReadStringWithLengthHeader(out var ksKey, ref ptr, end);

                    switch (ksKey)
                    {
                        case "notes":
                            RespReadUtils.ReadStringWithLengthHeader(out notes, ref ptr, end);
                            break;
                        case "flags":
                            RespReadUtils.ReadArrayLength(out var ksFlagsCount, ref ptr, end);
                            for (var ksFlagIdx = 0; ksFlagIdx < ksFlagsCount; ksFlagIdx++)
                            {
                                RespReadUtils.ReadSimpleString(out var strKsFlag, ref ptr, end);
                                Assert.IsTrue(
                                    EnumUtils.TryParseEnumFromDescription<KeySpecificationFlags>(strKsFlag, out var ksFlag));
                                ksFlags |= ksFlag;
                            }

                            break;
                        case "begin_search":
                        case "find_keys":
                            RespReadUtils.ReadArrayLength(out var ksTypeElemCount, ref ptr, end);
                            Assert.AreEqual(4, ksTypeElemCount);
                            RespReadUtils.ReadStringWithLengthHeader(out var ksTypeStr, ref ptr, end);
                            Assert.AreEqual("type", ksTypeStr);
                            RespReadUtils.ReadStringWithLengthHeader(out var ksType, ref ptr, end);
                            RespReadUtils.ReadStringWithLengthHeader(out var ksSpecStr, ref ptr, end);
                            Assert.AreEqual("spec", ksSpecStr);
                            RespReadUtils.ReadArrayLength(out var ksSpecElemCount, ref ptr, end);

                            string ksArgKey;
                            if (ksKey == "begin_search")
                            {
                                switch (ksType)
                                {
                                    case "index":
                                        Assert.AreEqual(2, ksSpecElemCount);
                                        RespReadUtils.ReadStringWithLengthHeader(out ksArgKey, ref ptr, end);
                                        Assert.AreEqual("index", ksArgKey);
                                        RespReadUtils.ReadIntegerAsString(out var strIndex, ref ptr, end);
                                        Assert.IsTrue(int.TryParse(strIndex, out var index));
                                        beginSearchKeySpec = new BeginSearchIndex(index);
                                        break;
                                    case "keyword":
                                        Assert.AreEqual(4, ksSpecElemCount);
                                        RespReadUtils.ReadStringWithLengthHeader(out ksArgKey, ref ptr, end);
                                        Assert.AreEqual("keyword", ksArgKey);
                                        RespReadUtils.ReadStringWithLengthHeader(out var keyword, ref ptr, end);
                                        RespReadUtils.ReadIntegerAsString(out var strStartFrom, ref ptr, end);
                                        Assert.IsTrue(int.TryParse(strStartFrom, out var startFrom));
                                        beginSearchKeySpec = new BeginSearchKeyword(keyword, startFrom);
                                        break;
                                    case "unknown":
                                        Assert.AreEqual(0, ksSpecElemCount);
                                        beginSearchKeySpec = new BeginSearchUnknown();
                                        break;
                                    default:
                                        Assert.Fail();
                                        break;
                                }
                            }
                            else if (ksKey == "find_keys")
                            {
                                switch (ksType)
                                {
                                    case "range":
                                        Assert.AreEqual(6, ksSpecElemCount);
                                        RespReadUtils.ReadStringWithLengthHeader(out ksArgKey, ref ptr, end);
                                        Assert.AreEqual("lastkey", ksArgKey);
                                        RespReadUtils.ReadIntegerAsString(out var strKsLastKey, ref ptr, end);
                                        Assert.IsTrue(int.TryParse(strKsLastKey, out var ksLastKey));
                                        RespReadUtils.ReadStringWithLengthHeader(out ksArgKey, ref ptr, end);
                                        Assert.AreEqual("keystep", ksArgKey);
                                        RespReadUtils.ReadIntegerAsString(out var strKsKeyStep, ref ptr, end);
                                        Assert.IsTrue(int.TryParse(strKsKeyStep, out var ksKeyStep));
                                        RespReadUtils.ReadStringWithLengthHeader(out ksArgKey, ref ptr, end);
                                        Assert.AreEqual("limit", ksArgKey);
                                        RespReadUtils.ReadIntegerAsString(out var strKsLimit, ref ptr, end);
                                        Assert.IsTrue(int.TryParse(strKsLimit, out var ksLimit));
                                        findKeysKeySpec = new FindKeysRange(ksLastKey, ksKeyStep, ksLimit);
                                        break;
                                    case "keynum":
                                        Assert.AreEqual(6, ksSpecElemCount);
                                        RespReadUtils.ReadStringWithLengthHeader(out ksArgKey, ref ptr, end);
                                        Assert.AreEqual("keynumidx", ksArgKey);
                                        RespReadUtils.ReadIntegerAsString(out var strKeyNumIdx, ref ptr, end);
                                        Assert.IsTrue(int.TryParse(strKeyNumIdx, out var keyNumIdx));
                                        RespReadUtils.ReadStringWithLengthHeader(out ksArgKey, ref ptr, end);
                                        Assert.AreEqual("firstkey", ksArgKey);
                                        RespReadUtils.ReadIntegerAsString(out var strKsFirstKey, ref ptr, end);
                                        Assert.IsTrue(int.TryParse(strKsFirstKey, out var ksFirstKey));
                                        RespReadUtils.ReadStringWithLengthHeader(out ksArgKey, ref ptr, end);
                                        Assert.AreEqual("keystep", ksArgKey);
                                        RespReadUtils.ReadIntegerAsString(out var strKeyStep, ref ptr, end);
                                        Assert.IsTrue(int.TryParse(strKeyStep, out var keyStep));
                                        findKeysKeySpec = new FindKeysKeyNum(keyNumIdx, ksFirstKey, keyStep);
                                        break;
                                    case "unknown":
                                        Assert.AreEqual(0, ksSpecElemCount);
                                        findKeysKeySpec = new FindKeysUnknown();
                                        break;
                                    default:
                                        Assert.Fail();
                                        break;
                                }
                            }

                            break;
                        default:
                            Assert.Fail();
                            break;
                    }
                }

                var keySpec = new RespCommandKeySpecifications()
                {
                    Notes = notes, Flags = ksFlags, BeginSearch = beginSearchKeySpec, FindKeys = findKeysKeySpec
                };

                keySpecifications[ksIdx] = keySpec;
            }

            // 10) SubCommands
            RespReadUtils.ReadArrayLength(out var scCount, ref ptr, end);
            var subCommands = new List<RespCommandsInfoNew>();
            for (var scIdx = 0; scIdx < scCount; scIdx++)
            {
                if (TryParseRespCommandInfo(ref ptr, end, true, out var subCommand) &&
                    SupportedSubCommands.ContainsKey(subCommand.Name))
                    subCommands.Add(subCommand);
            }

            var respCommand = !isSubCommand && SupportedCommands.ContainsKey(name) ? SupportedCommands[name]
                : (isSubCommand && SupportedSubCommands.ContainsKey(name) ? SupportedSubCommands[name] : RespCommand.NONE);

            command = new RespCommandsInfoNew()
            {
                Name = name.ToUpper(),
                Command = respCommand,
                Arity = arity,
                Flags = flags,
                FirstKey = firstKey,
                LastKey = lastKey,
                Step = step,
                AclCategories = aclCategories,
                Tips = tips,
                KeySpecifications = keySpecifications.Length == 0 ? null : keySpecifications[0],
                SubCommands = subCommands.ToArray()
            };

            return true;
        }


        [Test]
        public unsafe void RespCommandTest()
        {
            var lightClient = new LightClientRequest("127.0.0.1", 6379, 0);
            var response = lightClient.SendCommand($"COMMAND INFO {string.Join(' ', SupportedCommands.Keys)}");

            fixed (byte* respPtr = response)
            {
                var ptr = (byte*)Unsafe.AsPointer(ref respPtr[0]);
                var end = ptr + response.Length;
                RespReadUtils.ReadArrayLength(out var cmdCount, ref ptr, end);

                var commands = new List<RespCommandsInfoNew>();
                for (var cmdIdx = 0; cmdIdx < cmdCount; cmdIdx++)
                {
                    if(TryParseRespCommandInfo(ref ptr, end, false, out var command) &&
                       SupportedCommands.ContainsKey(command.Name))
                        commands.Add(command);
                }

                var outputPath = "D:\\Tal\\SupportedRespCommands.json";
                var serializerOptions = new JsonSerializerOptions()
                {
                    WriteIndented = true,
                    Converters = { new JsonStringEnumConverter(), new KeySpecConverter() }
                };
                var jsonString = JsonSerializer.Serialize(commands.ToArray(), serializerOptions);
                File.WriteAllText(outputPath, jsonString);
            }
        }
    }
}
