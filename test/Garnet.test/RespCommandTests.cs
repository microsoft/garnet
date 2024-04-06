// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
        private class SupportedCommand
        {
            public RespCommand Command { get; init; }

            public byte? ArrayCommand { get; init; }

            public IDictionary<string, SupportedCommand> SupportedSubCommands { get; init; }

            public SupportedCommand(RespCommand command, byte? arrayCommand = null, IDictionary<string, SupportedCommand> supportedSubCommands = null)
            {
                this.Command = command;
                this.ArrayCommand = arrayCommand;
                this.SupportedSubCommands = supportedSubCommands;
            }
        }

        private static readonly Dictionary<string, SupportedCommand> SupportedCommands =
            new(StringComparer.OrdinalIgnoreCase)
            {
                {   "GET",  new SupportedCommand(RespCommand.GET)   },
                {   "SET",  new SupportedCommand(RespCommand.SET)   },
                {   "GETRANGE", new SupportedCommand(RespCommand.GETRANGE)  },
                {   "SETRANGE", new SupportedCommand(RespCommand.SETRANGE)  },
                {   "PFADD",    new SupportedCommand(RespCommand.PFADD) },
                {   "PFCOUNT",  new SupportedCommand(RespCommand.PFCOUNT)   },
                {   "PFMERGE",  new SupportedCommand(RespCommand.PFMERGE)   },
                {   "SETEX",    new SupportedCommand(RespCommand.SETEX) },
                {   "PSETEX",   new SupportedCommand(RespCommand.PSETEX)    },
                {   "DEL",  new SupportedCommand(RespCommand.DEL)   },
                {   "EXISTS",   new SupportedCommand(RespCommand.EXISTS)    },
                {   "RENAME",   new SupportedCommand(RespCommand.RENAME)    },
                {   "INCR", new SupportedCommand(RespCommand.INCR)  },
                {   "INCRBY",   new SupportedCommand(RespCommand.INCRBY)    },
                {   "DECR", new SupportedCommand(RespCommand.DECR)  },
                {   "DECRBY",   new SupportedCommand(RespCommand.DECRBY)    },
                {   "EXPIRE",   new SupportedCommand(RespCommand.EXPIRE)    },
                {   "PEXPIRE",  new SupportedCommand(RespCommand.PEXPIRE)   },
                {   "PERSIST",  new SupportedCommand(RespCommand.PERSIST)   },
                {   "TTL",  new SupportedCommand(RespCommand.TTL)   },
                {   "PTTL", new SupportedCommand(RespCommand.PTTL)  },
                {   "SETBIT",   new SupportedCommand(RespCommand.SETBIT)    },
                {   "GETBIT",   new SupportedCommand(RespCommand.GETBIT)    },
                {   "BITCOUNT", new SupportedCommand(RespCommand.BITCOUNT)  },
                {   "BITPOS",   new SupportedCommand(RespCommand.BITPOS)    },
                {   "BITFIELD", new SupportedCommand(RespCommand.BITFIELD)  },
                {   "MSET", new SupportedCommand(RespCommand.MSET)  },
                {   "MSETNX",   new SupportedCommand(RespCommand.MSETNX)    },
                {   "MGET", new SupportedCommand(RespCommand.MGET)  },
                {   "UNLINK",   new SupportedCommand(RespCommand.UNLINK)    },
                {   "MULTI",    new SupportedCommand(RespCommand.MULTI) },
                {   "EXEC", new SupportedCommand(RespCommand.EXEC)  },
                {   "WATCH",    new SupportedCommand(RespCommand.WATCH) },
                {   "UNWATCH",  new SupportedCommand(RespCommand.UNWATCH)   },
                {   "DISCARD",  new SupportedCommand(RespCommand.DISCARD)   },
                {   "GETDEL",   new SupportedCommand(RespCommand.GETDEL)    },
                {   "APPEND",   new SupportedCommand(RespCommand.APPEND)    },
                {   "ECHO", new SupportedCommand(RespCommand.ECHO)  },
                {   "REPLICAOF",    new SupportedCommand(RespCommand.REPLICAOF) },
                {   "SLAVEOF",  new SupportedCommand(RespCommand.SECONDARYOF)   },
                {   "CONFIG",   new SupportedCommand(RespCommand.CONFIG, null, new Dictionary<string, SupportedCommand>()
                    {
                        {   "CONFIG|GET",   new SupportedCommand(RespCommand.CONFIG)    },
                        {   "CONFIG|SET",   new SupportedCommand(RespCommand.CONFIG)    },
                        {   "CONFIG|REWRITE",   new SupportedCommand(RespCommand.CONFIG)    },
                    })
                },
                {   "CLIENT",   new SupportedCommand(RespCommand.CLIENT)    },
                {   "ZADD", new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZADD)  },
                {   "ZMSCORE",  new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZMSCORE)   },
                {   "ZREM", new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZREM)  },
                {   "ZCARD",    new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZCARD) },
                {   "ZPOPMAX",  new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZPOPMAX)   },
                {   "ZSCORE",   new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZSCORE)    },
                {   "ZCOUNT",   new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZCOUNT)    },
                {   "ZINCRBY",  new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZINCRBY)   },
                {   "ZRANK",    new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZRANK) },
                {   "ZRANGE",   new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZRANGE)    },
                {   "ZRANGEBYSCORE",    new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZRANGEBYSCORE) },
                {   "ZREVRANK", new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZREVRANK)  },
                {   "ZREMRANGEBYLEX",   new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZREMRANGEBYLEX)    },
                {   "ZREMRANGEBYRANK",  new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZREMRANGEBYRANK)   },
                {   "ZREMRANGEBYSCORE", new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZREMRANGEBYSCORE)  },
                {   "ZLEXCOUNT",    new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZLEXCOUNT) },
                {   "ZPOPMIN",  new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZPOPMIN)   },
                {   "ZRANDMEMBER",  new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZRANDMEMBER)   },
                {   "GEOADD",   new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.GEOADD)    },
                {   "GEOHASH",  new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.GEOHASH)   },
                {   "GEODIST",  new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.GEODIST)   },
                {   "GEOPOS",   new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.GEOPOS)    },
                {   "GEOSEARCH",    new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.GEOSEARCH) },
                {   "ZREVRANGE",    new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZREVRANGE) },
                {   "ZSCAN",    new SupportedCommand(RespCommand.SortedSet, (byte)SortedSetOperation.ZSCAN) },
                {   "LPUSH",    new SupportedCommand(RespCommand.List,  (byte)ListOperation.LPUSH)  },
                {   "LPOP", new SupportedCommand(RespCommand.List,  (byte)ListOperation.LPOP)   },
                {   "RPUSH",    new SupportedCommand(RespCommand.List,  (byte)ListOperation.RPUSH)  },
                {   "RPOP", new SupportedCommand(RespCommand.List,  (byte)ListOperation.RPOP)   },
                {   "LLEN", new SupportedCommand(RespCommand.List,  (byte)ListOperation.LLEN)   },
                {   "LTRIM",    new SupportedCommand(RespCommand.List,  (byte)ListOperation.LTRIM)  },
                {   "LRANGE",   new SupportedCommand(RespCommand.List,  (byte)ListOperation.LRANGE) },
                {   "LINDEX",   new SupportedCommand(RespCommand.List,  (byte)ListOperation.LINDEX) },
                {   "LINSERT",  new SupportedCommand(RespCommand.List,  (byte)ListOperation.LINSERT)    },
                {   "LREM", new SupportedCommand(RespCommand.List,  (byte)ListOperation.LREM)   }, 
                {   "HSET", new SupportedCommand(RespCommand.Hash,  (byte)HashOperation.HSET)   },
                {   "HMSET",    new SupportedCommand(RespCommand.Hash,  (byte)HashOperation.HMSET)  },
                {   "HGET", new SupportedCommand(RespCommand.Hash,  (byte)HashOperation.HGET)   },
                {   "HMGET",    new SupportedCommand(RespCommand.Hash,  (byte)HashOperation.HMGET)  },
                {   "HGETALL",  new SupportedCommand(RespCommand.Hash,  (byte)HashOperation.HGETALL)    },
                {   "HDEL", new SupportedCommand(RespCommand.Hash,  (byte)HashOperation.HDEL)   },
                {   "HLEN", new SupportedCommand(RespCommand.Hash,  (byte)HashOperation.HLEN)   },
                {   "HEXISTS",  new SupportedCommand(RespCommand.Hash,  (byte)HashOperation.HEXISTS)    },
                {   "HKEYS",    new SupportedCommand(RespCommand.Hash,  (byte)HashOperation.HKEYS)  },
                {   "HVALS",    new SupportedCommand(RespCommand.Hash,  (byte)HashOperation.HVALS)  },
                {   "HINCRBY",  new SupportedCommand(RespCommand.Hash,  (byte)HashOperation.HINCRBY)    },
                {   "HINCRBYFLOAT", new SupportedCommand(RespCommand.Hash,  (byte)HashOperation.HINCRBYFLOAT)   },
                {   "HSETNX",   new SupportedCommand(RespCommand.Hash,  (byte)HashOperation.HSETNX) },
                {   "HRANDFIELD",   new SupportedCommand(RespCommand.Hash,  (byte)HashOperation.HRANDFIELD) },
                {   "HSCAN",    new SupportedCommand(RespCommand.Hash,  (byte)HashOperation.HSCAN)  },
                {   "HSTRLEN",  new SupportedCommand(RespCommand.Hash,  (byte)HashOperation.HSTRLEN)    },
                {   "SADD", new SupportedCommand(RespCommand.Set,   (byte)SetOperation.SADD)    },
                {   "SMEMBERS", new SupportedCommand(RespCommand.Set,   (byte)SetOperation.SMEMBERS)    },
                {   "SREM", new SupportedCommand(RespCommand.Set,   (byte)SetOperation.SREM)    },
                {   "SCARD",    new SupportedCommand(RespCommand.Set,   (byte)SetOperation.SCARD)   },
                {   "SPOP", new SupportedCommand(RespCommand.Set,   (byte)SetOperation.SPOP)    },
                {   "SSCAN",    new SupportedCommand(RespCommand.Set,   (byte)SetOperation.SSCAN)   },
                {   "ACL",  new SupportedCommand(RespCommand.ACL, null, new Dictionary<string, SupportedCommand>
                    {
                        {   "ACL|CAT",  new SupportedCommand(RespCommand.ACL)   },
                        {   "ACL|DELUSER",  new SupportedCommand(RespCommand.ACL)   },
                        {   "ACL|LIST", new SupportedCommand(RespCommand.ACL)   },
                        {   "ACL|LOAD", new SupportedCommand(RespCommand.ACL)   },
                        {   "ACL|SETUSER",  new SupportedCommand(RespCommand.ACL)   },
                        {   "ACL|USERS",    new SupportedCommand(RespCommand.ACL)   },
                        {   "ACL|WHOAMI",   new SupportedCommand(RespCommand.ACL)   },
                    })
                },
                {   "CLUSTER",  new SupportedCommand(RespCommand.CLUSTER, null, new Dictionary<string, SupportedCommand>
                    {
                        {   "CLUSTER|ADDSLOTS", new SupportedCommand(RespCommand.CLUSTER)   },
                        {   "CLUSTER|ADDSLOTSRANGE",    new SupportedCommand(RespCommand.CLUSTER)   },
                        {   "CLUSTER|BUMPEPOCH",    new SupportedCommand(RespCommand.CLUSTER)   },
                        {   "CLUSTER|COUNTKEYSINSLOT",  new SupportedCommand(RespCommand.CLUSTER)   },
                        {   "CLUSTER|DELSLOTS", new SupportedCommand(RespCommand.CLUSTER)   },
                        {   "CLUSTER|DELSLOTSRANGE",    new SupportedCommand(RespCommand.CLUSTER)   },
                        {   "CLUSTER|FAILOVER", new SupportedCommand(RespCommand.CLUSTER)   },
                        {   "CLUSTER|FORGET",   new SupportedCommand(RespCommand.CLUSTER)   },
                        {   "CLUSTER|GETKEYSINSLOT",    new SupportedCommand(RespCommand.CLUSTER)   },
                        {   "CLUSTER|INFO", new SupportedCommand(RespCommand.CLUSTER)   },
                        {   "CLUSTER|KEYSLOT",  new SupportedCommand(RespCommand.CLUSTER)   },
                        {   "CLUSTER|MEET", new SupportedCommand(RespCommand.CLUSTER)   },
                        {   "CLUSTER|MYID", new SupportedCommand(RespCommand.CLUSTER)   },
                        {   "CLUSTER|NODES",    new SupportedCommand(RespCommand.CLUSTER)   },
                        {   "CLUSTER|REPLICAS", new SupportedCommand(RespCommand.CLUSTER)   },
                        {   "CLUSTER|REPLICATE",    new SupportedCommand(RespCommand.CLUSTER)   },
                        {   "CLUSTER|RESET",    new SupportedCommand(RespCommand.CLUSTER)   },
                        {   "CLUSTER|SET-CONFIG-EPOCH", new SupportedCommand(RespCommand.CLUSTER)   },
                        {   "CLUSTER|SETSLOT",  new SupportedCommand(RespCommand.CLUSTER)   },
                        {   "CLUSTER|SLOTS",    new SupportedCommand(RespCommand.CLUSTER)   },
                    })
                },
                {   "MEMORY",  new SupportedCommand(RespCommand.MEMORY, null, new Dictionary<string, SupportedCommand>
                    {
                        {   "MEMORY|USAGE", new SupportedCommand(RespCommand.MEMORY)    },
                    })
                },
                {   "LATENCY",  new SupportedCommand(RespCommand.LATENCY, null, new Dictionary<string, SupportedCommand>
                    {
                        {   "LATENCY|HISTOGRAM",    new SupportedCommand(RespCommand.LATENCY)   },
                        {   "LATENCY|RESET",    new SupportedCommand(RespCommand.LATENCY)   },
                    })
                },
            };

        private unsafe bool TryParseRespCommandInfo(ref byte* ptr, byte* end, out RespCommandsInfo command, string parentCommand = null)
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
            var subCommands = new List<RespCommandsInfo>();
            for (var scIdx = 0; scIdx < scCount; scIdx++)
            {
                if (TryParseRespCommandInfo(ref ptr, end, out var subCommand, name))
                    subCommands.Add(subCommand);
            }

            var isSubCommand = !string.IsNullOrEmpty(parentCommand);

            var supportedCommand = !isSubCommand && SupportedCommands.ContainsKey(name) ? SupportedCommands[name]
                : (isSubCommand && SupportedCommands.ContainsKey(parentCommand) &&
                   SupportedCommands[parentCommand].SupportedSubCommands != null && SupportedCommands[parentCommand].SupportedSubCommands.ContainsKey(name)
                    ? SupportedCommands[parentCommand].SupportedSubCommands[name]
                    : null);

            if (supportedCommand != null)
            {
                command = new RespCommandsInfo
                {
                    Name = name.ToUpper(),
                    Command = supportedCommand.Command,
                    ArrayCommand = supportedCommand.ArrayCommand,
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

            return false;
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

                var commands = new List<RespCommandsInfo>();
                for (var cmdIdx = 0; cmdIdx < cmdCount; cmdIdx++)
                {
                    if(TryParseRespCommandInfo(ref ptr, end, out var command))
                        commands.Add(command);
                }

                var outputPath = "D:\\Tal\\SupportedRespCommandsInfo.json";
                var serializerOptions = new JsonSerializerOptions()
                {
                    WriteIndented = true,
                    Converters = { new JsonStringEnumConverter(), new KeySpecConverter() }
                };
                var jsonString = JsonSerializer.Serialize(commands.ToArray(), serializerOptions);
                File.WriteAllText(outputPath, jsonString);

                var outputPath2 = "D:\\Tal\\SupportedRespCommands.json";
                var supportedCommands = SupportedCommands.OrderBy(kvp => kvp.Key);
                jsonString = JsonSerializer.Serialize(supportedCommands, serializerOptions);
                File.WriteAllText(outputPath2, jsonString);
            }
        }
    }
}
