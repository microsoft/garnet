// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;
using System.Text.Json;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using StackExchange.Redis;
using SetOperation = Garnet.server.SetOperation;

namespace Garnet.test
{
    [TestFixture]
    public class RespCommandTests
    {
        GarnetServer server;
        private IReadOnlyDictionary<string, RespCommandsInfo> respCommandsInfo;
        private IReadOnlyDictionary<string, RespCommandsInfo> respCustomCommandsInfo;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            Assert.IsTrue(TestUtils.TryGetCommandsInfo(NullLogger.Instance, out respCommandsInfo));
            Assert.IsTrue(TestUtils.TryGetCustomCommandsInfo(NullLogger.Instance, out respCustomCommandsInfo));
            Assert.IsNotNull(respCommandsInfo);
            Assert.IsNotNull(respCustomCommandsInfo);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void GetCommandsInfoTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Get all commands using COMMAND command
            var results = (RedisResult[])db.Execute("COMMAND");

            Assert.IsNotNull(results);
            Assert.AreEqual(respCommandsInfo.Count, results.Length);

            // Get all commands using COMMAND INFO command
            results = (RedisResult[])db.Execute("COMMAND", "INFO");

            Assert.IsNotNull(results);
            Assert.AreEqual(respCommandsInfo.Count, results.Length);

            // Get command count
            var commandCount = (int)db.Execute("COMMAND", "COUNT");
            
            Assert.AreEqual(respCommandsInfo.Count, commandCount);

            // Register custom commands
            var factory = new MyDictFactory();
            server.Register.NewCommand("SETIFPM", 2, CommandType.ReadModifyWrite, new SetIfPMCustomCommand(), respCustomCommandsInfo["SETIFPM"]);
            server.Register.NewCommand("MYDICTSET", 2, CommandType.ReadModifyWrite, factory, respCustomCommandsInfo["MYDICTSET"]);
            server.Register.NewCommand("MYDICTGET", 1, CommandType.Read, factory, respCustomCommandsInfo["MYDICTGET"]);

            // Get all commands (including custom commands) using COMMAND command
            results = (RedisResult[])db.Execute("COMMAND");

            Assert.IsNotNull(results);
            Assert.AreEqual(respCommandsInfo.Count + 3, results.Length);

            // Get all commands (including custom commands) using COMMAND INFO command
            results = (RedisResult[])db.Execute("COMMAND", "INFO");

            Assert.IsNotNull(results);
            Assert.AreEqual(respCommandsInfo.Count + 3, results.Length);

            // Get command count (including custom commands)
            commandCount = (int)db.Execute("COMMAND", "COUNT");

            Assert.AreEqual(respCommandsInfo.Count + 3, commandCount);
        }

        [Test]
        public void GetCommandsInfoWithCommandNamesTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Get basic commands using COMMAND INFO command
            var results = (RedisResult[])db.Execute("COMMAND", "INFO", "GET", "SET");

            Assert.IsNotNull(results);
            Assert.AreEqual(2, results.Length);

            var getInfo = (RedisResult[])results[0];
            VerifyCommandInfo("GET", getInfo);

            var setInfo = (RedisResult[])results[1];
            VerifyCommandInfo("SET", setInfo);
        }

        private void VerifyCommandInfo(string cmdName, RedisResult[] result)
        {
            Assert.IsTrue(respCommandsInfo.ContainsKey(cmdName));
            var cmdInfo = respCommandsInfo[cmdName];

            Assert.IsNotNull(result);
            Assert.AreEqual(10, result.Length);
            Assert.AreEqual(cmdInfo.Name, (string)result[0]);
            Assert.AreEqual(cmdInfo.Arity, (int)result[1]);
        }

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

        [Test]
        public unsafe void RespCommandTestTest()
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
                    Assert.IsTrue(RespCommandInfoParser.TryReadFromResp(ref ptr, end, out RespCommandsInfo command));
                    Assert.IsNotNull(command);
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
