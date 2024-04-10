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
            public string Command { get; init; }

            public string[] SubCommands { get; init; }

            public SupportedCommand(string command, string[] subcommands)
            {
                this.Command = command;
                this.SubCommands = subcommands;
            }
        }

        private static Dictionary<string, string[]> Commands = new(StringComparer.OrdinalIgnoreCase)
        {
            { "GET", null },
            { "SET", null },
            { "GETRANGE", null },
            { "SETRANGE", null },
            { "PFADD", null },
            { "PFCOUNT", null },
            { "PFMERGE", null },
            { "SETEX", null },
            { "PSETEX", null },
            { "DEL", null },
            { "EXISTS", null },
            { "RENAME", null },
            { "INCR", null },
            { "INCRBY", null },
            { "DECR", null },
            { "DECRBY", null },
            { "EXPIRE", null },
            { "PEXPIRE", null },
            { "PERSIST", null },
            { "TTL", null },
            { "PTTL", null },
            { "SETBIT", null },
            { "GETBIT", null },
            { "BITCOUNT", null },
            { "BITPOS", null },
            { "BITFIELD", null },
            { "MSET", null },
            { "MSETNX", null },
            { "MGET", null },
            { "UNLINK", null },
            { "MULTI", null },
            { "EXEC", null },
            { "WATCH", null },
            { "UNWATCH", null },
            { "DISCARD", null },
            { "GETDEL", null },
            { "APPEND", null },
            { "ECHO", null },
            { "REPLICAOF", null },
            { "SLAVEOF", null },
            { "CONFIG", new[] { "CONFIG|GET", "CONFIG|SET", "CONFIG|REWRITE" } },
            { "CLIENT", null },
            { "ZADD", null },
            { "ZMSCORE", null },
            { "ZREM", null },
            { "ZCARD", null },
            { "ZPOPMAX", null },
            { "ZSCORE", null },
            { "ZCOUNT", null },
            { "ZINCRBY", null },
            { "ZRANK", null },
            { "ZRANGE", null },
            { "ZRANGEBYSCORE", null },
            { "ZREVRANK", null },
            { "ZREMRANGEBYLEX", null },
            { "ZREMRANGEBYRANK", null },
            { "ZREMRANGEBYSCORE", null },
            { "ZLEXCOUNT", null },
            { "ZPOPMIN", null },
            { "ZRANDMEMBER", null },
            { "GEOADD", null },
            { "GEOHASH", null },
            { "GEODIST", null },
            { "GEOPOS", null },
            { "GEOSEARCH", null },
            { "ZREVRANGE", null },
            { "ZSCAN", null },
            { "LPUSH", null },
            { "LPOP", null },
            { "RPUSH", null },
            { "RPOP", null },
            { "LLEN", null },
            { "LTRIM", null },
            { "LRANGE", null },
            { "LINDEX", null },
            { "LINSERT", null },
            { "LREM", null },
            { "HSET", null },
            { "HMSET", null },
            { "HGET", null },
            { "HMGET", null },
            { "HGETALL", null },
            { "HDEL", null },
            { "HLEN", null },
            { "HEXISTS", null },
            { "HKEYS", null },
            { "HVALS", null },
            { "HINCRBY", null },
            { "HINCRBYFLOAT", null },
            { "HSETNX", null },
            { "HRANDFIELD", null },
            { "HSCAN", null },
            { "HSTRLEN", null },
            { "SADD", null },
            { "SMEMBERS", null },
            { "SREM", null },
            { "SCARD", null },
            { "SPOP", null },
            { "SSCAN", null },
            {
                "ACL",
                new[] { "ACL|CAT", "ACL|DELUSER", "ACL|LIST", "ACL|LOAD", "ACL|SETUSER", "ACL|USERS", "ACL|WHOAMI" }
            },
            {
                "CLUSTER",
                new[]
                {
                    "CLUSTER|ADDSLOTS", "CLUSTER|ADDSLOTSRANGE", "CLUSTER|BUMPEPOCH", "CLUSTER|COUNTKEYSINSLOT",
                    "CLUSTER|DELSLOTS", "CLUSTER|DELSLOTSRANGE", "CLUSTER|FAILOVER", "CLUSTER|FORGET",
                    "CLUSTER|GETKEYSINSLOT", "CLUSTER|INFO", "CLUSTER|KEYSLOT", "CLUSTER|MEET", "CLUSTER|MYID",
                    "CLUSTER|NODES", "CLUSTER|REPLICAS", "CLUSTER|REPLICATE", "CLUSTER|RESET",
                    "CLUSTER|SET-CONFIG-EPOCH", "CLUSTER|SETSLOT", "CLUSTER|SLOTS"
                }
            },
            { "MEMORY", new[] { "MEMORY|USAGE" } },
            { "LATENCY", new[] { "LATENCY|HISTOGRAM", "LATENCY|RESET" } }
        };

        [Test]
        public unsafe void RespCommandTestTest()
        {
            var lightClient = new LightClientRequest("127.0.0.1", 6379, 0);
            var response = lightClient.SendCommand($"COMMAND INFO {string.Join(' ', Commands.Keys)}");

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
                var supportedCommands = Commands.OrderBy(kvp => kvp.Key).Select(c => new SupportedCommand(c.Key, c.Value)).ToArray();
                jsonString = JsonSerializer.Serialize(supportedCommands, serializerOptions);
                File.WriteAllText(outputPath2, jsonString);
            }
        }


    }
}
