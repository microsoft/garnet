// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.server;
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
            Assert.IsTrue(RespCommandsInfo.TryGetRespCommandsInfo(out respCommandsInfo));
            Assert.IsTrue(TestUtils.TryGetCustomCommandsInfo(out respCustomCommandsInfo));
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
        public void CommandsInfoCoverageTest()
        {
            var existingCombinations = new Dictionary<RespCommand, HashSet<byte>>();
            foreach (var commandInfo in respCommandsInfo.Values)
            {
                if (!existingCombinations.ContainsKey(commandInfo.Command))
                    existingCombinations.Add(commandInfo.Command, new HashSet<byte>());
                if (commandInfo.ArrayCommand.HasValue)
                    existingCombinations[commandInfo.Command].Add(commandInfo.ArrayCommand.Value);
            }

            var ignoreCommands = new HashSet<RespCommand>()
            {
                RespCommand.NONE,
                RespCommand.COSCAN,
                RespCommand.CustomCmd,
                RespCommand.CustomObjCmd,
                RespCommand.CustomTxn,
                RespCommand.INVALID,
            };

            var missingCombinations = new List<(RespCommand, byte)>();
            foreach (var respCommand in Enum.GetValues<RespCommand>())
            {
                if (ignoreCommands.Contains(respCommand)) continue;

                var arrayCommandEnumType = (respCommand) switch
                {
                    RespCommand.Set => typeof(SetOperation),
                    RespCommand.Hash => typeof(HashOperation),
                    RespCommand.List => typeof(ListOperation),
                    RespCommand.SortedSet => typeof(SortedSetOperation),
                    _ => default
                };

                if (arrayCommandEnumType != default)
                {
                    foreach (var arrayCommand in Enum.GetValues(arrayCommandEnumType))
                    {
                        if (!existingCombinations.ContainsKey(respCommand) ||
                            !existingCombinations[respCommand].Contains((byte)arrayCommand))
                        {
                            missingCombinations.Add((respCommand, (byte)arrayCommand));
                        }
                    }
                }
                else if (respCommand == RespCommand.All)
                {
                    if (!existingCombinations.ContainsKey(respCommand) ||
                        !existingCombinations[respCommand].Contains((byte)RespCommand.COSCAN))
                    {
                        missingCombinations.Add((respCommand, (byte)RespCommand.COSCAN));
                    }
                }
                else
                {
                    if (!existingCombinations.ContainsKey(respCommand))
                        missingCombinations.Add((respCommand, 0));
                }
            }

            Assert.IsEmpty(missingCombinations);
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
    }
}