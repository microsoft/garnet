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
    /// <summary>
    /// This test class tests the RESP COMMAND and COMMAND INFO commands
    /// </summary>
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

        /// <summary>
        /// Verify that all existing combinations of RespCommand and subcommand byte (if relevant)
        /// have a matching RespCommandInfo objects defined in RespCommandsInfo
        /// </summary>
        [Test]
        public void CommandsInfoCoverageTest()
        {
            // Get all command-subcommand combinations that have RespCommandInfo objects defined
            var existingCombinations = new HashSet<RespCommand>();
            foreach (var commandInfo in respCommandsInfo.Values)
            {
                if (!existingCombinations.Contains(commandInfo.Command))
                    existingCombinations.Add(commandInfo.Command);
            }

            // RespCommands that can be ignored
            var ignoreCommands = new HashSet<RespCommand>()
            {
                RespCommand.NONE,
                RespCommand.COSCAN,
                RespCommand.CustomCmd,
                RespCommand.CustomObjCmd,
                RespCommand.CustomTxn,
                RespCommand.INVALID,
                RespCommand.SETEXNX,
                RespCommand.SETEXXX,
                RespCommand.SETKEEPTTL,
                RespCommand.SETKEEPTTLXX,
            };

            var missingCombinations = new List<(RespCommand, byte)>();
            foreach (var respCommand in Enum.GetValues<RespCommand>())
            {
                if (ignoreCommands.Contains(respCommand)) continue;

                if (!existingCombinations.Contains(respCommand))
                    missingCombinations.Add((respCommand, 0));
            }

            // Verify that there are no missing combinations
            Assert.IsEmpty(missingCombinations);
        }

        /// <summary>
        /// Test COMMAND command
        /// </summary>
        [Test]
        public void CommandTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Get all commands using COMMAND command
            var results = (RedisResult[])db.Execute("COMMAND");

            Assert.IsNotNull(results);
            Assert.AreEqual(respCommandsInfo.Count, results.Length);

            // Register custom commands
            var customCommandsRegistered = RegisterCustomCommands();

            // Get all commands (including custom commands) using COMMAND command
            results = (RedisResult[])db.Execute("COMMAND");

            Assert.IsNotNull(results);
            Assert.AreEqual(respCommandsInfo.Count + customCommandsRegistered, results.Length);
        }

        /// <summary>
        /// Test COMMAND INFO command
        /// </summary>
        [Test]
        public void CommandInfoTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Get all commands using COMMAND INFO command
            var results = (RedisResult[])db.Execute("COMMAND", "INFO");

            Assert.IsNotNull(results);
            Assert.AreEqual(respCommandsInfo.Count, results.Length);

            // Register custom commands
            var customCommandsRegistered = RegisterCustomCommands();

            // Get all commands (including custom commands) using COMMAND INFO command
            results = (RedisResult[])db.Execute("COMMAND", "INFO");

            Assert.IsNotNull(results);
            Assert.AreEqual(respCommandsInfo.Count + customCommandsRegistered, results.Length);
        }

        /// <summary>
        /// Test COMMAND COUNT command
        /// </summary>
        [Test]
        public void CommandCountTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Get command count
            var commandCount = (int)db.Execute("COMMAND", "COUNT");

            Assert.AreEqual(respCommandsInfo.Count, commandCount);

            // Register custom commands
            var customCommandsRegistered = RegisterCustomCommands();

            // Get command count (including custom commands)
            commandCount = (int)db.Execute("COMMAND", "COUNT");

            Assert.AreEqual(respCommandsInfo.Count + customCommandsRegistered, commandCount);
        }

        /// <summary>
        /// Test COMMAND DOCS command
        /// This is not yet implemented, yet it should return an empty array
        /// so to not crash clients that use this command at initialization
        /// </summary>
        [Test]
        public void CommandDocsTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Get all commands using COMMAND INFO command
            var results = (RedisResult[])db.Execute("COMMAND", "DOCS");

            Assert.IsNotNull(results);
            Assert.IsEmpty(results);
        }

        /// <summary>
        /// Test COMMAND with unknown subcommand
        /// </summary>
        [Test]
        public void CommandUnknownSubcommandTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var unknownSubCommand = "UNKNOWN";

            // Get all commands using COMMAND INFO command
            try
            {
                db.Execute("COMMAND", unknownSubCommand);
                Assert.Fail();
            }
            catch (RedisServerException e)
            {
                Assert.AreEqual("ERR unknown command", e.Message);
            }
        }

        /// <summary>
        /// Test COMMAND INFO [command-name [command-name ...]]
        /// </summary>
        [Test]
        public void CommandInfoWithCommandNamesTest()
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

        private int RegisterCustomCommands()
        {
            var factory = new MyDictFactory();
            server.Register.NewCommand("SETIFPM", 2, CommandType.ReadModifyWrite, new SetIfPMCustomCommand(), respCustomCommandsInfo["SETIFPM"]);
            server.Register.NewCommand("MYDICTSET", 2, CommandType.ReadModifyWrite, factory, respCustomCommandsInfo["MYDICTSET"]);
            server.Register.NewCommand("MYDICTGET", 1, CommandType.Read, factory, respCustomCommandsInfo["MYDICTGET"]);

            return 3;
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