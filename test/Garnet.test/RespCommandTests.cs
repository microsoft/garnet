// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
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
        private string extTestDir;
        private IReadOnlyDictionary<string, RespCommandsInfo> respCommandsInfo;
        private IReadOnlyDictionary<string, RespCommandsInfo> respCustomCommandsInfo;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            extTestDir = Path.Combine(TestUtils.MethodTestDir, "test");
            Assert.IsTrue(RespCommandsInfo.TryGetRespCommandsInfo(out respCommandsInfo));
            Assert.IsTrue(TestUtils.TryGetCustomCommandsInfo(out respCustomCommandsInfo));
            Assert.IsNotNull(respCommandsInfo);
            Assert.IsNotNull(respCustomCommandsInfo);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: true,
                extensionBinPaths: [extTestDir]);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.DeleteDirectory(Directory.GetParent(extTestDir)?.FullName);
        }

        /// <summary>
        /// Verify that all existing combinations of RespCommand and subcommand byte (if relevant)
        /// have a matching RespCommandInfo objects defined in RespCommandsInfo
        /// </summary>
        [Test]
        public void CommandsInfoCoverageTest()
        {
            // Get all command-subcommand combinations that have RespCommandInfo objects defined
            var existingCombinations = new Dictionary<RespCommand, HashSet<byte>>();
            foreach (var commandInfo in respCommandsInfo.Values)
            {
                if (!existingCombinations.ContainsKey(commandInfo.Command))
                    existingCombinations.Add(commandInfo.Command, new HashSet<byte>());
                if (commandInfo.ArrayCommand.HasValue)
                    existingCombinations[commandInfo.Command].Add(commandInfo.ArrayCommand.Value);
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

            // Dynamically register custom commands
            var customCommandsRegisteredDyn = DynamicallyRegisterCustomCommands(db);

            // Get all commands (including custom commands) using COMMAND command
            results = (RedisResult[])db.Execute("COMMAND");

            Assert.IsNotNull(results);
            Assert.AreEqual(respCommandsInfo.Count + customCommandsRegistered.Length + customCommandsRegisteredDyn.Length, results.Length);
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

            // Dynamically register custom commands
            var customCommandsRegisteredDyn = DynamicallyRegisterCustomCommands(db);

            // Get all commands (including custom commands) using COMMAND INFO command
            results = (RedisResult[])db.Execute("COMMAND", "INFO");

            Assert.IsNotNull(results);
            Assert.AreEqual(respCommandsInfo.Count + customCommandsRegistered.Length + customCommandsRegisteredDyn.Length, results.Length);

            Assert.IsTrue(results.All(res => res.Length == 10));
            Assert.IsTrue(results.All(res => (string)res[0] != null));
            var cmdNameToResult = results.ToDictionary(res => (string)res[0], res => res);

            foreach (var cmdName in respCommandsInfo.Keys.Union(customCommandsRegistered).Union(customCommandsRegisteredDyn))
            {
                Assert.Contains(cmdName, cmdNameToResult.Keys);
                VerifyCommandInfo(cmdName, cmdNameToResult[cmdName]);
            }
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

            // Dynamically register custom commands
            var customCommandsRegisteredDyn = DynamicallyRegisterCustomCommands(db);

            // Get command count (including custom commands)
            commandCount = (int)db.Execute("COMMAND", "COUNT");

            Assert.AreEqual(respCommandsInfo.Count + customCommandsRegistered.Length + customCommandsRegisteredDyn.Length, commandCount);
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
                var expectedErrorMessage = string.Format(CmdStrings.GenericErrUnknownSubCommand, unknownSubCommand, RespCommand.COMMAND);
                Assert.AreEqual(expectedErrorMessage, e.Message);
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

            var getInfo = results[0];
            VerifyCommandInfo("GET", getInfo);

            var setInfo = results[1];
            VerifyCommandInfo("SET", setInfo);
        }

        private string[] RegisterCustomCommands()
        {
            var registeredCommands = new[] { "SETIFPM", "MYDICTSET", "MGETIFPM" };

            var factory = new MyDictFactory();
            server.Register.NewCommand("SETIFPM", 2, CommandType.ReadModifyWrite, new SetIfPMCustomCommand(), respCustomCommandsInfo["SETIFPM"]);
            server.Register.NewCommand("MYDICTSET", 2, CommandType.ReadModifyWrite, factory, respCustomCommandsInfo["MYDICTSET"]);
            server.Register.NewTransactionProc("MGETIFPM", () => new MGetIfPM(), respCustomCommandsInfo["MGETIFPM"]);

            return registeredCommands;
        }

        private (string, string) CreateTestLibrary()
        {
            var runtimePath = RuntimeEnvironment.GetRuntimeDirectory();
            var binPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            Assert.IsNotNull(binPath);

            var namespaces = new[]
            {
                "Tsavorite.core",
                "Garnet.common",
                "Garnet.server",
                "System",
                "System.Buffers",
                "System.Collections.Generic",
                "System.Diagnostics",
                "System.IO",
                "System.Text",
            };

            var referenceFiles = new[]
            {
                Path.Combine(runtimePath, "System.dll"),
                Path.Combine(runtimePath, "System.Collections.dll"),
                Path.Combine(runtimePath, "System.Core.dll"),
                Path.Combine(runtimePath, "System.Private.CoreLib.dll"),
                Path.Combine(runtimePath, "System.Runtime.dll"),
                Path.Combine(binPath, "Tsavorite.core.dll"),
                Path.Combine(binPath, "Garnet.common.dll"),
                Path.Combine(binPath, "Garnet.server.dll"),
            };

            var dir1 = Path.Combine(this.extTestDir, Path.GetFileName(TestUtils.MethodTestDir));

            Directory.CreateDirectory(this.extTestDir);
            Directory.CreateDirectory(dir1);

            var libPathToFiles = new Dictionary<string, string[]>
            {
                {
                    Path.Combine(dir1, "testLib1.dll"),
                    new[]
                    {
                        Path.GetFullPath(@"../main/GarnetServer/Extensions/MyDictObject.cs", TestUtils.RootTestsProjectPath),
                        Path.GetFullPath(@"../main/GarnetServer/Extensions/ReadWriteTxn.cs", TestUtils.RootTestsProjectPath)
                    }
                },
            };

            foreach (var ltf in libPathToFiles)
            {
                TestUtils.CreateTestLibrary(namespaces, referenceFiles, ltf.Value, ltf.Key);
            }

            var cmdInfoPath = Path.Combine(dir1, Path.GetFileName(TestUtils.CustomRespCommandInfoJsonPath)!);
            File.Copy(TestUtils.CustomRespCommandInfoJsonPath!, cmdInfoPath);

            return (cmdInfoPath, Path.Combine(dir1, "testLib1.dll"));
        }

        private string[] DynamicallyRegisterCustomCommands(IDatabase db)
        {
            var registeredCommands = new[] { "READWRITETX", "MYDICTGET" };
            var (cmdInfoPath, srcPath) = CreateTestLibrary();

            var args = new List<object>
            {
                "TXN", "READWRITETX", 3, "ReadWriteTxn",
                "READ", "MYDICTGET", 1, "MyDictFactory",
                "INFO", cmdInfoPath,
                "SRC", srcPath
            };

            // Register select custom commands and transactions
            var result = (string)db.Execute($"REGISTERCS",
                args.ToArray());
            Assert.AreEqual("OK", result);

            return registeredCommands;
        }

        private void VerifyCommandInfo(string cmdName, RedisResult result)
        {
            RespCommandsInfo cmdInfo = default;
            if (respCommandsInfo.ContainsKey(cmdName))
            {
                cmdInfo = respCommandsInfo[cmdName];
            }
            else if (respCustomCommandsInfo.ContainsKey(cmdName))
            {
                cmdInfo = respCustomCommandsInfo[cmdName];
            }
            else Assert.Fail();

            Assert.IsNotNull(result);
            Assert.AreEqual(10, result.Length);
            Assert.AreEqual(cmdInfo.Name, (string)result[0]);
            Assert.AreEqual(cmdInfo.Arity, (int)result[1]);
        }
    }
}