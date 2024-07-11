// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using Garnet.server;
using NUnit.Framework;
using StackExchange.Redis;

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
            Assert.IsTrue(RespCommandsInfo.TryGetRespCommandsInfo(out respCommandsInfo, externalOnly: true));
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
        [TestCase(new object[] { "GET", "SET", "COSCAN" })]
        [TestCase(new object[] { "get", "set", "coscan" })]
        public void CommandInfoWithCommandNamesTest(params string[] commands)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var args = new object[] { "INFO" }.Union(commands).ToArray();

            // Get basic commands using COMMAND INFO command
            var results = (RedisResult[])db.Execute("COMMAND", args);

            Assert.IsNotNull(results);
            Assert.AreEqual(commands.Length, results.Length);

            for (var i = 0; i < commands.Length; i++)
            {
                var info = results[i];
                VerifyCommandInfo(commands[i], info);
            }
        }

        /// <summary>
        /// Test COMMAND INFO with custom commands
        /// </summary>
        [Test]
        [TestCase(new object[] { "SETIFPM", "MYDICTSET", "MGETIFPM", "READWRITETX", "MYDICTGET" })]
        [TestCase(new object[] { "setifpm", "mydictset", "mgetifpm", "readwritetx", "mydictget" })]
        public void CommandInfoWithCustomCommandNamesTest(params string[] commands)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Register custom commands
            RegisterCustomCommands();

            // Dynamically register custom commands
            DynamicallyRegisterCustomCommands(db);

            var args = new object[] { "INFO" }.Union(commands).ToArray();

            // Get basic commands using COMMAND INFO command
            var results = (RedisResult[])db.Execute("COMMAND", args);

            Assert.IsNotNull(results);
            Assert.AreEqual(commands.Length, results.Length);

            for (var i = 0; i < commands.Length; i++)
            {
                var info = results[i];
                VerifyCommandInfo(commands[i], info);
            }
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