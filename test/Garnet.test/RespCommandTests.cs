// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using Allure.NUnit;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// This test class tests the RESP COMMAND and COMMAND INFO commands
    /// </summary>
    [AllureNUnit]
    [TestFixture]
    public class RespCommandTests : AllureTestBase
    {
        GarnetServer server;
        private string extTestDir;
        private IReadOnlyDictionary<string, RespCommandsInfo> respCommandsInfo;
        private IReadOnlyDictionary<string, RespCommandsInfo> externalRespCommandsInfo;
        private IReadOnlyDictionary<string, RespCommandsInfo> respSubCommandsInfo;
        private IReadOnlyDictionary<string, RespCommandDocs> respCommandsDocs;
        private IReadOnlyDictionary<string, RespCommandDocs> externalRespCommandsDocs;
        private IReadOnlyDictionary<string, RespCommandDocs> respSubCommandsDocs;
        private IReadOnlyDictionary<string, RespCommandsInfo> respCustomCommandsInfo;
        private IReadOnlyDictionary<string, RespCommandDocs> respCustomCommandsDocs;
        private HashSet<RespCommand> internalOnlyCommands;

        // List of commands that don't require metadata (info / docs)
        // These are either marker commands (e.g. NONE, INVALID)
        // or pseudo-commands or subcommands that are only used internally (e.g. SETEXNX, SETEXXX, etc.)
        private readonly HashSet<RespCommand> noMetadataCommands =
        [
            RespCommand.NONE,
            RespCommand.SETEXNX,
            RespCommand.SETEXXX,
            RespCommand.SETKEEPTTL,
            RespCommand.SETKEEPTTLXX,
            RespCommand.BITOP_AND,
            RespCommand.BITOP_OR,
            RespCommand.BITOP_XOR,
            RespCommand.BITOP_NOT,
            RespCommand.BITOP_DIFF,
            RespCommand.INVALID,
            RespCommand.DELIFEXPIM
        ];

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            extTestDir = Path.Combine(TestUtils.MethodTestDir, "test");
            ClassicAssert.IsTrue(RespCommandsInfo.TryGetRespCommandsInfo(out respCommandsInfo));
            ClassicAssert.IsTrue(RespCommandsInfo.TryGetRespCommandsInfo(out externalRespCommandsInfo, externalOnly: true));
            ClassicAssert.IsTrue(RespCommandsInfo.TryGetRespSubCommandsInfo(out respSubCommandsInfo));
            ClassicAssert.IsTrue(RespCommandDocs.TryGetRespCommandsDocs(out respCommandsDocs));
            ClassicAssert.IsTrue(RespCommandDocs.TryGetRespCommandsDocs(out externalRespCommandsDocs, externalOnly: true));
            ClassicAssert.IsTrue(RespCommandDocs.TryGetRespSubCommandsDocs(out respSubCommandsDocs));
            ClassicAssert.IsTrue(TestUtils.TryGetCustomCommandsInfo(out respCustomCommandsInfo));
            ClassicAssert.IsTrue(TestUtils.TryGetCustomCommandsDocs(out respCustomCommandsDocs));
            ClassicAssert.IsNotNull(respCommandsInfo);
            ClassicAssert.IsNotNull(externalRespCommandsInfo);
            ClassicAssert.IsNotNull(respCommandsDocs);
            ClassicAssert.IsNotNull(externalRespCommandsDocs);
            ClassicAssert.IsNotNull(respCustomCommandsInfo);
            ClassicAssert.IsNotNull(respCustomCommandsDocs);
            internalOnlyCommands =
            [
                .. respCommandsInfo.Values.Where(ci => ci.IsInternal).Select(ci => ci.Command)
                    .Union(respSubCommandsInfo.Values.Where(sc => sc.IsInternal).Select(sc => sc.Command))
            ];

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: true,
                enableModuleCommand: Garnet.server.Auth.Settings.ConnectionProtectionOption.Yes,
                extensionBinPaths: [extTestDir]);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown(waitForDelete: true);
            TestUtils.DeleteDirectory(Directory.GetParent(extTestDir)?.FullName);
        }

        /// <summary>
        /// Verify that all existing values of RespCommand
        /// have a matching RespCommandInfo objects defined in RespCommandsInfo.json / GarnetCommandInfo.json
        /// </summary>
        [Test]
        public void CommandsInfoCoverageTest()
        {
            // Get all commands that have RespCommandInfo objects defined
            var commandsWithInfo = new HashSet<RespCommand>();
            foreach (var commandInfo in respCommandsInfo.Values)
            {
                commandsWithInfo.Add(commandInfo.Command);

                if (commandInfo.SubCommands != null)
                {
                    foreach (var subCommandInfo in commandInfo.SubCommands)
                    {
                        commandsWithInfo.Add(subCommandInfo.Command);
                    }
                }
            }

            var allCommands = Enum.GetValues<RespCommand>().Except(noMetadataCommands);
            CollectionAssert.AreEquivalent(allCommands, commandsWithInfo, "Some commands have missing info. Please see https://microsoft.github.io/garnet/docs/dev/garnet-api#adding-command-info for more details.");
        }

        /// <summary>
        /// Verify that all existing values of RespCommand
        /// have a matching RespCommandDocs objects defined in RespCommandsDocs.json / GarnetCommandDocs.json
        /// </summary>
        [Test]
        public void CommandsDocsCoverageTest()
        {
            // Get all commands that have RespCommandInfo objects defined
            var commandsWithDocs = new HashSet<RespCommand>();
            foreach (var commandDocs in respCommandsDocs.Values)
            {
                commandsWithDocs.Add(commandDocs.Command);

                if (commandDocs.SubCommands != null)
                {
                    foreach (var subCommandInfo in commandDocs.SubCommands)
                    {
                        commandsWithDocs.Add(subCommandInfo.Command);
                    }
                }
            }

            var allCommands = Enum.GetValues<RespCommand>().Except(noMetadataCommands).Except(internalOnlyCommands);
            Assert.That(commandsWithDocs, Is.SupersetOf(allCommands),
                "Some commands have missing docs. Please see https://microsoft.github.io/garnet/docs/dev/garnet-api#adding-command-info for more details.");
        }

        /// <summary>
        /// Verify info in SimpleRespCommandInfo matches information in full RespCommandsInfo
        /// </summary>
        [Test]
        public void SimpleCommandsInfoTest()
        {
            var actualSimpleCommandInfo = new Dictionary<RespCommand, SimpleRespCommandInfo>();
            var allCommands = Enum.GetValues<RespCommand>().Except(noMetadataCommands).ToHashSet();

            // Get actual SimpleRespCommandInfo for all commands
            foreach (var cmd in allCommands)
            {
                if (RespCommandsInfo.TryGetSimpleRespCommandInfo(cmd, out var cmdSimpleInfo))
                    actualSimpleCommandInfo[cmd] = cmdSimpleInfo;
            }

            // Verify that all commands have SimpleRespCommandInfo
            CollectionAssert.AreEquivalent(allCommands, actualSimpleCommandInfo.Keys);

            // Get full RespCommandsInfo for all commands
            var fullCommandInfo = new Dictionary<RespCommand, RespCommandsInfo>();
            foreach (var commandInfo in respCommandsInfo.Values)
            {
                fullCommandInfo.TryAdd(commandInfo.Command, commandInfo);

                if (commandInfo.SubCommands != null)
                {
                    foreach (var subCommandInfo in commandInfo.SubCommands)
                    {
                        fullCommandInfo.Add(subCommandInfo.Command, subCommandInfo);
                    }
                }
            }

            // Verify that all commands have both SimpleRespCommandInfo and RespCommandsInfo
            CollectionAssert.AreEquivalent(fullCommandInfo.Keys, actualSimpleCommandInfo.Keys);

            // Populate set of commands with mismatched info
            var offendingCommands = new HashSet<RespCommand>();
            foreach (var cmd in allCommands)
            {
                var actualCmdInfo = actualSimpleCommandInfo[cmd];
                var cmdInfo = fullCommandInfo[cmd];
                var expArity = cmdInfo.Arity;
                var expIsParent = (cmdInfo.SubCommands?.Length ?? 0) > 0;
                var expIsSubCommand = cmdInfo.Parent != null;
                var expAllowedInTxn = (cmdInfo.Flags & RespCommandFlags.NoMulti) == 0;

                if (actualCmdInfo.Arity != expArity ||
                    actualCmdInfo.IsParent != expIsParent ||
                    actualCmdInfo.IsSubCommand != expIsSubCommand ||
                    actualCmdInfo.AllowedInTxn != expAllowedInTxn)
                    offendingCommands.Add(cmd);
            }

            // Verify that there are no commands with mismatched info
            CollectionAssert.IsEmpty(offendingCommands);
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

            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(externalRespCommandsInfo.Count, results.Length);

            // Register custom commands
            var customCommandsRegistered = RegisterCustomCommands(["DELIFM", "MGETIFPM", "MYDICTSET", "SETIFPM", "SETWPIFPGT"]);

            // Dynamically register custom commands
            var customCommandsRegisteredDyn = DynamicallyRegisterCustomCommands(db);

            // Get all commands (including custom commands) using COMMAND command
            results = (RedisResult[])db.Execute("COMMAND");

            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(externalRespCommandsInfo.Count + customCommandsRegistered.Length + customCommandsRegisteredDyn.Length, results.Length);
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

            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(externalRespCommandsInfo.Count, results.Length);

            // Register custom commands
            var customCommandsRegistered = RegisterCustomCommands(["DELIFM", "MGETIFPM", "MYDICTSET", "SETIFPM", "SETWPIFPGT"]);

            // Dynamically register custom commands
            var customCommandsRegisteredDyn = DynamicallyRegisterCustomCommands(db);

            // Get all commands (including custom commands) using COMMAND INFO command
            results = (RedisResult[])db.Execute("COMMAND", "INFO");

            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(externalRespCommandsInfo.Count + customCommandsRegistered.Length + customCommandsRegisteredDyn.Length, results.Length);

            ClassicAssert.IsTrue(results.All(res => res.Length == 10));
            ClassicAssert.IsTrue(results.All(res => (string)res[0] != null));
            var cmdNameToResult = results.ToDictionary(res => (string)res[0], res => res);

            foreach (var cmdName in externalRespCommandsInfo.Keys.Union(customCommandsRegistered).Union(customCommandsRegisteredDyn))
            {
                ClassicAssert.Contains(cmdName, cmdNameToResult.Keys);
                VerifyCommandInfo(cmdName, cmdNameToResult[cmdName]);
            }
        }

        /// <summary>
        /// Test COMMAND DOCS command
        /// </summary>
        [Test]
        public void CommandDocsTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Get all commands using COMMAND DOCS command
            var results = (RedisResult[])db.Execute("COMMAND", "DOCS");

            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(externalRespCommandsDocs.Count, results.Length / 2);

            // Register custom commands
            var customCommandsRegistered = RegisterCustomCommands();

            // Dynamically register custom commands
            var customCommandsRegisteredDyn = DynamicallyRegisterCustomCommands(db);

            // Get all commands (including custom commands) using COMMAND DOCS command
            results = (RedisResult[])db.Execute("COMMAND", "DOCS");

            ClassicAssert.IsNotNull(results);
            var expectedCommands =
                externalRespCommandsDocs.Keys
                    .Union(customCommandsRegistered)
                    .Union(customCommandsRegisteredDyn).OrderBy(c => c);

            var cmdNameToResult = new Dictionary<string, RedisResult>();
            for (var i = 0; i < results.Length; i += 2)
            {
                cmdNameToResult.Add(results[i].ToString(), results[i + 1]);
            }

            var actualCommands = cmdNameToResult.Keys.OrderBy(c => c);
            CollectionAssert.AreEqual(expectedCommands, actualCommands);

            foreach (var cmdName in externalRespCommandsDocs.Keys.Union(customCommandsRegistered).Union(customCommandsRegisteredDyn))
            {
                ClassicAssert.Contains(cmdName, cmdNameToResult.Keys);
                VerifyCommandDocs(cmdName, cmdNameToResult[cmdName]);
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

            ClassicAssert.AreEqual(externalRespCommandsInfo.Count, commandCount);

            // Register custom commands
            var customCommandsRegistered = RegisterCustomCommands(["DELIFM", "MGETIFPM", "MYDICTSET", "SETIFPM", "SETWPIFPGT"]);

            // Dynamically register custom commands
            var customCommandsRegisteredDyn = DynamicallyRegisterCustomCommands(db);

            // Get command count (including custom commands)
            commandCount = (int)db.Execute("COMMAND", "COUNT");

            ClassicAssert.AreEqual(externalRespCommandsInfo.Count + customCommandsRegistered.Length + customCommandsRegisteredDyn.Length, commandCount);
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
            var e = Assert.Throws<RedisServerException>(() => db.Execute("COMMAND", unknownSubCommand));
            ClassicAssert.AreEqual("ERR unknown subcommand 'UNKNOWN'.", e.Message);
        }

        /// <summary>
        /// Test COMMAND INFO [command-name [command-name ...]]
        /// </summary>
        [Test]
        [TestCase(["GET", "SET", "COSCAN", "ACL|LOAD", "WATCH"])]
        [TestCase(["get", "set", "coscan", "acl|load", "watch"])]
        public void CommandInfoWithCommandNamesTest(params string[] commands)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var args = new object[] { "INFO" }.Union(commands).ToArray();

            // Get basic commands using COMMAND INFO command
            var results = (RedisResult[])db.Execute("COMMAND", args);

            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(commands.Length, results.Length);

            for (var i = 0; i < commands.Length; i++)
            {
                var info = results[i];
                VerifyCommandInfo(commands[i], info);
            }
        }

        /// <summary>
        /// Test COMMAND DOCS [command-name [command-name ...]]
        /// </summary>
        [Test]
        [TestCase(["GET", "SET", "COSCAN", "ACL|LOAD", "WATCH"])]
        [TestCase(["get", "set", "coscan", "acl|load", "watch"])]
        public void CommandDocsWithCommandNamesTest(params string[] commands)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var args = new object[] { "DOCS" }.Union(commands).ToArray();

            // Get basic commands using COMMAND INFO command
            var results = (RedisResult[])db.Execute("COMMAND", args);

            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(commands.Length, results.Length / 2);

            for (var i = 0; i < commands.Length; i++)
            {
                ClassicAssert.AreEqual(commands[i].ToUpper(), results[2 * i].ToString());
                var info = results[(2 * i) + 1];
                VerifyCommandDocs(commands[i], info);
            }
        }

        /// <summary>
        /// Test COMMAND INFO with custom commands
        /// </summary>
        [Test]
        [TestCase(["SETIFPM", "MYDICTSET", "MGETIFPM", "READWRITETX", "MYDICTGET"])]
        [TestCase(["setifpm", "mydictset", "mgetifpm", "readwritetx", "mydictget"])]
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

            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(commands.Length, results.Length);

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
        [TestCase(["SETIFPM", "MYDICTSET", "MGETIFPM", "READWRITETX", "MYDICTGET"])]
        [TestCase(["setifpm", "mydictset", "mgetifpm", "readwritetx", "mydictget"])]
        public void CommandDocsWithCustomCommandNamesTest(params string[] commands)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Register custom commands
            RegisterCustomCommands();

            // Dynamically register custom commands
            DynamicallyRegisterCustomCommands(db);

            var args = new object[] { "DOCS" }.Union(commands).ToArray();

            // Get basic commands using COMMAND DOCS command
            var results = (RedisResult[])db.Execute("COMMAND", args);

            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(commands.Length, results.Length / 2);

            for (var i = 0; i < commands.Length; i++)
            {
                ClassicAssert.AreEqual(commands[i].ToUpper(), results[2 * i].ToString());
                var info = results[(2 * i) + 1];
                VerifyCommandDocs(commands[i], info);
            }
        }

        [Test]
        public void AofIndependentCommandsTest()
        {
            RespCommand[] aofIndpendentCmds = [
                RespCommand.ASYNC,
                RespCommand.PING,
                RespCommand.SELECT,
                RespCommand.SWAPDB,
                RespCommand.ECHO,
                RespCommand.MONITOR,
                RespCommand.MODULE_LOADCS,
                RespCommand.REGISTERCS,
                RespCommand.INFO,
                RespCommand.TIME,
                RespCommand.LASTSAVE,
                // ACL
                RespCommand.ACL_CAT,
                RespCommand.ACL_DELUSER,
                RespCommand.ACL_GENPASS,
                RespCommand.ACL_GETUSER,
                RespCommand.ACL_LIST,
                RespCommand.ACL_LOAD,
                RespCommand.ACL_SAVE,
                RespCommand.ACL_SETUSER,
                RespCommand.ACL_USERS,
                RespCommand.ACL_WHOAMI,
                // Client
                RespCommand.CLIENT_ID,
                RespCommand.CLIENT_INFO,
                RespCommand.CLIENT_LIST,
                RespCommand.CLIENT_KILL,
                RespCommand.CLIENT_GETNAME,
                RespCommand.CLIENT_SETNAME,
                RespCommand.CLIENT_SETINFO,
                RespCommand.CLIENT_UNBLOCK,
                // Command
                RespCommand.COMMAND,
                RespCommand.COMMAND_COUNT,
                RespCommand.COMMAND_DOCS,
                RespCommand.COMMAND_INFO,
                RespCommand.COMMAND_GETKEYS,
                RespCommand.COMMAND_GETKEYSANDFLAGS,
                RespCommand.MEMORY_USAGE,
                // Config
                RespCommand.CONFIG_GET,
                RespCommand.CONFIG_REWRITE,
                RespCommand.CONFIG_SET,
                // Latency
                RespCommand.LATENCY_HELP,
                RespCommand.LATENCY_HISTOGRAM,
                RespCommand.LATENCY_RESET,
                // Slowlog
                RespCommand.SLOWLOG_HELP,
                RespCommand.SLOWLOG_LEN,
                RespCommand.SLOWLOG_GET,
                RespCommand.SLOWLOG_RESET,
                // Transactions
                RespCommand.MULTI,
            ];

            foreach (var cmd in Enum.GetValues<RespCommand>().Where(cmd => cmd != RespCommand.INVALID))
            {
                var expectedAofIndependence = Array.IndexOf(aofIndpendentCmds, cmd) != -1;
                ClassicAssert.AreEqual(expectedAofIndependence, cmd.IsAofIndependent());
            }
        }

        private string[] RegisterCustomCommands(List<string> registerCommandsSubset = null)
        {
            var commands = new HashSet<string> { "DELIFM", "MGETIFPM", "MYDICTGET", "MYDICTSET", "READWRITETX", "SETIFPM", "SETWPIFPGT" };
            if (registerCommandsSubset != null)
            {
                commands.IntersectWith(registerCommandsSubset);
                CollectionAssert.IsNotEmpty(commands);
            }

            var factory = new MyDictFactory();
            if (commands.Contains("DELIFM"))
                server.Register.NewCommand("DELIFM", CommandType.ReadModifyWrite, new DeleteIfMatchCustomCommand(), respCustomCommandsInfo["DELIFM"], respCustomCommandsDocs["DELIFM"]);
            if (commands.Contains("MGETIFPM"))
                server.Register.NewTransactionProc("MGETIFPM", () => new MGetIfPM(), respCustomCommandsInfo["MGETIFPM"], respCustomCommandsDocs["MGETIFPM"]);
            if (commands.Contains("MYDICTGET"))
                server.Register.NewCommand("MYDICTGET", CommandType.Read, factory, new MyDictGet(), respCustomCommandsInfo["MYDICTGET"], respCustomCommandsDocs["MYDICTGET"]);
            if (commands.Contains("MYDICTSET"))
                server.Register.NewCommand("MYDICTSET", CommandType.ReadModifyWrite, factory, new MyDictSet(), respCustomCommandsInfo["MYDICTSET"], respCustomCommandsDocs["MYDICTSET"]);
            if (commands.Contains("READWRITETX"))
                server.Register.NewTransactionProc("READWRITETX", () => new ReadWriteTxn(), respCustomCommandsInfo["READWRITETX"], respCustomCommandsDocs["READWRITETX"]);
            if (commands.Contains("SETIFPM"))
                server.Register.NewCommand("SETIFPM", CommandType.ReadModifyWrite, new SetIfPMCustomCommand(), respCustomCommandsInfo["SETIFPM"], respCustomCommandsDocs["SETIFPM"]);
            if (commands.Contains("SETWPIFPGT"))
                server.Register.NewCommand("SETWPIFPGT", CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand(), respCustomCommandsInfo["SETWPIFPGT"], respCustomCommandsDocs["SETWPIFPGT"]);

            return commands.ToArray();
        }

        private (string, string, string) CreateTestLibrary()
        {
            var runtimePath = RuntimeEnvironment.GetRuntimeDirectory();
            var binPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            ClassicAssert.IsNotNull(binPath);

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
                    Path.Combine(dir1, "testLib5.dll"),
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

            var cmdDocsPath = Path.Combine(dir1, Path.GetFileName(TestUtils.CustomRespCommandDocsJsonPath)!);
            File.Copy(TestUtils.CustomRespCommandDocsJsonPath!, cmdDocsPath);

            return (cmdInfoPath, cmdDocsPath, Path.Combine(dir1, "testLib5.dll"));
        }

        private string[] DynamicallyRegisterCustomCommands(IDatabase db)
        {
            var registeredCommands = new[] { "READWRITETX", "MYDICTGET" };
            var (cmdInfoPath, cmdDocsPath, srcPath) = CreateTestLibrary();

            var args = new List<object>
            {
                "TXN", "READWRITETX", 3, "ReadWriteTxn",
                "READ", "MYDICTGET", 1, "MyDictFactory",
                "INFO", cmdInfoPath,
                "DOCS", cmdDocsPath,
                "SRC", srcPath
            };

            // Register select custom commands and transactions
            var result = (string)db.Execute($"REGISTERCS",
                [.. args]);
            ClassicAssert.AreEqual("OK", result);

            return registeredCommands;
        }

        private void VerifyCommandInfo(string cmdName, RedisResult result)
        {
            if (!respCommandsInfo.TryGetValue(cmdName, out var cmdInfo) &&
                !respSubCommandsInfo.TryGetValue(cmdName, out cmdInfo) &&
                !respCustomCommandsInfo.TryGetValue(cmdName, out cmdInfo))
                Assert.Fail();

            ClassicAssert.IsNotNull(result);
            ClassicAssert.AreEqual(10, result.Length);
            ClassicAssert.AreEqual(cmdInfo.Name, (string)result[0]);
            ClassicAssert.AreEqual(cmdInfo.Arity, (int)result[1]);
        }

        private void VerifyCommandDocs(string cmdName, RedisResult result)
        {
            ClassicAssert.IsNotNull(result);

            if (!externalRespCommandsDocs.TryGetValue(cmdName, out var cmdDoc) &&
                !respSubCommandsDocs.TryGetValue(cmdName, out cmdDoc) &&
                !respCustomCommandsDocs.TryGetValue(cmdName, out cmdDoc))
                Assert.Fail();

            for (var i = 0; i < result.Length; i += 2)
            {
                var key = result[i].ToString();
                var value = result[i + 1];

                switch (key)
                {
                    case "summary":
                        ClassicAssert.AreEqual(cmdDoc.Summary, value.ToString());
                        break;
                    case "group":
                        if (cmdDoc.Group == RespCommandGroup.None) continue;
                        ClassicAssert.IsTrue(EnumUtils.TryParseEnumFromDescription(value.ToString(), out RespCommandGroup group));
                        ClassicAssert.AreEqual(cmdDoc.Group, group);
                        break;
                    case "arguments":
                        ClassicAssert.AreEqual(cmdDoc.Arguments.Length, value.Length);
                        break;
                    case "subcommands":
                        ClassicAssert.AreEqual(cmdDoc.SubCommands.Length, value.Length / 2);
                        break;
                }
            }
        }

        /// <summary>
        /// Test COMMAND GETKEYS command with various command signatures
        /// </summary>
        [Test]
        [TestCase("SET", new[] { "mykey", "value" }, false, new[] { "mykey" })]
        [TestCase("MSET", new[] { "key1", "value1", "key2", "value2" }, false, new[] { "key1", "key2" })]
        [TestCase("MGET", new[] { "key1", "key2", "key3" }, false, new[] { "key1", "key2", "key3" })]
        [TestCase("ZUNIONSTORE", new[] { "destination", "2", "key1", "key2" }, false, new[] { "destination", "key1", "key2" })]
        [TestCase("EVAL", new[] { "return redis.call('GET', KEYS[1])", "2", "key1", "key2" }, false, new[] { "key1", "key2" })]
        [TestCase("EXPIRE", new[] { "mykey", "100", "NX" }, false, new[] { "mykey" })]
        [TestCase("MIGRATE", new[] { "127.0.0.1", "6379", "", "0", "5000", "KEYS", "key1", "key2" }, false, new[] { "key1", "key2" })]
        [TestCase("GEOSEARCHSTORE", new[] { "dst", "src", "FROMMEMBER", "member", "COUNT", "10", "ASC" }, false, new[] { "dst", "src" })]
        [TestCase("DELIFM", new[] { "mykey", "value" }, true, new[] { "mykey" })]
        [TestCase("MGETIFPM", new[] { "prefix", "key1", "key2", "key3" }, true, new[] { "key1", "key2", "key3" })]
        [TestCase("MYDICTGET", new[] { "mykey", "key1" }, true, new[] { "mykey" })]
        [TestCase("MYDICTSET", new[] { "mykey", "key1", "val1" }, true, new[] { "mykey" })]
        [TestCase("READWRITETX", new[] { "readkey", "writekey1", "writekey2" }, true, new[] { "readkey", "writekey1", "writekey2" })]
        [TestCase("SETIFPM", new[] { "mykey", "myvalue", "prefix" }, true, new[] { "mykey" })]
        [TestCase("SETWPIFPGT", new[] { "mykey", "myvalue", "prefix" }, true, new[] { "mykey" })]
        public void CommandGetKeysTest(string command, string[] args, bool isCustomCmd, string[] expectedKeys)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            if (isCustomCmd)
                RegisterCustomCommands();

            var results = (RedisResult[])db.Execute("COMMAND", new object[] { "GETKEYS", command }.Union(args).ToArray());

            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(expectedKeys.Length, results!.Length);

            var actualKeys = results.Select(r => r.ToString()).ToArray();
            CollectionAssert.AreEqual(expectedKeys, actualKeys);
        }

        /// <summary>
        /// Test COMMAND GETKEYSANDFLAGS command with various command signatures
        /// </summary>
        [Test]
        [TestCase("SET", new[] { "mykey", "value" }, false, new[] { "mykey" }, new[] { KeySpecificationFlags.RW | KeySpecificationFlags.Access | KeySpecificationFlags.Update | KeySpecificationFlags.VariableFlags })]
        [TestCase("MSET", new[] { "key1", "value1", "key2", "value2" }, false, new[] { "key1", "key2" }, new[] { KeySpecificationFlags.OW | KeySpecificationFlags.Update })]
        [TestCase("MGET", new[] { "key1", "key2", "key3" }, false, new[] { "key1", "key2", "key3" }, new[] { KeySpecificationFlags.RO | KeySpecificationFlags.Access })]
        [TestCase("ZUNIONSTORE", new[] { "destination", "2", "key1", "key2" }, false, new[] { "destination", "key1", "key2" }, new[] { KeySpecificationFlags.OW | KeySpecificationFlags.Update, KeySpecificationFlags.RO | KeySpecificationFlags.Access, KeySpecificationFlags.RO | KeySpecificationFlags.Access })]
        [TestCase("EVAL", new[] { "return redis.call('GET', KEYS[1])", "2", "key1", "key2" }, false, new[] { "key1", "key2" }, new[] { KeySpecificationFlags.RW | KeySpecificationFlags.Access | KeySpecificationFlags.Update })]
        [TestCase("EXPIRE", new[] { "mykey", "100", "NX" }, false, new[] { "mykey" }, new[] { KeySpecificationFlags.RW | KeySpecificationFlags.Update })]
        [TestCase("MIGRATE", new[] { "127.0.0.1", "6379", "", "0", "5000", "KEYS", "key1", "key2" }, false, new[] { "key1", "key2" }, new[] { KeySpecificationFlags.RW | KeySpecificationFlags.Access | KeySpecificationFlags.Delete | KeySpecificationFlags.Incomplete })]
        [TestCase("GEOSEARCHSTORE", new[] { "dst", "src", "FROMMEMBER", "member", "COUNT", "10", "ASC" }, false, new[] { "dst", "src" }, new[] { KeySpecificationFlags.OW | KeySpecificationFlags.Update, KeySpecificationFlags.RO | KeySpecificationFlags.Access })]
        [TestCase("DELIFM", new[] { "mykey", "value" }, true, new[] { "mykey" }, new[] { KeySpecificationFlags.RM | KeySpecificationFlags.Delete })]
        [TestCase("MGETIFPM", new[] { "prefix", "key1", "key2", "key3" }, true, new[] { "key1", "key2", "key3" }, new[] { KeySpecificationFlags.RO | KeySpecificationFlags.Access })]
        [TestCase("MYDICTGET", new[] { "mykey", "key1" }, true, new[] { "mykey" }, new[] { KeySpecificationFlags.RO | KeySpecificationFlags.Access })]
        [TestCase("MYDICTSET", new[] { "mykey", "key1", "val1" }, true, new[] { "mykey" }, new[] { KeySpecificationFlags.RW | KeySpecificationFlags.Access | KeySpecificationFlags.Update })]
        [TestCase("READWRITETX", new[] { "readkey", "writekey1", "writekey2" }, true, new[] { "readkey", "writekey1", "writekey2" }, new[] { KeySpecificationFlags.RO | KeySpecificationFlags.Access, KeySpecificationFlags.OW | KeySpecificationFlags.Update, KeySpecificationFlags.OW | KeySpecificationFlags.Update })]
        [TestCase("SETIFPM", new[] { "mykey", "myvalue", "prefix" }, true, new[] { "mykey" }, new[] { KeySpecificationFlags.RW | KeySpecificationFlags.Access | KeySpecificationFlags.Update })]
        [TestCase("SETWPIFPGT", new[] { "mykey", "myvalue", "prefix" }, true, new[] { "mykey" }, new[] { KeySpecificationFlags.RW | KeySpecificationFlags.Access | KeySpecificationFlags.Update })]
        public void CommandGetKeysAndFlagsTest(string command, string[] args, bool isCustomCmd, string[] expectedKeys, KeySpecificationFlags[] keySpecFlags)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            if (isCustomCmd)
                RegisterCustomCommands();

            var results = (RedisResult[])db.Execute("COMMAND", new object[] { "GETKEYSANDFLAGS", command }.Union(args).ToArray());

            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(expectedKeys.Length, results!.Length);

            var expectedFlags = keySpecFlags.Select(EnumUtils.GetEnumDescriptions).ToArray();
            ClassicAssert.IsTrue(expectedKeys.Length == expectedFlags.Length || expectedFlags.Length == 1);

            // If we are given a single flags argument, we multiply it for all keys
            if (expectedFlags.Length == 1 && expectedKeys.Length > 1)
            {
                expectedFlags = Enumerable.Range(0, expectedKeys.Length).Select(_ => expectedFlags[0]).ToArray();
            }

            for (var i = 0; i < expectedKeys.Length; i++)
            {
                var keyInfo = (RedisResult[])results[i];
                ClassicAssert.IsNotNull(keyInfo);

                ClassicAssert.AreEqual(2, keyInfo!.Length);
                var actualKey = keyInfo[0].ToString();
                ClassicAssert.AreEqual(expectedKeys[i], actualKey);

                ClassicAssert.IsNotNull((RedisResult[])keyInfo[1]);
                var actualFlags = ((RedisResult[])keyInfo[1])!.Select(r => r.ToString()).ToArray();
                ClassicAssert.IsNotNull(actualFlags);
                CollectionAssert.AreEquivalent(expectedFlags[i], actualFlags);
            }
        }

        /// <summary>
        /// Test COMMAND GETKEYS and GETKEYSANDFLAGS with invalid input
        /// </summary>
        [Test]
        [TestCase("GETKEYS", Description = "GETKEYS with no command")]
        [TestCase("GETKEYSANDFLAGS", Description = "GETKEYSANDFLAGS with no command")]
        public void CommandGetKeysInvalidInputTest(string subcommand)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            Assert.Throws<RedisServerException>(() => db.Execute("COMMAND", subcommand));
            Assert.Throws<RedisServerException>(() => db.Execute("COMMAND", subcommand, "INVALIDCOMMAND", "key1"));
        }
    }
}