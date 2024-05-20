// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Garnet.server;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test.Resp.ACL
{
    public class RespCommandTests
    {
        private const string DefaultPassword = nameof(RespCommandTests);

        private GarnetServer server;


        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void AllCommandsCovered()
        {
            IEnumerable<MethodInfo> tests = typeof(RespCommandTests).GetMethods().Where(static mtd => mtd.GetCustomAttribute<TestAttribute>() != null);

            HashSet<string> covered = new();

            foreach (MethodInfo test in tests)
            {
                if (test.Name == nameof(AllCommandsCovered))
                {
                    continue;
                }

                Assert.IsTrue(test.Name.EndsWith("ACLs"), $"Expected all tests in {nameof(RespCommandTests)} except {nameof(AllCommandsCovered)} to be per-command and end with ACLs, unexpected test: {test.Name}");

                string command = test.Name[..^"ALCs".Length];

                covered.Add(command);
            }

            Assert.IsTrue(RespCommandsInfo.TryGetRespCommandsInfo(out IReadOnlyDictionary<string, RespCommandsInfo> allInfo), "Couldn't load all command details");

            // exclude things like ACL, CLIENT, CLUSTER which are "commands" but only their sub commands can be run
            IEnumerable<string> withOnlySubCommands = allInfo.Where(static x => (x.Value.SubCommands?.Length ?? 0) != 0 && x.Value.Flags == RespCommandFlags.None).Select(static x => x.Key);

            IEnumerable<string> notCoveredByACLs = allInfo.Where(static x => x.Value.Flags.HasFlag(RespCommandFlags.NoAuth)).Select(static kv => kv.Key);

            Assert.IsTrue(RespCommandsInfo.TryGetRespCommandNames(out IReadOnlySet<string> advertisedCommands), "Couldn't get advertised RESP commands");

            IEnumerable<string> deSubCommanded = advertisedCommands.Except(withOnlySubCommands).Select(static x => x.Replace("|", "").Replace("_", "").Replace("-", ""));

            IEnumerable<string> notCovered = deSubCommanded.Except(covered, StringComparer.OrdinalIgnoreCase).Except(notCoveredByACLs, StringComparer.OrdinalIgnoreCase);

            Assert.IsEmpty(notCovered, $"Commands not covered by ACL Tests:{Environment.NewLine}{string.Join(Environment.NewLine, notCovered.OrderBy(static x => x))}");
        }

        [Test]
        public void AclCatACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ACL CAT",
                server,
                ["slow"],
                [DoAclCat]
            );

            void DoAclCat()
            {
                RedisResult val = db.Execute("ACL", "CAT");
                Assert.AreEqual(ResultType.MultiBulk, val.Type);
            }
        }

        [Test]
        public void AclDelUserACLs()
        {
            // test is a bit more verbose since it involves @admin

            string[] categories = ["admin", "slow", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoAclDelUser), "Denied when should have been permitted");
                    Assert.True(CheckAuthFailure(DoAclDelUserMulti), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoAclDelUser), "Permitted when should have been denied");
                    Assert.False(CheckAuthFailure(DoAclDelUserMulti), "Permitted when should have been denied");

                    void DoAclDelUser()
                    {
                        RedisResult val = db.Execute("ACL", "DELUSER", "does-not-exist");
                        Assert.AreEqual(0, (int)val);
                    }

                    void DoAclDelUserMulti()
                    {
                        RedisResult val = db.Execute("ACL", "DELUSER", "does-not-exist-1", "does-not-exist-2");
                        Assert.AreEqual(0, (int)val);
                    }
                }
            }
        }

        [Test]
        public void AclListACLs()
        {
            // test is a bit more verbose since it involves @admin

            string[] categories = ["admin", "slow", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoAclList), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoAclList), "Permitted when should have been denied");

                    void DoAclList()
                    {
                        RedisResult val = db.Execute("ACL", "LIST");
                        Assert.AreEqual(ResultType.MultiBulk, val.Type);
                    }
                }
            }
        }

        [Test]
        public void AclLoadACLs()
        {
            // test is a bit more verbose since it involves @admin

            string[] categories = ["admin", "slow", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoAclLoad), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoAclLoad), "Permitted when should have been denied");

                    void DoAclLoad()
                    {
                        try
                        {
                            RedisResult val = db.Execute("ACL", "LOAD");

                            Assert.Fail("No ACL file, so this should have failed");
                        }
                        catch (RedisException e)
                        {
                            if (e.Message != "ERR Cannot find ACL configuration file ''")
                            {
                                throw;
                            }
                        }

                    }
                }
            }
        }

        [Test]
        public void AclSetUserACLs()
        {
            // test is a bit more verbose since it involves @admin

            string[] categories = ["admin", "slow", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoAclSetUserOn), "Denied when should have been permitted");
                    Assert.True(CheckAuthFailure(DoAclSetUserCategory), "Denied when should have been permitted");
                    Assert.True(CheckAuthFailure(DoAclSetUserCategoryOnCategory), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoAclSetUserOn), "Permitted when should have been denied");
                    Assert.False(CheckAuthFailure(DoAclSetUserCategory), "Permitted when should have been denied");
                    Assert.False(CheckAuthFailure(DoAclSetUserCategoryOnCategory), "Permitted when should have been denied");

                    void DoAclSetUserOn()
                    {
                        RedisResult res = db.Execute("ACL", "SETUSER", "foo", "on");
                        Assert.AreEqual("OK", (string)res);
                    }

                    void DoAclSetUserCategory()
                    {
                        RedisResult res = db.Execute("ACL", "SETUSER", "foo", "+@read");
                        Assert.AreEqual("OK", (string)res);
                    }

                    void DoAclSetUserCategoryOnCategory()
                    {
                        RedisResult res = db.Execute("ACL", "SETUSER", "foo", "on", "+@read");
                        Assert.AreEqual("OK", (string)res);
                    }
                }
            }
        }

        [Test]
        public void AclUsersACLs()
        {
            // test is a bit more verbose since it involves @admin

            string[] categories = ["admin", "slow", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoAclUsers), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoAclUsers), "Permitted when should have been denied");

                    void DoAclUsers()
                    {
                        RedisResult res = db.Execute("ACL", "USERS");
                        Assert.AreEqual(ResultType.MultiBulk, res.Type);
                    }
                }
            }
        }

        [Test]
        public void AppendACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "APPEND",
                server,
                ["write", "string", "fast"],
                [DoAppend]
            );

            void DoAppend()
            {
                RedisValue val = db.StringAppend("key", "foo");

                Assert.AreEqual(3, (int)val);
            }
        }

        [Test]
        public void AskingACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ASKING",
                server,
                ["connection", "fast"],
                [DoAsking]
            );

            void DoAsking()
            {
                RedisResult val = db.Execute("ASKING");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void BGSaveACLs()
        {
            // test is a bit more verbose since it involves @admin

            string[] categories = ["admin", "slow", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoBGSave), "Denied when should have been permitted");
                    Assert.True(CheckAuthFailure(DoBGSaveSchedule), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoBGSave), "Permitted when should have been denied");
                    Assert.False(CheckAuthFailure(DoBGSaveSchedule), "Permitted when should have been denied");

                    void DoBGSave()
                    {
                        try
                        {
                            RedisResult res = db.Execute("BGSAVE");

                            Assert.IsTrue("Background saving started" == (string)res || "Background saving scheduled" == (string)res);
                        }
                        catch (RedisException e)
                        {
                            if (e.Message != "ERR checkpoint already in progress")
                            {
                                throw;
                            }
                        }
                    }

                    void DoBGSaveSchedule()
                    {
                        try
                        {
                            RedisResult res = db.Execute("BGSAVE", "SCHEDULE");
                            Assert.IsTrue("Background saving started" == (string)res || "Background saving scheduled" == (string)res);
                        }
                        catch (RedisException e)
                        {
                            if (e.Message != "ERR checkpoint already in progress")
                            {
                                throw;
                            }
                        }
                    }
                }
            }
        }

        [Test]
        public void BitcountACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "BITCOUNT",
                server,
                ["read", "bitmap", "slow"],
                [DoBitCount, DoBitCountStartEnd, DoBitCountStartEndBit, DoBitCountStartEndByte]
            );


            void DoBitCount()
            {
                RedisValue val = db.StringBitCount("empty-key");

                // if we aren't denied, make sure the value makes sense
                Assert.AreEqual(0, (int)val);
            }

            void DoBitCountStartEnd()
            {
                RedisValue val = db.StringBitCount("empty-key", 1, 1);

                // if we aren't denied, make sure the value makes sense
                Assert.AreEqual(0, (int)val);
            }

            void DoBitCountStartEndByte()
            {
                RedisValue val = db.StringBitCount("empty-key", 1, 1, indexType: StringIndexType.Byte);

                // if we aren't denied, make sure the value makes sense
                Assert.AreEqual(0, (int)val);
            }

            void DoBitCountStartEndBit()
            {
                RedisValue val = db.StringBitCount("empty-key", 1, 1, indexType: StringIndexType.Bit);

                // if we aren't denied, make sure the value makes sense
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void BitfieldACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "BITFIELD",
                server,
                ["write", "bitmap", "slow"],
                [
                    DoBitFieldGet, DoBitFieldGetWrap, DoBitFieldGetSat, DoBitFieldGetFail,
                    DoBitFieldSet, DoBitFieldSetWrap, DoBitFieldSetSat, DoBitFieldSetFail,
                    DoBitFieldIncrBy, DoBitFieldIncrByWrap, DoBitFieldIncrBySat, DoBitFieldIncrByFail,
                    DoBitFieldMulti
                ]
            );

            void DoBitFieldGet()
            {
                RedisResult val = db.Execute("BITFIELD", "empty-a", "GET", "u4", "0");
                Assert.AreEqual(0, (int)val);
            }

            void DoBitFieldGetWrap()
            {
                RedisResult val = db.Execute("BITFIELD", "empty-b", "GET", "u4", "0", "OVERFLOW", "WRAP");
                Assert.AreEqual(0, (int)val);
            }

            void DoBitFieldGetSat()
            {
                RedisResult val = db.Execute("BITFIELD", "empty-c", "GET", "u4", "0", "OVERFLOW", "SAT");
                Assert.AreEqual(0, (int)val);
            }

            void DoBitFieldGetFail()
            {
                RedisResult val = db.Execute("BITFIELD", "empty-d", "GET", "u4", "0", "OVERFLOW", "FAIL");
                Assert.AreEqual(0, (int)val);
            }

            void DoBitFieldSet()
            {
                RedisResult val = db.Execute("BITFIELD", "empty-e", "SET", "u4", "0", "1");
                Assert.AreEqual(0, (int)val);
            }

            void DoBitFieldSetWrap()
            {
                RedisResult val = db.Execute("BITFIELD", "empty-f", "SET", "u4", "0", "1", "OVERFLOW", "WRAP");
                Assert.AreEqual(0, (int)val);
            }

            void DoBitFieldSetSat()
            {
                RedisResult val = db.Execute("BITFIELD", "empty-g", "SET", "u4", "0", "1", "OVERFLOW", "SAT");
                Assert.AreEqual(0, (int)val);
            }

            void DoBitFieldSetFail()
            {
                RedisResult val = db.Execute("BITFIELD", "empty-h", "SET", "u4", "0", "1", "OVERFLOW", "FAIL");
                Assert.AreEqual(0, (int)val);
            }

            void DoBitFieldIncrBy()
            {
                RedisResult val = db.Execute("BITFIELD", "empty-i", "INCRBY", "u4", "0", "4");
                Assert.AreEqual(4, (int)val);
            }

            void DoBitFieldIncrByWrap()
            {
                RedisResult val = db.Execute("BITFIELD", "empty-j", "INCRBY", "u4", "0", "4", "OVERFLOW", "WRAP");
                Assert.AreEqual(4, (int)val);
            }

            void DoBitFieldIncrBySat()
            {
                RedisResult val = db.Execute("BITFIELD", "empty-k", "INCRBY", "u4", "0", "4", "OVERFLOW", "SAT");
                Assert.AreEqual(4, (int)val);
            }

            void DoBitFieldIncrByFail()
            {
                RedisResult val = db.Execute("BITFIELD", "empty-l", "INCRBY", "u4", "0", "4", "OVERFLOW", "FAIL");
                Assert.AreEqual(4, (int)val);
            }

            void DoBitFieldMulti()
            {
                RedisResult val = db.Execute("BITFIELD", "empty-m", "OVERFLOW", "WRAP", "GET", "u4", "1", "SET", "u4", "2", "1", "OVERFLOW", "FAIL", "INCRBY", "u4", "6", "2");

                RedisValue[] arr = (RedisValue[])val;

                Assert.AreEqual(3, arr.Length);

                RedisValue v0 = arr[0];
                RedisValue v1 = arr[1];
                RedisValue v2 = arr[2];

                Assert.AreEqual(0, (int)v0);
                Assert.AreEqual(0, (int)v1);
                Assert.AreEqual(2, (int)v2);
            }
        }

        [Test]
        public void BitfieldROACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "Bitfield_RO",
                server,
                ["read", "bitmap", "fast"],
                [DoBitFieldROGet, DoBitFieldROMulti]
            );

            void DoBitFieldROGet()
            {
                RedisResult val = db.Execute("BITFIELD_RO", "empty-a", "GET", "u4", "0");
                Assert.AreEqual(0, (int)val);
            }

            void DoBitFieldROMulti()
            {
                RedisResult val = db.Execute("BITFIELD_RO", "empty-b", "GET", "u4", "0", "GET", "u4", "3");

                RedisValue[] arr = (RedisValue[])val;

                Assert.AreEqual(2, arr.Length);

                RedisValue v0 = arr[0];
                RedisValue v1 = arr[1];

                Assert.AreEqual(0, (int)v0);
                Assert.AreEqual(0, (int)v1);
            }
        }

        [Test]
        public void BitOpACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            // todo: BITOP does not like working on empty keys... that's a separate thing to fix
            db.StringSetBit("zero", 0, false);
            db.StringSetBit("one", 0, true);

            CheckCommands(
                "BITOP",
                server,
                ["write", "bitmap", "slow"],
                [DoBitOpAnd, DoBitOpAndMulti, DoBitOpOr, DoBitOpOrMulti, DoBitOpXor, DoBitOpXorMulti, DoBitOpNot]
            );

            void DoBitOpAnd()
            {
                RedisResult val = db.Execute("BITOP", "AND", "zero", "zero");
                Assert.AreEqual(1, (int)val);
            }

            void DoBitOpAndMulti()
            {
                RedisResult val = db.Execute("BITOP", "AND", "zero", "zero", "one", "zero");
                Assert.AreEqual(1, (int)val);
            }

            void DoBitOpOr()
            {
                RedisResult val = db.Execute("BITOP", "OR", "one", "one");
                Assert.AreEqual(1, (int)val);
            }

            void DoBitOpOrMulti()
            {
                RedisResult val = db.Execute("BITOP", "OR", "one", "one", "one", "one");
                Assert.AreEqual(1, (int)val);
            }

            void DoBitOpXor()
            {
                RedisResult val = db.Execute("BITOP", "XOR", "one", "zero");
                Assert.AreEqual(1, (int)val);
            }

            void DoBitOpXorMulti()
            {
                RedisResult val = db.Execute("BITOP", "XOR", "one", "one", "one", "zero");
                Assert.AreEqual(1, (int)val);
            }

            void DoBitOpNot()
            {
                RedisResult val = db.Execute("BITOP", "NOT", "one", "zero");
                Assert.AreEqual(1, (int)val);
            }
        }

        [Test]
        public void BitPosACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "BITPOS",
                server,
                ["read", "bitmap", "slow"],
                [DoBitPos, DoBitPosStart, DoBitPosStartEnd, DoBitPosStartEndBit, DoBitPosStartEndByte]
            );

            void DoBitPos()
            {
                RedisResult val = db.Execute("BITPOS", "empty", "1");
                Assert.AreEqual(-1, (int)val);
            }

            void DoBitPosStart()
            {
                RedisResult val = db.Execute("BITPOS", "empty", "1", "5");
                Assert.AreEqual(-1, (int)val);
            }

            void DoBitPosStartEnd()
            {
                RedisResult val = db.Execute("BITPOS", "empty", "1", "5", "7");
                Assert.AreEqual(-1, (int)val);
            }

            void DoBitPosStartEndBit()
            {
                RedisResult val = db.Execute("BITPOS", "empty", "1", "5", "7", "BIT");
                Assert.AreEqual(-1, (int)val);
            }

            void DoBitPosStartEndByte()
            {
                RedisResult val = db.Execute("BITPOS", "empty", "1", "5", "7", "BYTE");
                Assert.AreEqual(-1, (int)val);
            }
        }

        [Test]
        public void ClientACLs()
        {
            // todo: client isn't really implemented looks like, so this is mostly a placeholder in case it gets implemented correctly

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "CLIENT",
                server,
                ["slow"],
                [DoClient]
            );

            void DoClient()
            {
                RedisResult val = db.Execute("CLIENT");
                Assert.AreEqual("OK", (string)val);
            }
        }

        // todo: cluster and subcommands are weird, do them later

        [Test]
        public void CommandACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "COMMAND",
                server,
                ["connection", "slow"],
                [DoCommand]
            );

            void DoCommand()
            {
                RedisResult val = db.Execute("COMMAND");
                Assert.AreEqual(ResultType.MultiBulk, val.Type);
            }
        }

        [Test]
        public void CommandCountACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "COMMAND COUNT",
                server,
                ["connection", "slow"],
                [DoCommandCount]
            );

            void DoCommandCount()
            {
                RedisResult val = db.Execute("COMMAND", "COUNT");
                Assert.IsTrue((int)val > 0);
            }
        }

        [Test]
        public void CommandDocsACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "COMMAND DOCS",
                server,
                ["connection", "slow"],
                [DoCommandDocs, DoCommandDocsOne, DoCommandDocsMulti]
            );

            void DoCommandDocs()
            {
                RedisResult val = db.Execute("COMMAND", "DOCS");
                Assert.AreEqual(ResultType.MultiBulk, val.Type);
            }

            void DoCommandDocsOne()
            {
                RedisResult val = db.Execute("COMMAND", "DOCS", "GET");
                Assert.AreEqual(ResultType.MultiBulk, val.Type);
            }

            void DoCommandDocsMulti()
            {
                RedisResult val = db.Execute("COMMAND", "DOCS", "GET", "SET", "APPEND");
                Assert.AreEqual(ResultType.MultiBulk, val.Type);
            }
        }

        [Test]
        public void CommandInfoACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "COMMAND INFO",
                server,
                ["connection", "slow"],
                [DoCommandInfo, DoCommandInfoOne, DoCommandInfoMulti]
            );

            void DoCommandInfo()
            {
                RedisResult val = db.Execute("COMMAND", "INFO");
                Assert.AreEqual(ResultType.MultiBulk, val.Type);
            }

            void DoCommandInfoOne()
            {
                RedisResult val = db.Execute("COMMAND", "INFO", "GET");
                Assert.AreEqual(ResultType.MultiBulk, val.Type);
            }

            void DoCommandInfoMulti()
            {
                RedisResult val = db.Execute("COMMAND", "INFO", "GET", "SET", "APPEND");
                Assert.AreEqual(ResultType.MultiBulk, val.Type);
            }
        }

        [Test]
        public void CommitAOFACLs()
        {
            // test is a bit more verbose since it involves @admin

            string[] categories = ["admin", "garnet"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoCommitAOF), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoCommitAOF), "Permitted when should have been denied");

                    void DoCommitAOF()
                    {
                        RedisResult val = db.Execute("COMMITAOF");
                        Assert.AreEqual("AOF file committed", (string)val);
                    }
                }
            }
        }

        [Test]
        public void ConfigGetACLs()
        {
            // todo: CONFIG GET doesn't implement multiple parameters, so that is untested

            // test is a bit more verbose since it involves @admin

            string[] categories = ["admin", "slow", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoConfigGetOne), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoConfigGetOne), "Permitted when should have been denied");

                    void DoConfigGetOne()
                    {
                        RedisResult res = db.Execute("CONFIG", "GET", "timeout");
                        RedisValue[] resArr = (RedisValue[])res;

                        Assert.AreEqual(2, resArr.Length);
                        Assert.AreEqual("timeout", (string)resArr[0]);
                        Assert.IsTrue((int)resArr[1] >= 0);
                    }
                }
            }
        }

        [Test]
        public void ConfigRewriteACLs()
        {
            // test is a bit more verbose since it involves @admin

            string[] categories = ["admin", "slow", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoConfigRewrite), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoConfigRewrite), "Permitted when should have been denied");

                    void DoConfigRewrite()
                    {
                        RedisResult res = db.Execute("CONFIG", "REWRITE");
                        Assert.AreEqual("OK", (string)res);
                    }
                }
            }
        }

        [Test]
        public void ConfigSetACLs()
        {
            // todo: CONFIG SET parameters are pretty limitted, so this uses error responses for "got past the ACL" validation - that's not great

            // test is a bit more verbose since it involves @admin

            string[] categories = ["admin", "slow", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoConfigSetOne), "Denied when should have been permitted");
                    Assert.True(CheckAuthFailure(DoConfigSetMulti), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoConfigSetOne), "Permitted when should have been denied");
                    Assert.False(CheckAuthFailure(DoConfigSetMulti), "Permitted when should have been denied");

                    void DoConfigSetOne()
                    {
                        try
                        {
                            RedisResult res = db.Execute("CONFIG", "SET", "foo", "bar");
                            Assert.Fail("Should have raised unknow config error");
                        }
                        catch (RedisException e)
                        {
                            if (e.Message == "ERR Unknown option or number of arguments for CONFIG SET - 'foo'")
                            {
                                return;
                            }

                            throw;
                        }
                    }

                    void DoConfigSetMulti()
                    {
                        try
                        {
                            RedisResult res = db.Execute("CONFIG", "SET", "foo", "bar", "fizz", "buzz");
                            Assert.Fail("Should have raised unknow config error");
                        }
                        catch (RedisException e)
                        {
                            if (e.Message == "ERR Unknown option or number of arguments for CONFIG SET - 'foo'")
                            {
                                return;
                            }

                            throw;
                        }
                    }
                }
            }
        }

        [Test]
        public void COScanACLs()
        {
            // todo: COSCAN parameters are unclear... add more cases later

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "COSCAN",
                server,
                ["read", "garnet", "slow"],
                [DoCOScan]
            );

            void DoCOScan()
            {
                RedisResult val = db.Execute("CUSTOMOBJECTSCAN", "foo", "0");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }
        }

        // todo: figure out CustomCmd ACL rules
        // todo: figure out CustomObjCmd ACL rules
        // todo: figure out CustomTxn ACL rules

        [Test]
        public void DBSizeACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "DBSIZE",
                server,
                ["keyspace", "read", "fast"],
                [DoDbSize]
            );

            void DoDbSize()
            {
                RedisResult val = db.Execute("DBSIZE");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void DecrACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "DECR",
                server,
                ["write", "string", "fast"],
                [DoDecr]
            );

            void DoDecr()
            {
                RedisResult val = db.Execute("DECR", "foo");
                Assert.AreEqual(-1, (int)val);
            }
        }

        [Test]
        public void DecrByACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "DECRBY",
                server,
                ["write", "string", "fast"],
                [DoDecrBy]
            );

            void DoDecrBy()
            {
                RedisResult val = db.Execute("DECRBY", "foo", "2");
                Assert.AreEqual(-2, (int)val);
            }
        }

        [Test]
        public void DelACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "DEL",
                server,
                ["keyspace", "write", "slow"],
                [DoDel, DoDelMulti]
            );

            void DoDel()
            {
                RedisResult val = db.Execute("DEL", "foo");
                Assert.AreEqual(0, (int)val);
            }

            void DoDelMulti()
            {
                RedisResult val = db.Execute("DEL", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void DiscardACLs()
        {
            // todo: discard is a little weird, so we're using exceptions for control flow here - don't love it

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "DISCARD",
                server,
                ["transaction", "fast"],
                [DoDiscard]
            );

            void DoDiscard()
            {
                try
                {
                    RedisResult val = db.Execute("DISCARD");
                    Assert.Fail("Shouldn't have reached this point, outside of a MULTI");
                }
                catch (RedisException e)
                {
                    if (e.Message == "ERR DISCARD without MULTI")
                    {
                        return;
                    }

                    throw;
                }
            }
        }

        [Test]
        public void EchoACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ECHO",
                server,
                ["connection", "fast"],
                [DoEcho]
            );

            void DoEcho()
            {
                RedisResult val = db.Execute("ECHO", "hello world");
                Assert.AreEqual("hello world", (string)val);
            }
        }

        [Test]
        public void ExecACLs()
        {
            // todo: exec is a little weird, so we're using exceptions for control flow here - don't love it

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "EXEC",
                server,
                ["transaction", "slow"],
                [DoExec]
            );

            void DoExec()
            {
                try
                {
                    RedisResult val = db.Execute("EXEC");
                    Assert.Fail("Shouldn't have reached this point, outside of a MULTI");
                }
                catch (RedisException e)
                {
                    if (e.Message == "ERR EXEC without MULTI")
                    {
                        return;
                    }

                    throw;
                }
            }
        }

        [Test]
        public void ExistsACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "EXISTS",
                server,
                ["keyspace", "read", "fast"],
                [DoExists, DoExistsMulti]
            );

            void DoExists()
            {
                RedisResult val = db.Execute("EXISTS", "foo");
                Assert.AreEqual(0, (int)val);
            }

            void DoExistsMulti()
            {
                RedisResult val = db.Execute("EXISTS", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ExpireACLs()
        {
            // todo: expire doesn't support combinations of flags (XX GT, XX LT are legal) so those will need to be tested when implemented

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "EXPIRE",
                server,
                ["keyspace", "write", "fast"],
                [DoExpire, DoExpireNX, DoExpireXX, DoExpireGT, DoExpireLT]
            );

            void DoExpire()
            {
                RedisResult val = db.Execute("EXPIRE", "foo", "10");
                Assert.AreEqual(0, (int)val);
            }

            void DoExpireNX()
            {
                RedisResult val = db.Execute("EXPIRE", "foo", "10", "NX");
                Assert.AreEqual(0, (int)val);
            }

            void DoExpireXX()
            {
                RedisResult val = db.Execute("EXPIRE", "foo", "10", "XX");
                Assert.AreEqual(0, (int)val);
            }

            void DoExpireGT()
            {
                RedisResult val = db.Execute("EXPIRE", "foo", "10", "GT");
                Assert.AreEqual(0, (int)val);
            }

            void DoExpireLT()
            {
                RedisResult val = db.Execute("EXPIRE", "foo", "10", "LT");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void FailoverACLs()
        {
            // failover is strange, so this more complicated that typical

            Action<IServer>[] cmds = [
                DoFailover,
                DoFailoverTo,
                DoFailoverAbort,
                DoFailoverToForce,
                DoFailoverToAbort,
                DoFailoverToForceAbort,
                DoFailoverToForceAbortTimeout,
            ];

            string[] acls = ["admin", "slow", "dangerous"];

            foreach (Action<IServer> cmd in cmds)
            {
                // check works with +@all
                Run(true, server => { }, cmd);

                // check denied with -@whatever
                foreach (string acl in acls)
                {
                    Run(false, server => SetUser(server, "default", $"-@{acl}"), cmd);
                }
            }

            void Run(bool expectSuccess, Action<IServer> before, Action<IServer> cmd)
            {
                // refresh Garnet instance
                TearDown();
                Setup();

                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                IServer server = redis.GetServers().Single();

                before(server);

                Assert.AreEqual(expectSuccess, CheckAuthFailure(() => cmd(server)));
            }

            static void DoFailover(IServer server)
            {
                try
                {
                    RedisResult val = server.Execute("FAILOVER");
                    Assert.Fail("Shouldn't be reachable, cluster not enabled");
                }
                catch (RedisException e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static void DoFailoverTo(IServer server)
            {
                try
                {
                    RedisResult val = server.Execute("FAILOVER", "TO", "127.0.0.1", "9999");
                    Assert.Fail("Shouldn't be reachable, cluster not enabled");
                }
                catch (RedisException e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static void DoFailoverAbort(IServer server)
            {
                try
                {
                    RedisResult val = server.Execute("FAILOVER", "ABORT");
                    Assert.Fail("Shouldn't be reachable, cluster not enabled");
                }
                catch (RedisException e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static void DoFailoverToForce(IServer server)
            {
                try
                {
                    RedisResult val = server.Execute("FAILOVER", "TO", "127.0.0.1", "9999", "FORCE");
                    Assert.Fail("Shouldn't be reachable, cluster not enabled");
                }
                catch (RedisException e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static void DoFailoverToAbort(IServer server)
            {
                try
                {
                    RedisResult val = server.Execute("FAILOVER", "TO", "127.0.0.1", "9999", "ABORT");
                    Assert.Fail("Shouldn't be reachable, cluster not enabled");
                }
                catch (RedisException e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static void DoFailoverToForceAbort(IServer server)
            {
                try
                {
                    RedisResult val = server.Execute("FAILOVER", "TO", "127.0.0.1", "9999", "FORCE", "ABORT");
                    Assert.Fail("Shouldn't be reachable, cluster not enabled");
                }
                catch (RedisException e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static void DoFailoverToForceAbortTimeout(IServer server)
            {
                try
                {
                    RedisResult val = server.Execute("FAILOVER", "TO", "127.0.0.1", "9999", "FORCE", "ABORT", "TIMEOUT", "1");
                    Assert.Fail("Shouldn't be reachable, cluster not enabled");
                }
                catch (RedisException e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }
        }

        [Test]
        public void FlushDBACLs()
        {
            // test is a bit more verbose since it involves @dangerous

            string[] categories = ["keyspace", "write", "dangerous", "slow"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoFlushDB), "Denied when should have been permitted");
                    Assert.True(CheckAuthFailure(DoFlushDBAsync), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoFlushDB), "Permitted when should have been denied");
                    Assert.False(CheckAuthFailure(DoFlushDBAsync), "Permitted when should have been denied");

                    void DoFlushDB()
                    {
                        RedisResult val = db.Execute("FLUSHDB");
                        Assert.AreEqual("OK", (string)val);
                    }

                    void DoFlushDBAsync()
                    {
                        RedisResult val = db.Execute("FLUSHDB", "ASYNC");
                        Assert.AreEqual("OK", (string)val);
                    }
                }
            }
        }

        [Test]
        public void ForceGCACLs()
        {
            // test is a bit more verbose since it involves @admin

            string[] categories = ["admin", "garnet"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoForceGC), "Denied when should have been permitted");
                    Assert.True(CheckAuthFailure(DoForceGCGen), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoForceGC), "Permitted when should have been denied");
                    Assert.False(CheckAuthFailure(DoForceGCGen), "Permitted when should have been denied");

                    void DoForceGC()
                    {
                        RedisResult val = db.Execute("FORCEGC");
                        Assert.AreEqual("GC completed", (string)val);
                    }

                    void DoForceGCGen()
                    {
                        RedisResult val = db.Execute("FORCEGC", "1");
                        Assert.AreEqual("GC completed", (string)val);
                    }
                }
            }
        }

        [Test]
        public void GetACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "GET",
                server,
                ["read", "string", "fast"],
                [DoGet]
            );

            void DoGet()
            {
                RedisResult val = db.Execute("GET", "foo");
                Assert.IsNull((string)val);
            }
        }

        [Test]
        public void GetBitACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "GETBIT",
                server,
                ["read", "bitmap", "fast"],
                [DoGetBit]
            );

            void DoGetBit()
            {
                RedisResult val = db.Execute("GETBIT", "foo", "4");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void GetDelACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "GETDEL",
                server,
                ["write", "string", "fast"],
                [DoGetDel]
            );

            void DoGetDel()
            {
                RedisResult val = db.Execute("GETDEL", "foo");
                Assert.IsNull((string)val);
            }
        }

        [Test]
        public void GetRangeACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "GETRANGE",
                server,
                ["read", "string", "slow"],
                [DoGetRange]
            );

            void DoGetRange()
            {
                RedisResult val = db.Execute("GETRANGE", "foo", "10", "15");
                Assert.IsNull((string)val);
            }
        }

        [Test]
        public void HDelACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "HDEL",
                server,
                ["write", "hash", "fast"],
                [DoHDel, DoHDelMulti]
            );

            void DoHDel()
            {
                RedisResult val = db.Execute("HDEL", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }

            void DoHDelMulti()
            {
                RedisResult val = db.Execute("HDEL", "foo", "bar", "fizz");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void HExistsACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "HEXISTS",
                server,
                ["read", "hash", "fast"],
                [DoHDel]
            );

            void DoHDel()
            {
                RedisResult val = db.Execute("HEXISTS", "foo", "bar");
                Assert.IsFalse((bool)val);
            }
        }

        [Test]
        public void HGetACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "HGET",
                server,
                ["read", "hash", "fast"],
                [DoHDel]
            );

            void DoHDel()
            {
                RedisResult val = db.Execute("HGET", "foo", "bar");
                Assert.IsNull((string)val);
            }
        }

        [Test]
        public void HGetAllACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "HGETALL",
                server,
                ["read", "hash", "slow"],
                [DoHDel]
            );

            void DoHDel()
            {
                RedisResult val = db.Execute("HGETALL", "foo");
                RedisValue[] valArr = (RedisValue[])val;

                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void HIncrByACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            int cur = 0;

            CheckCommands(
                "HINCRBY",
                server,
                ["write", "hash", "fast"],
                [DoHIncrBy]
            );

            void DoHIncrBy()
            {
                RedisResult val = db.Execute("HINCRBY", "foo", "bar", "2");
                cur += 2;
                Assert.AreEqual(cur, (int)val);
            }
        }

        [Test]
        public void HIncrByFloatACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            double cur = 0;

            CheckCommands(
                "HINCRBYFLOAT",
                server,
                ["write", "hash", "fast"],
                [DoHIncrByFloat]
            );

            void DoHIncrByFloat()
            {
                RedisResult val = db.Execute("HINCRBYFLOAT", "foo", "bar", "1.0");
                cur += 1.0;
                Assert.AreEqual(cur, (double)val);
            }
        }

        [Test]
        public void HKeysACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "HKEYS",
                server,
                ["read", "hash", "slow"],
                [DoHKeys]
            );

            void DoHKeys()
            {
                RedisResult val = db.Execute("HKEYS", "foo");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void HLenACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "HLEN",
                server,
                ["read", "hash", "fast"],
                [DoHLen]
            );

            void DoHLen()
            {
                RedisResult val = db.Execute("HLEN", "foo");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void HMGetACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "HMGET",
                server,
                ["read", "hash", "fast"],
                [DoHMGet, DoHMGetMulti]
            );

            void DoHMGet()
            {
                RedisResult val = db.Execute("HMGET", "foo", "bar");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(1, valArr.Length);
                Assert.IsNull((string)valArr[0]);
            }

            void DoHMGetMulti()
            {
                RedisResult val = db.Execute("HMGET", "foo", "bar", "fizz");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(2, valArr.Length);
                Assert.IsNull((string)valArr[0]);
                Assert.IsNull((string)valArr[1]);
            }
        }

        [Test]
        public void HMSetACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "HMSET",
                server,
                ["write", "hash", "fast"],
                [DoHMSet, DoHMSetMulti]
            );

            void DoHMSet()
            {
                RedisResult val = db.Execute("HMSET", "foo", "bar", "fizz");
                Assert.AreEqual("OK", (string)val);
            }

            void DoHMSetMulti()
            {
                RedisResult val = db.Execute("HMSET", "foo", "bar", "fizz", "hello", "world");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void HRandFieldACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "HRANDFIELD",
                server,
                ["read", "hash", "slow"],
                [DoHRandField, DoHRandFieldCount, DoHRandFieldCountWithValues]
            );

            void DoHRandField()
            {
                RedisResult val = db.Execute("HRANDFIELD", "foo");
                Assert.IsNull((string)val);
            }

            void DoHRandFieldCount()
            {
                RedisResult val = db.Execute("HRANDFIELD", "foo", "1");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            void DoHRandFieldCountWithValues()
            {
                RedisResult val = db.Execute("HRANDFIELD", "foo", "1", "WITHVALUES");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void HScanACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "HSCAN",
                server,
                ["read", "hash", "slow"],
                [DoHScan, DoHScanMatch, DoHScanCount, DoHScanNoValues, DoHScanMatchCount, DoHScanMatchNoValues, DoHScanCountNoValues, DoHScanMatchCountNoValues]
            );

            void DoHScan()
            {
                RedisResult val = db.Execute("HSCAN", "foo", "0");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            void DoHScanMatch()
            {
                RedisResult val = db.Execute("HSCAN", "foo", "0", "MATCH", "*");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            void DoHScanCount()
            {
                RedisResult val = db.Execute("HSCAN", "foo", "0", "COUNT", "2");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            void DoHScanNoValues()
            {
                RedisResult val = db.Execute("HSCAN", "foo", "0", "NOVALUES");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            void DoHScanMatchCount()
            {
                RedisResult val = db.Execute("HSCAN", "foo", "0", "MATCH", "*", "COUNT", "2");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            void DoHScanMatchNoValues()
            {
                RedisResult val = db.Execute("HSCAN", "foo", "0", "MATCH", "*", "NOVALUES");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            void DoHScanCountNoValues()
            {
                RedisResult val = db.Execute("HSCAN", "foo", "0", "COUNT", "0", "NOVALUES");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            void DoHScanMatchCountNoValues()
            {
                RedisResult val = db.Execute("HSCAN", "foo", "0", "MATCH", "*", "COUNT", "0", "NOVALUES");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }
        }

        [Test]
        public void HSetACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            int keyIx = 0;

            CheckCommands(
                "HSET",
                server,
                ["write", "hash", "fast"],
                [DoHSet, DoHSetMulti]
            );
            void DoHSet()
            {
                RedisResult val = db.Execute("HSET", $"foo-{keyIx}", "bar", "fizz");
                keyIx++;

                Assert.AreEqual(1, (int)val);
            }

            void DoHSetMulti()
            {
                RedisResult val = db.Execute("HSET", $"foo-{keyIx}", "bar", "fizz", "hello", "world");
                keyIx++;

                Assert.AreEqual(2, (int)val);
            }
        }

        [Test]
        public void HSetNXACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            int keyIx = 0;

            CheckCommands(
                "HSETNX",
                server,
                ["write", "hash", "fast"],
                [DoHSetNX]
            );

            void DoHSetNX()
            {
                RedisResult val = db.Execute("HSETNX", $"foo-{keyIx}", "bar", "fizz");
                keyIx++;

                Assert.AreEqual(1, (int)val);
            }
        }

        [Test]
        public void HStrLenACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "HSTRLEN",
                server,
                ["read", "hash", "fast"],
                [DoHStrLen]
            );

            void DoHStrLen()
            {
                RedisResult val = db.Execute("HSTRLEN", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void HValsACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "HVALS",
                server,
                ["read", "hash", "slow"],
                [DoHVals]
            );

            void DoHVals()
            {
                RedisResult val = db.Execute("HVALS", "foo");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void IncrACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "INCR",
                server,
                ["write", "string", "fast"],
                [DoIncr]
            );

            void DoIncr()
            {
                RedisResult val = db.Execute("INCR", "foo");
                Assert.AreEqual(1, (int)val);
            }
        }

        [Test]
        public void IncrByACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "INCRBY",
                server,
                ["write", "string", "fast"],
                [DoIncrBy]
            );

            void DoIncrBy()
            {
                RedisResult val = db.Execute("INCRBY", "foo", "2");
                Assert.AreEqual(2, (int)val);
            }
        }

        [Test]
        public void InfoACLs()
        {
            // test is a bit more verbose since it involves @dangerous

            string[] categories = ["slow", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoInfo), "Denied when should have been permitted");
                    Assert.True(CheckAuthFailure(DoInfoSingle), "Denied when should have been permitted");
                    Assert.True(CheckAuthFailure(DoInfoMulti), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoInfo), "Permitted when should have been denied");
                    Assert.False(CheckAuthFailure(DoInfoSingle), "Permitted when should have been denied");
                    Assert.False(CheckAuthFailure(DoInfoMulti), "Permitted when should have been denied");

                    void DoInfo()
                    {
                        RedisResult val = db.Execute("INFO");
                        Assert.IsNotEmpty((string)val);
                    }

                    void DoInfoSingle()
                    {
                        RedisResult val = db.Execute("INFO", "SERVER");
                        Assert.IsNotEmpty((string)val);
                    }

                    void DoInfoMulti()
                    {
                        RedisResult val = db.Execute("INFO", "SERVER", "MEMORY");
                        Assert.IsNotEmpty((string)val);
                    }
                }
            }
        }

        [Test]
        public void KeysACLs()
        {
            // test is a bit more verbose since it involves @dangerous

            string[] categories = ["keyspace", "read", "slow", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoKeys), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoKeys), "Permitted when should have been denied");

                    void DoKeys()
                    {
                        RedisResult val = db.Execute("KEYS", "*");
                        RedisResult[] valArr = (RedisResult[])val;
                        Assert.AreEqual(0, valArr.Length);
                    }
                }
            }
        }

        [Test]
        public void LastSaveACLs()
        {
            // test is a bit more verbose since it involves @admin

            string[] categories = ["admin", "fast", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoLastSave), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoLastSave), "Permitted when should have been denied");

                    void DoLastSave()
                    {
                        RedisResult val = db.Execute("LASTSAVE");
                        Assert.AreEqual(0, (long)val);
                    }
                }
            }
        }

        // todo: figure out what to do with LATENCY|HELP

        [Test]
        public void LatencyHistogramACLs()
        {
            // test is a bit more verbose since it involves @admin

            string[] categories = ["admin", "slow", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoLatencyHistogram), "Denied when should have been permitted");
                    Assert.True(CheckAuthFailure(DoLatencyHistogramSingle), "Denied when should have been permitted");
                    Assert.True(CheckAuthFailure(DoLatencyHistogramMulti), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoLatencyHistogram), "Permitted when should have been denied");
                    Assert.False(CheckAuthFailure(DoLatencyHistogramSingle), "Permitted when should have been denied");
                    Assert.False(CheckAuthFailure(DoLatencyHistogramMulti), "Permitted when should have been denied");

                    void DoLatencyHistogram()
                    {
                        RedisResult val = db.Execute("LATENCY", "HISTOGRAM");
                        RedisResult[] valArr = (RedisResult[])val;
                        Assert.AreEqual(0, valArr.Length);
                    }

                    void DoLatencyHistogramSingle()
                    {
                        RedisResult val = db.Execute("LATENCY", "HISTOGRAM", "NET_RS_LAT");
                        RedisResult[] valArr = (RedisResult[])val;
                        Assert.AreEqual(0, valArr.Length);
                    }

                    void DoLatencyHistogramMulti()
                    {
                        RedisResult val = db.Execute("LATENCY", "HISTOGRAM", "NET_RS_LAT", "NET_RS_LAT_ADMIN");
                        RedisResult[] valArr = (RedisResult[])val;
                        Assert.AreEqual(0, valArr.Length);
                    }
                }
            }
        }

        [Test]
        public void LatencyResetACLs()
        {
            // test is a bit more verbose since it involves @admin

            string[] categories = ["admin", "slow", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoLatencyReset), "Denied when should have been permitted");
                    Assert.True(CheckAuthFailure(DoLatencyResetSingle), "Denied when should have been permitted");
                    Assert.True(CheckAuthFailure(DoLatencyResetMulti), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoLatencyReset), "Permitted when should have been denied");
                    Assert.False(CheckAuthFailure(DoLatencyResetSingle), "Permitted when should have been denied");
                    Assert.False(CheckAuthFailure(DoLatencyResetMulti), "Permitted when should have been denied");

                    void DoLatencyReset()
                    {
                        RedisResult val = db.Execute("LATENCY", "RESET");
                        Assert.AreEqual("OK", (string)val);
                    }

                    void DoLatencyResetSingle()
                    {
                        RedisResult val = db.Execute("LATENCY", "RESET", "NET_RS_LAT");
                        Assert.AreEqual("OK", (string)val);
                    }

                    void DoLatencyResetMulti()
                    {
                        RedisResult val = db.Execute("LATENCY", "RESET", "NET_RS_LAT", "NET_RS_LAT_ADMIN");
                        Assert.AreEqual("OK", (string)val);
                    }
                }
            }
        }

        [Test]
        public void LPopACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "LPOP",
                server,
                ["write", "list", "fast"],
                [DoLPop, DoLPopCount]
            );

            void DoLPop()
            {
                RedisResult val = db.Execute("LPOP", "foo");
                Assert.IsTrue(val.IsNull);
            }

            void DoLPopCount()
            {
                RedisResult val = db.Execute("LPOP", "foo", "4");
                Assert.IsTrue(val.IsNull);
            }
        }

        [Test]
        public void LPushACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            int count = 0;

            CheckCommands(
                "LPUSH",
                server,
                ["write", "list", "fast"],
                [DoLPush, DoLPushMulti]
            );

            void DoLPush()
            {
                RedisResult val = db.Execute("LPUSH", "foo", "bar");
                count++;

                Assert.AreEqual(count, (int)val);
            }

            void DoLPushMulti()
            {
                RedisResult val = db.Execute("LPUSH", "foo", "bar", "buzz");
                count += 2;

                Assert.AreEqual(count, (int)val);
            }
        }

        [Test]
        public void LPushXACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "LPUSHX",
                server,
                ["write", "list", "fast"],
                [DoLPushX, DoLPushXMulti]
            );

            void DoLPushX()
            {
                RedisResult val = db.Execute("LPUSHX", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }

            void DoLPushXMulti()
            {
                RedisResult val = db.Execute("LPUSHX", "foo", "bar", "buzz");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void RPopACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "RPOP",
                server,
                ["write", "list", "fast"],
                [DoRPop, DoRPopCount]
            );

            void DoRPop()
            {
                RedisResult val = db.Execute("RPOP", "foo");
                Assert.IsTrue(val.IsNull);
            }

            void DoRPopCount()
            {
                RedisResult val = db.Execute("RPOP", "foo", "4");
                Assert.IsTrue(val.IsNull);
            }
        }

        [Test]
        public void LRushACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            int count = 0;

            CheckCommands(
                "RPUSH",
                server,
                ["write", "list", "fast"],
                [DoRPush, DoRPushMulti]
            );

            void DoRPush()
            {
                RedisResult val = db.Execute("RPUSH", "foo", "bar");
                count++;

                Assert.AreEqual(count, (int)val);
            }

            void DoRPushMulti()
            {
                RedisResult val = db.Execute("RPUSH", "foo", "bar", "buzz");
                count += 2;

                Assert.AreEqual(count, (int)val);
            }
        }

        [Test]
        public void RPushACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            int count = 0;

            CheckCommands(
                "RPUSHX",
                server,
                ["write", "list", "fast"],
                [DoRPushX, DoRPushXMulti]
            );

            void DoRPushX()
            {
                RedisResult val = db.Execute("RPUSH", $"foo-{count}", "bar");
                count++;
                Assert.AreEqual(1, (int)val);
            }

            void DoRPushXMulti()
            {
                RedisResult val = db.Execute("RPUSH", $"foo-{count}", "bar", "buzz");
                count++;
                Assert.AreEqual(2, (int)val);
            }
        }

        [Test]
        public void RPushXACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "RPUSHX",
                server,
                ["write", "list", "fast"],
                [DoRPushX, DoRPushXMulti]
            );

            void DoRPushX()
            {
                RedisResult val = db.Execute("RPUSHX", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }

            void DoRPushXMulti()
            {
                RedisResult val = db.Execute("RPUSHX", "foo", "bar", "buzz");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void LLenACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "LLEN",
                server,
                ["read", "list", "fast"],
                [DoLLen]
            );

            void DoLLen()
            {
                RedisResult val = db.Execute("LLEN", "foo");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void LTrimACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "LLEN",
                server,
                ["write", "list", "slow"],
                [DoLTrim]
            );

            void DoLTrim()
            {
                RedisResult val = db.Execute("LTRIM", "foo", "4", "10");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void LRangeACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "LRANGE",
                server,
                ["read", "list", "slow"],
                [DoLRange]
            );

            void DoLRange()
            {
                RedisResult val = db.Execute("LRANGE", "foo", "4", "10");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void LIndexACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "LINDEX",
                server,
                ["read", "list", "slow"],
                [DoLIndex]
            );

            void DoLIndex()
            {
                RedisResult val = db.Execute("LINDEX", "foo", "4");
                Assert.IsTrue(val.IsNull);
            }
        }

        [Test]
        public void LInsertACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "LINSERT",
                server,
                ["write", "list", "slow"],
                [DoLInsertBefore, DoLInsertAfter]
            );

            void DoLInsertBefore()
            {
                RedisResult val = db.Execute("LINSERT", "foo", "BEFORE", "hello", "world");
                Assert.AreEqual(0, (int)val);
            }

            void DoLInsertAfter()
            {
                RedisResult val = db.Execute("LINSERT", "foo", "AFTER", "hello", "world");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void LRemACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "LREM",
                server,
                ["write", "list", "slow"],
                [DoLRem]
            );

            void DoLRem()
            {
                RedisResult val = db.Execute("LREM", "foo", "0", "hello");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void RPopLPushACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "RPOPLPUSH",
                server,
                ["write", "list", "slow"],
                [DoLRem]
            );

            void DoLRem()
            {
                RedisResult val = db.Execute("RPOPLPUSH", "foo", "bar");
                Assert.IsTrue(val.IsNull);
            }
        }

        [Test]
        public void LMoveACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "LMOVE",
                server,
                ["write", "list", "slow"],
                [DoLMove]
            );

            void DoLMove()
            {
                RedisResult val = db.Execute("LMOVE", "foo", "bar", "LEFT", "RIGHT");
                Assert.IsTrue(val.IsNull);
            }
        }

        [Test]
        public void LSetACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            db.ListLeftPush("foo", "fizz");

            CheckCommands(
                "LSET",
                server,
                ["write", "list", "slow"],
                [DoLMove]
            );

            void DoLMove()
            {
                RedisResult val = db.Execute("LSET", "foo", "0", "bar");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void MemoryUsageACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "MEMORY USAGE",
                server,
                ["read", "slow"],
                [DoMemoryUsage, DoMemoryUsageSamples]
            );

            void DoMemoryUsage()
            {
                RedisResult val = db.Execute("MEMORY", "USAGE", "foo");
                Assert.AreEqual(0, (int)val);
            }

            void DoMemoryUsageSamples()
            {
                RedisResult val = db.Execute("MEMORY", "USAGE", "foo", "SAMPLES", "10");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void MGetACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "MGET",
                server,
                ["read", "string", "fast"],
                [DoMemorySingle, DoMemoryMulti]
            );

            void DoMemorySingle()
            {
                RedisResult val = db.Execute("MGET", "foo");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(1, valArr.Length);
                Assert.IsTrue(valArr[0].IsNull);
            }

            void DoMemoryMulti()
            {
                RedisResult val = db.Execute("MGET", "foo", "bar");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
                Assert.IsTrue(valArr[0].IsNull);
                Assert.IsTrue(valArr[1].IsNull);
            }
        }

        [Test]
        public void MigrateACLs()
        {
            // uses exceptions for control flow, as we're not setting up replicas here

            // todo: migrate has a ton of options, test other variants

            // test is a bit more verbose since it involves @dangerous

            string[] categories = ["keyspace", "write", "dangerous", "slow"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoMigrate), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoMigrate), "Permitted when should have been denied");

                    void DoMigrate()
                    {
                        try
                        {
                            db.Execute("MIGRATE", "127.0.0.1", "9999", "KEY", "0", "1000");
                            Assert.Fail("Shouldn't succeed, no replicas are attached");
                        }
                        catch (RedisException e)
                        {
                            if (e.Message == "ERR This instance has cluster support disabled")
                            {
                                return;
                            }

                            throw;
                        }
                    }
                }
            }
        }

        [Test]
        public void ModuleACLs()
        {
            // todo: MODULE isn't a proper redis command, but this is the placeholder today... so validate it for completeness

            // test is a bit more verbose since it involves @admin

            string[] categories = ["admin", "slow", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoModuleList), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoModuleList), "Permitted when should have been denied");

                    void DoModuleList()
                    {
                        RedisResult val = db.Execute("MODULE", "LIST");
                        RedisResult[] valArr = (RedisResult[])val;
                        Assert.AreEqual(0, valArr.Length);
                    }
                }
            }
        }

        [Test]
        public void MonitorACLs()
        {
            // MONITOR isn't actually implemented, and doesn't fit nicely into SE.Redis anyway, so we only check the DENY cases here

            // test is a bit more involved since @admin is present
            string[] categories = ["admin", "slow", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoMonitor), "Permitted when should have been denied");

                    void DoMonitor()
                    {
                        db.Execute("MONITOR");
                        Assert.Fail("Should never reach this point");
                    }
                }
            }
        }

        [Test]
        public void MSetACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "MSET",
                server,
                ["write", "string", "slow"],
                [DoMSetSingle, DoMSetMulti]
            );

            void DoMSetSingle()
            {
                RedisResult val = db.Execute("MSET", "foo", "bar");
                Assert.AreEqual("OK", (string)val);
            }

            void DoMSetMulti()
            {
                RedisResult val = db.Execute("MSET", "foo", "bar", "fizz", "buzz");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void MSetNXACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            int count = 0;

            CheckCommands(
                "MSETNX",
                server,
                ["write", "string", "slow"],
                [DoMSetNXSingle, DoMSetNXMulti]
            );

            void DoMSetNXSingle()
            {
                RedisResult val = db.Execute("MSETNX", $"foo-{count}", "bar");
                count++;

                Assert.AreEqual(1, (int)val);
            }

            void DoMSetNXMulti()
            {
                RedisResult val = db.Execute("MSETNX", $"foo-{count}", "bar", $"fizz-{count}", "buzz");
                count++;

                Assert.AreEqual(1, (int)val);
            }
        }

        [Test]
        public void MultiACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "MULTI",
                server,
                ["transaction", "fast"],
                [DoMulti]
            );

            void DoMulti()
            {
                RedisResult val = db.Execute("MULTI");
                Assert.AreEqual("OK", (string)val);

                // if we got here, abort the transaction
                db.Execute("DISCARD");
            }
        }

        [Test]
        public void PersistACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "PERSIST",
                server,
                ["keyspace", "write", "fast"],
                [DoPersist]
            );

            void DoPersist()
            {
                RedisResult val = db.Execute("PERSIST", "foo");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void PExpireACLs()
        {
            // todo: pexpire doesn't support combinations of flags (XX GT, XX LT are legal) so those will need to be tested when implemented

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "PEXPIRE",
                server,
                ["keyspace", "write", "fast"],
                [DoPExpire, DoPExpireNX, DoPExpireXX, DoPExpireGT, DoPExpireLT]
            );

            void DoPExpire()
            {
                RedisResult val = db.Execute("PEXPIRE", "foo", "10");
                Assert.AreEqual(0, (int)val);
            }

            void DoPExpireNX()
            {
                RedisResult val = db.Execute("PEXPIRE", "foo", "10", "NX");
                Assert.AreEqual(0, (int)val);
            }

            void DoPExpireXX()
            {
                RedisResult val = db.Execute("PEXPIRE", "foo", "10", "XX");
                Assert.AreEqual(0, (int)val);
            }

            void DoPExpireGT()
            {
                RedisResult val = db.Execute("PEXPIRE", "foo", "10", "GT");
                Assert.AreEqual(0, (int)val);
            }

            void DoPExpireLT()
            {
                RedisResult val = db.Execute("PEXPIRE", "foo", "10", "LT");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void PFAddACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            int count = 0;

            CheckCommands(
                "PFADD",
                server,
                ["hyperloglog", "write", "fast"],
                [DoPFAddSingle, DoPFAddMulti]
            );

            void DoPFAddSingle()
            {
                RedisResult val = db.Execute("PFADD", $"foo-{count}", "bar");
                count++;

                Assert.AreEqual(1, (int)val);
            }

            void DoPFAddMulti()
            {
                RedisResult val = db.Execute("PFADD", $"foo-{count}", "bar", "fizz");
                count++;

                Assert.AreEqual(1, (int)val);
            }
        }

        [Test]
        public void PFCountACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "PFCOUNT",
                server,
                ["hyperloglog", "read", "slow"],
                [DoPFCountSingle, DoPFCountMulti]
            );

            void DoPFCountSingle()
            {
                RedisResult val = db.Execute("PFCOUNT", "foo");
                Assert.AreEqual(0, (int)val);
            }

            void DoPFCountMulti()
            {
                RedisResult val = db.Execute("PFCOUNT", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void PFMergeACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "PFMERGE",
                server,
                ["hyperloglog", "write", "slow"],
                [DoPFMergeSingle, DoPFMergeMulti]
            );

            void DoPFMergeSingle()
            {
                RedisResult val = db.Execute("PFMERGE", "foo");
                Assert.AreEqual("OK", (string)val);
            }

            void DoPFMergeMulti()
            {
                RedisResult val = db.Execute("PFMERGE", "foo", "bar");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void PingACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "PING",
                server,
                ["connection", "fast"],
                [DoPing, DoPingMessage]
            );

            void DoPing()
            {
                RedisResult val = db.Execute("PING");
                Assert.AreEqual("PONG", (string)val);
            }

            void DoPingMessage()
            {
                RedisResult val = db.Execute("PING", "hello");
                Assert.AreEqual("hello", (string)val);
            }
        }

        [Test]
        public void PSetEXACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "PSETEX",
                server,
                ["string", "write", "slow"],
                [DoPSetEX]
            );

            void DoPSetEX()
            {
                RedisResult val = db.Execute("PSETEX", "foo", "10", "bar");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void PSubscribeACLs()
        {
            // this is a strange test, because we have to contort ourselves to get SE.Redis to actually issue the expected command

            // todo: not testing the multiple pattern version

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword, disablePubSub: false));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();
            ISubscriber sub = redis.GetSubscriber();

            int count = 0;

            CheckCommands(
                "PSUBSCRIBE",
                server,
                ["pubsub", "slow"],
                [DoPSubscribePattern]
            );

            void DoPSubscribePattern()
            {
                // have to do the (bad) async version to make sure we actually get the error
                sub.SubscribeAsync(new RedisChannel($"channel-{count}", RedisChannel.PatternMode.Pattern)).GetAwaiter().GetResult();
                count++;
            }
        }

        [Test]
        public void PUnsubscribeACLs()
        {
            // this is a strange test, because we have to contort ourselves to get SE.Redis to actually issue the expected command

            // todo: not testing the 0 argument version of PUNSUBSCRIBE, as it's a pain

            const int ChannelCount = 10;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword, disablePubSub: false));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();
            ISubscriber sub = redis.GetSubscriber();

            // gotta subscribe or SE.Redis won't issue the commands
            HashSet<string> openChannels = new();
            for (var i = 0; i < ChannelCount; i++)
            {
                string ch = $"channel-{i}";
                sub.Subscribe(new(ch, RedisChannel.PatternMode.Pattern));

                openChannels.Add(ch);
            }

            CheckCommands(
                "PUNSUBSCRIBE",
                server,
                ["pubsub", "slow"],
                [DoPUnsubscribePattern]
            );

            void DoPUnsubscribePattern()
            {
                string toUnSub = openChannels.First();

                // we remove before trying, because SE.Redis will toss the subscription from it's internal state ANYWAY
                openChannels.Remove(toUnSub);

                // gotta use the async version (incorrectly) so we actually wait for the response
                redis.GetSubscriber().UnsubscribeAsync(new(toUnSub, RedisChannel.PatternMode.Pattern)).GetAwaiter().GetResult();
            }
        }

        [Test]
        public void PTTLACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "PTTL",
                server,
                ["keyspace", "read", "fast"],
                [DoPTTL]
            );

            void DoPTTL()
            {
                RedisResult val = db.Execute("PTTL", "foo");
                Assert.AreEqual(-1, (int)val);
            }
        }

        [Test]
        public void PublishACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();
            ISubscriber sub = redis.GetSubscriber();

            CheckCommands(
                "PTTL",
                server,
                ["pubsub", "fast"],
                [DoPublish]
            );

            void DoPublish()
            {
                long count = sub.Publish("foo", "bar");
                Assert.AreEqual(0, count);
            }
        }

        [Test]
        public void ReadOnlyACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "READONLY",
                server,
                ["connection", "fast"],
                [DoReadOnly]
            );

            void DoReadOnly()
            {
                RedisResult val = db.Execute("READONLY");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void ReadWriteACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "READONLY",
                server,
                ["connection", "fast"],
                [DoReadWrite]
            );

            void DoReadWrite()
            {
                RedisResult val = db.Execute("READWRITE");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void RegisterCSACLs()
        {
            // todo: REGISTERCS has a complicated syntax, test proper commands later

            // test is a bit more verbose since it involves @admin

            string[] categories = ["admin", "garnet", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoRegisterCS), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoRegisterCS), "Permitted when should have been denied");

                    void DoRegisterCS()
                    {
                        try
                        {
                            db.Execute("REGISTERCS");
                            Assert.Fail("Should be unreachable, command is malfoemd");
                        }
                        catch (RedisException e)
                        {
                            if (e.Message == "ERR malformed REGISTERCS command.")
                            {
                                return;
                            }

                            throw;
                        }
                    }
                }
            }
        }

        [Test]
        public void RenameACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "RENAME",
                server,
                ["keyspace", "write", "slow"],
                [DoPTTL]
            );

            void DoPTTL()
            {
                try
                {
                    RedisResult val = db.Execute("RENAME", "foo", "bar");
                    Assert.Fail("Shouldn't succeed, key doesn't exist");
                }
                catch (RedisException e)
                {
                    if (e.Message == "ERR no such key")
                    {
                        return;
                    }

                    throw;
                }
            }
        }

        [Test]
        public void ReplicaOfACLs()
        {
            // uses exceptions as control flow, since clustering is disabled in these tests

            // test is a bit more verbose since it involves @admin

            string[] categories = ["admin", "slow", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoReplicaOf), "Denied when should have been permitted");
                    Assert.True(CheckAuthFailure(DoReplicaOfNoOne), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoReplicaOf), "Permitted when should have been denied");
                    Assert.False(CheckAuthFailure(DoReplicaOfNoOne), "Permitted when should have been denied");

                    void DoReplicaOf()
                    {
                        try
                        {
                            db.Execute("REPLICAOF", "127.0.0.1", "9999");
                            Assert.Fail("Should be unreachable, cluster is disabled");
                        }
                        catch (RedisException e)
                        {
                            if (e.Message == "ERR This instance has cluster support disabled")
                            {
                                return;
                            }

                            throw;
                        }
                    }

                    void DoReplicaOfNoOne()
                    {
                        try
                        {
                            db.Execute("REPLICAOF", "NO", "ONE");
                            Assert.Fail("Should be unreachable, cluster is disabled");
                        }
                        catch (RedisException e)
                        {
                            if (e.Message == "ERR This instance has cluster support disabled")
                            {
                                return;
                            }

                            throw;
                        }
                    }
                }
            }
        }

        [Test]
        public void RunTxpACLs()
        {
            // todo: RUNTXP semantics are a bit unclear... expand test later

            // todo: RUNTXP breaks the stream when command is malformed

            string[] categories = ["transaction", "garnet"];

            foreach (string cat in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                // works
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(() => DoRunTxp(db)), "Denied when should have been permitted");
                }

                // denied
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    SetUser(server, "temp-admin", $"-@{cat}");

                    Assert.False(CheckAuthFailure(() => DoRunTxp(db)), "Permitted when should have been denied");
                }
            }

            static void DoRunTxp(IDatabase db)
            {
                try
                {
                    db.Execute("RUNTXP", "foo");
                    Assert.Fail("Should be reachable, command is malformed");
                }
                catch (RedisException e)
                {
                    if (e.Message == "ERR Protocol Error: Unable to parse number: foo")
                    {
                        return;
                    }

                    throw;
                }
            }
        }

        [Test]
        public void SaveACLs()
        {
            // test is a bit more verbose since it involves @admin

            string[] categories = ["admin", "slow", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoSave), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoSave), "Permitted when should have been denied");

                    void DoSave()
                    {
                        RedisResult val = db.Execute("SAVE");
                        Assert.AreEqual("OK", (string)val);
                    }
                }
            }
        }

        [Test]
        public void ScanACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "SCAN",
                server,
                ["keyspace", "read", "slow"],
                [DoScan, DoScanMatch, DoScanCount, DoScanType, DoScanMatchCount, DoScanMatchType, DoScanCountType, DoScanMatchCountType]
            );

            void DoScan()
            {
                RedisResult val = db.Execute("SCAN", "0");
                Assert.IsFalse(val.IsNull);
            }

            void DoScanMatch()
            {
                RedisResult val = db.Execute("SCAN", "0", "MATCH", "*");
                Assert.IsFalse(val.IsNull);
            }

            void DoScanCount()
            {
                RedisResult val = db.Execute("SCAN", "0", "COUNT", "5");
                Assert.IsFalse(val.IsNull);
            }

            void DoScanType()
            {
                RedisResult val = db.Execute("SCAN", "0", "TYPE", "zset");
                Assert.IsFalse(val.IsNull);
            }

            void DoScanMatchCount()
            {
                RedisResult val = db.Execute("SCAN", "0", "MATCH", "*", "COUNT", "5");
                Assert.IsFalse(val.IsNull);
            }

            void DoScanMatchType()
            {
                RedisResult val = db.Execute("SCAN", "0", "MATCH", "*", "TYPE", "zset");
                Assert.IsFalse(val.IsNull);
            }

            void DoScanCountType()
            {
                RedisResult val = db.Execute("SCAN", "0", "COUNT", "5", "TYPE", "zset");
                Assert.IsFalse(val.IsNull);
            }

            void DoScanMatchCountType()
            {
                RedisResult val = db.Execute("SCAN", "0", "MATCH", "*", "COUNT", "5", "TYPE", "zset");
                Assert.IsFalse(val.IsNull);
            }
        }

        [Test]
        public void SelectACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "SELECT",
                server,
                ["connection", "fast"],
                [DoSelect]
            );

            void DoSelect()
            {
                RedisResult val = db.Execute("SELECT", "0");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void SetACLs()
        {
            // SET doesn't support most extra commands, so this is just key value

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "SET",
                server,
                ["string", "write", "slow"],
                [DoSet, DoSetExNx, DoSetXxNx, DoSetKeepTtl, DoSetKeepTtlXx]
            );

            void DoSet()
            {
                RedisResult val = db.Execute("SET", "foo", "bar");
                Assert.AreEqual("OK", (string)val);
            }

            void DoSetExNx()
            {
                RedisResult val = db.Execute("SET", "foo", "bar", "NX", "EX", "100");
                Assert.IsTrue(val.IsNull);
            }

            void DoSetXxNx()
            {
                RedisResult val = db.Execute("SET", "foo", "bar", "XX", "EX", "100");
                Assert.AreEqual("OK", (string)val);
            }

            void DoSetKeepTtl()
            {
                RedisResult val = db.Execute("SET", "foo", "bar", "KEEPTTL");
                Assert.AreEqual("OK", (string)val);
            }

            void DoSetKeepTtlXx()
            {
                RedisResult val = db.Execute("SET", "foo", "bar", "XX", "KEEPTTL");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void SetBitACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "SETBIT",
                server,
                ["bitmap", "write", "slow"],
                [DoSetBit]
            );

            void DoSetBit()
            {
                RedisResult val = db.Execute("SETBIT", "foo", "10", "1");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void SetEXACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "SETEX",
                server,
                ["string", "write", "slow"],
                [DoSetEX]
            );

            void DoSetEX()
            {
                RedisResult val = db.Execute("SETEX", "foo", "10", "bar");
                Assert.AreEqual("OK", (string)val);
            }
        }

        // todo: SETKEEPTTL, SETKEEPTTLXX - all non-standard, what are these for?

        [Test]
        public void SetRangeACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "SETRANGE",
                server,
                ["string", "write", "slow"],
                [DoSetRange]
            );

            void DoSetRange()
            {
                RedisResult val = db.Execute("SETRANGE", "foo", "10", "bar");
                Assert.AreEqual(13, (int)val);
            }
        }

        [Test]
        public void StrLenACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "SETRANGE",
                server,
                ["string", "read", "fast"],
                [DoStrLen]
            );

            void DoStrLen()
            {
                RedisResult val = db.Execute("STRLEN", "foo");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void SAddACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            int count = 0;

            CheckCommands(
                "SADD",
                server,
                ["write", "set", "fast"],
                [DoSAdd, DoSAddMulti]
            );

            void DoSAdd()
            {
                RedisResult val = db.Execute("SADD", $"foo-{count}", "bar");
                count++;

                Assert.AreEqual(1, (int)val);
            }

            void DoSAddMulti()
            {
                RedisResult val = db.Execute("SADD", $"foo-{count}", "bar", "fizz");
                count++;

                Assert.AreEqual(2, (int)val);
            }
        }

        [Test]
        public void SRemACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "SREM",
                server,
                ["write", "set", "fast"],
                [DoSRem, DoSRemMulti]
            );

            void DoSRem()
            {
                RedisResult val = db.Execute("SREM", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }

            void DoSRemMulti()
            {
                RedisResult val = db.Execute("SREM", "foo", "bar", "fizz");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void SPopACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "SPOP",
                server,
                ["write", "set", "fast"],
                [DoSPop, DoSPopCount]
            );

            void DoSPop()
            {
                RedisResult val = db.Execute("SPOP", "foo");
                Assert.IsTrue(val.IsNull);
            }

            void DoSPopCount()
            {
                RedisResult val = db.Execute("SPOP", "foo", "11");
                Assert.IsTrue(val.IsNull);
            }
        }

        [Test]
        public void SMembersACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "SMEMBERS",
                server,
                ["read", "set", "slow"],
                [DoSMembers]
            );

            void DoSMembers()
            {
                RedisResult val = db.Execute("SMEMBERS", "foo");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void SCardACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "SCARD",
                server,
                ["read", "set", "fast"],
                [DoSCard]
            );

            void DoSCard()
            {
                RedisResult val = db.Execute("SCARD", "foo");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void SScanACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "SSCAN",
                server,
                ["set", "read", "slow"],
                [DoSScan, DoSScanMatch, DoSScanCount, DoSScanMatchCount]
            );

            void DoSScan()
            {
                RedisResult val = db.Execute("SSCAN", "foo", "0");
                Assert.IsFalse(val.IsNull);
            }

            void DoSScanMatch()
            {
                RedisResult val = db.Execute("SSCAN", "foo", "0", "MATCH", "*");
                Assert.IsFalse(val.IsNull);
            }

            void DoSScanCount()
            {
                RedisResult val = db.Execute("SSCAN", "foo", "0", "COUNT", "5");
                Assert.IsFalse(val.IsNull);
            }

            void DoSScanMatchCount()
            {
                RedisResult val = db.Execute("SSCAN", "foo", "0", "MATCH", "*", "COUNT", "5");
                Assert.IsFalse(val.IsNull);
            }
        }

        [Test]
        public void SlaveOfACLs()
        {
            // uses exceptions as control flow, since clustering is disabled in these tests

            // test is a bit more verbose since it involves @admin

            string[] categories = ["admin", "slow", "dangerous"];

            foreach (string category in categories)
            {
                // spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(DoSlaveOf), "Denied when should have been permitted");
                    Assert.True(CheckAuthFailure(DoSlaveOfNoOne), "Denied when should have been permitted");

                    SetUser(server, "temp-admin", [$"-@{category}"]);

                    Assert.False(CheckAuthFailure(DoSlaveOf), "Permitted when should have been denied");
                    Assert.False(CheckAuthFailure(DoSlaveOfNoOne), "Permitted when should have been denied");

                    void DoSlaveOf()
                    {
                        try
                        {
                            db.Execute("SLAVEOF", "127.0.0.1", "9999");
                            Assert.Fail("Should be unreachable, cluster is disabled");
                        }
                        catch (RedisException e)
                        {
                            if (e.Message == "ERR This instance has cluster support disabled")
                            {
                                return;
                            }

                            throw;
                        }
                    }

                    void DoSlaveOfNoOne()
                    {
                        try
                        {
                            db.Execute("SLAVEOF", "NO", "ONE");
                            Assert.Fail("Should be unreachable, cluster is disabled");
                        }
                        catch (RedisException e)
                        {
                            if (e.Message == "ERR This instance has cluster support disabled")
                            {
                                return;
                            }

                            throw;
                        }
                    }
                }
            }
        }

        [Test]
        public void SMoveACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "SMOVE",
                server,
                ["set", "write", "fast"],
                [DoSMove]
            );

            void DoSMove()
            {
                RedisResult val = db.Execute("SMOVE", "foo", "bar", "fizz");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void SRandMemberACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "SRANDMEMBER",
                server,
                ["set", "read", "slow"],
                [DoSRandMember, DoSRandMemberCount]
            );

            void DoSRandMember()
            {
                RedisResult val = db.Execute("SRANDMEMBER", "foo");
                Assert.IsTrue(val.IsNull);
            }

            void DoSRandMemberCount()
            {
                RedisResult val = db.Execute("SRANDMEMBER", "foo", "5");
                Assert.IsTrue(val.IsNull);
            }
        }

        [Test]
        public void SIsMemberACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "SISMEMBER",
                server,
                ["set", "read", "fast"],
                [DoSIsMember]
            );

            void DoSIsMember()
            {
                RedisResult val = db.Execute("SISMEMBER", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void SubscribeACLs()
        {
            // this is a strange test, because we have to contort ourselves to get SE.Redis to actually issue the expected command

            // todo: not testing the multiple pattern version

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword, disablePubSub: false));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();
            ISubscriber sub = redis.GetSubscriber();

            int count = 0;

            CheckCommands(
                "SUBSCRIBE",
                server,
                ["pubsub", "slow"],
                [DoSubscribe]
            );

            void DoSubscribe()
            {
                // have to do the (bad) async version to make sure we actually get the error
                sub.SubscribeAsync(new RedisChannel($"channel-{count}", RedisChannel.PatternMode.Literal)).GetAwaiter().GetResult();
                count++;
            }
        }

        [Test]
        public void SUnionACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "SUNION",
                server,
                ["set", "read", "slow"],
                [DoSUnion, DoSUnionMulti]
            );

            void DoSUnion()
            {
                RedisResult val = db.Execute("SUNION", "foo");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            void DoSUnionMulti()
            {
                RedisResult val = db.Execute("SUNION", "foo", "bar");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void SUnionStoreACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "SUNIONSTORE",
                server,
                ["set", "write", "slow"],
                [DoSUnionStore, DoSUnionStoreMulti]
            );

            void DoSUnionStore()
            {
                RedisResult val = db.Execute("SUNIONSTORE", "dest", "foo");
                Assert.AreEqual(0, (int)val);
            }

            void DoSUnionStoreMulti()
            {
                RedisResult val = db.Execute("SUNIONSTORE", "dest", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void SDiffACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "SDIFF",
                server,
                ["set", "read", "slow"],
                [DoSDiff, DoSDiffMulti]
            );

            void DoSDiff()
            {
                RedisResult val = db.Execute("SDIFF", "foo");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            void DoSDiffMulti()
            {
                RedisResult val = db.Execute("SDIFF", "foo", "bar");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void SDiffStoreACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "SDIFFSTORE",
                server,
                ["set", "write", "slow"],
                [DoSDiffStore, DoSDiffStoreMulti]
            );

            void DoSDiffStore()
            {
                RedisResult val = db.Execute("SDIFFSTORE", "dest", "foo");
                Assert.AreEqual(0, (int)val);
            }

            void DoSDiffStoreMulti()
            {
                RedisResult val = db.Execute("SDIFFSTORE", "dest", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void GeoAddACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            int count = 0;

            CheckCommands(
                "GEOADD",
                server,
                ["geo", "write", "slow"],
                [DoGeoAdd, DoGeoAddNX, DoGeoAddNXCH, DoGeoAddMulti, DoGeoAddNXMulti, DoGeoAddNXCHMulti]
            );

            void DoGeoAdd()
            {
                RedisResult val = db.Execute("GEOADD", $"foo-{count}", "90", "90", "bar");
                count++;

                Assert.AreEqual(1, (int)val);
            }

            void DoGeoAddNX()
            {
                RedisResult val = db.Execute("GEOADD", $"foo-{count}", "NX", "90", "90", "bar");
                count++;

                Assert.AreEqual(1, (int)val);
            }

            void DoGeoAddNXCH()
            {
                RedisResult val = db.Execute("GEOADD", $"foo-{count}", "NX", "CH", "90", "90", "bar");
                count++;

                Assert.AreEqual(0, (int)val);
            }

            void DoGeoAddMulti()
            {
                RedisResult val = db.Execute("GEOADD", $"foo-{count}", "90", "90", "bar", "45", "45", "fizz");
                count++;

                Assert.AreEqual(2, (int)val);
            }

            void DoGeoAddNXMulti()
            {
                RedisResult val = db.Execute("GEOADD", $"foo-{count}", "NX", "90", "90", "bar", "45", "45", "fizz");
                count++;

                Assert.AreEqual(2, (int)val);
            }

            void DoGeoAddNXCHMulti()
            {
                RedisResult val = db.Execute("GEOADD", $"foo-{count}", "NX", "CH", "90", "90", "bar", "45", "45", "fizz");
                count++;

                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void GeoHashACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            // todo: GEOHASH doesn't deal with empty keys appropriately - correct when that's fixed
            Assert.AreEqual(1, (int)db.Execute("GEOADD", "foo", "10", "10", "bar"));
            Assert.AreEqual(1, (int)db.Execute("GEOADD", "foo", "20", "20", "fizz"));

            CheckCommands(
                "GEOHASH",
                server,
                ["geo", "read", "slow"],
                [DoGeoHash, DoGeoHashSingle, DoGeoHashMulti]
            );

            void DoGeoHash()
            {
                RedisResult val = db.Execute("GEOHASH", "foo");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            void DoGeoHashSingle()
            {
                RedisResult val = db.Execute("GEOHASH", "foo", "bar");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(1, valArr.Length);
                Assert.IsFalse(valArr[0].IsNull);
            }

            void DoGeoHashMulti()
            {
                RedisResult val = db.Execute("GEOHASH", "foo", "bar", "fizz");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(2, valArr.Length);
                Assert.IsFalse(valArr[0].IsNull);
                Assert.IsFalse(valArr[1].IsNull);
            }
        }

        [Test]
        public void GeoDistACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            // todo: GEODIST fails on missing keys, which is incorrect, so putting values in to get ACL test passing
            Assert.AreEqual(1, (int)db.Execute("GEOADD", "foo", "10", "10", "bar"));
            Assert.AreEqual(1, (int)db.Execute("GEOADD", "foo", "20", "20", "fizz"));

            CheckCommands(
                "GEODIST",
                server,
                ["geo", "read", "slow"],
                [DoGetDist, DoGetDistM]
            );

            void DoGetDist()
            {
                RedisResult val = db.Execute("GEODIST", "foo", "bar", "fizz");
                Assert.IsTrue((double)val > 0);
            }

            void DoGetDistM()
            {
                RedisResult val = db.Execute("GEODIST", "foo", "bar", "fizz", "M");
                Assert.IsTrue((double)val > 0);
            }
        }

        [Test]
        public void GeoPosACLs()
        {
            lock (this)
            {
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                IServer server = redis.GetServers().Single();
                IDatabase db = redis.GetDatabase();

                // todo: GEOPOS gets desynced if key doesn't exist, remove after that's fixed
                Assert.AreEqual(1, (int)db.Execute("GEOADD", "foo", "10", "10", "bar"));
                Assert.AreEqual(1, (int)db.Execute("GEOADD", "foo", "20", "20", "fizz"));

                CheckCommands(
                    "GEOPOS",
                    server,
                    ["geo", "read", "slow"],
                    [DoGeoPos, DoGeoPosMulti]
                );

                void DoGeoPos()
                {
                    RedisResult val = db.Execute("GEOPOS", "foo");
                    RedisResult[] valArr = (RedisResult[])val;
                    Assert.AreEqual(0, valArr.Length);
                }

                void DoGeoPosMulti()
                {
                    RedisResult val = db.Execute("GEOPOS", "foo", "bar");
                    RedisResult[] valArr = (RedisResult[])val;
                    Assert.AreEqual(1, valArr.Length);
                }
            }
        }

        [Test]
        public void GeoSearchACLs()
        {
            // todo: there are a LOT of GeoSearch variants (not all of them implemented), come back and cover all the lengths appropriately

            // todo: GEOSEARCH appears to be very broken, so this structured oddly - can be simplified once fixed

            string[] categories = ["geo", "read", "slow"];

            foreach (string category in categories)
            {
                // fresh Garnet for the allow version
                TearDown();
                Setup();

                {
                    // fresh connection, as GEOSEARCH seems to break connections pretty easily
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(() => DoGeoSearch(db)), "Denied when should have been permitted");
                }

                // fresh Garnet for the reject version
                TearDown();
                Setup();

                {
                    // fresh connection, as GEOSEARCH seems to break connections pretty easily
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    SetUser(server, "default", $"-@{category}");

                    Assert.False(CheckAuthFailure(() => DoGeoSearch(db)), "Permitted when should have been denied");
                }

                static void DoGeoSearch(IDatabase db)
                {
                    RedisResult val = db.Execute("GEOSEARCH", "foo", "FROMMEMBER", "bar", "BYBOX", "2", "2", "M");
                    RedisValue[] valArr = (RedisValue[])val;
                    Assert.AreEqual(0, valArr.Length);
                }
            }
        }

        [Test]
        public void ZAddACLs()
        {
            // todo: ZADD doesn't implement NX XX GT LT CH INCR; expand to cover all lengths when implemented

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            int count = 0;

            CheckCommands(
                "ZADD",
                server,
                ["sortedset", "write", "fast"],
                [DoZAdd, DoZAddMulti]
            );

            void DoZAdd()
            {
                RedisResult val = db.Execute("ZADD", $"foo-{count}", "10", "bar");
                count++;
                Assert.AreEqual(1, (int)val);
            }

            void DoZAddMulti()
            {
                RedisResult val = db.Execute("ZADD", $"foo-{count}", "10", "bar", "20", "fizz");
                count++;
                Assert.AreEqual(2, (int)val);
            }
        }

        [Test]
        public void ZCardACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ZCARD",
                server,
                ["sortedset", "read", "fast"],
                [DoZCard]
            );

            void DoZCard()
            {
                RedisResult val = db.Execute("ZCARD", "foo");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ZPopMaxACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ZPOPMAX",
                server,
                ["sortedset", "write", "fast"],
                [DoZPopMax, DoZPopMaxCount]
            );

            void DoZPopMax()
            {
                RedisResult val = db.Execute("ZPOPMAX", "foo");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            void DoZPopMaxCount()
            {
                RedisResult val = db.Execute("ZPOPMAX", "foo", "10");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void ZScoreACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ZSCORE",
                server,
                ["sortedset", "read", "fast"],
                [DoZScore]
            );

            void DoZScore()
            {
                RedisResult val = db.Execute("ZSCORE", "foo", "bar");
                Assert.IsTrue(val.IsNull);
            }
        }

        [Test]
        public void ZRemACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ZREM",
                server,
                ["sortedset", "write", "fast"],
                [DoZRem, DoZRemMulti]
            );

            void DoZRem()
            {
                RedisResult val = db.Execute("ZREM", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }

            void DoZRemMulti()
            {
                RedisResult val = db.Execute("ZREM", "foo", "bar", "fizz");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ZCountACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ZCOUNT",
                server,
                ["sortedset", "read", "fast"],
                [DoZCount]
            );

            void DoZCount()
            {
                RedisResult val = db.Execute("ZCOUNT", "foo", "10", "20");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ZIncrByACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            int count = 0;

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ZINCRBY",
                server,
                ["sortedset", "write", "fast"],
                [DoZIncrBy]
            );

            void DoZIncrBy()
            {
                RedisResult val = db.Execute("ZINCRBY", $"foo-{count}", "10", "bar");
                count++;
                Assert.AreEqual(10, (double)val);
            }
        }

        [Test]
        public void ZRankACLs()
        {
            // todo: ZRANK doesn't implement WITHSCORE (removed code that half did) - come back and add when fixed

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            // todo: ZRANK errors if key does not exist, which is incorrect - creating key for this test
            Assert.AreEqual(1, (int)db.Execute("ZADD", "foo", "10", "bar"));

            CheckCommands(
                "ZRANK",
                server,
                ["sortedset", "read", "fast"],
                [DoZRank]
            );

            void DoZRank()
            {
                RedisResult val = db.Execute("ZRANK", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ZRangeACLs()
        {
            // todo: ZRange has loads of options, come back and test all the different lengths

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ZRANGE",
                server,
                ["sortedset", "read", "slow"],
                [DoZRange]
            );

            void DoZRange()
            {
                RedisResult val = db.Execute("ZRANGE", "key", "10", "20");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void ZRangeByScoreACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ZRANGEBYSCORE",
                server,
                ["sortedset", "read", "slow"],
                [DoZRangeByScore, DoZRangeByScoreWithScores, DoZRangeByScoreLimit, DoZRangeByScoreWithScoresLimit]
            );

            void DoZRangeByScore()
            {
                RedisResult val = db.Execute("ZRANGEBYSCORE", "key", "10", "20");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            void DoZRangeByScoreWithScores()
            {
                RedisResult val = db.Execute("ZRANGEBYSCORE", "key", "10", "20", "WITHSCORES");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            void DoZRangeByScoreLimit()
            {
                RedisResult val = db.Execute("ZRANGEBYSCORE", "key", "10", "20", "LIMIT", "2", "3");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            void DoZRangeByScoreWithScoresLimit()
            {
                RedisResult val = db.Execute("ZRANGEBYSCORE", "key", "10", "20", "WITHSCORES", "LIMIT", "2", "3");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void ZRevRangeACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ZREVRANGE",
                server,
                ["sortedset", "read", "slow"],
                [DoZRevRange, DoZRevRangeWithScores]
            );

            void DoZRevRange()
            {
                RedisResult val = db.Execute("ZREVRANGE", "key", "10", "20");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            void DoZRevRangeWithScores()
            {
                RedisResult val = db.Execute("ZREVRANGE", "key", "10", "20", "WITHSCORES");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void ZRevRankACLs()
        {
            // todo: ZREVRANK doesn't implement WITHSCORE (removed code that half did) - come back and add when fixed

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            // todo: ZREVRANK errors if key does not exist, which is incorrect - creating key for this test
            Assert.AreEqual(1, (int)db.Execute("ZADD", "foo", "10", "bar"));

            CheckCommands(
                "ZREVRANK",
                server,
                ["sortedset", "read", "fast"],
                [DoZRevRank]
            );

            void DoZRevRank()
            {
                RedisResult val = db.Execute("ZREVRANK", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ZRemRangeByLexACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ZREMRANGEBYLEX",
                server,
                ["sortedset", "write", "slow"],
                [DoZRemRangeByLex]
            );

            void DoZRemRangeByLex()
            {
                RedisResult val = db.Execute("ZREMRANGEBYLEX", "foo", "abc", "def");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ZRemRangeByRankACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ZREMRANGEBYRANK",
                server,
                ["sortedset", "write", "slow"],
                [DoZRemRangeByRank]
            );

            void DoZRemRangeByRank()
            {
                RedisResult val = db.Execute("ZREMRANGEBYRANK", "foo", "10", "20");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ZRemRangeByScoreACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ZREMRANGEBYSCORE",
                server,
                ["sortedset", "write", "slow"],
                [DoZRemRangeByRank]
            );

            void DoZRemRangeByRank()
            {
                RedisResult val = db.Execute("ZREMRANGEBYSCORE", "foo", "10", "20");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ZLexCountACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ZLEXCOUNT",
                server,
                ["sortedset", "read", "fast"],
                [DoZLexCount]
            );

            void DoZLexCount()
            {
                RedisResult val = db.Execute("ZLEXCOUNT", "foo", "abc", "def");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ZPopMinACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ZPOPMIN",
                server,
                ["sortedset", "write", "fast"],
                [DoZPopMin, DoZPopMinCount]
            );

            void DoZPopMin()
            {
                RedisResult val = db.Execute("ZPOPMIN", "foo");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            void DoZPopMinCount()
            {
                RedisResult val = db.Execute("ZPOPMIN", "foo", "10");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void ZRandMemberACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ZRANDMEMBER",
                server,
                ["sortedset", "read", "slow"],
                [DoZRandMember, DoZRandMemberCount, DoZRandMemberCountWithScores]
            );

            void DoZRandMember()
            {
                RedisResult val = db.Execute("ZRANDMEMBER", "foo");
                Assert.IsTrue(val.IsNull);
            }

            void DoZRandMemberCount()
            {
                RedisResult val = db.Execute("ZRANDMEMBER", "foo", "10");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            void DoZRandMemberCountWithScores()
            {
                RedisResult val = db.Execute("ZRANDMEMBER", "foo", "10", "WITHSCORES");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void ZDiffACLs()
        {
            // todo: ZDIFF doesn't implement WITHSCORES correctly right now - come back and cover when fixed

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ZDIFF",
                server,
                ["sortedset", "read", "slow"],
                [DoZDiff, DoZDiffMulti]
            );

            void DoZDiff()
            {
                RedisResult val = db.Execute("ZDIFF", "1", "foo");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            void DoZDiffMulti()
            {
                RedisResult val = db.Execute("ZDIFF", "2", "foo", "bar");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void ZScanACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ZSCAN",
                server,
                ["read", "sortedset", "slow"],
                [DoZScan, DoZScanMatch, DoZScanCount, DoZScanNoValues, DoZScanMatchCount, DoZScanMatchNoValues, DoZScanCountNoValues, DoZScanMatchCountNoValues]
            );

            void DoZScan()
            {
                RedisResult val = db.Execute("ZSCAN", "foo", "0");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            void DoZScanMatch()
            {
                RedisResult val = db.Execute("ZSCAN", "foo", "0", "MATCH", "*");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            void DoZScanCount()
            {
                RedisResult val = db.Execute("ZSCAN", "foo", "0", "COUNT", "2");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            void DoZScanNoValues()
            {
                RedisResult val = db.Execute("ZSCAN", "foo", "0", "NOVALUES");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            void DoZScanMatchCount()
            {
                RedisResult val = db.Execute("ZSCAN", "foo", "0", "MATCH", "*", "COUNT", "2");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            void DoZScanMatchNoValues()
            {
                RedisResult val = db.Execute("ZSCAN", "foo", "0", "MATCH", "*", "NOVALUES");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            void DoZScanCountNoValues()
            {
                RedisResult val = db.Execute("ZSCAN", "foo", "0", "COUNT", "0", "NOVALUES");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            void DoZScanMatchCountNoValues()
            {
                RedisResult val = db.Execute("ZSCAN", "foo", "0", "MATCH", "*", "COUNT", "0", "NOVALUES");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }
        }

        [Test]
        public void ZMScoreACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "ZMSCORE",
                server,
                ["sortedset", "read", "fast"],
                [DoZDiff, DoZDiffMulti]
            );

            void DoZDiff()
            {
                RedisResult val = db.Execute("ZMSCORE", "foo", "bar");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(1, valArr.Length);
                Assert.IsTrue(valArr[0].IsNull);
            }

            void DoZDiffMulti()
            {
                RedisResult val = db.Execute("ZMSCORE", "foo", "bar", "fizz");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(2, valArr.Length);
                Assert.IsTrue(valArr[0].IsNull);
                Assert.IsTrue(valArr[1].IsNull);
            }
        }

        [Test]
        public void TimeACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "TIME",
                server,
                ["fast"],
                [DoTime]
            );

            void DoTime()
            {
                RedisResult val = db.Execute("TIME");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
                Assert.IsTrue((long)valArr[0] > 0);
                Assert.IsTrue((long)valArr[1] >= 0);
            }
        }

        [Test]
        public void TTLACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "TTL",
                server,
                ["keyspace", "read", "fast"],
                [DoPTTL]
            );

            void DoPTTL()
            {
                RedisResult val = db.Execute("TTL", "foo");
                Assert.AreEqual(-1, (int)val);
            }
        }

        [Test]
        public void TypeACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "TTL",
                server,
                ["keyspace", "read", "fast"],
                [DoType]
            );

            void DoType()
            {
                RedisResult val = db.Execute("TYPE", "foo");
                Assert.AreEqual("none", (string)val);
            }
        }

        [Test]
        public void UnlinkACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "UNLINK",
                server,
                ["keyspace", "write", "fast"],
                [DoUnlink, DoUnlinkMulti]
            );

            void DoUnlink()
            {
                RedisResult val = db.Execute("UNLINK", "foo");
                Assert.AreEqual(0, (int)val);
            }

            void DoUnlinkMulti()
            {
                RedisResult val = db.Execute("UNLINK", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void UnsubscribeACLs()
        {
            // this is a strange test, because we have to contort ourselves to get SE.Redis to actually issue the expected command

            // todo: not testing the 0 argument version of UNSUBSCRIBE, as it's a pain

            const int ChannelCount = 10;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword, disablePubSub: false));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();
            ISubscriber sub = redis.GetSubscriber();

            // gotta subscribe or SE.Redis won't issue the commands
            HashSet<string> openChannels = new();
            for (var i = 0; i < ChannelCount; i++)
            {
                string ch = $"channel-{i}";
                sub.Subscribe(new(ch, RedisChannel.PatternMode.Literal));

                openChannels.Add(ch);
            }

            CheckCommands(
                "UNSUBSCRIBE",
                server,
                ["pubsub", "slow"],
                [DoUnsubscribePattern]
            );

            void DoUnsubscribePattern()
            {
                string toUnSub = openChannels.First();

                // we remove before trying, because SE.Redis will toss the subscription from it's internal state ANYWAY
                openChannels.Remove(toUnSub);

                // gotta use the async version (incorrectly) so we actually wait for the response
                redis.GetSubscriber().UnsubscribeAsync(new(toUnSub, RedisChannel.PatternMode.Literal)).GetAwaiter().GetResult();
            }
        }

        [Test]
        public void WatchACLs()
        {
            // todo: should watch fail outside of a transaction?
            // todo: multi key WATCH isn't implemented correctly, add once fixed

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword, disablePubSub: false));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "WATCH",
                server,
                ["transaction", "fast"],
                [DoWatch]
            );

            void DoWatch()
            {
                RedisResult val = db.Execute("WATCH", "foo");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void WatchMSACLs()
        {
            // todo: should watch fail outside of a transaction?

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword, disablePubSub: false));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "WATCH MS",
                server,
                ["transaction", "fast", "garnet"],
                [DoWatchMS]
            );

            void DoWatchMS()
            {
                RedisResult val = db.Execute("WATCH", "MS", "foo");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void WatchOSACLs()
        {
            // todo: should watch fail outside of a transaction?

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword, disablePubSub: false));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "WATCH OS",
                server,
                ["transaction", "fast", "garnet"],
                [DoWatchOS]
            );

            void DoWatchOS()
            {
                RedisResult val = db.Execute("WATCH", "OS", "foo");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void UnwatchACLs()
        {
            // todo: should watch fail outside of a transaction?

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword, disablePubSub: false));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            CheckCommands(
                "UNWATCH",
                server,
                ["transaction", "fast"],
                [DoUnwatch]
            );

            void DoUnwatch()
            {
                RedisResult val = db.Execute("UNWATCH");
                Assert.AreEqual("OK", (string)val);
            }
        }

        /// <summary>
        /// Check permissions are denied if individual categories are removed from a +@all user.
        /// </summary>
        private static void CheckCommands(
            string command,
            IServer server,
            string[] categories,
            Action[] commands
        )
        {
            // check legal with +@all
            ResetDefaultUser(server);
            foreach (Action del in commands)
            {
                Assert.True(CheckAuthFailure(del), $"{command} denied when should have been permitted (user had +@all)");
            }

            // check that deny each category causes the command to fail
            foreach (string category in categories)
            {
                ResetDefaultUser(server);
                SetUser(server, "default", $"-@{category}");
                foreach (Action del in commands)
                {
                    Assert.False(CheckAuthFailure(del), $"{command} permitted when should have been denied (user had -@{category})");
                }

                // if not -@fast and not -@connection, we can check that the connection still works with PING
                if (!(category == "fast" || category == "connection"))
                {
                    CheckConnection(server);
                }
            }
        }

        /// <summary>
        /// Adds +@all to the default user ACL.
        /// </summary>
        private static void ResetDefaultUser(IServer server)
        => SetUser(server, "default", "+@all");

        /// <summary>
        /// Runs ACL SETUSER default [aclPatterns]
        /// </summary>
        private static void SetUser(IServer server, string user, params string[] aclPatterns)
        {
            RedisResult res = server.Execute("ACL", ["SETUSER", user, .. aclPatterns]);

            Assert.AreEqual("OK", (string)res, $"Updating user ({user}) failed");
        }

        /// <summary>
        /// Returns true if no AUTH failure.
        /// Returns false AUTH failure.
        /// 
        /// Throws if anything else.
        /// </summary>
        private static bool CheckAuthFailure(Action act)
        {
            try
            {
                act();
                return true;
            }
            catch (RedisConnectionException e)
            {
                if (e.FailureType != ConnectionFailureType.AuthenticationFailure)
                {
                    throw;
                }

                return false;
            }
            catch (RedisException e)
            {
                if (e.Message != "NOAUTH Authentication required.")
                {
                    throw;
                }

                return false;
            }
        }

        /// <summary>
        /// Check that connection is still in a good state.
        /// </summary>
        private static void CheckConnection(IServer server)
        {
            server.Ping();
        }
    }
}
