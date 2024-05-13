// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading;
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
        public void BGSave()
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

        // todo: COMMITAOF is strange, handle in later change

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
        public void ConfigRewrite()
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
                        catch(RedisException e)
                        {
                            if(e.Message == "ERR Unknown option or number of arguments for CONFIG SET - 'foo'")
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
                        catch(RedisException e)
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
