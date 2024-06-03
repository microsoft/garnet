// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Reflection;
using Garnet.server;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test.Resp.ACL
{
    public class RespCommandTests
    {
        private const string DefaultPassword = nameof(RespCommandTests);
        private const string DefaultUser = "default";

        private IReadOnlyDictionary<string, RespCommandsInfo> respCustomCommandsInfo;

        private GarnetServer server;


        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, defaultPassword: DefaultPassword, useAcl: true);

            // Register custom commands so we can test ACL'ing them
            Assert.IsTrue(TestUtils.TryGetCustomCommandsInfo(out respCustomCommandsInfo));
            Assert.IsNotNull(respCustomCommandsInfo);

            server.Register.NewCommand("SETWPIFPGT", 2, CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand(), respCustomCommandsInfo["SETWPIFPGT"]);
            server.Register.NewCommand("MYDICTGET", 1, CommandType.Read, new MyDictFactory(), respCustomCommandsInfo["MYDICTGET"]);
            server.Register.NewTransactionProc("READWRITETX", 3, () => new ReadWriteTxn());

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
            Assert.IsTrue(RespCommandsInfo.TryGetRespCommandNames(out IReadOnlySet<string> advertisedCommands), "Couldn't get advertised RESP commands");

            IEnumerable<string> withOnlySubCommands = allInfo.Where(static x => (x.Value.SubCommands?.Length ?? 0) != 0 && x.Value.Flags == RespCommandFlags.None).Select(static x => x.Key);
            IEnumerable<string> notCoveredByACLs = allInfo.Where(static x => x.Value.Flags.HasFlag(RespCommandFlags.NoAuth)).Select(static kv => kv.Key);

            // Check tests against RespCommandsInfo
            {
                // Exclude things like ACL, CLIENT, CLUSTER which are "commands" but only their sub commands can be run
                IEnumerable<string> subCommands = allInfo.Where(static x => x.Value.SubCommands != null).SelectMany(static x => x.Value.SubCommands).Select(static x => x.Name);
                IEnumerable<string> deSubCommanded = advertisedCommands.Except(withOnlySubCommands).Union(subCommands).Select(static x => x.Replace("|", "").Replace("_", "").Replace("-", ""));
                IEnumerable<string> notCovered = deSubCommanded.Except(covered, StringComparer.OrdinalIgnoreCase).Except(notCoveredByACLs, StringComparer.OrdinalIgnoreCase);

                Assert.IsEmpty(notCovered, $"Commands in RespCommandsInfo not covered by ACL Tests:{Environment.NewLine}{string.Join(Environment.NewLine, notCovered.OrderBy(static x => x))}");
            }

            // Check tests against RespCommand
            {
                IEnumerable<RespCommand> allValues = Enum.GetValues<RespCommand>().Select(static x => x.NormalizeForACLs()).Distinct();
                IEnumerable<RespCommand> testableValues =
                    allValues
                    .Except([RespCommand.NONE, RespCommand.INVALID])
                    .Where(cmd => !withOnlySubCommands.Contains(cmd.ToString().Replace("_", ""), StringComparer.OrdinalIgnoreCase))
                    .Where(cmd => !notCoveredByACLs.Contains(cmd.ToString().Replace("_", ""), StringComparer.OrdinalIgnoreCase));
                IEnumerable<RespCommand> notCovered = testableValues.Where(cmd => !covered.Contains(cmd.ToString().Replace("_", ""), StringComparer.OrdinalIgnoreCase));

                Assert.IsEmpty(notCovered, $"Commands in RespCOmmand not covered by ACL Tests:{Environment.NewLine}{string.Join(Environment.NewLine, notCovered.OrderBy(static x => x))}");
            }
        }

        [Test]
        public void AclCatACLs()
        {
            CheckCommands(
                "ACL CAT",
                [DoAclCat]
            );

            static void DoAclCat(IServer server)
            {
                RedisResult val = server.Execute("ACL", "CAT");
                Assert.AreEqual(ResultType.Array, val.Resp2Type);
            }
        }

        [Test]
        public void AclDelUserACLs()
        {
            CheckCommands(
                "ACL DELUSER",
                [DoAclDelUser, DoAclDelUserMulti]
            );

            static void DoAclDelUser(IServer server)
            {
                RedisResult val = server.Execute("ACL", "DELUSER", "does-not-exist");
                Assert.AreEqual(0, (int)val);
            }

            void DoAclDelUserMulti(IServer server)
            {
                RedisResult val = server.Execute("ACL", "DELUSER", "does-not-exist-1", "does-not-exist-2");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void AclListACLs()
        {
            CheckCommands(
                "ACL LIST",
                [DoAclList]
            );

            static void DoAclList(IServer server)
            {
                RedisResult val = server.Execute("ACL", "LIST");
                Assert.AreEqual(ResultType.Array, val.Resp2Type);
            }
        }

        [Test]
        public void AclLoadACLs()
        {
            CheckCommands(
                "ACL LOAD",
                [DoAclLoad]
            );

            static void DoAclLoad(IServer server)
            {
                try
                {
                    RedisResult val = server.Execute("ACL", "LOAD");

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

        [Test]
        public void AclSetUserACLs()
        {
            CheckCommands(
                "ACL SETUSER",
                [DoAclSetUserOn, DoAclSetUserCategory, DoAclSetUserOnCategory]
            );

            static void DoAclSetUserOn(IServer server)
            {
                RedisResult res = server.Execute("ACL", "SETUSER", "foo", "on");
                Assert.AreEqual("OK", (string)res);
            }

            static void DoAclSetUserCategory(IServer server)
            {
                RedisResult res = server.Execute("ACL", "SETUSER", "foo", "+@read");
                Assert.AreEqual("OK", (string)res);
            }

            static void DoAclSetUserOnCategory(IServer server)
            {
                RedisResult res = server.Execute("ACL", "SETUSER", "foo", "on", "+@read");
                Assert.AreEqual("OK", (string)res);
            }
        }

        [Test]
        public void AclUsersACLs()
        {
            CheckCommands(
                "ACL USERS",
                [DoAclUsers]
            );

            static void DoAclUsers(IServer server)
            {
                RedisResult val = server.Execute("ACL", "USERS");
                Assert.AreEqual(ResultType.Array, val.Resp2Type);
            }
        }

        [Test]
        public void AclWhoAmIACLs()
        {
            CheckCommands(
                "ACL WHOAMI",
                [DoAclWhoAmI]
            );

            static void DoAclWhoAmI(IServer server)
            {
                RedisResult val = server.Execute("ACL", "WHOAMI");
                Assert.AreNotEqual("", (string)val);
            }
        }

        [Test]
        public void AppendACLs()
        {
            int count = 0;

            CheckCommands(
                "APPEND",
                [DoAppend]
            );

            void DoAppend(IServer server)
            {
                RedisResult val = server.Execute("APPEND", $"key-{count}", "foo");
                count++;

                Assert.AreEqual(3, (int)val);
            }
        }

        [Test]
        public void AskingACLs()
        {
            CheckCommands(
                "ASKING",
                [DoAsking]
            );

            void DoAsking(IServer server)
            {
                RedisResult val = server.Execute("ASKING");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void BGSaveACLs()
        {
            CheckCommands(
                "BGSAVE",
                [DoBGSave, DoBGSaveSchedule]
            );



            static void DoBGSave(IServer server)
            {
                try
                {
                    RedisResult res = server.Execute("BGSAVE");

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

            static void DoBGSaveSchedule(IServer server)
            {
                try
                {
                    RedisResult res = server.Execute("BGSAVE", "SCHEDULE");
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

        [Test]
        public void BitcountACLs()
        {
            CheckCommands(
                "BITCOUNT",
                [DoBitCount, DoBitCountStartEnd, DoBitCountStartEndBit, DoBitCountStartEndByte]
            );

            static void DoBitCount(IServer server)
            {
                RedisResult val = server.Execute("BITCOUNT", "empty-key");
                Assert.AreEqual(0, (int)val);
            }

            static void DoBitCountStartEnd(IServer server)
            {
                RedisResult val = server.Execute("BITCOUNT", "empty-key", "1", "1");
                Assert.AreEqual(0, (int)val);
            }

            static void DoBitCountStartEndByte(IServer server)
            {
                RedisResult val = server.Execute("BITCOUNT", "empty-key", "1", "1", "BYTE");
                Assert.AreEqual(0, (int)val);
            }

            static void DoBitCountStartEndBit(IServer server)
            {
                RedisResult val = server.Execute("BITCOUNT", "empty-key", "1", "1", "BIT");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void BitfieldACLs()
        {
            int count = 0;

            CheckCommands(
                "BITFIELD",
                [
                    DoBitFieldGet, DoBitFieldGetWrap, DoBitFieldGetSat, DoBitFieldGetFail,
                    DoBitFieldSet, DoBitFieldSetWrap, DoBitFieldSetSat, DoBitFieldSetFail,
                    DoBitFieldIncrBy, DoBitFieldIncrByWrap, DoBitFieldIncrBySat, DoBitFieldIncrByFail,
                    DoBitFieldMulti
                ]
            );

            void DoBitFieldGet(IServer server)
            {
                RedisResult val = server.Execute("BITFIELD", $"empty-{count}", "GET", "u4", "0");
                count++;
                Assert.AreEqual(0, (int)val);
            }

            void DoBitFieldGetWrap(IServer server)
            {
                RedisResult val = server.Execute("BITFIELD", $"empty-{count}", "GET", "u4", "0", "OVERFLOW", "WRAP");
                count++;
                Assert.AreEqual(0, (int)val);
            }

            void DoBitFieldGetSat(IServer server)
            {
                RedisResult val = server.Execute("BITFIELD", $"empty-{count}", "GET", "u4", "0", "OVERFLOW", "SAT");
                count++;
                Assert.AreEqual(0, (int)val);
            }

            void DoBitFieldGetFail(IServer server)
            {
                RedisResult val = server.Execute("BITFIELD", $"empty-{count}", "GET", "u4", "0", "OVERFLOW", "FAIL");
                count++;
                Assert.AreEqual(0, (int)val);
            }

            void DoBitFieldSet(IServer server)
            {
                RedisResult val = server.Execute("BITFIELD", $"empty-{count}", "SET", "u4", "0", "1");
                count++;
                Assert.AreEqual(0, (int)val);
            }

            void DoBitFieldSetWrap(IServer server)
            {
                RedisResult val = server.Execute("BITFIELD", $"empty-{count}", "SET", "u4", "0", "1", "OVERFLOW", "WRAP");
                count++;
                Assert.AreEqual(0, (int)val);
            }

            void DoBitFieldSetSat(IServer server)
            {
                RedisResult val = server.Execute("BITFIELD", $"empty-{count}", "SET", "u4", "0", "1", "OVERFLOW", "SAT");
                count++;
                Assert.AreEqual(0, (int)val);
            }

            void DoBitFieldSetFail(IServer server)
            {
                RedisResult val = server.Execute("BITFIELD", $"empty-{count}", "SET", "u4", "0", "1", "OVERFLOW", "FAIL");
                count++;
                Assert.AreEqual(0, (int)val);
            }

            void DoBitFieldIncrBy(IServer server)
            {
                RedisResult val = server.Execute("BITFIELD", $"empty-{count}", "INCRBY", "u4", "0", "4");
                count++;
                Assert.AreEqual(4, (int)val);
            }

            void DoBitFieldIncrByWrap(IServer server)
            {
                RedisResult val = server.Execute("BITFIELD", $"empty-{count}", "INCRBY", "u4", "0", "4", "OVERFLOW", "WRAP");
                count++;
                Assert.AreEqual(4, (int)val);
            }

            void DoBitFieldIncrBySat(IServer server)
            {
                RedisResult val = server.Execute("BITFIELD", $"empty-{count}", "INCRBY", "u4", "0", "4", "OVERFLOW", "SAT");
                count++;
                Assert.AreEqual(4, (int)val);
            }

            void DoBitFieldIncrByFail(IServer server)
            {
                RedisResult val = server.Execute("BITFIELD", $"empty-{count}", "INCRBY", "u4", "0", "4", "OVERFLOW", "FAIL");
                count++;
                Assert.AreEqual(4, (int)val);
            }

            void DoBitFieldMulti(IServer server)
            {
                RedisResult val = server.Execute("BITFIELD", $"empty-{count}", "OVERFLOW", "WRAP", "GET", "u4", "1", "SET", "u4", "2", "1", "OVERFLOW", "FAIL", "INCRBY", "u4", "6", "2");
                count++;

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
            CheckCommands(
                "BITFIELD_RO",
                [DoBitFieldROGet, DoBitFieldROMulti]
            );

            static void DoBitFieldROGet(IServer server)
            {
                RedisResult val = server.Execute("BITFIELD_RO", "empty-a", "GET", "u4", "0");
                Assert.AreEqual(0, (int)val);
            }

            static void DoBitFieldROMulti(IServer server)
            {
                RedisResult val = server.Execute("BITFIELD_RO", "empty-b", "GET", "u4", "0", "GET", "u4", "3");

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
            CheckCommands(
                "BITOP",
                [DoBitOpAnd, DoBitOpAndMulti, DoBitOpOr, DoBitOpOrMulti, DoBitOpXor, DoBitOpXorMulti, DoBitOpNot]
            );

            static void DoBitOpAnd(IServer server)
            {
                RedisResult val = server.Execute("BITOP", "AND", "zero", "zero");
                Assert.AreEqual(0, (int)val);
            }

            static void DoBitOpAndMulti(IServer server)
            {
                RedisResult val = server.Execute("BITOP", "AND", "zero", "zero", "one", "zero");
                Assert.AreEqual(0, (int)val);
            }

            static void DoBitOpOr(IServer server)
            {
                RedisResult val = server.Execute("BITOP", "OR", "one", "one");
                Assert.AreEqual(0, (int)val);
            }

            static void DoBitOpOrMulti(IServer server)
            {
                RedisResult val = server.Execute("BITOP", "OR", "one", "one", "one", "one");
                Assert.AreEqual(0, (int)val);
            }

            static void DoBitOpXor(IServer server)
            {
                RedisResult val = server.Execute("BITOP", "XOR", "one", "zero");
                Assert.AreEqual(0, (int)val);
            }

            static void DoBitOpXorMulti(IServer server)
            {
                RedisResult val = server.Execute("BITOP", "XOR", "one", "one", "one", "zero");
                Assert.AreEqual(0, (int)val);
            }

            static void DoBitOpNot(IServer server)
            {
                RedisResult val = server.Execute("BITOP", "NOT", "one", "zero");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void BitPosACLs()
        {
            CheckCommands(
                "BITPOS",
                [DoBitPos, DoBitPosStart, DoBitPosStartEnd, DoBitPosStartEndBit, DoBitPosStartEndByte]
            );

            static void DoBitPos(IServer server)
            {
                RedisResult val = server.Execute("BITPOS", "empty", "1");
                Assert.AreEqual(-1, (int)val);
            }

            static void DoBitPosStart(IServer server)
            {
                RedisResult val = server.Execute("BITPOS", "empty", "1", "5");
                Assert.AreEqual(-1, (int)val);
            }

            static void DoBitPosStartEnd(IServer server)
            {
                RedisResult val = server.Execute("BITPOS", "empty", "1", "5", "7");
                Assert.AreEqual(-1, (int)val);
            }

            static void DoBitPosStartEndBit(IServer server)
            {
                RedisResult val = server.Execute("BITPOS", "empty", "1", "5", "7", "BIT");
                Assert.AreEqual(-1, (int)val);
            }

            static void DoBitPosStartEndByte(IServer server)
            {
                RedisResult val = server.Execute("BITPOS", "empty", "1", "5", "7", "BYTE");
                Assert.AreEqual(-1, (int)val);
            }
        }

        [Test]
        public void ClientACLs()
        {
            // TODO: client isn't really implemented looks like, so this is mostly a placeholder in case it gets implemented correctly

            CheckCommands(
                "CLIENT",
                [DoClient]
            );

            static void DoClient(IServer server)
            {
                RedisResult val = server.Execute("CLIENT");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void ClusterAddSlotsACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER ADDSLOTS",
                [DoClusterAddSlots, DoClusterAddSlotsMulti]
            );

            static void DoClusterAddSlots(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "ADDSLOTS", "1");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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

            static void DoClusterAddSlotsMulti(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "ADDSLOTS", "1", "2");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterAddSlotsRangeACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER ADDSLOTSRANGE",
                [DoClusterAddSlotsRange, DoClusterAddSlotsRangeMulti]
            );

            static void DoClusterAddSlotsRange(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "ADDSLOTSRANGE", "1", "3");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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

            static void DoClusterAddSlotsRangeMulti(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "ADDSLOTSRANGE", "1", "3", "7", "9");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterAofSyncACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER AOFSYNC",
                [DoClusterAofSync]
            );

            static void DoClusterAofSync(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "AOFSYNC", "abc", "def");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterAppendLogACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER APPENDLOG",
                [DoClusterAppendLog]
            );

            static void DoClusterAppendLog(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "APPENDLOG", "a", "b", "c", "d", "e");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterBanListACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER BANLIST",
                [DoClusterBanList]
            );

            static void DoClusterBanList(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "BANLIST");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterBeginReplicaRecoverACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER BEGIN_REPLICA_RECOVER",
                [DoClusterBeginReplicaFailover]
            );

            static void DoClusterBeginReplicaFailover(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "BEGIN_REPLICA_RECOVER", "1", "2", "3", "4", "5", "6", "7");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterBumpEpochACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER BUMPEPOCH",
                [DoClusterBumpEpoch]
            );

            static void DoClusterBumpEpoch(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "BUMPEPOCH");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterCountKeysInSlotACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER COUNTKEYSINSLOT",
                [DoClusterBumpEpoch]
            );

            static void DoClusterBumpEpoch(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "COUNTKEYSINSLOT", "1");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterDelKeysInSlotACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER DELKEYSINSLOT",
                [DoClusterDelKeysInSlot]
            );

            static void DoClusterDelKeysInSlot(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "DELKEYSINSLOT", "1");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterDelKeysInSlotRangeACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER DELKEYSINSLOTRANGE",
                [DoClusterDelKeysInSlotRange, DoClusterDelKeysInSlotRangeMulti]
            );

            static void DoClusterDelKeysInSlotRange(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "DELKEYSINSLOTRANGE", "1", "3");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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

            static void DoClusterDelKeysInSlotRangeMulti(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "DELKEYSINSLOTRANGE", "1", "3", "5", "9");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterDelSlotsACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER DELSLOTS",
                [DoClusterDelSlots, DoClusterDelSlotsMulti]
            );

            static void DoClusterDelSlots(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "DELSLOTS", "1");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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

            static void DoClusterDelSlotsMulti(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "DELSLOTS", "1", "2");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterDelSlotsRangeACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER DELSLOTSRANGE",
                [DoClusterDelSlotsRange, DoClusterDelSlotsRangeMulti]
            );

            static void DoClusterDelSlotsRange(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "DELSLOTSRANGE", "1", "3");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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

            static void DoClusterDelSlotsRangeMulti(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "DELSLOTSRANGE", "1", "3", "9", "11");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterEndpointACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER ENDPOINT",
                [DoClusterEndpoint]
            );

            static void DoClusterEndpoint(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "ENDPOINT", "abcd");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterFailoverACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER FAILOVER",
                [DoClusterFailover, DoClusterFailoverForce]
            );

            static void DoClusterFailover(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "FAILOVER");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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

            static void DoClusterFailoverForce(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "FAILOVER", "FORCE");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterFailStopWritesACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER FAILSTOPWRITES",
                [DoClusterFailStopWrites]
            );

            static void DoClusterFailStopWrites(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "FAILSTOPWRITES", "foo");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterFailReplicationOffsetACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER FAILREPLICATIONOFFSET",
                [DoClusterFailStopWrites]
            );

            static void DoClusterFailStopWrites(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "FAILREPLICATIONOFFSET", "foo");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterForgetACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER FORGET",
                [DoClusterForget]
            );

            static void DoClusterForget(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "FORGET", "foo");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterGetKeysInSlotACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER GETKEYSINSLOT",
                [DoClusterGetKeysInSlot]
            );

            static void DoClusterGetKeysInSlot(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "GETKEYSINSLOT", "foo", "3");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterGossipACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER GOSSIP",
                [DoClusterGossip]
            );

            static void DoClusterGossip(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "GOSSIP", "foo", "3");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterHelpACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER HELP",
                [DoClusterHelp]
            );

            static void DoClusterHelp(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "HELP");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterInfoACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER INFO",
                [DoClusterInfo]
            );

            static void DoClusterInfo(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "INFO");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterInitiateReplicaSyncACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER INITIATE_REPLICA_SYNC",
                [DoClusterInfo]
            );

            static void DoClusterInfo(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "INITIATE_REPLICA_SYNC", "1", "2", "3", "4", "5");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterKeySlotACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER KEYSLOT",
                [DoClusterKeySlot]
            );

            static void DoClusterKeySlot(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "KEYSLOT", "foo");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterMeetACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER MEET",
                [DoClusterMeet, DoClusterMeetPort]
            );

            static void DoClusterMeet(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "MEET", "127.0.0.1", "1234");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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

            static void DoClusterMeetPort(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "MEET", "127.0.0.1", "1234", "6789");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterMigrateACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER MIGRATE",
                [DoClusterMigrate]
            );

            static void DoClusterMigrate(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "MIGRATE", "a", "b", "c");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterMTasksACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER MTASKS",
                [DoClusterMTasks]
            );

            static void DoClusterMTasks(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "MTASKS");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterMyIdACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER MYID",
                [DoClusterMyId]
            );

            static void DoClusterMyId(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "MYID");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterMyParentIdACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER MYPARENTID",
                [DoClusterMyParentId]
            );

            static void DoClusterMyParentId(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "MYPARENTID");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterNodesACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER NODES",
                [DoClusterNodes]
            );

            static void DoClusterNodes(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "NODES");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterReplicasACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER REPLICAS",
                [DoClusterReplicas]
            );

            static void DoClusterReplicas(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "REPLICAS", "foo");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterReplicateACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER REPLICATE",
                [DoClusterReplicate]
            );

            static void DoClusterReplicate(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "REPLICATE", "foo");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterResetACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER RESET",
                [DoClusterReset, DoClusteResetHard]
            );

            static void DoClusterReset(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "RESET");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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

            static void DoClusteResetHard(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "RESET", "HARD");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterSendCkptFileSegmentACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER SEND_CKPT_FILE_SEGMENT",
                [DoClusterSendCkptFileSegment]
            );

            static void DoClusterSendCkptFileSegment(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "SEND_CKPT_FILE_SEGMENT", "1", "2", "3", "4", "5");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterSendCkptMetadataACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER SEND_CKPT_METADATA",
                [DoClusterSendCkptMetadata]
            );

            static void DoClusterSendCkptMetadata(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "SEND_CKPT_METADATA", "1", "2", "3", "4", "5");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterSetConfigEpochACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER SET-CONFIG-EPOCH",
                [DoClusterSetConfigEpoch]
            );

            static void DoClusterSetConfigEpoch(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "SET-CONFIG-EPOCH", "foo");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterSetSlotACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER SETSLOT",
                [DoClusterSetSlot, DoClusterSetSlotStable, DoClusterSetSlotImporting]
            );

            static void DoClusterSetSlot(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "SETSLOT", "1");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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

            static void DoClusterSetSlotStable(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "SETSLOT", "1", "STABLE");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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

            static void DoClusterSetSlotImporting(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "SETSLOT", "1", "IMPORTING", "foo");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterSetSlotsRangeACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER SETSLOTSRANGE",
                [DoClusterSetSlotsRangeStable, DoClusterSetSlotsRangeStableMulti, DoClusterSetSlotsRangeImporting, DoClusterSetSlotsRangeImportingMulti]
            );

            static void DoClusterSetSlotsRangeStable(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "SETSLOTSRANGE", "STABLE", "1", "5");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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

            static void DoClusterSetSlotsRangeStableMulti(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "SETSLOTSRANGE", "STABLE", "1", "5", "10", "15");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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

            static void DoClusterSetSlotsRangeImporting(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "SETSLOTSRANGE", "IMPORTING", "foo", "1", "5");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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

            static void DoClusterSetSlotsRangeImportingMulti(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "SETSLOTSRANGE", "IMPORTING", "foo", "1", "5", "10", "15");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterShardsACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER SHARDS",
                [DoClusterShards]
            );

            static void DoClusterShards(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "SHARDS");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterSlotsACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER SLOTS",
                [DoClusterSlots]
            );

            static void DoClusterSlots(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "SLOTS");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void ClusterSlotStateACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            CheckCommands(
                "CLUSTER SLOTSTATE",
                [DoClusterSlotState]
            );

            static void DoClusterSlotState(IServer server)
            {
                try
                {
                    server.Execute("CLUSTER", "SLOTSTATE");
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
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
        public void CommandACLs()
        {
            CheckCommands(
                "COMMAND",
                [DoCommand]
            );

            static void DoCommand(IServer server)
            {
                RedisResult val = server.Execute("COMMAND");
                Assert.AreEqual(ResultType.Array, val.Resp2Type);
            }
        }

        [Test]
        public void CommandCountACLs()
        {
            CheckCommands(
                "COMMAND COUNT",
                [DoCommandCount]
            );

            static void DoCommandCount(IServer server)
            {
                RedisResult val = server.Execute("COMMAND", "COUNT");
                Assert.IsTrue((int)val > 0);
            }
        }

        [Test]
        public void CommandDocsACLs()
        {
            CheckCommands(
                "COMMAND DOCS",
                [DoCommandDocs, DoCommandDocsOne, DoCommandDocsMulti]
            );

            static void DoCommandDocs(IServer server)
            {
                RedisResult val = server.Execute("COMMAND", "DOCS");
                Assert.AreEqual(ResultType.Array, val.Resp2Type);
            }

            static void DoCommandDocsOne(IServer server)
            {
                RedisResult val = server.Execute("COMMAND", "DOCS", "GET");
                Assert.AreEqual(ResultType.Array, val.Resp2Type);
            }

            static void DoCommandDocsMulti(IServer server)
            {
                RedisResult val = server.Execute("COMMAND", "DOCS", "GET", "SET", "APPEND");
                Assert.AreEqual(ResultType.Array, val.Resp2Type);
            }
        }

        [Test]
        public void CommandInfoACLs()
        {
            CheckCommands(
                "COMMAND INFO",
                [DoCommandInfo, DoCommandInfoOne, DoCommandInfoMulti]
            );

            static void DoCommandInfo(IServer server)
            {
                RedisResult val = server.Execute("COMMAND", "INFO");
                Assert.AreEqual(ResultType.Array, val.Resp2Type);
            }

            static void DoCommandInfoOne(IServer server)
            {
                RedisResult val = server.Execute("COMMAND", "INFO", "GET");
                Assert.AreEqual(ResultType.Array, val.Resp2Type);
            }

            static void DoCommandInfoMulti(IServer server)
            {
                RedisResult val = server.Execute("COMMAND", "INFO", "GET", "SET", "APPEND");
                Assert.AreEqual(ResultType.Array, val.Resp2Type);
            }
        }

        [Test]
        public void CommitAOFACLs()
        {
            CheckCommands(
                "COMMITAOF",
                [DoCommitAOF]
            );

            static void DoCommitAOF(IServer server)
            {
                RedisResult val = server.Execute("COMMITAOF");
                Assert.AreEqual("AOF file committed", (string)val);
            }
        }

        [Test]
        public void ConfigGetACLs()
        {
            // TODO: CONFIG GET doesn't implement multiple parameters, so that is untested

            CheckCommands(
                "CONFIG GET",
                [DoConfigGetOne]
            );

            static void DoConfigGetOne(IServer server)
            {
                RedisResult res = server.Execute("CONFIG", "GET", "timeout");
                RedisValue[] resArr = (RedisValue[])res;

                Assert.AreEqual(2, resArr.Length);
                Assert.AreEqual("timeout", (string)resArr[0]);
                Assert.IsTrue((int)resArr[1] >= 0);
            }
        }

        [Test]
        public void ConfigRewriteACLs()
        {
            CheckCommands(
                "CONFIG REWRITE",
                [DoConfigRewrite]
            );

            static void DoConfigRewrite(IServer server)
            {
                RedisResult res = server.Execute("CONFIG", "REWRITE");
                Assert.AreEqual("OK", (string)res);
            }
        }

        [Test]
        public void ConfigSetACLs()
        {
            // CONFIG SET parameters are pretty limitted, so this uses error responses for "got past the ACL" validation - that's not great

            CheckCommands(
                "CONFIG SET",
                [DoConfigSetOne, DoConfigSetMulti]
            );

            static void DoConfigSetOne(IServer server)
            {
                try
                {
                    RedisResult res = server.Execute("CONFIG", "SET", "foo", "bar");
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

            static void DoConfigSetMulti(IServer server)
            {
                try
                {
                    RedisResult res = server.Execute("CONFIG", "SET", "foo", "bar", "fizz", "buzz");
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

        [Test]
        public void COScanACLs()
        {
            // TODO: COSCAN parameters are unclear... add more cases later

            CheckCommands(
                "COSCAN",
                [DoCOScan]
            );

            static void DoCOScan(IServer server)
            {
                RedisResult val = server.Execute("CUSTOMOBJECTSCAN", "foo", "0");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }
        }

        [Test]
        public void CustomCmdACLs()
        {
            // TODO: it probably makes sense to expose ACLs for registered commands, but for now just a blanket ACL for all custom commands is all we have

            int count = 0;

            CheckCommands(
                "CUSTOMCMD",
                [DoSetWpIfPgt],
                knownCategories: ["garnet", "custom", "dangerous"]
            );

            void DoSetWpIfPgt(IServer server)
            {
                RedisResult res = server.Execute("SETWPIFPGT", $"foo-{count}", "bar", BitConverter.GetBytes((long)0));
                count++;

                Assert.AreEqual("OK", (string)res);
            }
        }

        [Test]
        public void CustomObjCmdACLs()
        {
            // TODO: it probably makes sense to expose ACLs for registered commands, but for now just a blanket ACL for all custom commands is all we have

            CheckCommands(
                "CUSTOMOBJCMD",
                [DoMyDictGet],
                knownCategories: ["garnet", "custom", "dangerous"]
            );

            static void DoMyDictGet(IServer server)
            {
                RedisResult res = server.Execute("MYDICTGET", "foo", "bar");
                Assert.IsTrue(res.IsNull);
            }
        }

        [Test]
        public void CustomTxnACLs()
        {
            // TODO: it probably makes sense to expose ACLs for registered commands, but for now just a blanket ACL for all custom commands is all we have

            CheckCommands(
                "CustomTxn",
                [DoReadWriteTx],
                knownCategories: ["garnet", "custom", "dangerous"]
            );

            static void DoReadWriteTx(IServer server)
            {
                RedisResult res = server.Execute("READWRITETX", "foo", "bar", "fizz");
                Assert.AreEqual("SUCCESS", (string)res);
            }
        }

        [Test]
        public void DBSizeACLs()
        {
            CheckCommands(
                "DBSIZE",
                [DoDbSize]
            );

            static void DoDbSize(IServer server)
            {
                RedisResult val = server.Execute("DBSIZE");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void DecrACLs()
        {
            int count = 0;

            CheckCommands(
                "DECR",
                [DoDecr]
            );

            void DoDecr(IServer server)
            {
                RedisResult val = server.Execute("DECR", $"foo-{count}");
                count++;
                Assert.AreEqual(-1, (int)val);
            }
        }

        [Test]
        public void DecrByACLs()
        {
            int count = 0;

            CheckCommands(
                "DECRBY",
                [DoDecrBy]
            );

            void DoDecrBy(IServer server)
            {
                RedisResult val = server.Execute("DECRBY", $"foo-{count}", "2");
                count++;
                Assert.AreEqual(-2, (int)val);
            }
        }

        [Test]
        public void DelACLs()
        {
            CheckCommands(
                "DEL",
                [DoDel, DoDelMulti]
            );

            static void DoDel(IServer server)
            {
                RedisResult val = server.Execute("DEL", "foo");
                Assert.AreEqual(0, (int)val);
            }

            static void DoDelMulti(IServer server)
            {
                RedisResult val = server.Execute("DEL", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void DiscardACLs()
        {
            // Discard is a little weird, so we're using exceptions for control flow here - don't love it

            CheckCommands(
                "DISCARD",
                [DoDiscard]
            );

            static void DoDiscard(IServer server)
            {
                try
                {
                    RedisResult val = server.Execute("DISCARD");
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
            CheckCommands(
                "ECHO",
                [DoEcho]
            );

            static void DoEcho(IServer server)
            {
                RedisResult val = server.Execute("ECHO", "hello world");
                Assert.AreEqual("hello world", (string)val);
            }
        }

        [Test]
        public void ExecACLs()
        {
            // EXEC is a little weird, so we're using exceptions for control flow here - don't love it

            CheckCommands(
                "EXEC",
                [DoExec]
            );

            static void DoExec(IServer server)
            {
                try
                {
                    RedisResult val = server.Execute("EXEC");
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
            CheckCommands(
                "EXISTS",
                [DoExists, DoExistsMulti]
            );

            static void DoExists(IServer server)
            {
                RedisResult val = server.Execute("EXISTS", "foo");
                Assert.AreEqual(0, (int)val);
            }

            static void DoExistsMulti(IServer server)
            {
                RedisResult val = server.Execute("EXISTS", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ExpireACLs()
        {
            // TODO: expire doesn't support combinations of flags (XX GT, XX LT are legal) so those will need to be tested when implemented

            CheckCommands(
                "EXPIRE",
                [DoExpire, DoExpireNX, DoExpireXX, DoExpireGT, DoExpireLT]
            );

            static void DoExpire(IServer server)
            {
                RedisResult val = server.Execute("EXPIRE", "foo", "10");
                Assert.AreEqual(0, (int)val);
            }

            static void DoExpireNX(IServer server)
            {
                RedisResult val = server.Execute("EXPIRE", "foo", "10", "NX");
                Assert.AreEqual(0, (int)val);
            }

            static void DoExpireXX(IServer server)
            {
                RedisResult val = server.Execute("EXPIRE", "foo", "10", "XX");
                Assert.AreEqual(0, (int)val);
            }

            static void DoExpireGT(IServer server)
            {
                RedisResult val = server.Execute("EXPIRE", "foo", "10", "GT");
                Assert.AreEqual(0, (int)val);
            }

            static void DoExpireLT(IServer server)
            {
                RedisResult val = server.Execute("EXPIRE", "foo", "10", "LT");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void FailoverACLs()
        {
            // Failover is strange, so this more complicated that typical

            Action<IServer>[] cmds = [
                DoFailover,
                DoFailoverTo,
                DoFailoverAbort,
                DoFailoverToForce,
                DoFailoverToAbort,
                DoFailoverToForceAbort,
                DoFailoverToForceAbortTimeout,
            ];

            // Check denied with -failover
            foreach (Action<IServer> cmd in cmds)
            {
                Run(false, server => SetUser(server, "default", $"-failover"), cmd);
            }

            string[] acls = ["admin", "slow", "dangerous"];

            foreach (Action<IServer> cmd in cmds)
            {
                // Check works with +@all
                Run(true, server => { }, cmd);

                // Check denied with -@whatever
                foreach (string acl in acls)
                {
                    Run(false, server => SetUser(server, "default", $"-@{acl}"), cmd);
                }
            }

            void Run(bool expectSuccess, Action<IServer> before, Action<IServer> cmd)
            {
                // Refresh Garnet instance
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
            CheckCommands(
                "FLUSHDB",
                [DoFlushDB, DoFlushDBAsync]
            );

            static void DoFlushDB(IServer server)
            {
                RedisResult val = server.Execute("FLUSHDB");
                Assert.AreEqual("OK", (string)val);
            }

            static void DoFlushDBAsync(IServer server)
            {
                RedisResult val = server.Execute("FLUSHDB", "ASYNC");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void ForceGCACLs()
        {
            CheckCommands(
                "FORCEGC",
                [DoForceGC, DoForceGCGen]
            );

            static void DoForceGC(IServer server)
            {
                RedisResult val = server.Execute("FORCEGC");
                Assert.AreEqual("GC completed", (string)val);
            }

            static void DoForceGCGen(IServer server)
            {
                RedisResult val = server.Execute("FORCEGC", "1");
                Assert.AreEqual("GC completed", (string)val);
            }
        }

        [Test]
        public void GetACLs()
        {
            CheckCommands(
                "GET",
                [DoGet]
            );

            static void DoGet(IServer server)
            {
                RedisResult val = server.Execute("GET", "foo");
                Assert.IsNull((string)val);
            }
        }

        [Test]
        public void GetBitACLs()
        {
            CheckCommands(
                "GETBIT",
                [DoGetBit]
            );

            static void DoGetBit(IServer server)
            {
                RedisResult val = server.Execute("GETBIT", "foo", "4");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void GetDelACLs()
        {
            CheckCommands(
                "GETDEL",
                [DoGetDel]
            );

            static void DoGetDel(IServer server)
            {
                RedisResult val = server.Execute("GETDEL", "foo");
                Assert.IsNull((string)val);
            }
        }

        [Test]
        public void GetRangeACLs()
        {
            CheckCommands(
                "GETRANGE",
                [DoGetRange]
            );

            static void DoGetRange(IServer server)
            {
                RedisResult val = server.Execute("GETRANGE", "foo", "10", "15");
                Assert.IsNull((string)val);
            }
        }

        [Test]
        public void HDelACLs()
        {
            CheckCommands(
                "HDEL",
                [DoHDel, DoHDelMulti]
            );

            static void DoHDel(IServer server)
            {
                RedisResult val = server.Execute("HDEL", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }

            static void DoHDelMulti(IServer server)
            {
                RedisResult val = server.Execute("HDEL", "foo", "bar", "fizz");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void HExistsACLs()
        {
            CheckCommands(
                "HEXISTS",
                [DoHDel]
            );

            static void DoHDel(IServer server)
            {
                RedisResult val = server.Execute("HEXISTS", "foo", "bar");
                Assert.IsFalse((bool)val);
            }
        }

        [Test]
        public void HGetACLs()
        {
            CheckCommands(
                "HGET",
                [DoHDel]
            );

            static void DoHDel(IServer server)
            {
                RedisResult val = server.Execute("HGET", "foo", "bar");
                Assert.IsNull((string)val);
            }
        }

        [Test]
        public void HGetAllACLs()
        {
            CheckCommands(
                "HGETALL",
                [DoHDel]
            );

            static void DoHDel(IServer server)
            {
                RedisResult val = server.Execute("HGETALL", "foo");
                RedisValue[] valArr = (RedisValue[])val;

                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void HIncrByACLs()
        {
            int cur = 0;

            CheckCommands(
                "HINCRBY",
                [DoHIncrBy]
            );

            void DoHIncrBy(IServer server)
            {
                RedisResult val = server.Execute("HINCRBY", "foo", "bar", "2");
                cur += 2;
                Assert.AreEqual(cur, (int)val);
            }
        }

        [Test]
        public void HIncrByFloatACLs()
        {
            double cur = 0;

            CheckCommands(
                "HINCRBYFLOAT",
                [DoHIncrByFloat]
            );

            void DoHIncrByFloat(IServer server)
            {
                RedisResult val = server.Execute("HINCRBYFLOAT", "foo", "bar", "1.0");
                cur += 1.0;
                Assert.AreEqual(cur, (double)val);
            }
        }

        [Test]
        public void HKeysACLs()
        {
            CheckCommands(
                "HKEYS",
                [DoHKeys]
            );

            static void DoHKeys(IServer server)
            {
                RedisResult val = server.Execute("HKEYS", "foo");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void HLenACLs()
        {
            CheckCommands(
                "HLEN",
                [DoHLen]
            );

            static void DoHLen(IServer server)
            {
                RedisResult val = server.Execute("HLEN", "foo");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void HMGetACLs()
        {
            CheckCommands(
                "HMGET",
                [DoHMGet, DoHMGetMulti]
            );

            static void DoHMGet(IServer server)
            {
                RedisResult val = server.Execute("HMGET", "foo", "bar");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(1, valArr.Length);
                Assert.IsNull((string)valArr[0]);
            }

            static void DoHMGetMulti(IServer server)
            {
                RedisResult val = server.Execute("HMGET", "foo", "bar", "fizz");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(2, valArr.Length);
                Assert.IsNull((string)valArr[0]);
                Assert.IsNull((string)valArr[1]);
            }
        }

        [Test]
        public void HMSetACLs()
        {
            CheckCommands(
                "HMSET",
                [DoHMSet, DoHMSetMulti]
            );

            static void DoHMSet(IServer server)
            {
                RedisResult val = server.Execute("HMSET", "foo", "bar", "fizz");
                Assert.AreEqual("OK", (string)val);
            }

            static void DoHMSetMulti(IServer server)
            {
                RedisResult val = server.Execute("HMSET", "foo", "bar", "fizz", "hello", "world");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void HRandFieldACLs()
        {
            CheckCommands(
                "HRANDFIELD",
                [DoHRandField, DoHRandFieldCount, DoHRandFieldCountWithValues]
            );

            static void DoHRandField(IServer server)
            {
                RedisResult val = server.Execute("HRANDFIELD", "foo");
                Assert.IsNull((string)val);
            }

            static void DoHRandFieldCount(IServer server)
            {
                RedisResult val = server.Execute("HRANDFIELD", "foo", "1");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            static void DoHRandFieldCountWithValues(IServer server)
            {
                RedisResult val = server.Execute("HRANDFIELD", "foo", "1", "WITHVALUES");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void HScanACLs()
        {
            CheckCommands(
                "HSCAN",
                [DoHScan, DoHScanMatch, DoHScanCount, DoHScanNoValues, DoHScanMatchCount, DoHScanMatchNoValues, DoHScanCountNoValues, DoHScanMatchCountNoValues]
            );

            static void DoHScan(IServer server)
            {
                RedisResult val = server.Execute("HSCAN", "foo", "0");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            static void DoHScanMatch(IServer server)
            {
                RedisResult val = server.Execute("HSCAN", "foo", "0", "MATCH", "*");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            static void DoHScanCount(IServer server)
            {
                RedisResult val = server.Execute("HSCAN", "foo", "0", "COUNT", "2");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            static void DoHScanNoValues(IServer server)
            {
                RedisResult val = server.Execute("HSCAN", "foo", "0", "NOVALUES");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            static void DoHScanMatchCount(IServer server)
            {
                RedisResult val = server.Execute("HSCAN", "foo", "0", "MATCH", "*", "COUNT", "2");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            static void DoHScanMatchNoValues(IServer server)
            {
                RedisResult val = server.Execute("HSCAN", "foo", "0", "MATCH", "*", "NOVALUES");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            static void DoHScanCountNoValues(IServer server)
            {
                RedisResult val = server.Execute("HSCAN", "foo", "0", "COUNT", "0", "NOVALUES");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            static void DoHScanMatchCountNoValues(IServer server)
            {
                RedisResult val = server.Execute("HSCAN", "foo", "0", "MATCH", "*", "COUNT", "0", "NOVALUES");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }
        }

        [Test]
        public void HSetACLs()
        {
            int keyIx = 0;

            CheckCommands(
                "HSET",
                [DoHSet, DoHSetMulti]
            );

            void DoHSet(IServer server)
            {
                RedisResult val = server.Execute("HSET", $"foo-{keyIx}", "bar", "fizz");
                keyIx++;

                Assert.AreEqual(1, (int)val);
            }

            void DoHSetMulti(IServer server)
            {
                RedisResult val = server.Execute("HSET", $"foo-{keyIx}", "bar", "fizz", "hello", "world");
                keyIx++;

                Assert.AreEqual(2, (int)val);
            }
        }

        [Test]
        public void HSetNXACLs()
        {
            int keyIx = 0;

            CheckCommands(
                "HSETNX",
                [DoHSetNX]
            );

            void DoHSetNX(IServer server)
            {
                RedisResult val = server.Execute("HSETNX", $"foo-{keyIx}", "bar", "fizz");
                keyIx++;

                Assert.AreEqual(1, (int)val);
            }
        }

        [Test]
        public void HStrLenACLs()
        {
            CheckCommands(
                "HSTRLEN",
                [DoHStrLen]
            );

            static void DoHStrLen(IServer server)
            {
                RedisResult val = server.Execute("HSTRLEN", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void HValsACLs()
        {
            CheckCommands(
                "HVALS",
                [DoHVals]
            );

            static void DoHVals(IServer server)
            {
                RedisResult val = server.Execute("HVALS", "foo");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void IncrACLs()
        {
            int count = 0;

            CheckCommands(
                "INCR",
                [DoIncr]
            );

            void DoIncr(IServer server)
            {
                RedisResult val = server.Execute("INCR", $"foo-{count}");
                count++;
                Assert.AreEqual(1, (int)val);
            }
        }

        [Test]
        public void IncrByACLs()
        {
            int count = 0;

            CheckCommands(
                "INCRBY",
                [DoIncrBy]
            );

            void DoIncrBy(IServer server)
            {
                RedisResult val = server.Execute("INCRBY", $"foo-{count}", "2");
                count++;
                Assert.AreEqual(2, (int)val);
            }
        }

        [Test]
        public void InfoACLs()
        {
            CheckCommands(
               "INFO",
               [DoInfo, DoInfoSingle, DoInfoMulti]
            );

            static void DoInfo(IServer server)
            {
                RedisResult val = server.Execute("INFO");
                Assert.IsNotEmpty((string)val);
            }

            static void DoInfoSingle(IServer server)
            {
                RedisResult val = server.Execute("INFO", "SERVER");
                Assert.IsNotEmpty((string)val);
            }

            static void DoInfoMulti(IServer server)
            {
                RedisResult val = server.Execute("INFO", "SERVER", "MEMORY");
                Assert.IsNotEmpty((string)val);
            }
        }

        [Test]
        public void KeysACLs()
        {
            CheckCommands(
               "KEYS",
               [DoKeys]
            );

            static void DoKeys(IServer server)
            {
                RedisResult val = server.Execute("KEYS", "*");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void LastSaveACLs()
        {
            CheckCommands(
               "LASTSAVE",
               [DoLastSave]
            );

            static void DoLastSave(IServer server)
            {
                RedisResult val = server.Execute("LASTSAVE");
                Assert.AreEqual(0, (long)val);
            }
        }

        [Test]
        public void LatencyHelpACLs()
        {
            CheckCommands(
               "LATENCY HELP",
               [DoLatencyHelp]
            );

            static void DoLatencyHelp(IServer server)
            {
                RedisResult val = server.Execute("LATENCY", "HELP");
                Assert.AreEqual(ResultType.Array, val.Resp2Type);
            }
        }

        [Test]
        public void LatencyHistogramACLs()
        {
            CheckCommands(
               "LATENCY HISTOGRAM",
               [DoLatencyHistogram, DoLatencyHistogramSingle, DoLatencyHistogramMulti]
            );

            static void DoLatencyHistogram(IServer server)
            {
                RedisResult val = server.Execute("LATENCY", "HISTOGRAM");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            static void DoLatencyHistogramSingle(IServer server)
            {
                RedisResult val = server.Execute("LATENCY", "HISTOGRAM", "NET_RS_LAT");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            static void DoLatencyHistogramMulti(IServer server)
            {
                RedisResult val = server.Execute("LATENCY", "HISTOGRAM", "NET_RS_LAT", "NET_RS_LAT_ADMIN");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void LatencyResetACLs()
        {
            CheckCommands(
               "LATENCY RESET",
               [DoLatencyReset, DoLatencyResetSingle, DoLatencyResetMulti]
            );

            static void DoLatencyReset(IServer server)
            {
                RedisResult val = server.Execute("LATENCY", "RESET");
                Assert.AreEqual(6, (int)val);
            }

            static void DoLatencyResetSingle(IServer server)
            {
                RedisResult val = server.Execute("LATENCY", "RESET", "NET_RS_LAT");
                Assert.AreEqual(1, (int)val);
            }

            static void DoLatencyResetMulti(IServer server)
            {
                RedisResult val = server.Execute("LATENCY", "RESET", "NET_RS_LAT", "NET_RS_LAT_ADMIN");
                Assert.AreEqual(2, (int)val);
            }
        }

        [Test]
        public void LPopACLs()
        {
            CheckCommands(
                "LPOP",
                [DoLPop, DoLPopCount]
            );

            static void DoLPop(IServer server)
            {
                RedisResult val = server.Execute("LPOP", "foo");
                Assert.IsTrue(val.IsNull);
            }

            static void DoLPopCount(IServer server)
            {
                RedisResult val = server.Execute("LPOP", "foo", "4");
                Assert.IsTrue(val.IsNull);
            }
        }

        [Test]
        public void LPushACLs()
        {
            int count = 0;

            CheckCommands(
                "LPUSH",
                [DoLPush, DoLPushMulti]
            );

            void DoLPush(IServer server)
            {
                RedisResult val = server.Execute("LPUSH", "foo", "bar");
                count++;

                Assert.AreEqual(count, (int)val);
            }

            void DoLPushMulti(IServer server)
            {
                RedisResult val = server.Execute("LPUSH", "foo", "bar", "buzz");
                count += 2;

                Assert.AreEqual(count, (int)val);
            }
        }

        [Test]
        public void LPushXACLs()
        {
            CheckCommands(
                "LPUSHX",
                [DoLPushX, DoLPushXMulti]
            );

            static void DoLPushX(IServer server)
            {
                RedisResult val = server.Execute("LPUSHX", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }

            void DoLPushXMulti(IServer server)
            {
                RedisResult val = server.Execute("LPUSHX", "foo", "bar", "buzz");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void RPopACLs()
        {
            CheckCommands(
                "RPOP",
                [DoRPop, DoRPopCount]
            );

            static void DoRPop(IServer server)
            {
                RedisResult val = server.Execute("RPOP", "foo");
                Assert.IsTrue(val.IsNull);
            }

            static void DoRPopCount(IServer server)
            {
                RedisResult val = server.Execute("RPOP", "foo", "4");
                Assert.IsTrue(val.IsNull);
            }
        }

        [Test]
        public void LRushACLs()
        {
            int count = 0;

            CheckCommands(
                "RPUSH",
                [DoRPush, DoRPushMulti]
            );

            void DoRPush(IServer server)
            {
                RedisResult val = server.Execute("RPUSH", "foo", "bar");
                count++;

                Assert.AreEqual(count, (int)val);
            }

            void DoRPushMulti(IServer server)
            {
                RedisResult val = server.Execute("RPUSH", "foo", "bar", "buzz");
                count += 2;

                Assert.AreEqual(count, (int)val);
            }
        }

        [Test]
        public void RPushACLs()
        {
            int count = 0;

            CheckCommands(
                "RPUSH",
                [DoRPushX, DoRPushXMulti]
            );

            void DoRPushX(IServer server)
            {
                RedisResult val = server.Execute("RPUSH", $"foo-{count}", "bar");
                count++;
                Assert.AreEqual(1, (int)val);
            }

            void DoRPushXMulti(IServer server)
            {
                RedisResult val = server.Execute("RPUSH", $"foo-{count}", "bar", "buzz");
                count++;
                Assert.AreEqual(2, (int)val);
            }
        }

        [Test]
        public void RPushXACLs()
        {
            CheckCommands(
                "RPUSHX",
                [DoRPushX, DoRPushXMulti]
            );

            static void DoRPushX(IServer server)
            {
                RedisResult val = server.Execute("RPUSHX", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }

            static void DoRPushXMulti(IServer server)
            {
                RedisResult val = server.Execute("RPUSHX", "foo", "bar", "buzz");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void LLenACLs()
        {
            CheckCommands(
                "LLEN",
                [DoLLen]
            );

            static void DoLLen(IServer server)
            {
                RedisResult val = server.Execute("LLEN", "foo");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void LTrimACLs()
        {
            CheckCommands(
                "LTRIM",
                [DoLTrim]
            );

            static void DoLTrim(IServer server)
            {
                RedisResult val = server.Execute("LTRIM", "foo", "4", "10");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void LRangeACLs()
        {
            CheckCommands(
                "LRANGE",
                [DoLRange]
            );

            static void DoLRange(IServer server)
            {
                RedisResult val = server.Execute("LRANGE", "foo", "4", "10");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void LIndexACLs()
        {
            CheckCommands(
                "LINDEX",
                [DoLIndex]
            );

            static void DoLIndex(IServer server)
            {
                RedisResult val = server.Execute("LINDEX", "foo", "4");
                Assert.IsTrue(val.IsNull);
            }
        }

        [Test]
        public void LInsertACLs()
        {
            CheckCommands(
                "LINSERT",
                [DoLInsertBefore, DoLInsertAfter]
            );

            static void DoLInsertBefore(IServer server)
            {
                RedisResult val = server.Execute("LINSERT", "foo", "BEFORE", "hello", "world");
                Assert.AreEqual(0, (int)val);
            }

            static void DoLInsertAfter(IServer server)
            {
                RedisResult val = server.Execute("LINSERT", "foo", "AFTER", "hello", "world");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void LRemACLs()
        {
            CheckCommands(
                "LREM",
                [DoLRem]
            );

            static void DoLRem(IServer server)
            {
                RedisResult val = server.Execute("LREM", "foo", "0", "hello");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void RPopLPushACLs()
        {
            CheckCommands(
                "RPOPLPUSH",
                [DoLRem]
            );

            static void DoLRem(IServer server)
            {
                RedisResult val = server.Execute("RPOPLPUSH", "foo", "bar");
                Assert.IsTrue(val.IsNull);
            }
        }

        [Test]
        public void LMoveACLs()
        {
            CheckCommands(
                "LMOVE",
                [DoLMove]
            );

            static void DoLMove(IServer server)
            {
                RedisResult val = server.Execute("LMOVE", "foo", "bar", "LEFT", "RIGHT");
                Assert.IsTrue(val.IsNull);
            }
        }

        [Test]
        public void LSetACLs()
        {
            // TODO: LSET with an empty key appears broken; clean up when fixed

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            db.ListLeftPush("foo", "fizz");

            CheckCommands(
                "LSET",
                [DoLMove]
            );

            static void DoLMove(IServer server)
            {
                RedisResult val = server.Execute("LSET", "foo", "0", "bar");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void MemoryUsageACLs()
        {
            CheckCommands(
                "MEMORY USAGE",
                [DoMemoryUsage, DoMemoryUsageSamples]
            );

            static void DoMemoryUsage(IServer server)
            {
                RedisResult val = server.Execute("MEMORY", "USAGE", "foo");
                Assert.AreEqual(0, (int)val);
            }

            static void DoMemoryUsageSamples(IServer server)
            {
                RedisResult val = server.Execute("MEMORY", "USAGE", "foo", "SAMPLES", "10");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void MGetACLs()
        {
            CheckCommands(
                "MGET",
                [DoMemorySingle, DoMemoryMulti]
            );

            static void DoMemorySingle(IServer server)
            {
                RedisResult val = server.Execute("MGET", "foo");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(1, valArr.Length);
                Assert.IsTrue(valArr[0].IsNull);
            }

            static void DoMemoryMulti(IServer server)
            {
                RedisResult val = server.Execute("MGET", "foo", "bar");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
                Assert.IsTrue(valArr[0].IsNull);
                Assert.IsTrue(valArr[1].IsNull);
            }
        }

        [Test]
        public void MigrateACLs()
        {
            // Uses exceptions for control flow, as we're not setting up replicas here

            CheckCommands(
                "MIGRATE",
                [DoMigrate]
            );

            static void DoMigrate(IServer server)
            {
                try
                {
                    server.Execute("MIGRATE", "127.0.0.1", "9999", "KEY", "0", "1000");
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

        [Test]
        public void ModuleACLs()
        {
            // MODULE isn't a proper redis command, but this is the placeholder today... so validate it for completeness

            CheckCommands(
                "MODULE",
                [DoModuleList]
            );

            static void DoModuleList(IServer server)
            {
                RedisResult val = server.Execute("MODULE", "LIST");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void MonitorACLs()
        {
            // MONITOR isn't actually implemented, and doesn't fit nicely into SE.Redis anyway, so we only check the DENY cases here

            // test just the command
            {
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                IServer server = redis.GetServers().Single();
                IDatabase db = redis.GetDatabase();

                SetUser(server, "default", [$"-monitor"]);

                Assert.False(CheckAuthFailure(() => DoMonitor(db)), "Permitted when should have been denied");
            }

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

                    Assert.False(CheckAuthFailure(() => DoMonitor(db)), "Permitted when should have been denied");
                }
            }

            static void DoMonitor(IDatabase db)
            {
                db.Execute("MONITOR");
                Assert.Fail("Should never reach this point");
            }
        }

        [Test]
        public void MSetACLs()
        {
            CheckCommands(
                "MSET",
                [DoMSetSingle, DoMSetMulti]
            );

            static void DoMSetSingle(IServer server)
            {
                RedisResult val = server.Execute("MSET", "foo", "bar");
                Assert.AreEqual("OK", (string)val);
            }

            static void DoMSetMulti(IServer server)
            {
                RedisResult val = server.Execute("MSET", "foo", "bar", "fizz", "buzz");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void MSetNXACLs()
        {
            int count = 0;

            CheckCommands(
                "MSETNX",
                [DoMSetNXSingle, DoMSetNXMulti]
            );

            void DoMSetNXSingle(IServer server)
            {
                RedisResult val = server.Execute("MSETNX", $"foo-{count}", "bar");
                count++;

                Assert.AreEqual(1, (int)val);
            }

            void DoMSetNXMulti(IServer server)
            {
                RedisResult val = server.Execute("MSETNX", $"foo-{count}", "bar", $"fizz-{count}", "buzz");
                count++;

                Assert.AreEqual(1, (int)val);
            }
        }

        [Test]
        public void MultiACLs()
        {
            CheckCommands(
                "MULTI",
                [DoMulti]
            );

            static void DoMulti(IServer server)
            {
                RedisResult val = server.Execute("MULTI");
                Assert.AreEqual("OK", (string)val);

                // if we got here, abort the transaction
                server.Execute("DISCARD");
            }
        }

        [Test]
        public void PersistACLs()
        {
            CheckCommands(
                "PERSIST",
                [DoPersist]
            );

            static void DoPersist(IServer server)
            {
                RedisResult val = server.Execute("PERSIST", "foo");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void PExpireACLs()
        {
            // TODO: pexpire doesn't support combinations of flags (XX GT, XX LT are legal) so those will need to be tested when implemented

            CheckCommands(
                "PEXPIRE",
                [DoPExpire, DoPExpireNX, DoPExpireXX, DoPExpireGT, DoPExpireLT]
            );

            static void DoPExpire(IServer server)
            {
                RedisResult val = server.Execute("PEXPIRE", "foo", "10");
                Assert.AreEqual(0, (int)val);
            }

            static void DoPExpireNX(IServer server)
            {
                RedisResult val = server.Execute("PEXPIRE", "foo", "10", "NX");
                Assert.AreEqual(0, (int)val);
            }

            static void DoPExpireXX(IServer server)
            {
                RedisResult val = server.Execute("PEXPIRE", "foo", "10", "XX");
                Assert.AreEqual(0, (int)val);
            }

            static void DoPExpireGT(IServer server)
            {
                RedisResult val = server.Execute("PEXPIRE", "foo", "10", "GT");
                Assert.AreEqual(0, (int)val);
            }

            static void DoPExpireLT(IServer server)
            {
                RedisResult val = server.Execute("PEXPIRE", "foo", "10", "LT");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void PFAddACLs()
        {
            int count = 0;

            CheckCommands(
                "PFADD",
                [DoPFAddSingle, DoPFAddMulti]
            );

            void DoPFAddSingle(IServer server)
            {
                RedisResult val = server.Execute("PFADD", $"foo-{count}", "bar");
                count++;

                Assert.AreEqual(1, (int)val);
            }

            void DoPFAddMulti(IServer server)
            {
                RedisResult val = server.Execute("PFADD", $"foo-{count}", "bar", "fizz");
                count++;

                Assert.AreEqual(1, (int)val);
            }
        }

        [Test]
        public void PFCountACLs()
        {
            CheckCommands(
                "PFCOUNT",
                [DoPFCountSingle, DoPFCountMulti]
            );

            static void DoPFCountSingle(IServer server)
            {
                RedisResult val = server.Execute("PFCOUNT", "foo");
                Assert.AreEqual(0, (int)val);
            }

            static void DoPFCountMulti(IServer server)
            {
                RedisResult val = server.Execute("PFCOUNT", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void PFMergeACLs()
        {
            CheckCommands(
                "PFMERGE",
                [DoPFMergeSingle, DoPFMergeMulti]
            );

            static void DoPFMergeSingle(IServer server)
            {
                RedisResult val = server.Execute("PFMERGE", "foo");
                Assert.AreEqual("OK", (string)val);
            }

            static void DoPFMergeMulti(IServer server)
            {
                RedisResult val = server.Execute("PFMERGE", "foo", "bar");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void PingACLs()
        {
            CheckCommands(
                "PING",
                [DoPing, DoPingMessage]
            );

            static void DoPing(IServer server)
            {
                RedisResult val = server.Execute("PING");
                Assert.AreEqual("PONG", (string)val);
            }

            static void DoPingMessage(IServer server)
            {
                RedisResult val = server.Execute("PING", "hello");
                Assert.AreEqual("hello", (string)val);
            }
        }

        [Test]
        public void PSetEXACLs()
        {
            CheckCommands(
                "PSETEX",
                [DoPSetEX]
            );

            static void DoPSetEX(IServer server)
            {
                RedisResult val = server.Execute("PSETEX", "foo", "10", "bar");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void PSubscribeACLs()
        {
            // TODO: not testing the multiple pattern version

            int count = 0;

            CheckCommands(
                "PSUBSCRIBE",
                [DoPSubscribePattern]
            );

            void DoPSubscribePattern(IServer server)
            {
                ISubscriber sub = server.Multiplexer.GetSubscriber();

                // have to do the (bad) async version to make sure we actually get the error
                sub.SubscribeAsync(new RedisChannel($"channel-{count}", RedisChannel.PatternMode.Pattern)).GetAwaiter().GetResult();
                count++;
            }
        }

        [Test]
        public void PUnsubscribeACLs()
        {
            CheckCommands(
                "PUNSUBSCRIBE",
                [DoPUnsubscribePattern]
            );

            static void DoPUnsubscribePattern(IServer server)
            {
                RedisResult res = server.Execute("PUNSUBSCRIBE", "foo");
                Assert.AreEqual(ResultType.Array, res.Resp2Type);
            }
        }

        [Test]
        public void PTTLACLs()
        {
            CheckCommands(
                "PTTL",
                [DoPTTL]
            );

            static void DoPTTL(IServer server)
            {
                RedisResult val = server.Execute("PTTL", "foo");
                Assert.AreEqual(-1, (int)val);
            }
        }

        [Test]
        public void PublishACLs()
        {
            CheckCommands(
                "PUBLISH",
                [DoPublish]
            );

            static void DoPublish(IServer server)
            {
                ISubscriber sub = server.Multiplexer.GetSubscriber();

                long count = sub.Publish(new RedisChannel("foo", RedisChannel.PatternMode.Literal), "bar");
                Assert.AreEqual(0, count);
            }
        }

        [Test]
        public void ReadOnlyACLs()
        {
            CheckCommands(
                "READONLY",
                [DoReadOnly]
            );

            static void DoReadOnly(IServer server)
            {
                RedisResult val = server.Execute("READONLY");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void ReadWriteACLs()
        {
            CheckCommands(
                "READWRITE",
                [DoReadWrite]
            );

            static void DoReadWrite(IServer server)
            {
                RedisResult val = server.Execute("READWRITE");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void RegisterCSACLs()
        {
            // TODO: REGISTERCS has a complicated syntax, test proper commands later

            CheckCommands(
                "REGISTERCS",
                [DoRegisterCS]
            );

            static void DoRegisterCS(IServer server)
            {
                try
                {
                    server.Execute("REGISTERCS");
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

        [Test]
        public void RenameACLs()
        {
            CheckCommands(
                "RENAME",
                [DoPTTL]
            );

            static void DoPTTL(IServer server)
            {
                try
                {
                    RedisResult val = server.Execute("RENAME", "foo", "bar");
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
            // Uses exceptions as control flow, since clustering is disabled in these tests

            CheckCommands(
                "REPLICAOF",
                [DoReplicaOf, DoReplicaOfNoOne]
            );

            static void DoReplicaOf(IServer server)
            {
                try
                {
                    server.Execute("REPLICAOF", "127.0.0.1", "9999");
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

            static void DoReplicaOfNoOne(IServer server)
            {
                try
                {
                    server.Execute("REPLICAOF", "NO", "ONE");
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

        [Test]
        public void RunTxpACLs()
        {
            // TODO: RUNTXP semantics are a bit unclear... expand test later

            // TODO: RUNTXP breaks the stream when command is malformed, rework this when that is fixed

            // Test denying just the command
            {
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    SetUser(server, "default", ["-runtxp"]);
                    Assert.False(CheckAuthFailure(() => DoRunTxp(db)), "Permitted when should have been denied");
                }
            }

            string[] categories = ["transaction", "garnet"];

            foreach (string cat in categories)
            {
                // Spin up a temp admin
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
                    Assert.AreEqual("OK", (string)setupAdmin);
                }

                // Permitted
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

                    IServer server = redis.GetServers().Single();
                    IDatabase db = redis.GetDatabase();

                    Assert.True(CheckAuthFailure(() => DoRunTxp(db)), "Denied when should have been permitted");
                }

                // Denied
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
            CheckCommands(
               "SAVE",
               [DoSave]
           );

            static void DoSave(IServer server)
            {
                RedisResult val = server.Execute("SAVE");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void ScanACLs()
        {
            CheckCommands(
                "SCAN",
                [DoScan, DoScanMatch, DoScanCount, DoScanType, DoScanMatchCount, DoScanMatchType, DoScanCountType, DoScanMatchCountType]
            );

            static void DoScan(IServer server)
            {
                RedisResult val = server.Execute("SCAN", "0");
                Assert.IsFalse(val.IsNull);
            }

            static void DoScanMatch(IServer server)
            {
                RedisResult val = server.Execute("SCAN", "0", "MATCH", "*");
                Assert.IsFalse(val.IsNull);
            }

            static void DoScanCount(IServer server)
            {
                RedisResult val = server.Execute("SCAN", "0", "COUNT", "5");
                Assert.IsFalse(val.IsNull);
            }

            static void DoScanType(IServer server)
            {
                RedisResult val = server.Execute("SCAN", "0", "TYPE", "zset");
                Assert.IsFalse(val.IsNull);
            }

            static void DoScanMatchCount(IServer server)
            {
                RedisResult val = server.Execute("SCAN", "0", "MATCH", "*", "COUNT", "5");
                Assert.IsFalse(val.IsNull);
            }

            static void DoScanMatchType(IServer server)
            {
                RedisResult val = server.Execute("SCAN", "0", "MATCH", "*", "TYPE", "zset");
                Assert.IsFalse(val.IsNull);
            }

            static void DoScanCountType(IServer server)
            {
                RedisResult val = server.Execute("SCAN", "0", "COUNT", "5", "TYPE", "zset");
                Assert.IsFalse(val.IsNull);
            }

            static void DoScanMatchCountType(IServer server)
            {
                RedisResult val = server.Execute("SCAN", "0", "MATCH", "*", "COUNT", "5", "TYPE", "zset");
                Assert.IsFalse(val.IsNull);
            }
        }

        [Test]
        public void SecondaryOfACLs()
        {
            // Uses exceptions as control flow, since clustering is disabled in these tests

            CheckCommands(
                "SECONDARYOF",
                [DoSecondaryOf, DoSecondaryOfNoOne]
            );

            static void DoSecondaryOf(IServer server)
            {
                try
                {
                    server.Execute("SECONDARYOF", "127.0.0.1", "9999");
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

            static void DoSecondaryOfNoOne(IServer server)
            {
                try
                {
                    server.Execute("SECONDARYOF", "NO", "ONE");
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

        [Test]
        public void SelectACLs()
        {
            CheckCommands(
                "SELECT",
                [DoSelect]
            );

            static void DoSelect(IServer server)
            {
                RedisResult val = server.Execute("SELECT", "0");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void SetACLs()
        {
            // SET doesn't support most extra commands, so this is just key value

            CheckCommands(
                "SET",
                [DoSet, DoSetExNx, DoSetXxNx, DoSetKeepTtl, DoSetKeepTtlXx]
            );

            static void DoSet(IServer server)
            {
                RedisResult val = server.Execute("SET", "foo", "bar");
                Assert.AreEqual("OK", (string)val);
            }

            static void DoSetExNx(IServer server)
            {
                RedisResult val = server.Execute("SET", "foo", "bar", "NX", "EX", "100");
                Assert.IsTrue(val.IsNull);
            }

            static void DoSetXxNx(IServer server)
            {
                RedisResult val = server.Execute("SET", "foo", "bar", "XX", "EX", "100");
                Assert.AreEqual("OK", (string)val);
            }

            static void DoSetKeepTtl(IServer server)
            {
                RedisResult val = server.Execute("SET", "foo", "bar", "KEEPTTL");
                Assert.AreEqual("OK", (string)val);
            }

            static void DoSetKeepTtlXx(IServer server)
            {
                RedisResult val = server.Execute("SET", "foo", "bar", "XX", "KEEPTTL");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void SetBitACLs()
        {
            int count = 0;

            CheckCommands(
                "SETBIT",
                [DoSetBit]
            );

            void DoSetBit(IServer server)
            {
                RedisResult val = server.Execute("SETBIT", $"foo-{count}", "10", "1");
                count++;
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void SetEXACLs()
        {
            CheckCommands(
                "SETEX",
                [DoSetEX]
            );

            static void DoSetEX(IServer server)
            {
                RedisResult val = server.Execute("SETEX", "foo", "10", "bar");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void SetRangeACLs()
        {
            CheckCommands(
                "SETRANGE",
                [DoSetRange]
            );

            static void DoSetRange(IServer server)
            {
                RedisResult val = server.Execute("SETRANGE", "foo", "10", "bar");
                Assert.AreEqual(13, (int)val);
            }
        }

        [Test]
        public void StrLenACLs()
        {
            CheckCommands(
                "STRLEN",
                [DoStrLen]
            );

            static void DoStrLen(IServer server)
            {
                RedisResult val = server.Execute("STRLEN", "foo");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void SAddACLs()
        {
            int count = 0;

            CheckCommands(
                "SADD",
                [DoSAdd, DoSAddMulti]
            );

            void DoSAdd(IServer server)
            {
                RedisResult val = server.Execute("SADD", $"foo-{count}", "bar");
                count++;

                Assert.AreEqual(1, (int)val);
            }

            void DoSAddMulti(IServer server)
            {
                RedisResult val = server.Execute("SADD", $"foo-{count}", "bar", "fizz");
                count++;

                Assert.AreEqual(2, (int)val);
            }
        }

        [Test]
        public void SRemACLs()
        {
            CheckCommands(
                "SREM",
                [DoSRem, DoSRemMulti]
            );

            static void DoSRem(IServer server)
            {
                RedisResult val = server.Execute("SREM", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }

            static void DoSRemMulti(IServer server)
            {
                RedisResult val = server.Execute("SREM", "foo", "bar", "fizz");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void SPopACLs()
        {
            CheckCommands(
                "SPOP",
                [DoSPop, DoSPopCount]
            );

            static void DoSPop(IServer server)
            {
                RedisResult val = server.Execute("SPOP", "foo");
                Assert.IsTrue(val.IsNull);
            }

            static void DoSPopCount(IServer server)
            {
                RedisResult val = server.Execute("SPOP", "foo", "11");
                Assert.IsTrue(val.IsNull);
            }
        }

        [Test]
        public void SMembersACLs()
        {
            CheckCommands(
                "SMEMBERS",
                [DoSMembers]
            );

            static void DoSMembers(IServer server)
            {
                RedisResult val = server.Execute("SMEMBERS", "foo");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void SCardACLs()
        {
            CheckCommands(
                "SCARD",
                [DoSCard]
            );

            static void DoSCard(IServer server)
            {
                RedisResult val = server.Execute("SCARD", "foo");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void SScanACLs()
        {
            CheckCommands(
                "SSCAN",
                [DoSScan, DoSScanMatch, DoSScanCount, DoSScanMatchCount]
            );

            static void DoSScan(IServer server)
            {
                RedisResult val = server.Execute("SSCAN", "foo", "0");
                Assert.IsFalse(val.IsNull);
            }

            static void DoSScanMatch(IServer server)
            {
                RedisResult val = server.Execute("SSCAN", "foo", "0", "MATCH", "*");
                Assert.IsFalse(val.IsNull);
            }

            static void DoSScanCount(IServer server)
            {
                RedisResult val = server.Execute("SSCAN", "foo", "0", "COUNT", "5");
                Assert.IsFalse(val.IsNull);
            }

            static void DoSScanMatchCount(IServer server)
            {
                RedisResult val = server.Execute("SSCAN", "foo", "0", "MATCH", "*", "COUNT", "5");
                Assert.IsFalse(val.IsNull);
            }
        }

        [Test]
        public void SlaveOfACLs()
        {
            // Uses exceptions as control flow, since clustering is disabled in these tests

            CheckCommands(
                "SLAVEOF",
                [DoSlaveOf, DoSlaveOfNoOne]
            );

            static void DoSlaveOf(IServer server)
            {
                try
                {
                    server.Execute("SLAVEOF", "127.0.0.1", "9999");
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

            static void DoSlaveOfNoOne(IServer server)
            {
                try
                {
                    server.Execute("SLAVEOF", "NO", "ONE");
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

        [Test]
        public void SMoveACLs()
        {
            CheckCommands(
                "SMOVE",
                [DoSMove]
            );

            static void DoSMove(IServer server)
            {
                RedisResult val = server.Execute("SMOVE", "foo", "bar", "fizz");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void SRandMemberACLs()
        {
            CheckCommands(
                "SRANDMEMBER",
                [DoSRandMember, DoSRandMemberCount]
            );

            static void DoSRandMember(IServer server)
            {
                RedisResult val = server.Execute("SRANDMEMBER", "foo");
                Assert.IsTrue(val.IsNull);
            }

            static void DoSRandMemberCount(IServer server)
            {
                RedisResult val = server.Execute("SRANDMEMBER", "foo", "5");
                Assert.IsTrue(val.IsNull);
            }
        }

        [Test]
        public void SIsMemberACLs()
        {
            CheckCommands(
                "SISMEMBER",
                [DoSIsMember]
            );

            static void DoSIsMember(IServer server)
            {
                RedisResult val = server.Execute("SISMEMBER", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void SubscribeACLs()
        {
            // TODO: not testing the multiple channel version

            int count = 0;

            CheckCommands(
                "SUBSCRIBE",
                [DoSubscribe]
            );

            void DoSubscribe(IServer server)
            {
                ISubscriber sub = server.Multiplexer.GetSubscriber();

                // have to do the (bad) async version to make sure we actually get the error
                sub.SubscribeAsync(new RedisChannel($"channel-{count}", RedisChannel.PatternMode.Literal)).GetAwaiter().GetResult();
                count++;
            }
        }

        [Test]
        public void SUnionACLs()
        {
            CheckCommands(
                "SUNION",
                [DoSUnion, DoSUnionMulti]
            );

            static void DoSUnion(IServer server)
            {
                RedisResult val = server.Execute("SUNION", "foo");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            static void DoSUnionMulti(IServer server)
            {
                RedisResult val = server.Execute("SUNION", "foo", "bar");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void SUnionStoreACLs()
        {
            CheckCommands(
                "SUNIONSTORE",
                [DoSUnionStore, DoSUnionStoreMulti]
            );

            static void DoSUnionStore(IServer server)
            {
                RedisResult val = server.Execute("SUNIONSTORE", "dest", "foo");
                Assert.AreEqual(0, (int)val);
            }

            static void DoSUnionStoreMulti(IServer server)
            {
                RedisResult val = server.Execute("SUNIONSTORE", "dest", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void SDiffACLs()
        {
            CheckCommands(
                "SDIFF",
                [DoSDiff, DoSDiffMulti]
            );

            static void DoSDiff(IServer server)
            {
                RedisResult val = server.Execute("SDIFF", "foo");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            static void DoSDiffMulti(IServer server)
            {
                RedisResult val = server.Execute("SDIFF", "foo", "bar");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void SDiffStoreACLs()
        {
            CheckCommands(
                "SDIFFSTORE",
                [DoSDiffStore, DoSDiffStoreMulti]
            );

            static void DoSDiffStore(IServer server)
            {
                RedisResult val = server.Execute("SDIFFSTORE", "dest", "foo");
                Assert.AreEqual(0, (int)val);
            }

            static void DoSDiffStoreMulti(IServer server)
            {
                RedisResult val = server.Execute("SDIFFSTORE", "dest", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void SInterACLs()
        {
            CheckCommands(
                "SINTER",
                [DoSDiff, DoSDiffMulti]
            );

            static void DoSDiff(IServer server)
            {
                RedisResult val = server.Execute("SINTER", "foo");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            static void DoSDiffMulti(IServer server)
            {
                RedisResult val = server.Execute("SINTER", "foo", "bar");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void SInterStoreACLs()
        {
            CheckCommands(
                "SINTERSTORE",
                [DoSDiffStore, DoSDiffStoreMulti]
            );

            static void DoSDiffStore(IServer server)
            {
                RedisResult val = server.Execute("SINTERSTORE", "dest", "foo");
                Assert.AreEqual(0, (int)val);
            }

            static void DoSDiffStoreMulti(IServer server)
            {
                RedisResult val = server.Execute("SINTERSTORE", "dest", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void GeoAddACLs()
        {
            int count = 0;

            CheckCommands(
                "GEOADD",
                [DoGeoAdd, DoGeoAddNX, DoGeoAddNXCH, DoGeoAddMulti, DoGeoAddNXMulti, DoGeoAddNXCHMulti]
            );

            void DoGeoAdd(IServer server)
            {
                RedisResult val = server.Execute("GEOADD", $"foo-{count}", "90", "90", "bar");
                count++;

                Assert.AreEqual(1, (int)val);
            }

            void DoGeoAddNX(IServer server)
            {
                RedisResult val = server.Execute("GEOADD", $"foo-{count}", "NX", "90", "90", "bar");
                count++;

                Assert.AreEqual(1, (int)val);
            }

            void DoGeoAddNXCH(IServer server)
            {
                RedisResult val = server.Execute("GEOADD", $"foo-{count}", "NX", "CH", "90", "90", "bar");
                count++;

                Assert.AreEqual(0, (int)val);
            }

            void DoGeoAddMulti(IServer server)
            {
                RedisResult val = server.Execute("GEOADD", $"foo-{count}", "90", "90", "bar", "45", "45", "fizz");
                count++;

                Assert.AreEqual(2, (int)val);
            }

            void DoGeoAddNXMulti(IServer server)
            {
                RedisResult val = server.Execute("GEOADD", $"foo-{count}", "NX", "90", "90", "bar", "45", "45", "fizz");
                count++;

                Assert.AreEqual(2, (int)val);
            }

            void DoGeoAddNXCHMulti(IServer server)
            {
                RedisResult val = server.Execute("GEOADD", $"foo-{count}", "NX", "CH", "90", "90", "bar", "45", "45", "fizz");
                count++;

                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void GeoHashACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IDatabase db = redis.GetDatabase();

            // TODO: GEOHASH doesn't deal with empty keys appropriately - correct when that's fixed
            Assert.AreEqual(1, (int)db.Execute("GEOADD", "foo", "10", "10", "bar"));
            Assert.AreEqual(1, (int)db.Execute("GEOADD", "foo", "20", "20", "fizz"));

            CheckCommands(
                "GEOHASH",
                [DoGeoHash, DoGeoHashSingle, DoGeoHashMulti]
            );

            static void DoGeoHash(IServer server)
            {
                RedisResult val = server.Execute("GEOHASH", "foo");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            static void DoGeoHashSingle(IServer server)
            {
                RedisResult val = server.Execute("GEOHASH", "foo", "bar");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(1, valArr.Length);
                Assert.IsFalse(valArr[0].IsNull);
            }

            static void DoGeoHashMulti(IServer server)
            {
                RedisResult val = server.Execute("GEOHASH", "foo", "bar", "fizz");
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

            IDatabase db = redis.GetDatabase();

            // TODO: GEODIST fails on missing keys, which is incorrect, so putting values in to get ACL test passing
            Assert.AreEqual(1, (int)db.Execute("GEOADD", "foo", "10", "10", "bar"));
            Assert.AreEqual(1, (int)db.Execute("GEOADD", "foo", "20", "20", "fizz"));

            CheckCommands(
                "GEODIST",
                [DoGetDist, DoGetDistM]
            );

            static void DoGetDist(IServer server)
            {
                RedisResult val = server.Execute("GEODIST", "foo", "bar", "fizz");
                Assert.IsTrue((double)val > 0);
            }

            static void DoGetDistM(IServer server)
            {
                RedisResult val = server.Execute("GEODIST", "foo", "bar", "fizz", "M");
                Assert.IsTrue((double)val > 0);
            }
        }

        [Test]
        public void GeoPosACLs()
        {
            lock (this)
            {
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

                IDatabase db = redis.GetDatabase();

                // TODO: GEOPOS gets desynced if key doesn't exist, remove after that's fixed
                Assert.AreEqual(1, (int)db.Execute("GEOADD", "foo", "10", "10", "bar"));
                Assert.AreEqual(1, (int)db.Execute("GEOADD", "foo", "20", "20", "fizz"));

                CheckCommands(
                    "GEOPOS",
                    [DoGeoPos, DoGeoPosMulti]
                );

                static void DoGeoPos(IServer server)
                {
                    RedisResult val = server.Execute("GEOPOS", "foo");
                    RedisResult[] valArr = (RedisResult[])val;
                    Assert.AreEqual(0, valArr.Length);
                }

                static void DoGeoPosMulti(IServer server)
                {
                    RedisResult val = server.Execute("GEOPOS", "foo", "bar");
                    RedisResult[] valArr = (RedisResult[])val;
                    Assert.AreEqual(1, valArr.Length);
                }
            }
        }

        [Test]
        public void GeoSearchACLs()
        {
            // TODO: there are a LOT of GeoSearch variants (not all of them implemented), come back and cover all the lengths appropriately

            // TODO: GEOSEARCH appears to be very broken, so this structured oddly - can be simplified once fixed

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
            // TODO: ZADD doesn't implement NX XX GT LT CH INCR; expand to cover all lengths when implemented

            int count = 0;

            CheckCommands(
                "ZADD",
                [DoZAdd, DoZAddMulti]
            );

            void DoZAdd(IServer server)
            {
                RedisResult val = server.Execute("ZADD", $"foo-{count}", "10", "bar");
                count++;
                Assert.AreEqual(1, (int)val);
            }

            void DoZAddMulti(IServer server)
            {
                RedisResult val = server.Execute("ZADD", $"foo-{count}", "10", "bar", "20", "fizz");
                count++;
                Assert.AreEqual(2, (int)val);
            }
        }

        [Test]
        public void ZCardACLs()
        {
            CheckCommands(
                "ZCARD",
                [DoZCard]
            );

            static void DoZCard(IServer server)
            {
                RedisResult val = server.Execute("ZCARD", "foo");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ZPopMaxACLs()
        {
            CheckCommands(
                "ZPOPMAX",
                [DoZPopMax, DoZPopMaxCount]
            );

            static void DoZPopMax(IServer server)
            {
                RedisResult val = server.Execute("ZPOPMAX", "foo");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            static void DoZPopMaxCount(IServer server)
            {
                RedisResult val = server.Execute("ZPOPMAX", "foo", "10");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void ZScoreACLs()
        {
            CheckCommands(
                "ZSCORE",
                [DoZScore]
            );

            static void DoZScore(IServer server)
            {
                RedisResult val = server.Execute("ZSCORE", "foo", "bar");
                Assert.IsTrue(val.IsNull);
            }
        }

        [Test]
        public void ZRemACLs()
        {
            CheckCommands(
                "ZREM",
                [DoZRem, DoZRemMulti]
            );

            static void DoZRem(IServer server)
            {
                RedisResult val = server.Execute("ZREM", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }

            static void DoZRemMulti(IServer server)
            {
                RedisResult val = server.Execute("ZREM", "foo", "bar", "fizz");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ZCountACLs()
        {
            CheckCommands(
                "ZCOUNT",
                [DoZCount]
            );

            static void DoZCount(IServer server)
            {
                RedisResult val = server.Execute("ZCOUNT", "foo", "10", "20");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ZIncrByACLs()
        {
            int count = 0;

            CheckCommands(
                "ZINCRBY",
                [DoZIncrBy]
            );

            void DoZIncrBy(IServer server)
            {
                RedisResult val = server.Execute("ZINCRBY", $"foo-{count}", "10", "bar");
                count++;
                Assert.AreEqual(10, (double)val);
            }
        }

        [Test]
        public void ZRankACLs()
        {
            // TODO: ZRANK doesn't implement WITHSCORE (removed code that half did) - come back and add when fixed

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IDatabase db = redis.GetDatabase();

            // TODO: ZRANK errors if key does not exist, which is incorrect - creating key for this test
            Assert.AreEqual(1, (int)db.Execute("ZADD", "foo", "10", "bar"));

            CheckCommands(
                "ZRANK",
                [DoZRank]
            );

            static void DoZRank(IServer server)
            {
                RedisResult val = server.Execute("ZRANK", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ZRangeACLs()
        {
            // TODO: ZRange has loads of options, come back and test all the different lengths

            CheckCommands(
                "ZRANGE",
                [DoZRange]
            );

            static void DoZRange(IServer server)
            {
                RedisResult val = server.Execute("ZRANGE", "key", "10", "20");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void ZRangeByScoreACLs()
        {
            CheckCommands(
                "ZRANGEBYSCORE",
                [DoZRangeByScore, DoZRangeByScoreWithScores, DoZRangeByScoreLimit, DoZRangeByScoreWithScoresLimit]
            );

            static void DoZRangeByScore(IServer server)
            {
                RedisResult val = server.Execute("ZRANGEBYSCORE", "key", "10", "20");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            static void DoZRangeByScoreWithScores(IServer server)
            {
                RedisResult val = server.Execute("ZRANGEBYSCORE", "key", "10", "20", "WITHSCORES");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            static void DoZRangeByScoreLimit(IServer server)
            {
                RedisResult val = server.Execute("ZRANGEBYSCORE", "key", "10", "20", "LIMIT", "2", "3");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            static void DoZRangeByScoreWithScoresLimit(IServer server)
            {
                RedisResult val = server.Execute("ZRANGEBYSCORE", "key", "10", "20", "WITHSCORES", "LIMIT", "2", "3");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void ZRevRangeACLs()
        {
            CheckCommands(
                "ZREVRANGE",
                [DoZRevRange, DoZRevRangeWithScores]
            );

            static void DoZRevRange(IServer server)
            {
                RedisResult val = server.Execute("ZREVRANGE", "key", "10", "20");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            static void DoZRevRangeWithScores(IServer server)
            {
                RedisResult val = server.Execute("ZREVRANGE", "key", "10", "20", "WITHSCORES");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void ZRevRankACLs()
        {
            // TODO: ZREVRANK doesn't implement WITHSCORE (removed code that half did) - come back and add when fixed

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IDatabase db = redis.GetDatabase();

            // TODO: ZREVRANK errors if key does not exist, which is incorrect - creating key for this test
            Assert.AreEqual(1, (int)db.Execute("ZADD", "foo", "10", "bar"));

            CheckCommands(
                "ZREVRANK",
                [DoZRevRank]
            );

            static void DoZRevRank(IServer server)
            {
                RedisResult val = server.Execute("ZREVRANK", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ZRemRangeByLexACLs()
        {
            CheckCommands(
                "ZREMRANGEBYLEX",
                [DoZRemRangeByLex]
            );

            static void DoZRemRangeByLex(IServer server)
            {
                RedisResult val = server.Execute("ZREMRANGEBYLEX", "foo", "abc", "def");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ZRemRangeByRankACLs()
        {
            CheckCommands(
                "ZREMRANGEBYRANK",
                [DoZRemRangeByRank]
            );

            static void DoZRemRangeByRank(IServer server)
            {
                RedisResult val = server.Execute("ZREMRANGEBYRANK", "foo", "10", "20");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ZRemRangeByScoreACLs()
        {
            CheckCommands(
                "ZREMRANGEBYSCORE",
                [DoZRemRangeByRank]
            );

            static void DoZRemRangeByRank(IServer server)
            {
                RedisResult val = server.Execute("ZREMRANGEBYSCORE", "foo", "10", "20");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ZLexCountACLs()
        {
            CheckCommands(
                "ZLEXCOUNT",
                [DoZLexCount]
            );

            static void DoZLexCount(IServer server)
            {
                RedisResult val = server.Execute("ZLEXCOUNT", "foo", "abc", "def");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void ZPopMinACLs()
        {
            CheckCommands(
                "ZPOPMIN",
                [DoZPopMin, DoZPopMinCount]
            );

            static void DoZPopMin(IServer server)
            {
                RedisResult val = server.Execute("ZPOPMIN", "foo");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            static void DoZPopMinCount(IServer server)
            {
                RedisResult val = server.Execute("ZPOPMIN", "foo", "10");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void ZRandMemberACLs()
        {
            CheckCommands(
                "ZRANDMEMBER",
                [DoZRandMember, DoZRandMemberCount, DoZRandMemberCountWithScores]
            );

            static void DoZRandMember(IServer server)
            {
                RedisResult val = server.Execute("ZRANDMEMBER", "foo");
                Assert.IsTrue(val.IsNull);
            }

            static void DoZRandMemberCount(IServer server)
            {
                RedisResult val = server.Execute("ZRANDMEMBER", "foo", "10");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            static void DoZRandMemberCountWithScores(IServer server)
            {
                RedisResult val = server.Execute("ZRANDMEMBER", "foo", "10", "WITHSCORES");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void ZDiffACLs()
        {
            // TODO: ZDIFF doesn't implement WITHSCORES correctly right now - come back and cover when fixed

            CheckCommands(
                "ZDIFF",
                [DoZDiff, DoZDiffMulti]
            );

            static void DoZDiff(IServer server)
            {
                RedisResult val = server.Execute("ZDIFF", "1", "foo");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }

            static void DoZDiffMulti(IServer server)
            {
                RedisResult val = server.Execute("ZDIFF", "2", "foo", "bar");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(0, valArr.Length);
            }
        }

        [Test]
        public void ZScanACLs()
        {
            CheckCommands(
                "ZSCAN",
                [DoZScan, DoZScanMatch, DoZScanCount, DoZScanNoValues, DoZScanMatchCount, DoZScanMatchNoValues, DoZScanCountNoValues, DoZScanMatchCountNoValues]
            );

            static void DoZScan(IServer server)
            {
                RedisResult val = server.Execute("ZSCAN", "foo", "0");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            static void DoZScanMatch(IServer server)
            {
                RedisResult val = server.Execute("ZSCAN", "foo", "0", "MATCH", "*");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            static void DoZScanCount(IServer server)
            {
                RedisResult val = server.Execute("ZSCAN", "foo", "0", "COUNT", "2");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            static void DoZScanNoValues(IServer server)
            {
                RedisResult val = server.Execute("ZSCAN", "foo", "0", "NOVALUES");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            static void DoZScanMatchCount(IServer server)
            {
                RedisResult val = server.Execute("ZSCAN", "foo", "0", "MATCH", "*", "COUNT", "2");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            static void DoZScanMatchNoValues(IServer server)
            {
                RedisResult val = server.Execute("ZSCAN", "foo", "0", "MATCH", "*", "NOVALUES");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            static void DoZScanCountNoValues(IServer server)
            {
                RedisResult val = server.Execute("ZSCAN", "foo", "0", "COUNT", "0", "NOVALUES");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }

            static void DoZScanMatchCountNoValues(IServer server)
            {
                RedisResult val = server.Execute("ZSCAN", "foo", "0", "MATCH", "*", "COUNT", "0", "NOVALUES");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
            }
        }

        [Test]
        public void ZMScoreACLs()
        {
            CheckCommands(
                "ZMSCORE",
                [DoZDiff, DoZDiffMulti]
            );

            static void DoZDiff(IServer server)
            {
                RedisResult val = server.Execute("ZMSCORE", "foo", "bar");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(1, valArr.Length);
                Assert.IsTrue(valArr[0].IsNull);
            }

            static void DoZDiffMulti(IServer server)
            {
                RedisResult val = server.Execute("ZMSCORE", "foo", "bar", "fizz");
                RedisValue[] valArr = (RedisValue[])val;
                Assert.AreEqual(2, valArr.Length);
                Assert.IsTrue(valArr[0].IsNull);
                Assert.IsTrue(valArr[1].IsNull);
            }
        }

        [Test]
        public void TimeACLs()
        {
            CheckCommands(
                "TIME",
                [DoTime]
            );

            static void DoTime(IServer server)
            {
                RedisResult val = server.Execute("TIME");
                RedisResult[] valArr = (RedisResult[])val;
                Assert.AreEqual(2, valArr.Length);
                Assert.IsTrue((long)valArr[0] > 0);
                Assert.IsTrue((long)valArr[1] >= 0);
            }
        }

        [Test]
        public void TTLACLs()
        {
            CheckCommands(
                "TTL",
                [DoTTL]
            );

            static void DoTTL(IServer server)
            {
                RedisResult val = server.Execute("TTL", "foo");
                Assert.AreEqual(-1, (int)val);
            }
        }

        [Test]
        public void TypeACLs()
        {
            CheckCommands(
                "TYPE",
                [DoType]
            );

            static void DoType(IServer server)
            {
                RedisResult val = server.Execute("TYPE", "foo");
                Assert.AreEqual("none", (string)val);
            }
        }

        [Test]
        public void UnlinkACLs()
        {
            CheckCommands(
                "UNLINK",
                [DoUnlink, DoUnlinkMulti]
            );

            static void DoUnlink(IServer server)
            {
                RedisResult val = server.Execute("UNLINK", "foo");
                Assert.AreEqual(0, (int)val);
            }

            static void DoUnlinkMulti(IServer server)
            {
                RedisResult val = server.Execute("UNLINK", "foo", "bar");
                Assert.AreEqual(0, (int)val);
            }
        }

        [Test]
        public void UnsubscribeACLs()
        {
            CheckCommands(
                "UNSUBSCRIBE",
                [DoUnsubscribePattern]
            );

            static void DoUnsubscribePattern(IServer server)
            {
                RedisResult res = server.Execute("UNSUBSCRIBE", "foo");
                Assert.AreEqual(ResultType.Array, res.Resp2Type);
            }
        }

        [Test]
        public void WatchACLs()
        {
            // TODO: should watch fail outside of a transaction?
            // TODO: multi key WATCH isn't implemented correctly, add once fixed

            CheckCommands(
                "WATCH",
                [DoWatch]
            );

            static void DoWatch(IServer server)
            {
                RedisResult val = server.Execute("WATCH", "foo");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void WatchMSACLs()
        {
            // TODO: should watch fail outside of a transaction?

            CheckCommands(
                "WATCH MS",
                [DoWatchMS]
            );

            static void DoWatchMS(IServer server)
            {
                RedisResult val = server.Execute("WATCH", "MS", "foo");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void WatchOSACLs()
        {
            // TODO: should watch fail outside of a transaction?

            CheckCommands(
                "WATCH OS",
                [DoWatchOS]
            );

            static void DoWatchOS(IServer server)
            {
                RedisResult val = server.Execute("WATCH", "OS", "foo");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public void UnwatchACLs()
        {
            // TODO: should watch fail outside of a transaction?

            CheckCommands(
                "UNWATCH",
                [DoUnwatch]
            );

            static void DoUnwatch(IServer server)
            {
                RedisResult val = server.Execute("UNWATCH");
                Assert.AreEqual("OK", (string)val);
            }
        }

        /// <summary>
        /// Take a command (or subcommand, with a space) and check that adding and removing
        /// command, subcommand, and categories ACLs behaves as expected.
        /// </summary>
        private static void CheckCommands(
            string command,
            Action<IServer>[] commands,
            List<string> knownCategories = null
        )
        {
            const string UserWithAll = "temp-all";
            const string UserWithNone = "temp-none";
            const string TestPassword = "foo";

            Assert.IsNotEmpty(commands, $"[{command}]: should have delegates to invoke");

            // Figure out the ACL categories that apply to this command
            List<string> categories = knownCategories;
            if (categories == null)
            {
                categories = new();

                RespCommandsInfo info;
                if (!command.Contains(" "))
                {
                    Assert.True(RespCommandsInfo.TryGetRespCommandInfo(command, out info), $"No RespCommandInfo for {command}, failed to discover categories");
                }
                else
                {
                    string parentCommand = command[..command.IndexOf(' ')];
                    string subCommand = command.Replace(' ', '|');

                    Assert.True(RespCommandsInfo.TryGetRespCommandInfo(parentCommand, out info), $"No RespCommandInfo for {command}, failed to discover categories");
                    info = info.SubCommands.Single(x => x.Name == subCommand);
                }

                RespAclCategories remainingCategories = info.AclCategories;
                while (remainingCategories != 0)
                {
                    byte bits = (byte)BitOperations.TrailingZeroCount((int)remainingCategories);
                    RespAclCategories single = (RespAclCategories)(1 << bits);

                    categories.Add(single.ToString());

                    remainingCategories &= ~single;
                }
            }

            Assert.IsNotEmpty(categories, $"[{command}]: should have some ACL categories");

            // Spin up one connection to use for all commands from the (admin) default user
            using (ConnectionMultiplexer defaultUserConnection = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: DefaultUser, authPassword: DefaultPassword)))
            {
                IServer defaultUserServer = defaultUserConnection.GetServers()[0];

                // Spin up test users, with all permissions so we can spin up connections without issue
                InitUser(defaultUserServer, UserWithAll, TestPassword);
                InitUser(defaultUserServer, UserWithNone, TestPassword);

                // Spin up two connections for users that we'll use as starting points for different ACL changes
                using (ConnectionMultiplexer allUserConnection = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: UserWithAll, authPassword: TestPassword)))
                using (ConnectionMultiplexer noneUserConnection = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: UserWithNone, authPassword: TestPassword)))
                {
                    IServer allUserServer = allUserConnection.GetServers()[0];
                    IServer nonUserServer = noneUserConnection.GetServers()[0];

                    // Check categories
                    foreach (string category in categories)
                    {
                        // Check removing category works
                        {
                            ResetUserWithAll(defaultUserServer);

                            AssertAllPermitted(defaultUserServer, UserWithAll, allUserServer, commands, $"[{command}]: Denied when should have been permitted (user had +@all)");

                            SetUser(defaultUserServer, UserWithAll, [$"-@{category}"]);

                            AssertAllDenied(defaultUserServer, UserWithAll, allUserServer, commands, $"[{command}]: Permitted when should have been denied (user had -@{category})");
                        }

                        // Check adding category works
                        {
                            ResetUserWithNone(defaultUserServer);

                            AssertAllDenied(defaultUserServer, UserWithNone, nonUserServer, commands, $"[{command}]: Permitted when should have been denied (user had -@all)");

                            SetACLOnUser(defaultUserServer, UserWithNone, [$"+@{category}"]);

                            AssertAllPermitted(defaultUserServer, UserWithNone, nonUserServer, commands, $"[{command}]: Denied when should have been permitted (user had +@{category})");
                        }
                    }

                    // Check (parent) command itself
                    {
                        string commandAcl = command.ToLowerInvariant();
                        if (commandAcl.Contains(" "))
                        {
                            commandAcl = commandAcl[..commandAcl.IndexOf(' ')];
                        }

                        // Check removing command works
                        {
                            ResetUserWithAll(defaultUserServer);

                            SetACLOnUser(defaultUserServer, UserWithAll, [$"-{commandAcl}"]);

                            AssertAllDenied(defaultUserServer, UserWithAll, allUserServer, commands, $"[{command}]: Permitted when should have been denied (user had -{commandAcl})");
                        }

                        // Check adding command works
                        {
                            ResetUserWithNone(defaultUserServer);

                            SetACLOnUser(defaultUserServer, UserWithNone, [$"+{commandAcl}"]);

                            AssertAllPermitted(defaultUserServer, UserWithNone, nonUserServer, commands, $"[{command}]: Denied when should have been permitted (user had +{commandAcl})");
                        }
                    }

                    // Check sub-command (if it is one)
                    if (command.Contains(" "))
                    {
                        string commandAcl = command[..command.IndexOf(' ')].ToLowerInvariant();
                        string subCommandAcl = command.Replace(" ", "|").ToLowerInvariant();

                        // Check removing subcommand works
                        {
                            ResetUserWithAll(defaultUserServer);

                            SetACLOnUser(defaultUserServer, UserWithAll, [$"-{subCommandAcl}"]);

                            AssertAllDenied(defaultUserServer, UserWithAll, allUserServer, commands, $"[{command}]: Permitted when should have been denied (user had -{subCommandAcl})");
                        }

                        // Check adding subcommand works
                        {
                            ResetUserWithNone(defaultUserServer);

                            SetACLOnUser(defaultUserServer, UserWithNone, [$"+{subCommandAcl}"]);

                            AssertAllPermitted(defaultUserServer, UserWithNone, nonUserServer, commands, $"[{command}]: Denied when should have been permitted (user had +{subCommandAcl})");
                        }

                        // Checking adding command but removing subcommand works
                        {
                            ResetUserWithNone(defaultUserServer);

                            SetACLOnUser(defaultUserServer, UserWithNone, [$"+{commandAcl}", $"-{subCommandAcl}"]);

                            AssertAllDenied(defaultUserServer, UserWithNone, nonUserServer, commands, $"[{command}]: Permitted when should have been denied (user had +{commandAcl} -{subCommandAcl})");
                        }

                        // Checking removing command but adding subcommand works
                        {
                            ResetUserWithAll(defaultUserServer);

                            SetACLOnUser(defaultUserServer, UserWithAll, [$"-{commandAcl}", $"+{subCommandAcl}"]);

                            AssertAllPermitted(defaultUserServer, UserWithAll, allUserServer, commands, $"[{command}]: Denied when should have been permitted (user had -{commandAcl} +{subCommandAcl})");
                        }
                    }
                }
            }

            // Use default user to update ACL on given user
            static void SetACLOnUser(IServer defaultUserServer, string user, string[] aclPatterns)
            {
                RedisResult res = defaultUserServer.Execute("ACL", ["SETUSER", user, .. aclPatterns]);

                Assert.AreEqual("OK", (string)res, $"Updating user ({user}) failed");
            }

            static void ResetUserWithAll(IServer defaultUserServer)
            {
                // Create or reset user, with all permissions
                RedisResult withAllRes = defaultUserServer.Execute("ACL", "SETUSER", UserWithAll, "on", $">{TestPassword}", "+@all");
                Assert.AreEqual("OK", (string)withAllRes);
            }

            // Get user that was initialized with -@all
            static void ResetUserWithNone(IServer defaultUserServer)
            {
                // Create or reset user, with no permissions
                RedisResult withNoneRes = defaultUserServer.Execute("ACL", "SETUSER", UserWithNone, "on", $">{TestPassword}", "-@all");
                Assert.AreEqual("OK", (string)withNoneRes);
            }

            // Check that all commands succeed
            static void AssertAllPermitted(IServer defaultUserServer, string currentUserName, IServer currentUserServer, Action<IServer>[] commands, string message)
            {
                foreach (Action<IServer> cmd in commands)
                {
                    Assert.True(CheckAuthFailure(() => cmd(currentUserServer)), message);
                }

                // Check we haven't desynced
                Ping(defaultUserServer, currentUserName, currentUserServer);
            }

            // Check that all commands fail with NOAUTH
            static void AssertAllDenied(IServer defaultUserServer, string currentUserName, IServer currentUserServer, Action<IServer>[] commands, string message)
            {
                foreach (Action<IServer> cmd in commands)
                {
                    Assert.False(CheckAuthFailure(() => cmd(currentUserServer)), message);
                }

                // Check we haven't desynced
                Ping(defaultUserServer, currentUserName, currentUserServer);
            }

            // Enable PING on user and issue PING on connection
            static void Ping(IServer defaultUserServer, string currentUserName, IServer currentUserServer)
            {
                // Have to add PING because it'll be denied by reset of test in many cases
                // since we do this towards the end of our asserts, it shouldn't invalidate
                // the rest of the test.
                RedisResult addPingRes = defaultUserServer.Execute("ACL", "SETUSER", currentUserName, "on", "+ping");
                Assert.AreEqual("OK", (string)addPingRes);

                // Actually execute the PING
                RedisResult pingRes = currentUserServer.Execute("PING");
                Assert.AreEqual("PONG", (string)pingRes);
            }

            // Create a user with all permissions
            static void InitUser(IServer defaultUserServer, string username, string password)
            {
                RedisResult res = defaultUserServer.Execute("ACL", "SETUSER", username, "on", $">{password}", "+@all");
                Assert.AreEqual("OK", (string)res);
            }
        }

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

    }
}