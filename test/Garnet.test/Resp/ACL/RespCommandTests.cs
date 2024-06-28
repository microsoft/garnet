// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.server;
using Garnet.server.ACL;
using Microsoft.CodeAnalysis;
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

            // TODO: See if these commands could be identified programmatically
            IEnumerable<string> withOnlySubCommands = ["ACL", "CLUSTER", "CONFIG", "LATENCY", "MEMORY", "MODULE"];
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
        public async Task AsyncACLsAsync()
        {
            // ASYNC is only support in Resp3, so we use exceptions for control flow here

            await CheckCommandsAsync(
                "ASYNC",
                [DoAsyncAsync]
            );

            static async Task DoAsyncAsync(GarnetClient server)
            {
                try
                {
                    await server.ExecuteForStringResultAsync("ASYNC", ["BARRIER"]);
                    Assert.Fail("Should be unreachable, ASYNC shouldn't work in Resp2");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR command not supported in RESP2")
                    {
                        return;
                    }

                    throw;
                }
            }
        }

        [Test]
        public async Task AclCatACLsAsync()
        {
            await CheckCommandsAsync(
                "ACL CAT",
                [DoAclCatAsync]
            );

            static async Task DoAclCatAsync(GarnetClient server)
            {
                string[] res = await server.ExecuteForStringArrayResultAsync("ACL", ["CAT"]);
                Assert.IsNotNull(res);
            }
        }

        [Test]
        public async Task AclDelUserACLs()
        {
            await CheckCommandsAsync(
                "ACL DELUSER",
                [DoAclDelUserAsync, DoAclDelUserMultiAsync]
            );

            static async Task DoAclDelUserAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ACL", ["DELUSER", "does-not-exist"]);
                Assert.AreEqual(0, val);
            }

            async Task DoAclDelUserMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ACL", ["DELUSER", "does-not-exist-1", "does-not-exist-2"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task AclListACLs()
        {
            await CheckCommandsAsync(
                "ACL LIST",
                [DoAclListAsync]
            );

            static async Task DoAclListAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ACL", ["LIST"]);
                Assert.IsNotNull(val);
            }
        }

        [Test]
        public async Task AclLoadACLs()
        {
            await CheckCommandsAsync(
                "ACL LOAD",
                [DoAclLoadAsync]
            );

            static async Task DoAclLoadAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("ACL", ["LOAD"]);

                    Assert.Fail("No ACL file, so this should have failed");
                }
                catch (Exception e)
                {
                    if (e.Message != "ERR Cannot find ACL configuration file ''")
                    {
                        throw;
                    }
                }
            }
        }

        [Test]
        public async Task AclSaveACLs()
        {
            await CheckCommandsAsync(
                "ACL SAVE",
                [DoAclSaveAsync]
            );

            static async Task DoAclSaveAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("ACL", ["SAVE"]);

                    Assert.Fail("No ACL file, so this should have failed");
                }
                catch (Exception e)
                {
                    if (e.Message != "ERR ACL configuration file not set.")
                    {
                        throw;
                    }
                }
            }
        }

        [Test]
        public async Task AclSetUserACLs()
        {
            await CheckCommandsAsync(
                "ACL SETUSER",
                [DoAclSetUserOnAsync, DoAclSetUserCategoryAsync, DoAclSetUserOnCategoryAsync]
            );

            static async Task DoAclSetUserOnAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("ACL", ["SETUSER", "foo", "on"]);
                Assert.AreEqual("OK", res);
            }

            static async Task DoAclSetUserCategoryAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("ACL", ["SETUSER", "foo", "+@read"]);
                Assert.AreEqual("OK", res);
            }

            static async Task DoAclSetUserOnCategoryAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("ACL", ["SETUSER", "foo", "on", "+@read"]);
                Assert.AreEqual("OK", res);
            }
        }

        [Test]
        public async Task AclUsersACLs()
        {
            await CheckCommandsAsync(
                "ACL USERS",
                [DoAclUsersAsync]
            );

            static async Task DoAclUsersAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ACL", ["USERS"]);
                Assert.IsNotNull(val);
            }
        }

        [Test]
        public async Task AclWhoAmIACLs()
        {
            await CheckCommandsAsync(
                "ACL WHOAMI",
                [DoAclWhoAmIAsync]
            );

            static async Task DoAclWhoAmIAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ACL", ["WHOAMI"]);
                Assert.AreNotEqual("", (string)val);
            }
        }

        [Test]
        public async Task AppendACLs()
        {
            int count = 0;

            await CheckCommandsAsync(
                "APPEND",
                [DoAppendAsync]
            );

            async Task DoAppendAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("APPEND", [$"key-{count}", "foo"]);
                count++;

                Assert.AreEqual(3, (int)val);
            }
        }

        [Test]
        public async Task AskingACLs()
        {
            await CheckCommandsAsync(
                "ASKING",
                [DoAskingAsync]
            );

            async Task DoAskingAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ASKING");
                Assert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public async Task BGSaveACLs()
        {
            await CheckCommandsAsync(
                "BGSAVE",
                [DoBGSaveAsync, DoBGSaveScheduleAsync]
            );

            static async Task DoBGSaveAsync(GarnetClient client)
            {
                try
                {
                    string res = await client.ExecuteForStringResultAsync("BGSAVE");

                    Assert.IsTrue("Background saving started" == res || "Background saving scheduled" == res);
                }
                catch (Exception e)
                {
                    if (e.Message != "ERR checkpoint already in progress")
                    {
                        throw;
                    }
                }
            }

            static async Task DoBGSaveScheduleAsync(GarnetClient client)
            {
                try
                {
                    string res = await client.ExecuteForStringResultAsync("BGSAVE", ["SCHEDULE"]);

                    Assert.IsTrue("Background saving started" == res || "Background saving scheduled" == res);
                }
                catch (Exception e)
                {
                    if (e.Message != "ERR checkpoint already in progress")
                    {
                        throw;
                    }
                }
            }
        }

        [Test]
        public async Task BitcountACLs()
        {
            await CheckCommandsAsync(
                "BITCOUNT",
                [DoBitCountAsync, DoBitCountStartEndAsync, DoBitCountStartEndBitAsync, DoBitCountStartEndByteAsync]
            );

            static async Task DoBitCountAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITCOUNT", ["empty-key"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoBitCountStartEndAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITCOUNT", ["empty-key", "1", "1"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoBitCountStartEndByteAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITCOUNT", ["empty-key", "1", "1", "BYTE"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoBitCountStartEndBitAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITCOUNT", ["empty-key", "1", "1", "BIT"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task BitfieldACLs()
        {
            int count = 0;

            await CheckCommandsAsync(
                "BITFIELD",
                [DoBitFieldGetAsync, DoBitFieldGetWrapAsync, DoBitFieldGetSatAsync, DoBitFieldGetFailAsync, DoBitFieldSetAsync, DoBitFieldSetWrapAsync, DoBitFieldSetSatAsync, DoBitFieldSetFailAsync, DoBitFieldIncrByAsync, DoBitFieldIncrByWrapAsync, DoBitFieldIncrBySatAsync, DoBitFieldIncrByFailAsync, DoBitFieldMultiAsync]
            );

            async Task DoBitFieldGetAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "GET", "u4", "0"]);
                count++;
                Assert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldGetWrapAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "GET", "u4", "0", "OVERFLOW", "WRAP"]);
                count++;
                Assert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldGetSatAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "GET", "u4", "0", "OVERFLOW", "SAT"]);
                count++;
                Assert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldGetFailAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "GET", "u4", "0", "OVERFLOW", "FAIL"]);
                count++;
                Assert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldSetAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "SET", "u4", "0", "1"]);
                count++;
                Assert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldSetWrapAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "SET", "u4", "0", "1", "OVERFLOW", "WRAP"]);
                count++;
                Assert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldSetSatAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "SET", "u4", "0", "1", "OVERFLOW", "SAT"]);
                count++;
                Assert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldSetFailAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "SET", "u4", "0", "1", "OVERFLOW", "FAIL"]);
                count++;
                Assert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldIncrByAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "INCRBY", "u4", "0", "4"]);
                count++;
                Assert.AreEqual(4, long.Parse(val[0]));
            }

            async Task DoBitFieldIncrByWrapAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "INCRBY", "u4", "0", "4", "OVERFLOW", "WRAP"]);
                count++;
                Assert.AreEqual(4, long.Parse(val[0]));
            }

            async Task DoBitFieldIncrBySatAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "INCRBY", "u4", "0", "4", "OVERFLOW", "SAT"]);
                count++;
                Assert.AreEqual(4, long.Parse(val[0]));
            }

            async Task DoBitFieldIncrByFailAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "INCRBY", "u4", "0", "4", "OVERFLOW", "FAIL"]);
                count++;
                Assert.AreEqual(4, long.Parse(val[0]));
            }

            async Task DoBitFieldMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "OVERFLOW", "WRAP", "GET", "u4", "1", "SET", "u4", "2", "1", "OVERFLOW", "FAIL", "INCRBY", "u4", "6", "2"]);
                count++;

                Assert.AreEqual(3, val.Length);

                string v0 = val[0];
                string v1 = val[1];
                string v2 = val[2];

                Assert.AreEqual("0", v0);
                Assert.AreEqual("0", v1);
                Assert.AreEqual("2", v2);
            }
        }

        [Test]
        public async Task BitfieldROACLs()
        {
            await CheckCommandsAsync(
                "BITFIELD_RO",
                [DoBitFieldROGetAsync, DoBitFieldROMultiAsync]
            );

            static async Task DoBitFieldROGetAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD_RO", ["empty-a", "GET", "u4", "0"]);
                Assert.AreEqual(0, long.Parse(val[0]));
            }

            static async Task DoBitFieldROMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD_RO", ["empty-b", "GET", "u4", "0", "GET", "u4", "3"]);

                Assert.AreEqual(2, val.Length);

                string v0 = val[0];
                string v1 = val[1];

                Assert.AreEqual("0", v0);
                Assert.AreEqual("0", v1);
            }
        }

        [Test]
        public async Task BitOpACLs()
        {
            await CheckCommandsAsync(
                "BITOP",
                [DoBitOpAndAsync, DoBitOpAndMultiAsync, DoBitOpOrAsync, DoBitOpOrMultiAsync, DoBitOpXorAsync, DoBitOpXorMultiAsync, DoBitOpNotAsync]
            );

            static async Task DoBitOpAndAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["AND", "zero", "zero"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoBitOpAndMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["AND", "zero", "zero", "one", "zero"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoBitOpOrAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["OR", "one", "one"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoBitOpOrMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["OR", "one", "one", "one", "one"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoBitOpXorAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["XOR", "one", "zero"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoBitOpXorMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["XOR", "one", "one", "one", "zero"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoBitOpNotAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["NOT", "one", "zero"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task BitPosACLs()
        {
            await CheckCommandsAsync(
                "BITPOS",
                [DoBitPosAsync, DoBitPosStartAsync, DoBitPosStartEndAsync, DoBitPosStartEndBitAsync, DoBitPosStartEndByteAsync]
            );

            static async Task DoBitPosAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITPOS", ["empty", "1"]);
                Assert.AreEqual(-1, val);
            }

            static async Task DoBitPosStartAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITPOS", ["empty", "1", "5"]);
                Assert.AreEqual(-1, val);
            }

            static async Task DoBitPosStartEndAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITPOS", ["empty", "1", "5", "7"]);
                Assert.AreEqual(-1, val);
            }

            static async Task DoBitPosStartEndBitAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITPOS", ["empty", "1", "5", "7", "BIT"]);
                Assert.AreEqual(-1, val);
            }

            static async Task DoBitPosStartEndByteAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITPOS", ["empty", "1", "5", "7", "BYTE"]);
                Assert.AreEqual(-1, val);
            }
        }

        [Test]
        public async Task ClientACLs()
        {
            // TODO: client isn't really implemented looks like, so this is mostly a placeholder in case it gets implemented correctly

            await CheckCommandsAsync(
                "CLIENT",
                [DoClientAsync]
            );

            static async Task DoClientAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("CLIENT");
                Assert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task ClusterAddSlotsACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER ADDSLOTS",
                [DoClusterAddSlotsAsync, DoClusterAddSlotsMultiAsync]
            );

            static async Task DoClusterAddSlotsAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["ADDSLOTS", "1"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static async Task DoClusterAddSlotsMultiAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["ADDSLOTS", "1", "2"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterAddSlotsRangeACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER ADDSLOTSRANGE",
                [DoClusterAddSlotsRangeAsync, DoClusterAddSlotsRangeMultiAsync]
            );

            static async Task DoClusterAddSlotsRangeAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["ADDSLOTSRANGE", "1", "3"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static async Task DoClusterAddSlotsRangeMultiAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["ADDSLOTSRANGE", "1", "3", "7", "9"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterAofSyncACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER AOFSYNC",
                [DoClusterAofSyncAsync]
            );

            static async Task DoClusterAofSyncAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["AOFSYNC", "abc", "def"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterAppendLogACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER APPENDLOG",
                [DoClusterAppendLogAsync]
            );

            static async Task DoClusterAppendLogAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["APPENDLOG", "a", "b", "c", "d", "e"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterBanListACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER BANLIST",
                [DoClusterBanListAsync]
            );

            static async Task DoClusterBanListAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["BANLIST"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterBeginReplicaRecoverACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER BEGIN_REPLICA_RECOVER",
                [DoClusterBeginReplicaFailoverAsync]
            );

            static async Task DoClusterBeginReplicaFailoverAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["BEGIN_REPLICA_RECOVER", "1", "2", "3", "4", "5", "6", "7"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterBumpEpochACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER BUMPEPOCH",
                [DoClusterBumpEpochAsync]
            );

            static async Task DoClusterBumpEpochAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["BUMPEPOCH"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterCountKeysInSlotACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER COUNTKEYSINSLOT",
                [DoClusterBumpEpochAsync]
            );

            static async Task DoClusterBumpEpochAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["COUNTKEYSINSLOT", "1"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterDelKeysInSlotACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER DELKEYSINSLOT",
                [DoClusterDelKeysInSlotAsync]
            );

            static async Task DoClusterDelKeysInSlotAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["DELKEYSINSLOT", "1"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterDelKeysInSlotRangeACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER DELKEYSINSLOTRANGE",
                [DoClusterDelKeysInSlotRangeAsync, DoClusterDelKeysInSlotRangeMultiAsync]
            );

            static async Task DoClusterDelKeysInSlotRangeAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["DELKEYSINSLOTRANGE", "1", "3"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static async Task DoClusterDelKeysInSlotRangeMultiAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["DELKEYSINSLOTRANGE", "1", "3", "5", "9"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterDelSlotsACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER DELSLOTS",
                [DoClusterDelSlotsAsync, DoClusterDelSlotsMultiAsync]
            );

            static async Task DoClusterDelSlotsAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["DELSLOTS", "1"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static async Task DoClusterDelSlotsMultiAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["DELSLOTS", "1", "2"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterDelSlotsRangeACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER DELSLOTSRANGE",
                [DoClusterDelSlotsRangeAsync, DoClusterDelSlotsRangeMultiAsync]
            );

            static async Task DoClusterDelSlotsRangeAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["DELSLOTSRANGE", "1", "3"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static async Task DoClusterDelSlotsRangeMultiAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["DELSLOTSRANGE", "1", "3", "9", "11"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterEndpointACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER ENDPOINT",
                [DoClusterEndpointAsync]
            );

            static async Task DoClusterEndpointAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["ENDPOINT", "abcd"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterFailoverACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER FAILOVER",
                [DoClusterFailoverAsync, DoClusterFailoverForceAsync]
            );

            static async Task DoClusterFailoverAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["FAILOVER"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static async Task DoClusterFailoverForceAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["FAILOVER", "FORCE"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterFailStopWritesACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER FAILSTOPWRITES",
                [DoClusterFailStopWritesAsync]
            );

            static async Task DoClusterFailStopWritesAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["FAILSTOPWRITES", "foo"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterFailReplicationOffsetACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER FAILREPLICATIONOFFSET",
                [DoClusterFailStopWritesAsync]
            );

            static async Task DoClusterFailStopWritesAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["FAILREPLICATIONOFFSET", "foo"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterForgetACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER FORGET",
                [DoClusterForgetAsync]
            );

            static async Task DoClusterForgetAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["FORGET", "foo"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterGetKeysInSlotACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER GETKEYSINSLOT",
                [DoClusterGetKeysInSlotAsync]
            );

            static async Task DoClusterGetKeysInSlotAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["GETKEYSINSLOT", "foo", "3"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterGossipACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER GOSSIP",
                [DoClusterGossipAsync]
            );

            static async Task DoClusterGossipAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["GOSSIP", "foo", "3"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterHelpACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER HELP",
                [DoClusterHelpAsync]
            );

            static async Task DoClusterHelpAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["HELP"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterInfoACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER INFO",
                [DoClusterInfoAsync]
            );

            static async Task DoClusterInfoAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["INFO"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterInitiateReplicaSyncACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER INITIATE_REPLICA_SYNC",
                [DoClusterInfoAsync]
            );

            static async Task DoClusterInfoAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["INITIATE_REPLICA_SYNC", "1", "2", "3", "4", "5"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterKeySlotACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER KEYSLOT",
                [DoClusterKeySlotAsync]
            );

            static async Task DoClusterKeySlotAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["KEYSLOT", "foo"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterMeetACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER MEET",
                [DoClusterMeetAsync, DoClusterMeetPortAsync]
            );

            static async Task DoClusterMeetAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["MEET", "127.0.0.1", "1234"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static async Task DoClusterMeetPortAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["MEET", "127.0.0.1", "1234", "6789"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterMigrateACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER MIGRATE",
                [DoClusterMigrateAsync]
            );

            static async Task DoClusterMigrateAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["MIGRATE", "a", "b", "c"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterMTasksACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER MTASKS",
                [DoClusterMTasksAsync]
            );

            static async Task DoClusterMTasksAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["MTASKS"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterMyIdACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER MYID",
                [DoClusterMyIdAsync]
            );

            static async Task DoClusterMyIdAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["MYID"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterMyParentIdACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER MYPARENTID",
                [DoClusterMyParentIdAsync]
            );

            static async Task DoClusterMyParentIdAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["MYPARENTID"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterNodesACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER NODES",
                [DoClusterNodesAsync]
            );

            static async Task DoClusterNodesAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["NODES"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterReplicasACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER REPLICAS",
                [DoClusterReplicasAsync]
            );

            static async Task DoClusterReplicasAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["REPLICAS", "foo"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterReplicateACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER REPLICATE",
                [DoClusterReplicateAsync]
            );

            static async Task DoClusterReplicateAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["REPLICATE", "foo"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterResetACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER RESET",
                [DoClusterResetAsync, DoClusteResetHardAsync]
            );

            static async Task DoClusterResetAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["RESET"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static async Task DoClusteResetHardAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["RESET", "HARD"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterSendCkptFileSegmentACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER SEND_CKPT_FILE_SEGMENT",
                [DoClusterSendCkptFileSegmentAsync]
            );

            static async Task DoClusterSendCkptFileSegmentAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SEND_CKPT_FILE_SEGMENT", "1", "2", "3", "4", "5"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterSendCkptMetadataACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER SEND_CKPT_METADATA",
                [DoClusterSendCkptMetadataAsync]
            );

            static async Task DoClusterSendCkptMetadataAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SEND_CKPT_METADATA", "1", "2", "3", "4", "5"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterSetConfigEpochACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER SET-CONFIG-EPOCH",
                [DoClusterSetConfigEpochAsync]
            );

            static async Task DoClusterSetConfigEpochAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SET-CONFIG-EPOCH", "foo"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterSetSlotACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER SETSLOT",
                [DoClusterSetSlotAsync, DoClusterSetSlotStableAsync, DoClusterSetSlotImportingAsync]
            );

            static async Task DoClusterSetSlotAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SETSLOT", "1"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static async Task DoClusterSetSlotStableAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SETSLOT", "1", "STABLE"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static async Task DoClusterSetSlotImportingAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SETSLOT", "1", "IMPORTING", "foo"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterSetSlotsRangeACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER SETSLOTSRANGE",
                [DoClusterSetSlotsRangeStableAsync, DoClusterSetSlotsRangeStableMultiAsync, DoClusterSetSlotsRangeImportingAsync, DoClusterSetSlotsRangeImportingMultiAsync]
            );

            static async Task DoClusterSetSlotsRangeStableAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SETSLOTSRANGE", "STABLE", "1", "5"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static async Task DoClusterSetSlotsRangeStableMultiAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SETSLOTSRANGE", "STABLE", "1", "5", "10", "15"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static async Task DoClusterSetSlotsRangeImportingAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SETSLOTSRANGE", "IMPORTING", "foo", "1", "5"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static async Task DoClusterSetSlotsRangeImportingMultiAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SETSLOTSRANGE", "IMPORTING", "foo", "1", "5", "10", "15"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterShardsACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER SHARDS",
                [DoClusterShardsAsync]
            );

            static async Task DoClusterShardsAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SHARDS"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterSlotsACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER SLOTS",
                [DoClusterSlotsAsync]
            );

            static async Task DoClusterSlotsAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SLOTS"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
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
        public async Task ClusterSlotStateACLs()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER SLOTSTATE",
                [DoClusterSlotStateAsync]
            );

            static async Task DoClusterSlotStateAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SLOTSTATE"]);
                    Assert.Fail("Shouldn't be reachable, cluster isn't enabled");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }
        }

        // todo: restore
        //[Test]
        //public async Task CommandACLs()
        //{
        //    await CheckCommandsAsync(
        //        "COMMAND",
        //        [DoCommandAsync]
        //    );

        //    static async Task DoCommandAsync(GarnetClient client)
        //    {
        //        string[] val = await client.ExecuteForStringArrayResultAsync("COMMAND");
        //        Assert.IsNotNull(val);
        //    }
        //}

        [Test]
        public async Task CommandCountACLs()
        {
            await CheckCommandsAsync(
                "COMMAND COUNT",
                [DoCommandCountAsync]
            );

            static async Task DoCommandCountAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("COMMAND", ["COUNT"]);
                Assert.IsTrue(val > 0);
            }
        }

        // todo: restore
        //[Test]
        //public async Task CommandInfoACLs()
        //{
        //    await CheckCommandsAsync(
        //        "COMMAND INFO",
        //        [DoCommandInfoAsync, DoCommandInfoOneAsync, DoCommandInfoMultiAsync]
        //    );

        //    static async Task DoCommandInfoAsync(GarnetClient client)
        //    {
        //        string[] val = await client.ExecuteForStringArrayResultAsync("COMMAND", ["INFO"]);
        //        Assert.IsNotNull(val);
        //    }

        //    static async Task DoCommandInfoOneAsync(GarnetClient client)
        //    {
        //        string[] val = await client.ExecuteForStringArrayResultAsync("COMMAND", ["INFO", "GET"]);
        //        Assert.IsNotNull(val);
        //    }

        //    static async Task DoCommandInfoMultiAsync(GarnetClient client)
        //    {
        //        string[] val = await client.ExecuteForStringArrayResultAsync("COMMAND", ["INFO", "GET", "SET", "APPEND"]);
        //        Assert.IsNotNull(val);
        //    }
        //}

        [Test]
        public async Task CommitAOFACLs()
        {
            await CheckCommandsAsync(
                "COMMITAOF",
                [DoCommitAOFAsync]
            );

            static async Task DoCommitAOFAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("COMMITAOF");
                Assert.AreEqual("AOF file committed", val);
            }
        }

        [Test]
        public async Task ConfigGetACLs()
        {
            // TODO: CONFIG GET doesn't implement multiple parameters, so that is untested

            await CheckCommandsAsync(
                "CONFIG GET",
                [DoConfigGetOneAsync]
            );

            static async Task DoConfigGetOneAsync(GarnetClient client)
            {
                string[] res = await client.ExecuteForStringArrayResultAsync("CONFIG", ["GET", "timeout"]);

                Assert.AreEqual(2, res.Length);
                Assert.AreEqual("timeout", (string)res[0]);
                Assert.IsTrue(int.Parse(res[1]) >= 0);
            }
        }

        [Test]
        public async Task ConfigRewriteACLs()
        {
            await CheckCommandsAsync(
                "CONFIG REWRITE",
                [DoConfigRewriteAsync]
            );

            static async Task DoConfigRewriteAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("CONFIG", ["REWRITE"]);
                Assert.AreEqual("OK", res);
            }
        }

        [Test]
        public async Task ConfigSetACLs()
        {
            // CONFIG SET parameters are pretty limitted, so this uses error responses for "got past the ACL" validation - that's not great

            await CheckCommandsAsync(
                "CONFIG SET",
                [DoConfigSetOneAsync, DoConfigSetMultiAsync]
            );

            static async Task DoConfigSetOneAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CONFIG", ["SET", "foo", "bar"]);
                    Assert.Fail("Should have raised unknow config error");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR Unknown option or number of arguments for CONFIG SET - 'foo'")
                    {
                        return;
                    }

                    throw;
                }
            }

            static async Task DoConfigSetMultiAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CONFIG", ["SET", "foo", "bar", "fizz", "buzz"]);
                    Assert.Fail("Should have raised unknow config error");
                }
                catch (Exception e)
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
        public async Task COScanACLs()
        {
            // TODO: COSCAN parameters are unclear... add more cases later

            await CheckCommandsAsync(
                "COSCAN",
                [DoCOScanAsync]
            );

            static async Task DoCOScanAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("CUSTOMOBJECTSCAN", ["foo", "0"]);
                Assert.AreEqual(2, val.Length);
            }
        }

        [Test]
        public async Task CustomCmdACLs()
        {
            // TODO: it probably makes sense to expose ACLs for registered commands, but for now just a blanket ACL for all custom commands is all we have

            int count = 0;

            await CheckCommandsAsync(
                "CUSTOMCMD",
                [DoSetWpIfPgtAsync],
                knownCategories: ["garnet", "custom", "dangerous"]
            );

            async Task DoSetWpIfPgtAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("SETWPIFPGT", [$"foo-{count}", "bar", "\0\0\0\0\0\0\0\0"]);
                count++;

                Assert.AreEqual("OK", (string)res);
            }
        }

        [Test]
        public async Task CustomObjCmdACLs()
        {
            // TODO: it probably makes sense to expose ACLs for registered commands, but for now just a blanket ACL for all custom commands is all we have

            await CheckCommandsAsync(
                "CUSTOMOBJCMD",
                [DoMyDictGetAsync],
                knownCategories: ["garnet", "custom", "dangerous"]
            );

            static async Task DoMyDictGetAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("MYDICTGET", ["foo", "bar"]);
                Assert.IsNull(res);
            }
        }

        [Test]
        public async Task CustomTxnACLs()
        {
            // TODO: it probably makes sense to expose ACLs for registered commands, but for now just a blanket ACL for all custom commands is all we have

            await CheckCommandsAsync(
                "CustomTxn",
                [DoReadWriteTxAsync],
                knownCategories: ["garnet", "custom", "dangerous"]
            );

            static async Task DoReadWriteTxAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("READWRITETX", ["foo", "bar", "fizz"]);
                Assert.AreEqual("SUCCESS", res);
            }
        }

        [Test]
        public async Task DBSizeACLs()
        {
            await CheckCommandsAsync(
                "DBSIZE",
                [DoDbSizeAsync]
            );

            static async Task DoDbSizeAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("DBSIZE");
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task DecrACLs()
        {
            int count = 0;

            await CheckCommandsAsync(
                "DECR",
                [DoDecrAsync]
            );

            async Task DoDecrAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("DECR", [$"foo-{count}"]);
                count++;
                Assert.AreEqual(-1, val);
            }
        }

        [Test]
        public async Task DecrByACLs()
        {
            int count = 0;

            await CheckCommandsAsync(
                "DECRBY",
                [DoDecrByAsync]
            );

            async Task DoDecrByAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("DECRBY", [$"foo-{count}", "2"]);
                count++;
                Assert.AreEqual(-2, val);
            }
        }

        [Test]
        public async Task DelACLs()
        {
            await CheckCommandsAsync(
                "DEL",
                [DoDelAsync, DoDelMultiAsync]
            );

            static async Task DoDelAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("DEL", ["foo"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoDelMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("DEL", ["foo", "bar"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task DiscardACLs()
        {
            // Discard is a little weird, so we're using exceptions for control flow here - don't love it

            await CheckCommandsAsync(
                "DISCARD",
                [DoDiscardAsync]
            );

            static async Task DoDiscardAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("DISCARD");
                    Assert.Fail("Shouldn't have reached this point, outside of a MULTI");
                }
                catch (Exception e)
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
        public async Task EchoACLs()
        {
            await CheckCommandsAsync(
                "ECHO",
                [DoEchoAsync]
            );

            static async Task DoEchoAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ECHO", ["hello world"]);
                Assert.AreEqual("hello world", val);
            }
        }

        [Test]
        public async Task ExecACLs()
        {
            // EXEC is a little weird, so we're using exceptions for control flow here - don't love it

            await CheckCommandsAsync(
                "EXEC",
                [DoExecAsync]
            );

            static async Task DoExecAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("EXEC");
                    Assert.Fail("Shouldn't have reached this point, outside of a MULTI");
                }
                catch (Exception e)
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
        public async Task ExistsACLs()
        {
            await CheckCommandsAsync(
                "EXISTS",
                [DoExistsAsync, DoExistsMultiAsync]
            );

            static async Task DoExistsAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXISTS", ["foo"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoExistsMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXISTS", ["foo", "bar"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ExpireACLs()
        {
            // TODO: expire doesn't support combinations of flags (XX GT, XX LT are legal) so those will need to be tested when implemented

            await CheckCommandsAsync(
                "EXPIRE",
                [DoExpireAsync, DoExpireNXAsync, DoExpireXXAsync, DoExpireGTAsync, DoExpireLTAsync]
            );

            static async Task DoExpireAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXPIRE", ["foo", "10"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoExpireNXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXPIRE", ["foo", "10", "NX"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoExpireXXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXPIRE", ["foo", "10", "XX"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoExpireGTAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXPIRE", ["foo", "10", "GT"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoExpireLTAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXPIRE", ["foo", "10", "LT"]);
                Assert.AreEqual(0, val);
            }
        }

        // todo: restore
        //[Test]
        //public async Task FailoverACLs()
        //{
        //    const string TestUser = "failover-user";
        //    const string TestPassword = "foo";

        //    // Failover is strange, so this more complicated that typical

        //    Func<GarnetClient, Task>[] cmds = [
        //        DoFailoverAsync,
        //        DoFailoverToAsync,
        //        DoFailoverAbortAsync,
        //        DoFailoverToForceAsync,
        //        DoFailoverToAbortAsync,
        //        DoFailoverToForceAbortAsync,
        //        DoFailoverToForceAbortTimeoutAsync,
        //    ];

        //    // Check denied with -failover
        //    foreach (Action<IServer> cmd in cmds)
        //    {
        //        Run(false, (defaultServer, testServer) => SetUser(defaultServer, TestUser, $"-failover"), cmd);
        //    }

        //    string[] acls = ["admin", "slow", "dangerous"];

        //    foreach (Action<IServer> cmd in cmds)
        //    {
        //        // Check works with +@all
        //        Run(true, (defaultServer, testServer) => { }, cmd);

        //        // Check denied with -@whatever
        //        foreach (string acl in acls)
        //        {
        //            Run(false, (defaultServer, testServer) => SetUser(defaultServer, TestUser, $"-@{acl}"), cmd);
        //        }
        //    }

        //    void Run(bool expectSuccess, Action<IServer, IServer> before, Action<IServer> cmd)
        //    {
        //        // Refresh Garnet instance
        //        TearDown();
        //        Setup();

        //        using ConnectionMultiplexer defaultRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: DefaultUser, authPassword: DefaultPassword));
        //        IServer defaultServer = defaultRedis.GetServers().Single();

        //        InitUser(defaultServer, "failover-user", "foo");

        //        using ConnectionMultiplexer failoverRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: TestUser, authPassword: TestPassword));

        //        IServer failoverServer = failoverRedis.GetServers().Single();

        //        before(defaultServer, failoverServer);

        //        Assert.AreEqual(expectSuccess, CheckAuthFailure(() => cmd(failoverServer)));
        //    }

        //    static async Task DoFailoverAsync(GarnetClient client)
        //    {
        //        try
        //        {
        //            await client.ExecuteForStringResultAsync("FAILOVER");
        //            Assert.Fail("Shouldn't be reachable, cluster not enabled");
        //        }
        //        catch (Exception e)
        //        {
        //            if (e.Message == "ERR This instance has cluster support disabled")
        //            {
        //                return;
        //            }

        //            throw;
        //        }
        //    }

        //    static async Task DoFailoverToAsync(GarnetClient client)
        //    {
        //        try
        //        {
        //            await client.ExecuteForStringResultAsync("FAILOVER", ["TO", "127.0.0.1", "9999"]);
        //            Assert.Fail("Shouldn't be reachable, cluster not enabled");
        //        }
        //        catch (Exception e)
        //        {
        //            if (e.Message == "ERR This instance has cluster support disabled")
        //            {
        //                return;
        //            }

        //            throw;
        //        }
        //    }

        //    static async Task DoFailoverAbortAsync(GarnetClient client)
        //    {
        //        try
        //        {
        //            await client.ExecuteForStringResultAsync("FAILOVER", ["ABORT"]);
        //            Assert.Fail("Shouldn't be reachable, cluster not enabled");
        //        }
        //        catch (Exception e)
        //        {
        //            if (e.Message == "ERR This instance has cluster support disabled")
        //            {
        //                return;
        //            }

        //            throw;
        //        }
        //    }

        //    static async Task DoFailoverToForceAsync(GarnetClient client)
        //    {
        //        try
        //        {
        //            await client.ExecuteForStringResultAsync("FAILOVER", ["TO", "127.0.0.1", "9999", "FORCE"]);
        //            Assert.Fail("Shouldn't be reachable, cluster not enabled");
        //        }
        //        catch (Exception e)
        //        {
        //            if (e.Message == "ERR This instance has cluster support disabled")
        //            {
        //                return;
        //            }

        //            throw;
        //        }
        //    }

        //    static async Task DoFailoverToAbortAsync(GarnetClient client)
        //    {
        //        try
        //        {
        //            await client.ExecuteForStringResultAsync("FAILOVER", ["TO", "127.0.0.1", "9999", "ABORT"]);
        //            Assert.Fail("Shouldn't be reachable, cluster not enabled");
        //        }
        //        catch (Exception e)
        //        {
        //            if (e.Message == "ERR This instance has cluster support disabled")
        //            {
        //                return;
        //            }

        //            throw;
        //        }
        //    }

        //    static async Task DoFailoverToForceAbortAsync(GarnetClient client)
        //    {
        //        try
        //        {
        //            await client.ExecuteForStringResultAsync("FAILOVER", ["TO", "127.0.0.1", "9999", "FORCE", "ABORT"]);
        //            Assert.Fail("Shouldn't be reachable, cluster not enabled");
        //        }
        //        catch (Exception e)
        //        {
        //            if (e.Message == "ERR This instance has cluster support disabled")
        //            {
        //                return;
        //            }

        //            throw;
        //        }
        //    }

        //    static async Task DoFailoverToForceAbortTimeoutAsync(GarnetClient client)
        //    {
        //        try
        //        {
        //            await client.ExecuteForStringResultAsync("FAILOVER", ["TO", "127.0.0.1", "9999", "FORCE", "ABORT", "TIMEOUT", "1"]);
        //            Assert.Fail("Shouldn't be reachable, cluster not enabled");
        //        }
        //        catch (Exception e)
        //        {
        //            if (e.Message == "ERR This instance has cluster support disabled")
        //            {
        //                return;
        //            }

        //            throw;
        //        }
        //    }
        //}

        [Test]
        public async Task FlushDBACLs()
        {
            await CheckCommandsAsync(
                "FLUSHDB",
                [DoFlushDBAsync, DoFlushDBAsyncAsync]
            );

            static async Task DoFlushDBAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("FLUSHDB");
                Assert.AreEqual("OK", val);
            }

            static async Task DoFlushDBAsyncAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("FLUSHDB", ["ASYNC"]);
                Assert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task ForceGCACLs()
        {
            await CheckCommandsAsync(
                "FORCEGC",
                [DoForceGCAsync, DoForceGCGenAsync]
            );

            static async Task DoForceGCAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("FORCEGC");
                Assert.AreEqual("GC completed", val);
            }

            static async Task DoForceGCGenAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("FORCEGC", ["1"]);
                Assert.AreEqual("GC completed", val);
            }
        }

        [Test]
        public async Task GetACLs()
        {
            await CheckCommandsAsync(
                "GET",
                [DoGetAsync]
            );

            static async Task DoGetAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("GET", ["foo"]);
                Assert.IsNull(val);
            }
        }

        [Test]
        public async Task GetBitACLs()
        {
            await CheckCommandsAsync(
                "GETBIT",
                [DoGetBitAsync]
            );

            static async Task DoGetBitAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GETBIT", ["foo", "4"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task GetDelACLs()
        {
            await CheckCommandsAsync(
                "GETDEL",
                [DoGetDelAsync]
            );

            static async Task DoGetDelAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("GETDEL", ["foo"]);
                Assert.IsNull(val);
            }
        }

        [Test]
        public async Task GetRangeACLs()
        {
            await CheckCommandsAsync(
                "GETRANGE",
                [DoGetRangeAsync]
            );

            static async Task DoGetRangeAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("GETRANGE", ["foo", "10", "15"]);
                Assert.AreEqual("", val);
            }
        }

        [Test]
        public async Task HDelACLs()
        {
            await CheckCommandsAsync(
                "HDEL",
                [DoHDelAsync, DoHDelMultiAsync]
            );

            static async Task DoHDelAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HDEL", ["foo", "bar"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoHDelMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HDEL", ["foo", "bar", "fizz"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task HExistsACLs()
        {
            await CheckCommandsAsync(
                "HEXISTS",
                [DoHDelAsync]
            );

            static async Task DoHDelAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HEXISTS", ["foo", "bar"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task HGetACLs()
        {
            await CheckCommandsAsync(
                "HGET",
                [DoHDelAsync]
            );

            static async Task DoHDelAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("HGET", ["foo", "bar"]);
                Assert.IsNull(val);
            }
        }

        [Test]
        public async Task HGetAllACLs()
        {
            await CheckCommandsAsync(
                "HGETALL",
                [DoHDelAsync]
            );

            static async Task DoHDelAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HGETALL", ["foo"]);

                Assert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task HIncrByACLs()
        {
            int cur = 0;

            await CheckCommandsAsync(
                "HINCRBY",
                [DoHIncrByAsync]
            );

            async Task DoHIncrByAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HINCRBY", ["foo", "bar", "2"]);
                cur += 2;
                Assert.AreEqual(cur, val);
            }
        }

        [Test]
        public async Task HIncrByFloatACLs()
        {
            double cur = 0;

            await CheckCommandsAsync(
                "HINCRBYFLOAT",
                [DoHIncrByFloatAsync]
            );

            async Task DoHIncrByFloatAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("HINCRBYFLOAT", ["foo", "bar", "1.0"]);
                cur += 1.0;
                Assert.AreEqual(cur, double.Parse(val));
            }
        }

        [Test]
        public async Task HKeysACLs()
        {
            await CheckCommandsAsync(
                "HKEYS",
                [DoHKeysAsync]
            );

            static async Task DoHKeysAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HKEYS", ["foo"]);
                Assert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task HLenACLs()
        {
            await CheckCommandsAsync(
                "HLEN",
                [DoHLenAsync]
            );

            static async Task DoHLenAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HLEN", ["foo"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task HMGetACLs()
        {
            await CheckCommandsAsync(
                "HMGET",
                [DoHMGetAsync, DoHMGetMultiAsync]
            );

            static async Task DoHMGetAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HMGET", ["foo", "bar"]);
                Assert.AreEqual(1, val.Length);
                Assert.IsNull(val[0]);
            }

            static async Task DoHMGetMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HMGET", ["foo", "bar", "fizz"]);
                Assert.AreEqual(2, val.Length);
                Assert.IsNull(val[0]);
                Assert.IsNull(val[1]);
            }
        }

        [Test]
        public async Task HMSetACLs()
        {
            await CheckCommandsAsync(
                "HMSET",
                [DoHMSetAsync, DoHMSetMultiAsync]
            );

            static async Task DoHMSetAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("HMSET", ["foo", "bar", "fizz"]);
                Assert.AreEqual("OK", val);
            }

            static async Task DoHMSetMultiAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("HMSET", ["foo", "bar", "fizz", "hello", "world"]);
                Assert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task HRandFieldACLs()
        {
            await CheckCommandsAsync(
                "HRANDFIELD",
                [DoHRandFieldAsync, DoHRandFieldCountAsync, DoHRandFieldCountWithValuesAsync]
            );

            static async Task DoHRandFieldAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("HRANDFIELD", ["foo"]);
                Assert.IsNull(val);
            }

            static async Task DoHRandFieldCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HRANDFIELD", ["foo", "1"]);
                Assert.AreEqual(0, val.Length);
            }

            static async Task DoHRandFieldCountWithValuesAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HRANDFIELD", ["foo", "1", "WITHVALUES"]);
                Assert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task HScanACLs()
        {
            await CheckCommandsAsync(
                "HSCAN",
                [DoHScanAsync, DoHScanMatchAsync, DoHScanCountAsync, DoHScanNoValuesAsync, DoHScanMatchCountAsync, DoHScanMatchNoValuesAsync, DoHScanCountNoValuesAsync, DoHScanMatchCountNoValuesAsync]
            );

            static async Task DoHScanAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HSCAN", ["foo", "0"]);
                Assert.AreEqual(2, val.Length);
            }

            static async Task DoHScanMatchAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HSCAN", ["foo", "0", "MATCH", "*"]);
                Assert.AreEqual(2, val.Length);
            }

            static async Task DoHScanCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HSCAN", ["foo", "0", "COUNT", "2"]);
                Assert.AreEqual(2, val.Length);
            }

            static async Task DoHScanNoValuesAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HSCAN", ["foo", "0", "NOVALUES"]);
                Assert.AreEqual(2, val.Length);
            }

            static async Task DoHScanMatchCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HSCAN", ["foo", "0", "MATCH", "*", "COUNT", "2"]);
                Assert.AreEqual(2, val.Length);
            }

            static async Task DoHScanMatchNoValuesAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HSCAN", ["foo", "0", "MATCH", "*", "NOVALUES"]);
                Assert.AreEqual(2, val.Length);
            }

            static async Task DoHScanCountNoValuesAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HSCAN", ["foo", "0", "COUNT", "0", "NOVALUES"]);
                Assert.AreEqual(2, val.Length);
            }

            static async Task DoHScanMatchCountNoValuesAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HSCAN", ["foo", "0", "MATCH", "*", "COUNT", "0", "NOVALUES"]);
                Assert.AreEqual(2, val.Length);
            }
        }

        [Test]
        public async Task HSetACLs()
        {
            int keyIx = 0;

            await CheckCommandsAsync(
                "HSET",
                [DoHSetAsync, DoHSetMultiAsync]
            );

            async Task DoHSetAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HSET", [$"foo-{keyIx}", "bar", "fizz"]);
                keyIx++;

                Assert.AreEqual(1, val);
            }

            async Task DoHSetMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HSET", [$"foo-{keyIx}", "bar", "fizz", "hello", "world"]);
                keyIx++;

                Assert.AreEqual(2, val);
            }
        }

        [Test]
        public async Task HSetNXACLs()
        {
            int keyIx = 0;

            await CheckCommandsAsync(
                "HSETNX",
                [DoHSetNXAsync]
            );

            async Task DoHSetNXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HSETNX", [$"foo-{keyIx}", "bar", "fizz"]);
                keyIx++;

                Assert.AreEqual(1, val);
            }
        }

        [Test]
        public async Task HStrLenACLs()
        {
            await CheckCommandsAsync(
                "HSTRLEN",
                [DoHStrLenAsync]
            );

            static async Task DoHStrLenAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HSTRLEN", ["foo", "bar"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task HValsACLs()
        {
            await CheckCommandsAsync(
                "HVALS",
                [DoHValsAsync]
            );

            static async Task DoHValsAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HVALS", ["foo"]);
                Assert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task IncrACLs()
        {
            int count = 0;

            await CheckCommandsAsync(
                "INCR",
                [DoIncrAsync]
            );

            async Task DoIncrAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("INCR", [$"foo-{count}"]);
                count++;
                Assert.AreEqual(1, val);
            }
        }

        [Test]
        public async Task IncrByACLs()
        {
            int count = 0;

            await CheckCommandsAsync(
                "INCRBY",
                [DoIncrByAsync]
            );

            async Task DoIncrByAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("INCRBY", [$"foo-{count}", "2"]);
                count++;
                Assert.AreEqual(2, val);
            }
        }

        [Test]
        public async Task InfoACLs()
        {
            await CheckCommandsAsync(
               "INFO",
               [DoInfoAsync, DoInfoSingleAsync, DoInfoMultiAsync]
            );

            static async Task DoInfoAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("INFO");
                Assert.IsNotEmpty(val);
            }

            static async Task DoInfoSingleAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("INFO", ["SERVER"]);
                Assert.IsNotEmpty(val);
            }

            static async Task DoInfoMultiAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("INFO", ["SERVER", "MEMORY"]);
                Assert.IsNotEmpty(val);
            }
        }

        [Test]
        public async Task KeysACLs()
        {
            await CheckCommandsAsync(
               "KEYS",
               [DoKeysAsync]
            );

            static async Task DoKeysAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("KEYS", ["*"]);
                Assert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task LastSaveACLs()
        {
            await CheckCommandsAsync(
               "LASTSAVE",
               [DoLastSaveAsync]
            );

            static async Task DoLastSaveAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LASTSAVE");
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task LatencyHelpACLs()
        {
            await CheckCommandsAsync(
               "LATENCY HELP",
               [DoLatencyHelpAsync]
            );

            static async Task DoLatencyHelpAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("LATENCY", ["HELP"]);
                Assert.IsNotNull(val);
            }
        }

        [Test]
        public async Task LatencyHistogramACLs()
        {
            await CheckCommandsAsync(
               "LATENCY HISTOGRAM",
               [DoLatencyHistogramAsync, DoLatencyHistogramSingleAsync, DoLatencyHistogramMultiAsync]
            );

            static async Task DoLatencyHistogramAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("LATENCY", ["HISTOGRAM"]);
                Assert.AreEqual(0, val.Length);
            }

            static async Task DoLatencyHistogramSingleAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("LATENCY", ["HISTOGRAM", "NET_RS_LAT"]);
                Assert.AreEqual(0, val.Length);
            }

            static async Task DoLatencyHistogramMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("LATENCY", ["HISTOGRAM", "NET_RS_LAT", "NET_RS_LAT_ADMIN"]);
                Assert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task LatencyResetACLs()
        {
            await CheckCommandsAsync(
               "LATENCY RESET",
               [DoLatencyResetAsync, DoLatencyResetSingleAsync, DoLatencyResetMultiAsync]
            );

            static async Task DoLatencyResetAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LATENCY", ["RESET"]);
                Assert.AreEqual(6, val);
            }

            static async Task DoLatencyResetSingleAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LATENCY", ["RESET", "NET_RS_LAT"]);
                Assert.AreEqual(1, val);
            }

            static async Task DoLatencyResetMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LATENCY", ["RESET", "NET_RS_LAT", "NET_RS_LAT_ADMIN"]);
                Assert.AreEqual(2, val);
            }
        }

        [Test]
        public async Task BLMoveACLs()
        {
            await CheckCommandsAsync(
                "BLMOVE",
                [DoBLMoveAsync]
            );

            static async Task DoBLMoveAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("BLMOVE", ["foo", "bar", "RIGHT", "LEFT", "1"]);
                Assert.IsNull(val);
            }
        }

        [Test]
        public async Task BLPopACLs()
        {
            await CheckCommandsAsync(
                "BLPOP",
                [DoBLPopAsync]
            );

            static async Task DoBLPopAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BLPOP", ["foo", "1"]);
                Assert.IsNull(val);
            }
        }

        [Test]
        public async Task BRPopACLs()
        {
            await CheckCommandsAsync(
                "BRPOP",
                [DoBRPopAsync]
            );

            static async Task DoBRPopAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BRPOP", ["foo", "1"]);
                Assert.IsNull(val);
            }
        }

        [Test]
        public async Task LPopACLs()
        {
            await CheckCommandsAsync(
                "LPOP",
                [DoLPopAsync, DoLPopCountAsync]
            );

            static async Task DoLPopAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LPOP", ["foo"]);
                Assert.IsNull(val);
            }

            static async Task DoLPopCountAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LPOP", ["foo", "4"]);
                Assert.IsNull(val);
            }
        }

        [Test]
        public async Task LPushACLs()
        {
            int count = 0;

            await CheckCommandsAsync(
                "LPUSH",
                [DoLPushAsync, DoLPushMultiAsync]
            );

            async Task DoLPushAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LPUSH", ["foo", "bar"]);
                count++;

                Assert.AreEqual(count, val);
            }

            async Task DoLPushMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LPUSH", ["foo", "bar", "buzz"]);
                count += 2;

                Assert.AreEqual(count, val);
            }
        }

        [Test]
        public async Task LPushXACLs()
        {
            await CheckCommandsAsync(
                "LPUSHX",
                [DoLPushXAsync, DoLPushXMultiAsync]
            );

            static async Task DoLPushXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LPUSHX", ["foo", "bar"]);
                Assert.AreEqual(0, val);
            }

            async Task DoLPushXMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LPUSHX", ["foo", "bar", "buzz"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task RPopACLs()
        {
            await CheckCommandsAsync(
                "RPOP",
                [DoRPopAsync, DoRPopCountAsync]
            );

            static async Task DoRPopAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("RPOP", ["foo"]);
                Assert.IsNull(val);
            }

            static async Task DoRPopCountAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("RPOP", ["foo", "4"]);
                Assert.IsNull(val);
            }
        }

        [Test]
        public async Task LRushACLs()
        {
            int count = 0;

            await CheckCommandsAsync(
                "RPUSH",
                [DoRPushAsync, DoRPushMultiAsync]
            );

            async Task DoRPushAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("RPUSH", ["foo", "bar"]);
                count++;

                Assert.AreEqual(count, val);
            }

            async Task DoRPushMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("RPUSH", ["foo", "bar", "buzz"]);
                count += 2;

                Assert.AreEqual(count, val);
            }
        }

        [Test]
        public async Task RPushACLs()
        {
            int count = 0;

            await CheckCommandsAsync(
                "RPUSH",
                [DoRPushXAsync, DoRPushXMultiAsync]
            );

            async Task DoRPushXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("RPUSH", [$"foo-{count}", "bar"]);
                count++;
                Assert.AreEqual(1, val);
            }

            async Task DoRPushXMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("RPUSH", [$"foo-{count}", "bar", "buzz"]);
                count++;
                Assert.AreEqual(2, val);
            }
        }

        [Test]
        public async Task RPushXACLs()
        {
            await CheckCommandsAsync(
                "RPUSHX",
                [DoRPushXAsync, DoRPushXMultiAsync]
            );

            static async Task DoRPushXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("RPUSHX", ["foo", "bar"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoRPushXMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("RPUSHX", ["foo", "bar", "buzz"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task LLenACLs()
        {
            await CheckCommandsAsync(
                "LLEN",
                [DoLLenAsync]
            );

            static async Task DoLLenAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LLEN", ["foo"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task LTrimACLs()
        {
            await CheckCommandsAsync(
                "LTRIM",
                [DoLTrimAsync]
            );

            static async Task DoLTrimAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LTRIM", ["foo", "4", "10"]);
                Assert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task LRangeACLs()
        {
            await CheckCommandsAsync(
                "LRANGE",
                [DoLRangeAsync]
            );

            static async Task DoLRangeAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("LRANGE", ["foo", "4", "10"]);
                Assert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task LIndexACLs()
        {
            await CheckCommandsAsync(
                "LINDEX",
                [DoLIndexAsync]
            );

            static async Task DoLIndexAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LINDEX", ["foo", "4"]);
                Assert.IsNull(val);
            }
        }

        [Test]
        public async Task LInsertACLs()
        {
            await CheckCommandsAsync(
                "LINSERT",
                [DoLInsertBeforeAsync, DoLInsertAfterAsync]
            );

            static async Task DoLInsertBeforeAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LINSERT", ["foo", "BEFORE", "hello", "world"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoLInsertAfterAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LINSERT", ["foo", "AFTER", "hello", "world"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task LRemACLs()
        {
            await CheckCommandsAsync(
                "LREM",
                [DoLRemAsync]
            );

            static async Task DoLRemAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LREM", ["foo", "0", "hello"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task RPopLPushACLs()
        {
            await CheckCommandsAsync(
                "RPOPLPUSH",
                [DoLRemAsync]
            );

            static async Task DoLRemAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("RPOPLPUSH", ["foo", "bar"]);
                Assert.IsNull(val);
            }
        }

        [Test]
        public async Task LMoveACLs()
        {
            await CheckCommandsAsync(
                "LMOVE",
                [DoLMoveAsync]
            );

            static async Task DoLMoveAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LMOVE", ["foo", "bar", "LEFT", "RIGHT"]);
                Assert.IsNull(val);
            }
        }

        [Test]
        public async Task LSetACLs()
        {
            // TODO: LSET with an empty key appears broken; clean up when fixed

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IServer server = redis.GetServers().Single();
            IDatabase db = redis.GetDatabase();

            db.ListLeftPush("foo", "fizz");

            await CheckCommandsAsync(
                "LSET",
                [DoLMoveAsync]
            );

            static async Task DoLMoveAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LSET", ["foo", "0", "bar"]);
                Assert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task MemoryUsageACLs()
        {
            await CheckCommandsAsync(
                "MEMORY USAGE",
                [DoMemoryUsageAsync, DoMemoryUsageSamplesAsync]
            );

            static async Task DoMemoryUsageAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("MEMORY", ["USAGE", "foo"]);
                Assert.IsNull(val);
            }

            static async Task DoMemoryUsageSamplesAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("MEMORY", ["USAGE", "foo", "SAMPLES", "10"]);
                Assert.IsNull(val);
            }
        }

        [Test]
        public async Task MGetACLs()
        {
            await CheckCommandsAsync(
                "MGET",
                [DoMemorySingleAsync, DoMemoryMultiAsync]
            );

            static async Task DoMemorySingleAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("MGET", ["foo"]);
                Assert.AreEqual(1, val.Length);
                Assert.IsNull(val[0]);
            }

            static async Task DoMemoryMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("MGET", ["foo", "bar"]);
                Assert.AreEqual(2, val.Length);
                Assert.IsNull(val[0]);
                Assert.IsNull(val[1]);
            }
        }

        [Test]
        public async Task MigrateACLs()
        {
            // Uses exceptions for control flow, as we're not setting up replicas here

            await CheckCommandsAsync(
                "MIGRATE",
                [DoMigrateAsync]
            );

            static async Task DoMigrateAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("MIGRATE", ["127.0.0.1", "9999", "KEY", "0", "1000"]);
                    Assert.Fail("Shouldn't succeed, no replicas are attached");
                }
                catch (Exception e)
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
        public async Task ModuleLoadCSACLs()
        {
            // MODULE isn't a proper redis command, but this is the placeholder today... so validate it for completeness

            await CheckCommandsAsync(
                "MODULE",
                [DoModuleLoadAsync]
            );

            static async Task DoModuleLoadAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("MODULE", ["LOADCS", "nonexisting.dll"]);
                    Assert.Fail("Shouldn't succeed using a non-existing binary");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR unable to access one or more binary files.")
                    {
                        return;
                    }

                    throw;
                }
            }
        }

        // todo: restore
        //[Test]
        //public async Task MonitorACLs()
        //{
        //    const string TestUser = "monitor-user";
        //    const string TestPassword = "fizz";

        //    // MONITOR isn't actually implemented, and doesn't fit nicely into SE.Redis anyway, so we only check the DENY cases here

        //    // test just the command
        //    {
        //        using ConnectionMultiplexer defaultRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));
        //        IServer defaultServer = defaultRedis.GetServers().Single();

        //        InitUser(defaultServer, TestUser, TestPassword);
        //        SetUser(defaultServer, TestUser, [$"-monitor"]);

        //        using ConnectionMultiplexer testRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: TestUser, authPassword: TestPassword));
        //        IServer testServer = testRedis.GetServers().Single();

        //        Assert.False(CheckAuthFailure(() => DoMonitor(testServer)), "Permitted when should have been denied");
        //    }

        //    // test is a bit more involved since @admin is present
        //    string[] categories = ["admin", "slow", "dangerous"];

        //    foreach (string category in categories)
        //    {
        //        using ConnectionMultiplexer defaultRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));
        //        IServer defaultServer = defaultRedis.GetServers().Single();

        //        InitUser(defaultServer, TestUser, TestPassword);
        //        SetUser(defaultServer, TestUser, [$"-@{category}"]);

        //        using ConnectionMultiplexer testRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: TestUser, authPassword: TestPassword));
        //        IServer testServer = testRedis.GetServers().Single();

        //        Assert.False(CheckAuthFailure(() => DoMonitor(testServer)), "Permitted when should have been denied");
        //    }

        //    static async Task DoMonitorAsync(GarnetClient client)
        //    {
        //        server.Execute("MONITOR");
        //        Assert.Fail("Should never reach this point");
        //    }
        //}

        [Test]
        public async Task MSetACLs()
        {
            await CheckCommandsAsync(
                "MSET",
                [DoMSetSingleAsync, DoMSetMultiAsync]
            );

            static async Task DoMSetSingleAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("MSET", ["foo", "bar"]);
                Assert.AreEqual("OK", val);
            }

            static async Task DoMSetMultiAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("MSET", ["foo", "bar", "fizz", "buzz"]);
                Assert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task MSetNXACLs()
        {
            int count = 0;

            await CheckCommandsAsync(
                "MSETNX",
                [DoMSetNXSingleAsync, DoMSetNXMultiAsync]
            );

            async Task DoMSetNXSingleAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("MSETNX", [$"foo-{count}", "bar"]);
                count++;

                Assert.AreEqual(1, val);
            }

            async Task DoMSetNXMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("MSETNX", [$"foo-{count}", "bar", $"fizz-{count}", "buzz"]);
                count++;

                Assert.AreEqual(1, val);
            }
        }

        [Test]
        public async Task MultiACLs()
        {
            await CheckCommandsAsync(
                "MULTI",
                [DoMultiAsync],
                skipPing: true
            );

            static async Task DoMultiAsync(GarnetClient client)
            {
                try
                {
                    string val = await client.ExecuteForStringResultAsync("MULTI");
                    Assert.AreEqual("OK", val);
                }
                catch (Exception e)
                {
                    // The "nested MULTI" error response is also legal, if we're ACL'd for MULTI
                    if (e.Message == "ERR MULTI calls can not be nested")
                    {
                        return;
                    }

                    throw;
                }
            }
        }

        [Test]
        public async Task PersistACLs()
        {
            await CheckCommandsAsync(
                "PERSIST",
                [DoPersistAsync]
            );

            static async Task DoPersistAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PERSIST", ["foo"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task PExpireACLs()
        {
            // TODO: pexpire doesn't support combinations of flags (XX GT, XX LT are legal) so those will need to be tested when implemented

            await CheckCommandsAsync(
                "PEXPIRE",
                [DoPExpireAsync, DoPExpireNXAsync, DoPExpireXXAsync, DoPExpireGTAsync, DoPExpireLTAsync]
            );

            static async Task DoPExpireAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PEXPIRE", ["foo", "10"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoPExpireNXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PEXPIRE", ["foo", "10", "NX"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoPExpireXXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PEXPIRE", ["foo", "10", "XX"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoPExpireGTAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PEXPIRE", ["foo", "10", "GT"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoPExpireLTAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PEXPIRE", ["foo", "10", "LT"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task PFAddACLs()
        {
            int count = 0;

            await CheckCommandsAsync(
                "PFADD",
                [DoPFAddSingleAsync, DoPFAddMultiAsync]
            );

            async Task DoPFAddSingleAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PFADD", [$"foo-{count}", "bar"]);
                count++;

                Assert.AreEqual(1, val);
            }

            async Task DoPFAddMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PFADD", [$"foo-{count}", "bar", "fizz"]);
                count++;

                Assert.AreEqual(1, val);
            }
        }

        [Test]
        public async Task PFCountACLs()
        {
            await CheckCommandsAsync(
                "PFCOUNT",
                [DoPFCountSingleAsync, DoPFCountMultiAsync]
            );

            static async Task DoPFCountSingleAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PFCOUNT", ["foo"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoPFCountMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PFCOUNT", ["foo", "bar"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task PFMergeACLs()
        {
            await CheckCommandsAsync(
                "PFMERGE",
                [DoPFMergeSingleAsync, DoPFMergeMultiAsync]
            );

            static async Task DoPFMergeSingleAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("PFMERGE", ["foo"]);
                Assert.AreEqual("OK", val);
            }

            static async Task DoPFMergeMultiAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("PFMERGE", ["foo", "bar"]);
                Assert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task PingACLs()
        {
            await CheckCommandsAsync(
                "PING",
                [DoPingAsync, DoPingMessageAsync]
            );

            static async Task DoPingAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("PING");
                Assert.AreEqual("PONG", val);
            }

            static async Task DoPingMessageAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("PING", ["hello"]);
                Assert.AreEqual("hello", val);
            }
        }

        [Test]
        public async Task PSetEXACLs()
        {
            await CheckCommandsAsync(
                "PSETEX",
                [DoPSetEXAsync]
            );

            static async Task DoPSetEXAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("PSETEX", ["foo", "10", "bar"]);
                Assert.AreEqual("OK", val);
            }
        }

        // todo: restore
        //[Test]
        //public async Task PSubscribeACLs()
        //{
        //    // TODO: not testing the multiple pattern version

        //    int count = 0;

        //    await CheckCommandsAsync(
        //        "PSUBSCRIBE",
        //        [DoPSubscribePatternAsync]
        //    );

        //    async Task DoPSubscribePatternAsync(GarnetClient client)
        //    {
        //        ISubscriber sub = server.Multiplexer.GetSubscriber();

        //        // have to do the (bad) async version to make sure we actually get the error
        //        sub.SubscribeAsync(new RedisChannel($"channel-{count}", RedisChannel.PatternMode.Pattern)).GetAwaiter().GetResult();
        //        count++;
        //    }
        //}

        [Test]
        public async Task PUnsubscribeACLs()
        {
            await CheckCommandsAsync(
                "PUNSUBSCRIBE",
                [DoPUnsubscribePatternAsync]
            );

            static async Task DoPUnsubscribePatternAsync(GarnetClient client)
            {
                string[] res = await client.ExecuteForStringArrayResultAsync("PUNSUBSCRIBE", ["foo"]);
                Assert.IsNotNull(res);
            }
        }

        [Test]
        public async Task PTTLACLs()
        {
            await CheckCommandsAsync(
                "PTTL",
                [DoPTTLAsync]
            );

            static async Task DoPTTLAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PTTL", ["foo"]);
                Assert.AreEqual(-2, val);
            }
        }

        // todo: restore
        //[Test]
        //public async Task PublishACLs()
        //{
        //    await CheckCommandsAsync(
        //        "PUBLISH",
        //        [DoPublishAsync]
        //    );

        //    static async Task DoPublishAsync(GarnetClient client)
        //    {
        //        ISubscriber sub = server.Multiplexer.GetSubscriber();

        //        long count = sub.Publish(new RedisChannel("foo", RedisChannel.PatternMode.Literal), "bar");
        //        Assert.AreEqual(0, count);
        //    }
        //}

        [Test]
        public async Task ReadOnlyACLs()
        {
            await CheckCommandsAsync(
                "READONLY",
                [DoReadOnlyAsync]
            );

            static async Task DoReadOnlyAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("READONLY");
                Assert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task ReadWriteACLs()
        {
            await CheckCommandsAsync(
                "READWRITE",
                [DoReadWriteAsync]
            );

            static async Task DoReadWriteAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("READWRITE");
                Assert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task RegisterCSACLs()
        {
            // TODO: REGISTERCS has a complicated syntax, test proper commands later

            await CheckCommandsAsync(
                "REGISTERCS",
                [DoRegisterCSAsync]
            );

            static async Task DoRegisterCSAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("REGISTERCS");
                    Assert.Fail("Should be unreachable, command is malfoemd");
                }
                catch (Exception e)
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
        public async Task RenameACLs()
        {
            await CheckCommandsAsync(
                "RENAME",
                [DoPTTLAsync]
            );

            static async Task DoPTTLAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("RENAME", ["foo", "bar"]);
                    Assert.Fail("Shouldn't succeed, key doesn't exist");
                }
                catch (Exception e)
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
        public async Task ReplicaOfACLs()
        {
            // Uses exceptions as control flow, since clustering is disabled in these tests

            await CheckCommandsAsync(
                "REPLICAOF",
                [DoReplicaOfAsync, DoReplicaOfNoOneAsync]
            );

            static async Task DoReplicaOfAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("REPLICAOF", ["127.0.0.1", "9999"]);
                    Assert.Fail("Should be unreachable, cluster is disabled");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static async Task DoReplicaOfNoOneAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("REPLICAOF", ["NO", "ONE"]);
                    Assert.Fail("Should be unreachable, cluster is disabled");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }
        }

        // todo: restore
        //[Test]
        //public async Task RunTxpACLs()
        //{
        //    // TODO: RUNTXP semantics are a bit unclear... expand test later

        //    // TODO: RUNTXP breaks the stream when command is malformed, rework this when that is fixed

        //    // Test denying just the command
        //    {
        //        {
        //            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

        //            IServer server = redis.GetServers().Single();
        //            IDatabase db = redis.GetDatabase();

        //            SetUser(server, "default", ["-runtxp"]);
        //            Assert.False(CheckAuthFailure(() => DoRunTxp(db)), "Permitted when should have been denied");
        //        }
        //    }

        //    string[] categories = ["transaction", "garnet"];

        //    foreach (string cat in categories)
        //    {
        //        // Spin up a temp admin
        //        {
        //            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

        //            IServer server = redis.GetServers().Single();
        //            IDatabase db = redis.GetDatabase();

        //            RedisResult setupAdmin = db.Execute("ACL", "SETUSER", "temp-admin", "on", ">foo", "+@all");
        //            Assert.AreEqual("OK", (string)setupAdmin);
        //        }

        //        // Permitted
        //        {
        //            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

        //            IServer server = redis.GetServers().Single();
        //            IDatabase db = redis.GetDatabase();

        //            Assert.True(CheckAuthFailure(() => DoRunTxp(db)), "Denied when should have been permitted");
        //        }

        //        // Denied
        //        {
        //            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "temp-admin", authPassword: "foo"));

        //            IServer server = redis.GetServers().Single();
        //            IDatabase db = redis.GetDatabase();

        //            SetUser(server, "temp-admin", $"-@{cat}");

        //            Assert.False(CheckAuthFailure(() => DoRunTxp(db)), "Permitted when should have been denied");
        //        }
        //    }

        //    static async Task DoRunTxp(IDatabase db)
        //    {
        //        try
        //        {
        //            db.Execute("RUNTXP", "4");

        //            Assert.Fail("Should be reachable, command is malformed");
        //        }
        //        catch (Exception e)
        //        {
        //            if (e.Message == "ERR Could not get transaction procedure")
        //            {
        //                return;
        //            }

        //            throw;
        //        }
        //    }
        //}

        [Test]
        public async Task SaveACLs()
        {
            await CheckCommandsAsync(
               "SAVE",
               [DoSaveAsync]
           );

            static async Task DoSaveAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SAVE");
                Assert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task ScanACLs()
        {
            await CheckCommandsAsync(
                "SCAN",
                [DoScanAsync, DoScanMatchAsync, DoScanCountAsync, DoScanTypeAsync, DoScanMatchCountAsync, DoScanMatchTypeAsync, DoScanCountTypeAsync, DoScanMatchCountTypeAsync]
            );

            static async Task DoScanAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SCAN", ["0"]);
                Assert.IsNotNull(val);
            }

            static async Task DoScanMatchAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SCAN", ["0", "MATCH", "*"]);
                Assert.IsNotNull(val);
            }

            static async Task DoScanCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SCAN", ["0", "COUNT", "5"]);
                Assert.IsNotNull(val);
            }

            static async Task DoScanTypeAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SCAN", ["0", "TYPE", "zset"]);
                Assert.IsNotNull(val);
            }

            static async Task DoScanMatchCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SCAN", ["0", "MATCH", "*", "COUNT", "5"]);
                Assert.IsNotNull(val);
            }

            static async Task DoScanMatchTypeAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SCAN", ["0", "MATCH", "*", "TYPE", "zset"]);
                Assert.IsNotNull(val);
            }

            static async Task DoScanCountTypeAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SCAN", ["0", "COUNT", "5", "TYPE", "zset"]);
                Assert.IsNotNull(val);
            }

            static async Task DoScanMatchCountTypeAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SCAN", ["0", "MATCH", "*", "COUNT", "5", "TYPE", "zset"]);
                Assert.IsNotNull(val);
            }
        }

        [Test]
        public async Task SecondaryOfACLs()
        {
            // Uses exceptions as control flow, since clustering is disabled in these tests

            await CheckCommandsAsync(
                "SECONDARYOF",
                [DoSecondaryOfAsync, DoSecondaryOfNoOneAsync]
            );

            static async Task DoSecondaryOfAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("SECONDARYOF", ["127.0.0.1", "9999"]);
                    Assert.Fail("Should be unreachable, cluster is disabled");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static async Task DoSecondaryOfNoOneAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("SECONDARYOF", ["NO", "ONE"]);
                    Assert.Fail("Should be unreachable, cluster is disabled");
                }
                catch (Exception e)
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
        public async Task SelectACLs()
        {
            await CheckCommandsAsync(
                "SELECT",
                [DoSelectAsync]
            );

            static async Task DoSelectAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SELECT", ["0"]);
                Assert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task SetACLs()
        {
            // SET doesn't support most extra commands, so this is just key value

            await CheckCommandsAsync(
                "SET",
                [DoSetAsync, DoSetExNxAsync, DoSetXxNxAsync, DoSetKeepTtlAsync, DoSetKeepTtlXxAsync]
            );

            static async Task DoSetAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SET", ["foo", "bar"]);
                Assert.AreEqual("OK", val);
            }

            static async Task DoSetExNxAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SET", ["foo", "bar", "NX", "EX", "100"]);
                Assert.IsNull(val);
            }

            static async Task DoSetXxNxAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SET", ["foo", "bar", "XX", "EX", "100"]);
                Assert.AreEqual("OK", val);
            }

            static async Task DoSetKeepTtlAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SET", ["foo", "bar", "KEEPTTL"]);
                Assert.AreEqual("OK", val);
            }

            static async Task DoSetKeepTtlXxAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SET", ["foo", "bar", "XX", "KEEPTTL"]);
                Assert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task SetBitACLs()
        {
            int count = 0;

            await CheckCommandsAsync(
                "SETBIT",
                [DoSetBitAsync]
            );

            async Task DoSetBitAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SETBIT", [$"foo-{count}", "10", "1"]);
                count++;
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SetEXACLs()
        {
            await CheckCommandsAsync(
                "SETEX",
                [DoSetEXAsync]
            );

            static async Task DoSetEXAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SETEX", ["foo", "10", "bar"]);
                Assert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task SetRangeACLs()
        {
            await CheckCommandsAsync(
                "SETRANGE",
                [DoSetRangeAsync]
            );

            static async Task DoSetRangeAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SETRANGE", ["foo", "10", "bar"]);
                Assert.AreEqual(13, val);
            }
        }

        [Test]
        public async Task StrLenACLs()
        {
            await CheckCommandsAsync(
                "STRLEN",
                [DoStrLenAsync]
            );

            static async Task DoStrLenAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("STRLEN", ["foo"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SAddACLs()
        {
            int count = 0;

            await CheckCommandsAsync(
                "SADD",
                [DoSAddAsync, DoSAddMultiAsync]
            );

            async Task DoSAddAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SADD", [$"foo-{count}", "bar"]);
                count++;

                Assert.AreEqual(1, val);
            }

            async Task DoSAddMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SADD", [$"foo-{count}", "bar", "fizz"]);
                count++;

                Assert.AreEqual(2, val);
            }
        }

        [Test]
        public async Task SRemACLs()
        {
            await CheckCommandsAsync(
                "SREM",
                [DoSRemAsync, DoSRemMultiAsync]
            );

            static async Task DoSRemAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SREM", ["foo", "bar"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoSRemMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SREM", ["foo", "bar", "fizz"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SPopACLs()
        {
            await CheckCommandsAsync(
                "SPOP",
                [DoSPopAsync, DoSPopCountAsync]
            );

            static async Task DoSPopAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SPOP", ["foo"]);
                Assert.IsNull(val);
            }

            static async Task DoSPopCountAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SPOP", ["foo", "11"]);
                Assert.IsNull(val);
            }
        }

        [Test]
        public async Task SMembersACLs()
        {
            await CheckCommandsAsync(
                "SMEMBERS",
                [DoSMembersAsync]
            );

            static async Task DoSMembersAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SMEMBERS", ["foo"]);
                Assert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task SCardACLs()
        {
            await CheckCommandsAsync(
                "SCARD",
                [DoSCardAsync]
            );

            static async Task DoSCardAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SCARD", ["foo"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SScanACLs()
        {
            await CheckCommandsAsync(
                "SSCAN",
                [DoSScanAsync, DoSScanMatchAsync, DoSScanCountAsync, DoSScanMatchCountAsync]
            );

            static async Task DoSScanAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SSCAN", ["foo", "0"]);
                Assert.IsNotNull(val);
            }

            static async Task DoSScanMatchAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SSCAN", ["foo", "0", "MATCH", "*"]);
                Assert.IsNotNull(val);
            }

            static async Task DoSScanCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SSCAN", ["foo", "0", "COUNT", "5"]);
                Assert.IsNotNull(val);
            }

            static async Task DoSScanMatchCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SSCAN", ["foo", "0", "MATCH", "*", "COUNT", "5"]);
                Assert.IsNotNull(val);
            }
        }

        [Test]
        public async Task SlaveOfACLs()
        {
            // Uses exceptions as control flow, since clustering is disabled in these tests

            await CheckCommandsAsync(
                "SLAVEOF",
                [DoSlaveOfAsync, DoSlaveOfNoOneAsync]
            );

            static async Task DoSlaveOfAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("SLAVEOF", ["127.0.0.1", "9999"]);
                    Assert.Fail("Should be unreachable, cluster is disabled");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR This instance has cluster support disabled")
                    {
                        return;
                    }

                    throw;
                }
            }

            static async Task DoSlaveOfNoOneAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("SLAVEOF", ["NO", "ONE"]);
                    Assert.Fail("Should be unreachable, cluster is disabled");
                }
                catch (Exception e)
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
        public async Task SMoveACLs()
        {
            await CheckCommandsAsync(
                "SMOVE",
                [DoSMoveAsync]
            );

            static async Task DoSMoveAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SMOVE", ["foo", "bar", "fizz"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SRandMemberACLs()
        {
            await CheckCommandsAsync(
                "SRANDMEMBER",
                [DoSRandMemberAsync, DoSRandMemberCountAsync]
            );

            static async Task DoSRandMemberAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SRANDMEMBER", ["foo"]);
                Assert.IsNull(val);
            }

            static async Task DoSRandMemberCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SRANDMEMBER", ["foo", "5"]);
                Assert.IsNotNull(val);
            }
        }

        [Test]
        public async Task SIsMemberACLs()
        {
            await CheckCommandsAsync(
                "SISMEMBER",
                [DoSIsMemberAsync]
            );

            static async Task DoSIsMemberAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SISMEMBER", ["foo", "bar"]);
                Assert.AreEqual(0, val);
            }
        }

        // todo: restore
        //[Test]
        //public void SubscribeACLs()
        //{
        //    // TODO: not testing the multiple channel version

        //    int count = 0;

        //    await CheckCommandsAsync(
        //        "SUBSCRIBE",
        //        [DoSubscribeAsync]
        //    );

        //    async Task DoSubscribeAsync(GarnetClient client)
        //    {
        //        ISubscriber sub = server.Multiplexer.GetSubscriber();

        //        // have to do the (bad) async version to make sure we actually get the error
        //        sub.SubscribeAsync(new RedisChannel($"channel-{count}", RedisChannel.PatternMode.Literal)).GetAwaiter().GetResult();
        //        count++;
        //    }
        //}

        [Test]
        public async Task SUnionACLs()
        {
            await CheckCommandsAsync(
                "SUNION",
                [DoSUnionAsync, DoSUnionMultiAsync]
            );

            static async Task DoSUnionAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SUNION", ["foo"]);
                Assert.AreEqual(0, val.Length);
            }

            static async Task DoSUnionMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SUNION", ["foo", "bar"]);
                Assert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task SUnionStoreACLs()
        {
            await CheckCommandsAsync(
                "SUNIONSTORE",
                [DoSUnionStoreAsync, DoSUnionStoreMultiAsync]
            );

            static async Task DoSUnionStoreAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SUNIONSTORE", ["dest", "foo"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoSUnionStoreMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SUNIONSTORE", ["dest", "foo", "bar"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SDiffACLs()
        {
            await CheckCommandsAsync(
                "SDIFF",
                [DoSDiffAsync, DoSDiffMultiAsync]
            );

            static async Task DoSDiffAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SDIFF", ["foo"]);
                Assert.AreEqual(0, val.Length);
            }

            static async Task DoSDiffMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SDIFF", ["foo", "bar"]);
                Assert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task SDiffStoreACLs()
        {
            await CheckCommandsAsync(
                "SDIFFSTORE",
                [DoSDiffStoreAsync, DoSDiffStoreMultiAsync]
            );

            static async Task DoSDiffStoreAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SDIFFSTORE", ["dest", "foo"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoSDiffStoreMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SDIFFSTORE", ["dest", "foo", "bar"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SInterACLs()
        {
            await CheckCommandsAsync(
                "SINTER",
                [DoSDiffAsync, DoSDiffMultiAsync]
            );

            static async Task DoSDiffAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SINTER", ["foo"]);
                Assert.AreEqual(0, val.Length);
            }

            static async Task DoSDiffMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SINTER", ["foo", "bar"]);
                Assert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task SInterStoreACLs()
        {
            await CheckCommandsAsync(
                "SINTERSTORE",
                [DoSDiffStoreAsync, DoSDiffStoreMultiAsync]
            );

            static async Task DoSDiffStoreAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SINTERSTORE", ["dest", "foo"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoSDiffStoreMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SINTERSTORE", ["dest", "foo", "bar"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task GeoAddACLs()
        {
            int count = 0;

            await CheckCommandsAsync(
                "GEOADD",
                [DoGeoAddAsync, DoGeoAddNXAsync, DoGeoAddNXCHAsync, DoGeoAddMultiAsync, DoGeoAddNXMultiAsync, DoGeoAddNXCHMultiAsync]
            );

            async Task DoGeoAddAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GEOADD", [$"foo-{count}", "90", "90", "bar"]);
                count++;

                Assert.AreEqual(1, val);
            }

            async Task DoGeoAddNXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GEOADD", [$"foo-{count}", "NX", "90", "90", "bar"]);
                count++;

                Assert.AreEqual(1, val);
            }

            async Task DoGeoAddNXCHAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GEOADD", [$"foo-{count}", "NX", "CH", "90", "90", "bar"]);
                count++;

                Assert.AreEqual(1, val);
            }

            async Task DoGeoAddMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GEOADD", [$"foo-{count}", "90", "90", "bar", "45", "45", "fizz"]);
                count++;

                Assert.AreEqual(2, val);
            }

            async Task DoGeoAddNXMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GEOADD", [$"foo-{count}", "NX", "90", "90", "bar", "45", "45", "fizz"]);
                count++;

                Assert.AreEqual(2, val);
            }

            async Task DoGeoAddNXCHMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GEOADD", [$"foo-{count}", "NX", "CH", "90", "90", "bar", "45", "45", "fizz"]);
                count++;

                Assert.AreEqual(2, val);
            }
        }

        [Test]
        public async Task GeoHashACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IDatabase db = redis.GetDatabase();

            // TODO: GEOHASH doesn't deal with empty keys appropriately - correct when that's fixed
            Assert.AreEqual(1, (int)db.Execute("GEOADD", "foo", "10", "10", "bar"));
            Assert.AreEqual(1, (int)db.Execute("GEOADD", "foo", "20", "20", "fizz"));

            await CheckCommandsAsync(
                "GEOHASH",
                [DoGeoHashAsync, DoGeoHashSingleAsync, DoGeoHashMultiAsync]
            );

            static async Task DoGeoHashAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("GEOHASH", ["foo"]);
                Assert.AreEqual(0, val.Length);
            }

            static async Task DoGeoHashSingleAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("GEOHASH", ["foo", "bar"]);
                Assert.AreEqual(1, val.Length);
                Assert.IsNull(val[0]);
            }

            static async Task DoGeoHashMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("GEOHASH", ["foo", "bar", "fizz"]);
                Assert.AreEqual(2, val.Length);
                Assert.IsNull(val[0]);
                Assert.IsNull(val[1]);
            }
        }

        [Test]
        public async Task GeoDistACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IDatabase db = redis.GetDatabase();

            // TODO: GEODIST fails on missing keys, which is incorrect, so putting values in to get ACL test passing
            Assert.AreEqual(1, (int)db.Execute("GEOADD", "foo", "10", "10", "bar"));
            Assert.AreEqual(1, (int)db.Execute("GEOADD", "foo", "20", "20", "fizz"));

            await CheckCommandsAsync(
                "GEODIST",
                [DoGetDistAsync, DoGetDistMAsync]
            );

            static async Task DoGetDistAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("GEODIST", ["foo", "bar", "fizz"]);
                Assert.IsTrue(double.Parse(val) > 0);
            }

            static async Task DoGetDistMAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("GEODIST", ["foo", "bar", "fizz", "M"]);
                Assert.IsTrue(double.Parse(val) > 0);
            }
        }

        [Test]
        public async Task GeoPosACLs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IDatabase db = redis.GetDatabase();

            // TODO: GEOPOS gets desynced if key doesn't exist, remove after that's fixed
            Assert.AreEqual(1, (int)db.Execute("GEOADD", "foo", "10", "10", "bar"));
            Assert.AreEqual(1, (int)db.Execute("GEOADD", "foo", "20", "20", "fizz"));

            await CheckCommandsAsync(
                "GEOPOS",
                [DoGeoPosAsync, DoGeoPosMultiAsync]
            );

            static async Task DoGeoPosAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("GEOPOS", ["foo"]);
                Assert.AreEqual(0, val.Length);
            }

            static async Task DoGeoPosMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("GEOPOS", ["foo", "bar"]);
                Assert.AreEqual(1, val.Length);
            }
        }

        // todo: restore
        //[Test]
        //public async Task GeoSearchACLs()
        //{
        //    const string TestUser = "geosearch-user";
        //    const string TestPassword = "bar";

        //    // TODO: there are a LOT of GeoSearch variants (not all of them implemented), come back and cover all the lengths appropriately

        //    // TODO: GEOSEARCH appears to be very broken, so this structured oddly - can be simplified once fixed

        //    string[] categories = ["geo", "read", "slow"];

        //    foreach (string category in categories)
        //    {
        //        // fresh Garnet for the allow version
        //        TearDown();
        //        Setup();

        //        {
        //            // fresh connection, as GEOSEARCH seems to break connections pretty easily
        //            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

        //            IServer server = redis.GetServers().Single();

        //            Assert.True(CheckAuthFailure(() => DoGeoSearchAsync(server)), "Denied when should have been permitted");
        //        }

        //        // fresh Garnet for the reject version
        //        TearDown();
        //        Setup();

        //        {
        //            // fresh connection, as GEOSEARCH seems to break connections pretty easily
        //            using ConnectionMultiplexer defaultRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));
        //            IServer defaultServer = defaultRedis.GetServers().Single();

        //            InitUser(defaultServer, TestUser, TestPassword);
        //            SetUser(defaultServer, TestUser, $"-@{category}");

        //            using ConnectionMultiplexer testRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: TestUser, authPassword: TestPassword));
        //            IServer testServer = testRedis.GetServers().Single();

        //            Assert.False(CheckAuthFailure(() => DoGeoSearchAsync(testServer)), "Permitted when should have been denied");
        //        }

        //        static async Task DoGeoSearchAsync(GarnetClient client)
        //        {
        //            string[] val = await client.ExecuteForStringArrayResultAsync("GEOSEARCH", ["foo", "FROMMEMBER", "bar", "BYBOX", "2", "2", "M"]);
        //            Assert.IsNotNull(val);
        //        }
        //    }
        //}

        [Test]
        public async Task ZAddACLs()
        {
            // TODO: ZADD doesn't implement NX XX GT LT CH INCR; expand to cover all lengths when implemented

            int count = 0;

            await CheckCommandsAsync(
                "ZADD",
                [DoZAddAsync, DoZAddMultiAsync]
            );

            async Task DoZAddAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZADD", [$"foo-{count}", "10", "bar"]);
                count++;
                Assert.AreEqual(1, val);
            }

            async Task DoZAddMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZADD", [$"foo-{count}", "10", "bar", "20", "fizz"]);
                count++;
                Assert.AreEqual(2, val);
            }
        }

        [Test]
        public async Task ZCardACLs()
        {
            await CheckCommandsAsync(
                "ZCARD",
                [DoZCardAsync]
            );

            static async Task DoZCardAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZCARD", ["foo"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZPopMaxACLs()
        {
            await CheckCommandsAsync(
                "ZPOPMAX",
                [DoZPopMaxAsync, DoZPopMaxCountAsync]
            );

            static async Task DoZPopMaxAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZPOPMAX", ["foo"]);
                Assert.AreEqual(0, val.Length);
            }

            static async Task DoZPopMaxCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZPOPMAX", ["foo", "10"]);
                Assert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZScoreACLs()
        {
            await CheckCommandsAsync(
                "ZSCORE",
                [DoZScoreAsync]
            );

            static async Task DoZScoreAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ZSCORE", ["foo", "bar"]);
                Assert.IsNull(val);
            }
        }

        [Test]
        public async Task ZRemACLs()
        {
            await CheckCommandsAsync(
                "ZREM",
                [DoZRemAsync, DoZRemMultiAsync]
            );

            static async Task DoZRemAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZREM", ["foo", "bar"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoZRemMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZREM", ["foo", "bar", "fizz"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZCountACLs()
        {
            await CheckCommandsAsync(
                "ZCOUNT",
                [DoZCountAsync]
            );

            static async Task DoZCountAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZCOUNT", ["foo", "10", "20"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZIncrByACLs()
        {
            int count = 0;

            await CheckCommandsAsync(
                "ZINCRBY",
                [DoZIncrByAsync]
            );

            async Task DoZIncrByAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ZINCRBY", [$"foo-{count}", "10", "bar"]);
                count++;
                Assert.AreEqual(10, double.Parse(val));
            }
        }

        [Test]
        public async Task ZRankACLs()
        {
            // TODO: ZRANK doesn't implement WITHSCORE (removed code that half did) - come back and add when fixed

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IDatabase db = redis.GetDatabase();

            // TODO: ZRANK errors if key does not exist, which is incorrect - creating key for this test
            Assert.AreEqual(1, (int)db.Execute("ZADD", "foo", "10", "bar"));

            await CheckCommandsAsync(
                "ZRANK",
                [DoZRankAsync]
            );

            static async Task DoZRankAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZRANK", ["foo", "bar"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZRangeACLs()
        {
            // TODO: ZRange has loads of options, come back and test all the different lengths

            await CheckCommandsAsync(
                "ZRANGE",
                [DoZRangeAsync]
            );

            static async Task DoZRangeAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANGE", ["key", "10", "20"]);
                Assert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZRangeByScoreACLs()
        {
            await CheckCommandsAsync(
                "ZRANGEBYSCORE",
                [DoZRangeByScoreAsync, DoZRangeByScoreWithScoresAsync, DoZRangeByScoreLimitAsync, DoZRangeByScoreWithScoresLimitAsync]
            );

            static async Task DoZRangeByScoreAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANGEBYSCORE", ["key", "10", "20"]);
                Assert.AreEqual(0, val.Length);
            }

            static async Task DoZRangeByScoreWithScoresAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANGEBYSCORE", ["key", "10", "20", "WITHSCORES"]);
                Assert.AreEqual(0, val.Length);
            }

            static async Task DoZRangeByScoreLimitAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANGEBYSCORE", ["key", "10", "20", "LIMIT", "2", "3"]);
                Assert.AreEqual(0, val.Length);
            }

            static async Task DoZRangeByScoreWithScoresLimitAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANGEBYSCORE", ["key", "10", "20", "WITHSCORES", "LIMIT", "2", "3"]);
                Assert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZRevRangeACLs()
        {
            await CheckCommandsAsync(
                "ZREVRANGE",
                [DoZRevRangeAsync, DoZRevRangeWithScoresAsync]
            );

            static async Task DoZRevRangeAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZREVRANGE", ["key", "10", "20"]);
                Assert.AreEqual(0, val.Length);
            }

            static async Task DoZRevRangeWithScoresAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZREVRANGE", ["key", "10", "20", "WITHSCORES"]);
                Assert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZRevRankACLs()
        {
            // TODO: ZREVRANK doesn't implement WITHSCORE (removed code that half did) - come back and add when fixed

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true, authUsername: "default", authPassword: DefaultPassword));

            IDatabase db = redis.GetDatabase();

            // TODO: ZREVRANK errors if key does not exist, which is incorrect - creating key for this test
            Assert.AreEqual(1, (int)db.Execute("ZADD", "foo", "10", "bar"));

            await CheckCommandsAsync(
                "ZREVRANK",
                [DoZRevRankAsync]
            );

            static async Task DoZRevRankAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZREVRANK", ["foo", "bar"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZRemRangeByLexACLs()
        {
            await CheckCommandsAsync(
                "ZREMRANGEBYLEX",
                [DoZRemRangeByLexAsync]
            );

            static async Task DoZRemRangeByLexAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZREMRANGEBYLEX", ["foo", "abc", "def"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZRemRangeByRankACLs()
        {
            await CheckCommandsAsync(
                "ZREMRANGEBYRANK",
                [DoZRemRangeByRankAsync]
            );

            static async Task DoZRemRangeByRankAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZREMRANGEBYRANK", ["foo", "10", "20"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZRemRangeByScoreACLs()
        {
            await CheckCommandsAsync(
                "ZREMRANGEBYSCORE",
                [DoZRemRangeByRankAsync]
            );

            static async Task DoZRemRangeByRankAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZREMRANGEBYSCORE", ["foo", "10", "20"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZLexCountACLs()
        {
            await CheckCommandsAsync(
                "ZLEXCOUNT",
                [DoZLexCountAsync]
            );

            static async Task DoZLexCountAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZLEXCOUNT", ["foo", "abc", "def"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZPopMinACLs()
        {
            await CheckCommandsAsync(
                "ZPOPMIN",
                [DoZPopMinAsync, DoZPopMinCountAsync]
            );

            static async Task DoZPopMinAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZPOPMIN", ["foo"]);
                Assert.AreEqual(0, val.Length);
            }

            static async Task DoZPopMinCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZPOPMIN", ["foo", "10"]);
                Assert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZRandMemberACLs()
        {
            await CheckCommandsAsync(
                "ZRANDMEMBER",
                [DoZRandMemberAsync, DoZRandMemberCountAsync, DoZRandMemberCountWithScoresAsync]
            );

            static async Task DoZRandMemberAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ZRANDMEMBER", ["foo"]);
                Assert.IsNull(val);
            }

            static async Task DoZRandMemberCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANDMEMBER", ["foo", "10"]);
                Assert.AreEqual(0, val.Length);
            }

            static async Task DoZRandMemberCountWithScoresAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANDMEMBER", ["foo", "10", "WITHSCORES"]);
                Assert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZDiffACLs()
        {
            // TODO: ZDIFF doesn't implement WITHSCORES correctly right now - come back and cover when fixed

            await CheckCommandsAsync(
                "ZDIFF",
                [DoZDiffAsync, DoZDiffMultiAsync]
            );

            static async Task DoZDiffAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZDIFF", ["1", "foo"]);
                Assert.AreEqual(0, val.Length);
            }

            static async Task DoZDiffMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZDIFF", ["2", "foo", "bar"]);
                Assert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZScanACLs()
        {
            await CheckCommandsAsync(
                "ZSCAN",
                [DoZScanAsync, DoZScanMatchAsync, DoZScanCountAsync, DoZScanNoValuesAsync, DoZScanMatchCountAsync, DoZScanMatchNoValuesAsync, DoZScanCountNoValuesAsync, DoZScanMatchCountNoValuesAsync]
            );

            static async Task DoZScanAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZSCAN", ["foo", "0"]);
                Assert.AreEqual(2, val.Length);
            }

            static async Task DoZScanMatchAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZSCAN", ["foo", "0", "MATCH", "*"]);
                Assert.AreEqual(2, val.Length);
            }

            static async Task DoZScanCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZSCAN", ["foo", "0", "COUNT", "2"]);
                Assert.AreEqual(2, val.Length);
            }

            static async Task DoZScanNoValuesAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZSCAN", ["foo", "0", "NOVALUES"]);
                Assert.AreEqual(2, val.Length);
            }

            static async Task DoZScanMatchCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZSCAN", ["foo", "0", "MATCH", "*", "COUNT", "2"]);
                Assert.AreEqual(2, val.Length);
            }

            static async Task DoZScanMatchNoValuesAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZSCAN", ["foo", "0", "MATCH", "*", "NOVALUES"]);
                Assert.AreEqual(2, val.Length);
            }

            static async Task DoZScanCountNoValuesAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZSCAN", ["foo", "0", "COUNT", "0", "NOVALUES"]);
                Assert.AreEqual(2, val.Length);
            }

            static async Task DoZScanMatchCountNoValuesAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZSCAN", ["foo", "0", "MATCH", "*", "COUNT", "0", "NOVALUES"]);
                Assert.AreEqual(2, val.Length);
            }
        }

        [Test]
        public async Task ZMScoreACLs()
        {
            await CheckCommandsAsync(
                "ZMSCORE",
                [DoZDiffAsync, DoZDiffMultiAsync]
            );

            static async Task DoZDiffAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZMSCORE", ["foo", "bar"]);
                Assert.AreEqual(1, val.Length);
                Assert.IsNull(val[0]);
            }

            static async Task DoZDiffMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZMSCORE", ["foo", "bar", "fizz"]);
                Assert.AreEqual(2, val.Length);
                Assert.IsNull(val[0]);
                Assert.IsNull(val[1]);
            }
        }

        [Test]
        public async Task TimeACLs()
        {
            await CheckCommandsAsync(
                "TIME",
                [DoTimeAsync]
            );

            static async Task DoTimeAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("TIME");
                Assert.AreEqual(2, val.Length);
                Assert.IsTrue(long.Parse(val[0]) > 0);
                Assert.IsTrue(long.Parse(val[1]) >= 0);
            }
        }

        [Test]
        public async Task TTLACLs()
        {
            await CheckCommandsAsync(
                "TTL",
                [DoTTLAsync]
            );

            static async Task DoTTLAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("TTL", ["foo"]);
                Assert.AreEqual(-2, val);
            }
        }

        [Test]
        public async Task TypeACLs()
        {
            await CheckCommandsAsync(
                "TYPE",
                [DoTypeAsync]
            );

            static async Task DoTypeAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("TYPE", ["foo"]);
                Assert.AreEqual("none", val);
            }
        }

        [Test]
        public async Task UnlinkACLs()
        {
            await CheckCommandsAsync(
                "UNLINK",
                [DoUnlinkAsync, DoUnlinkMultiAsync]
            );

            static async Task DoUnlinkAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("UNLINK", ["foo"]);
                Assert.AreEqual(0, val);
            }

            static async Task DoUnlinkMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("UNLINK", ["foo", "bar"]);
                Assert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task UnsubscribeACLs()
        {
            await CheckCommandsAsync(
                "UNSUBSCRIBE",
                [DoUnsubscribePatternAsync]
            );

            static async Task DoUnsubscribePatternAsync(GarnetClient client)
            {
                string[] res = await client.ExecuteForStringArrayResultAsync("UNSUBSCRIBE", ["foo"]);
                Assert.IsNotNull(res);
            }
        }

        [Test]
        public async Task WatchACLs()
        {
            // TODO: should watch fail outside of a transaction?
            // TODO: multi key WATCH isn't implemented correctly, add once fixed

            await CheckCommandsAsync(
                "WATCH",
                [DoWatchAsync]
            );

            static async Task DoWatchAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("WATCH", ["foo"]);
                Assert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task WatchMSACLs()
        {
            // TODO: should watch fail outside of a transaction?

            await CheckCommandsAsync(
                "WATCH MS",
                [DoWatchMSAsync]
            );

            static async Task DoWatchMSAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("WATCH", ["MS", "foo"]);
                Assert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task WatchOSACLs()
        {
            // TODO: should watch fail outside of a transaction?

            await CheckCommandsAsync(
                "WATCH OS",
                [DoWatchOSAsync]
            );

            static async Task DoWatchOSAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("WATCH", ["OS", "foo"]);
                Assert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task UnwatchACLs()
        {
            // TODO: should watch fail outside of a transaction?

            await CheckCommandsAsync(
                "UNWATCH",
                [DoUnwatchAsync]
            );

            static async Task DoUnwatchAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("UNWATCH");
                Assert.AreEqual("OK", val);
            }
        }

        /// <summary>
        /// Take a command (or subcommand, with a space) and check that adding and removing
        /// command, subcommand, and categories ACLs behaves as expected.
        /// </summary>
        private static async Task CheckCommandsAsync(
            string command,
            Func<GarnetClient, Task>[] commands,
            List<string> knownCategories = null,
            bool skipPing = false
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
            using (GarnetClient defaultUserClient = await CreateGarnetClientAsync(DefaultUser, DefaultPassword))
            {
                // Spin up test users, with all permissions so we can spin up connections without issue
                await InitUserAsync(defaultUserClient, UserWithAll, TestPassword);
                await InitUserAsync(defaultUserClient, UserWithNone, TestPassword);

                // Spin up two connections for users that we'll use as starting points for different ACL changes
                using (GarnetClient allUserClient = await CreateGarnetClientAsync(UserWithAll, TestPassword))
                using (GarnetClient noneUserClient = await CreateGarnetClientAsync(UserWithNone, TestPassword))
                {
                    // Check categories
                    foreach (string category in categories)
                    {
                        // Check removing category works
                        {
                            await ResetUserWithAllAsync(defaultUserClient);

                            await AssertAllPermittedAsync(defaultUserClient, UserWithAll, allUserClient, commands, $"[{command}]: Denied when should have been permitted (user had +@all)", skipPing);

                            await SetUserAsync(defaultUserClient, UserWithAll, [$"-@{category}"]);

                            await AssertAllDeniedAsync(defaultUserClient, UserWithAll, allUserClient, commands, $"[{command}]: Permitted when should have been denied (user had -@{category})", skipPing);
                        }

                        // Check adding category works
                        {
                            await ResetUserWithNoneAsync(defaultUserClient);

                            await AssertAllDeniedAsync(defaultUserClient, UserWithNone, noneUserClient, commands, $"[{command}]: Permitted when should have been denied (user had -@all)", skipPing);

                            await SetACLOnUserAsync(defaultUserClient, UserWithNone, [$"+@{category}"]);

                            await AssertAllPermittedAsync(defaultUserClient, UserWithNone, noneUserClient, commands, $"[{command}]: Denied when should have been permitted (user had +@{category})", skipPing);
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
                            await ResetUserWithAllAsync(defaultUserClient);

                            await SetACLOnUserAsync(defaultUserClient, UserWithAll, [$"-{commandAcl}"]);

                            await AssertAllDeniedAsync(defaultUserClient, UserWithAll, allUserClient, commands, $"[{command}]: Permitted when should have been denied (user had -{commandAcl})", skipPing);
                        }

                        // Check adding command works
                        {
                            await ResetUserWithNoneAsync(defaultUserClient);

                            await SetACLOnUserAsync(defaultUserClient, UserWithNone, [$"+{commandAcl}"]);

                            await AssertAllPermittedAsync(defaultUserClient, UserWithNone, noneUserClient, commands, $"[{command}]: Denied when should have been permitted (user had +{commandAcl})", skipPing);
                        }
                    }

                    // Check sub-command (if it is one)
                    if (command.Contains(" "))
                    {
                        string commandAcl = command[..command.IndexOf(' ')].ToLowerInvariant();
                        string subCommandAcl = command.Replace(" ", "|").ToLowerInvariant();

                        // Check removing subcommand works
                        {
                            await ResetUserWithAllAsync(defaultUserClient);

                            await SetACLOnUserAsync(defaultUserClient, UserWithAll, [$"-{subCommandAcl}"]);

                            await AssertAllDeniedAsync(defaultUserClient, UserWithAll, allUserClient, commands, $"[{command}]: Permitted when should have been denied (user had -{subCommandAcl})", skipPing);
                        }

                        // Check adding subcommand works
                        {
                            await ResetUserWithNoneAsync(defaultUserClient);

                            await SetACLOnUserAsync(defaultUserClient, UserWithNone, [$"+{subCommandAcl}"]);

                            await AssertAllPermittedAsync(defaultUserClient, UserWithNone, noneUserClient, commands, $"[{command}]: Denied when should have been permitted (user had +{subCommandAcl})", skipPing);
                        }

                        // Checking adding command but removing subcommand works
                        {
                            await ResetUserWithNoneAsync(defaultUserClient);

                            await SetACLOnUserAsync(defaultUserClient, UserWithNone, [$"+{commandAcl}", $"-{subCommandAcl}"]);

                            await AssertAllDeniedAsync(defaultUserClient, UserWithNone, noneUserClient, commands, $"[{command}]: Permitted when should have been denied (user had +{commandAcl} -{subCommandAcl})", skipPing);
                        }

                        // Checking removing command but adding subcommand works
                        {
                            await ResetUserWithAllAsync(defaultUserClient);

                            await SetACLOnUserAsync(defaultUserClient, UserWithAll, [$"-{commandAcl}", $"+{subCommandAcl}"]);

                            await AssertAllPermittedAsync(defaultUserClient, UserWithAll, allUserClient, commands, $"[{command}]: Denied when should have been permitted (user had -{commandAcl} +{subCommandAcl})", skipPing);
                        }
                    }
                }
            }

            // create a GarnetClient authed as the given user
            static async Task<GarnetClient> CreateGarnetClientAsync(string username, string password)
            {
                GarnetClient ret = TestUtils.GetGarnetClient();
                await ret.ConnectAsync();

                string authRes = await ret.ExecuteForStringResultAsync("AUTH", [username, password]);
                Assert.AreEqual("OK", authRes);

                return ret;
            }

            // Use default user to update ACL on given user
            static async Task SetACLOnUserAsync(GarnetClient defaultUserClient, string user, string[] aclPatterns)
            {
                string aclRes = await defaultUserClient.ExecuteForStringResultAsync("ACL", ["SETUSER", user, .. aclPatterns]);
                Assert.AreEqual("OK", aclRes);
            }

            static async Task ResetUserWithAllAsync(GarnetClient defaultUserClient)
            {
                // Create or reset user, with all permissions
                string aclRes = await defaultUserClient.ExecuteForStringResultAsync("ACL", ["SETUSER", UserWithAll, "on", $">{TestPassword}", "+@all"]);
                Assert.AreEqual("OK", aclRes);
            }

            // Get user that was initialized with -@all
            static async Task ResetUserWithNoneAsync(GarnetClient defaultUserClient)
            {
                // Create or reset user, with no permissions
                string aclRes = await defaultUserClient.ExecuteForStringResultAsync("ACL", ["SETUSER", UserWithNone, "on", $">{TestPassword}", "-@all"]);
                Assert.AreEqual("OK", aclRes);
            }

            // Check that all commands succeed
            static async Task AssertAllPermittedAsync(GarnetClient defaultUserClient, string currentUserName, GarnetClient currentUserClient, Func<GarnetClient, Task>[] commands, string message, bool skipPing)
            {
                foreach (Func<GarnetClient, Task> cmd in commands)
                {
                    Assert.True(await CheckAuthFailureAsync(() => cmd(currentUserClient)), message);
                }

                if (!skipPing)
                {
                    // Check we haven't desynced
                    await PingAsync(defaultUserClient, currentUserName, currentUserClient);
                }
            }

            // Check that all commands fail with NOAUTH
            static async Task AssertAllDeniedAsync(GarnetClient defaultUserClient, string currentUserName, GarnetClient currentUserClient, Func<GarnetClient, Task>[] commands, string message, bool skipPing)
            {
                foreach (Func<GarnetClient, Task> cmd in commands)
                {
                    Assert.False(await CheckAuthFailureAsync(() => cmd(currentUserClient)), message);
                }

                if (!skipPing)
                {
                    // Check we haven't desynced
                    await PingAsync(defaultUserClient, currentUserName, currentUserClient);
                }
            }

            // Enable PING on user and issue PING on connection
            static async Task PingAsync(GarnetClient defaultUserClient, string currentUserName, GarnetClient currentUserClient)
            {
                // Have to add PING because it'll be denied by reset of test in many cases
                // since we do this towards the end of our asserts, it shouldn't invalidate
                // the rest of the test.
                string addPingRes = await defaultUserClient.ExecuteForStringResultAsync("ACL", ["SETUSER", currentUserName, "on", "+ping"]);
                Assert.AreEqual("OK", addPingRes);

                // Actually execute the PING
                string pingRes = await currentUserClient.ExecuteForStringResultAsync("PING");
                Assert.AreEqual("PONG", pingRes);
            }
        }

        /// <summary>
        /// Create a user with +@all permissions.
        /// </summary>
        private static async Task InitUserAsync(GarnetClient defaultUserClient, string username, string password)
        {
            string res = await defaultUserClient.ExecuteForStringResultAsync("ACL", ["SETUSER", username, "on", $">{password}", "+@all"]);
            Assert.AreEqual("OK", res);
        }

        /// <summary>
        /// Runs ACL SETUSER default [aclPatterns] and checks that they are reflected in ACL LIST.
        /// </summary>
        private static async Task SetUserAsync(GarnetClient client, string user, params string[] aclPatterns)
        {
            string aclLinePreSet = await GetUserAsync(client, user);

            string setRes = await client.ExecuteForStringResultAsync("ACL", ["SETUSER", user, .. aclPatterns]);
            Assert.AreEqual("OK", setRes, $"Updating user ({user}) failed");

            string aclLinePostSet = await GetUserAsync(client, user);

            string expectedAclLine = $"{aclLinePreSet} {string.Join(" ", aclPatterns)}";

            CommandPermissionSet actualUserPerms = ACLParser.ParseACLRule(aclLinePostSet).CopyCommandPermissionSet();
            CommandPermissionSet expectedUserPerms = ACLParser.ParseACLRule(expectedAclLine).CopyCommandPermissionSet();

            Assert.IsTrue(expectedUserPerms.IsEquivalentTo(actualUserPerms), $"User permissions were not equivalent after running SETUSER with {string.Join(" ", aclPatterns)}");

            // TODO: if and when ACL GETUSER is implemented, just use that
            static async Task<string> GetUserAsync(GarnetClient client, string user)
            {
                string ret = null;
                string[] resArr = await client.ExecuteForStringArrayResultAsync("ACL", ["LIST"]);
                foreach (string res in resArr)
                {
                    ret = res;
                    if (ret.StartsWith($"user {user} on "))
                    {
                        break;
                    }
                }

                Assert.IsNotNull(ret, $"Couldn't get user from ACL LIST");

                return ret;
            }
        }

        /// <summary>
        /// Returns true if no AUTH failure.
        /// Returns false AUTH failure.
        /// 
        /// Throws if anything else.
        /// </summary>
        private static async Task<bool> CheckAuthFailureAsync(Func<Task> act)
        {
            try
            {
                await act();
                return true;
            }
            catch (Exception e)
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