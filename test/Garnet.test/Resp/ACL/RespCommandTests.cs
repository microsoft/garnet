// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.server;
using Garnet.server.ACL;
using NUnit.Framework;
using NUnit.Framework.Legacy;

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
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, defaultPassword: DefaultPassword,
                                                  useAcl: true, enableLua: true,
                                                  enableModuleCommand: Garnet.server.Auth.Settings.ConnectionProtectionOption.Yes);

            // Register custom commands so we can test ACL'ing them
            ClassicAssert.IsTrue(TestUtils.TryGetCustomCommandsInfo(out respCustomCommandsInfo));
            ClassicAssert.IsNotNull(respCustomCommandsInfo);

            server.Register.NewCommand("SETWPIFPGT", CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand(), respCustomCommandsInfo["SETWPIFPGT"]);
            server.Register.NewCommand("MYDICTGET", CommandType.Read, new MyDictFactory(), new MyDictGet(), respCustomCommandsInfo["MYDICTGET"]);
            server.Register.NewTransactionProc("READWRITETX", () => new ReadWriteTxn(), new RespCommandsInfo { Arity = 4 });
            server.Register.NewProcedure("SUM", () => new Sum());

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

                ClassicAssert.IsTrue(test.Name.EndsWith("ACLs") || test.Name.EndsWith("ACLsAsync"), $"Expected all tests in {nameof(RespCommandTests)} except {nameof(AllCommandsCovered)} to be per-command and end with ACLs, unexpected test: {test.Name}");

                string command;
                if (test.Name.EndsWith("ACLs"))
                {
                    command = test.Name[..^"ALCs".Length];
                }
                else
                {
                    command = test.Name[..^"ACLsAsync".Length];
                }

                covered.Add(command);
            }

            ClassicAssert.IsTrue(RespCommandsInfo.TryGetRespCommandsInfo(out IReadOnlyDictionary<string, RespCommandsInfo> allInfo), "Couldn't load all command details");
            ClassicAssert.IsTrue(RespCommandsInfo.TryGetRespCommandNames(out IReadOnlySet<string> advertisedCommands), "Couldn't get advertised RESP commands");

            // TODO: See if these commands could be identified programmatically
            IEnumerable<string> withOnlySubCommands = ["ACL", "CLIENT", "CLUSTER", "CONFIG", "LATENCY", "MEMORY", "MODULE", "PUBSUB", "SCRIPT", "SLOWLOG"];
            IEnumerable<string> notCoveredByACLs = allInfo.Where(static x => x.Value.Flags.HasFlag(RespCommandFlags.NoAuth)).Select(static kv => kv.Key);
            IEnumerable<string> metaCommands = allInfo.Where(static x => (x.Value.AclCategories & RespAclCategories.Meta) == RespAclCategories.Meta)
                .Select(static x => x.Key);

            // Check tests against RespCommandsInfo
            {
                // Exclude things like ACL, CLIENT, CLUSTER which are "commands" but only their sub commands can be run
                IEnumerable<string> subCommands = allInfo.Where(static x => x.Value.SubCommands != null).SelectMany(static x => x.Value.SubCommands).Select(static x => x.Name);
                IEnumerable<string> deSubCommanded = advertisedCommands.Except(withOnlySubCommands).Except(metaCommands).Union(subCommands).Select(static x => x.Replace("|", "").Replace("_", "").Replace("-", ""));
                IEnumerable<string> notCovered = deSubCommanded.Except(covered, StringComparer.OrdinalIgnoreCase).Except(notCoveredByACLs, StringComparer.OrdinalIgnoreCase);

                ClassicAssert.IsEmpty(notCovered, $"Commands in RespCommandsInfo not covered by ACL Tests:{Environment.NewLine}{string.Join(Environment.NewLine, notCovered.OrderBy(static x => x))}");
            }

            // Check tests against RespCommand
            {
                IEnumerable<RespCommand> allValues = Enum.GetValues<RespCommand>().Select(static x => x.NormalizeForACLs()).Distinct();
                IEnumerable<RespCommand> testableValues =
                    allValues
                    .Except([RespCommand.NONE, RespCommand.INVALID, RespCommand.DELIFEXPIM])
                    .Where(cmd => !withOnlySubCommands.Contains(cmd.ToString().Replace("_", ""), StringComparer.OrdinalIgnoreCase))
                    .Where(cmd => !notCoveredByACLs.Contains(cmd.ToString().Replace("_", ""), StringComparer.OrdinalIgnoreCase))
                    .Where(cmd => !metaCommands.Contains(cmd.ToString(), StringComparer.OrdinalIgnoreCase));
                IEnumerable<RespCommand> notCovered = testableValues.Where(cmd => !covered.Contains(cmd.ToString().Replace("_", ""), StringComparer.OrdinalIgnoreCase));

                ClassicAssert.IsEmpty(notCovered, $"Commands in RespCommand not covered by ACL Tests:{Environment.NewLine}{string.Join(Environment.NewLine, notCovered.OrderBy(static x => x))}");
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
                ClassicAssert.IsNotNull(res);
            }
        }

        [Test]
        public async Task AclDelUserACLsAsync()
        {
            await CheckCommandsAsync(
                "ACL DELUSER",
                [DoAclDelUserAsync, DoAclDelUserMultiAsync]
            );

            static async Task DoAclDelUserAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ACL", ["DELUSER", "does-not-exist"]);
                ClassicAssert.AreEqual(0, val);
            }

            async Task DoAclDelUserMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ACL", ["DELUSER", "does-not-exist-1", "does-not-exist-2"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task AclGenPassACLsAsync()
        {
            await CheckCommandsAsync(
                "ACL GENPASS",
                [DoAclGenPassAsync]
            );

            static async Task DoAclGenPassAsync(GarnetClient client)
            {
                var result = await client.ExecuteForStringResultAsync("ACL", ["GENPASS"]);
                ClassicAssert.AreEqual(64, result.Length);
            }
        }

        [Test]
        public async Task AclGetUserACLsAsync()
        {
            await CheckCommandsAsync(
                "ACL GETUSER",
                [DoAclGetUserAsync],
                skipPermitted: true
            );

            static async Task DoAclGetUserAsync(GarnetClient client)
            {
                // ACL GETUSER returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("ACL", ["GETUSER", "default"]);
            }
        }

        [Test]
        public async Task AclListACLsAsync()
        {
            await CheckCommandsAsync(
                "ACL LIST",
                [DoAclListAsync]
            );

            static async Task DoAclListAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ACL", ["LIST"]);
                ClassicAssert.IsNotNull(val);
            }
        }

        [Test]
        public async Task AclLoadACLsAsync()
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
                    if (e.Message != "ERR This Garnet instance is not configured to use an ACL file. Please restart server with --acl-file option.")
                    {
                        throw;
                    }
                }
            }
        }

        [Test]
        public async Task AclSaveACLsAsync()
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
                    if (e.Message != "ERR This Garnet instance is not configured to use an ACL file. Please restart server with --acl-file option.")
                    {
                        throw;
                    }
                }
            }
        }

        [Test]
        public async Task AclSetUserACLsAsync()
        {
            await CheckCommandsAsync(
                "ACL SETUSER",
                [DoAclSetUserOnAsync, DoAclSetUserCategoryAsync, DoAclSetUserOnCategoryAsync]
            );

            static async Task DoAclSetUserOnAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("ACL", ["SETUSER", "foo", "on"]);
                ClassicAssert.AreEqual("OK", res);
            }

            static async Task DoAclSetUserCategoryAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("ACL", ["SETUSER", "foo", "+@read"]);
                ClassicAssert.AreEqual("OK", res);
            }

            static async Task DoAclSetUserOnCategoryAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("ACL", ["SETUSER", "foo", "on", "+@read"]);
                ClassicAssert.AreEqual("OK", res);
            }
        }

        [Test]
        public async Task AclUsersACLsAsync()
        {
            await CheckCommandsAsync(
                "ACL USERS",
                [DoAclUsersAsync]
            );

            static async Task DoAclUsersAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ACL", ["USERS"]);
                ClassicAssert.IsNotNull(val);
            }
        }

        [Test]
        public async Task AclWhoAmIACLsAsync()
        {
            await CheckCommandsAsync(
                "ACL WHOAMI",
                [DoAclWhoAmIAsync]
            );

            static async Task DoAclWhoAmIAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ACL", ["WHOAMI"]);
                ClassicAssert.AreNotEqual("", (string)val);
            }
        }

        [Test]
        public async Task ExpDelScanACLsAsync()
        {
            await CheckCommandsAsync(
                "EXPDELSCAN",
                [DoExpDelScanAsync]
            );

            static async Task DoExpDelScanAsync(GarnetClient client)
            {
                await client.ExecuteForStringResultAsync("EXPDELSCAN");
            }
        }

        [Test]
        public async Task AppendACLsAsync()
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

                ClassicAssert.AreEqual(3, (int)val);
            }
        }

        [Test]
        public async Task AskingACLsAsync()
        {
            await CheckCommandsAsync(
                "ASKING",
                [DoAskingAsync]
            );

            async Task DoAskingAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ASKING");
                ClassicAssert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public async Task BGSaveACLsAsync()
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

                    ClassicAssert.IsTrue("Background saving started" == res || "Background saving scheduled" == res);
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

                    ClassicAssert.IsTrue("Background saving started" == res || "Background saving scheduled" == res);
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
        public async Task BitcountACLsAsync()
        {
            await CheckCommandsAsync(
                "BITCOUNT",
                [DoBitCountAsync, DoBitCountStartEndAsync, DoBitCountStartEndBitAsync, DoBitCountStartEndByteAsync]
            );

            static async Task DoBitCountAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITCOUNT", ["empty-key"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoBitCountStartEndAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITCOUNT", ["empty-key", "1", "1"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoBitCountStartEndByteAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITCOUNT", ["empty-key", "1", "1", "BYTE"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoBitCountStartEndBitAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITCOUNT", ["empty-key", "1", "1", "BIT"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task BitfieldACLsAsync()
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
                ClassicAssert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldGetWrapAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "GET", "u4", "0", "OVERFLOW", "WRAP"]);
                count++;
                ClassicAssert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldGetSatAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "GET", "u4", "0", "OVERFLOW", "SAT"]);
                count++;
                ClassicAssert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldGetFailAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "GET", "u4", "0", "OVERFLOW", "FAIL"]);
                count++;
                ClassicAssert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldSetAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "SET", "u4", "0", "1"]);
                count++;
                ClassicAssert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldSetWrapAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "SET", "u4", "0", "1", "OVERFLOW", "WRAP"]);
                count++;
                ClassicAssert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldSetSatAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "SET", "u4", "0", "1", "OVERFLOW", "SAT"]);
                count++;
                ClassicAssert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldSetFailAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "SET", "u4", "0", "1", "OVERFLOW", "FAIL"]);
                count++;
                ClassicAssert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldIncrByAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "INCRBY", "u4", "0", "4"]);
                count++;
                ClassicAssert.AreEqual(4, long.Parse(val[0]));
            }

            async Task DoBitFieldIncrByWrapAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "INCRBY", "u4", "0", "4", "OVERFLOW", "WRAP"]);
                count++;
                ClassicAssert.AreEqual(4, long.Parse(val[0]));
            }

            async Task DoBitFieldIncrBySatAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "INCRBY", "u4", "0", "4", "OVERFLOW", "SAT"]);
                count++;
                ClassicAssert.AreEqual(4, long.Parse(val[0]));
            }

            async Task DoBitFieldIncrByFailAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "INCRBY", "u4", "0", "4", "OVERFLOW", "FAIL"]);
                count++;
                ClassicAssert.AreEqual(4, long.Parse(val[0]));
            }

            async Task DoBitFieldMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "OVERFLOW", "WRAP", "GET", "u4", "1", "SET", "u4", "2", "1", "OVERFLOW", "FAIL", "INCRBY", "u4", "6", "2"]);
                count++;

                ClassicAssert.AreEqual(3, val.Length);

                string v0 = val[0];
                string v1 = val[1];
                string v2 = val[2];

                ClassicAssert.AreEqual("0", v0);
                ClassicAssert.AreEqual("0", v1);
                ClassicAssert.AreEqual("2", v2);
            }
        }

        [Test]
        public async Task BitfieldROACLsAsync()
        {
            await CheckCommandsAsync(
                "BITFIELD_RO",
                [DoBitFieldROGetAsync, DoBitFieldROMultiAsync]
            );

            static async Task DoBitFieldROGetAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD_RO", ["empty-a", "GET", "u4", "0"]);
                ClassicAssert.AreEqual(0, long.Parse(val[0]));
            }

            static async Task DoBitFieldROMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD_RO", ["empty-b", "GET", "u4", "0", "GET", "u4", "3"]);

                ClassicAssert.AreEqual(2, val.Length);

                string v0 = val[0];
                string v1 = val[1];

                ClassicAssert.AreEqual("0", v0);
                ClassicAssert.AreEqual("0", v1);
            }
        }

        [Test]
        public async Task BitOpACLsAsync()
        {
            await CheckCommandsAsync(
                "BITOP",
                [DoBitOpAndAsync, DoBitOpAndMultiAsync, DoBitOpOrAsync, DoBitOpOrMultiAsync, DoBitOpXorAsync, DoBitOpXorMultiAsync, DoBitOpNotAsync]
            );

            static async Task DoBitOpAndAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["AND", "zero", "zero"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoBitOpAndMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["AND", "zero", "zero", "one", "zero"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoBitOpOrAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["OR", "one", "one"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoBitOpOrMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["OR", "one", "one", "one", "one"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoBitOpXorAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["XOR", "one", "zero"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoBitOpXorMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["XOR", "one", "one", "one", "zero"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoBitOpNotAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["NOT", "one", "zero"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task BitPosACLsAsync()
        {
            await CheckCommandsAsync(
                "BITPOS",
                [DoBitPosAsync, DoBitPosStartAsync, DoBitPosStartEndAsync, DoBitPosStartEndBitAsync, DoBitPosStartEndByteAsync]
            );

            static async Task DoBitPosAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITPOS", ["empty", "1"]);
                ClassicAssert.AreEqual(-1, val);
            }

            static async Task DoBitPosStartAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITPOS", ["empty", "1", "5"]);
                ClassicAssert.AreEqual(-1, val);
            }

            static async Task DoBitPosStartEndAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITPOS", ["empty", "1", "5", "7"]);
                ClassicAssert.AreEqual(-1, val);
            }

            static async Task DoBitPosStartEndBitAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITPOS", ["empty", "1", "5", "7", "BIT"]);
                ClassicAssert.AreEqual(-1, val);
            }

            static async Task DoBitPosStartEndByteAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITPOS", ["empty", "1", "5", "7", "BYTE"]);
                ClassicAssert.AreEqual(-1, val);
            }
        }

        [Test]
        public async Task ClientIdACLsAsync()
        {
            await CheckCommandsAsync(
                "CLIENT ID",
                [DoClientIdAsync]
            );

            static async Task DoClientIdAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("CLIENT", ["ID"]);
                ClassicAssert.AreNotEqual(0, val);
            }
        }

        [Test]
        public async Task ClientInfoACLsAsync()
        {
            await CheckCommandsAsync(
                "CLIENT INFO",
                [DoClientInfoAsync]
            );

            static async Task DoClientInfoAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("CLIENT", ["INFO"]);
                ClassicAssert.IsNotEmpty(val);
            }
        }

        [Test]
        public async Task ClientListACLsAsync()
        {
            await CheckCommandsAsync(
                "CLIENT LIST",
                [DoClientListAsync, DoClientListTypeAsync, DoClientListIdAsync, DoClientListIdsAsync]
            );

            static async Task DoClientListAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("CLIENT", ["LIST"]);
                ClassicAssert.IsNotEmpty(val);
            }

            static async Task DoClientListTypeAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("CLIENT", ["LIST", "TYPE", "NORMAL"]);
                ClassicAssert.IsNotEmpty(val);
            }

            static async Task DoClientListIdAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("CLIENT", ["LIST", "ID", "1"]);
                ClassicAssert.IsNotEmpty(val);
            }

            static async Task DoClientListIdsAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("CLIENT", ["LIST", "ID", "1", "2"]);
                ClassicAssert.IsNotEmpty(val);
            }
        }

        [Test]
        public async Task ClientKillACLsAsync()
        {
            await CheckCommandsAsync(
                "CLIENT KILL",
                [DoClientKillAsync, DoClientFilterAsync]
            );

            static async Task DoClientKillAsync(GarnetClient client)
            {
                try
                {
                    _ = await client.ExecuteForStringResultAsync("CLIENT", ["KILL", "foo"]);
                }
                catch (Exception ex)
                {
                    if (ex.Message.Equals("ERR No such client"))
                    {
                        return;
                    }

                    throw;
                }
            }

            static async Task DoClientFilterAsync(GarnetClient client)
            {
                var count = await client.ExecuteForLongResultAsync("CLIENT", ["KILL", "ID", "123"]);
                ClassicAssert.AreEqual(0, count);
            }
        }

        [Test]
        public async Task ClientGetNameACLsAsync()
        {
            await CheckCommandsAsync(
                "CLIENT GETNAME",
                [DoClientGetNameAsync]
            );

            static async Task DoClientGetNameAsync(GarnetClient client)
            {
                var name = await client.ExecuteForStringResultAsync("CLIENT", ["GETNAME"]);
                ClassicAssert.IsNotEmpty(name);
            }
        }

        [Test]
        public async Task ClientSetNameACLsAsync()
        {
            await CheckCommandsAsync(
                "CLIENT SETNAME",
                [DoClientSetNameAsync]
            );

            static async Task DoClientSetNameAsync(GarnetClient client)
            {
                var count = await client.ExecuteForStringResultAsync("CLIENT", ["SETNAME", "foo"]);
                ClassicAssert.IsNotEmpty(count);
            }
        }

        [Test]
        public async Task ClientSetInfoACLsAsync()
        {
            await CheckCommandsAsync(
                "CLIENT SETINFO",
                [DoClientSetInfoAsync]
            );

            static async Task DoClientSetInfoAsync(GarnetClient client)
            {
                var count = await client.ExecuteForStringResultAsync("CLIENT", ["SETINFO", "LIB-NAME", "foo"]);
                ClassicAssert.IsNotEmpty(count);
            }
        }

        [Test]
        public async Task SSubscribeACLsAsync()
        {
            // SUBSCRIBE is sufficient weird that all we care to test is forbidding it
            await CheckCommandsAsync(
                "SSUBSCRIBE",
                [DoSSubscribeAsync],
                skipPermitted: true
            );

            static async Task DoSSubscribeAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("SSUBSCRIBE", ["channel"]);
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
        public async Task SPublishACLsAsync()
        {
            // SUBSCRIBE is sufficient weird that all we care to test is forbidding it
            await CheckCommandsAsync(
                "SPUBLISH",
                [DoSPublishAsync],
                skipPermitted: true
            );

            static async Task DoSPublishAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("SPUBLISH", ["channel", "message"]);
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
        public async Task ClientUnblockACLsAsync()
        {
            await CheckCommandsAsync(
                "CLIENT UNBLOCK",
                [DoClientUnblockAsync]
            );

            static async Task DoClientUnblockAsync(GarnetClient client)
            {
                var count = await client.ExecuteForLongResultAsync("CLIENT", ["UNBLOCK", "123"]);
                ClassicAssert.AreEqual(0, count);
            }
        }

        [Test]
        public async Task ClusterAddSlotsACLsAsync()
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
        public async Task ClusterAddSlotsRangeACLsAsync()
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
        public async Task ClusterAofSyncACLsAsync()
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
        public async Task ClusterAppendLogACLsAsync()
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
        public async Task ClusterAttachSyncACLsAsync()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER ATTACH_SYNC",
                [DoClusterAttachSyncAsync]
            );

            static async Task DoClusterAttachSyncAsync(GarnetClient client)
            {
                var ms = new MemoryStream();
                var writer = new BinaryWriter(ms, Encoding.ASCII);
                // See SyncMetadata
                writer.Write(0);
                writer.Write(0);

                writer.Write(0);
                writer.Write(0);

                writer.Write(0);
                writer.Write(0);

                writer.Write(0);

                byte[] byteBuffer = ms.ToArray();
                writer.Dispose();
                ms.Dispose();

                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["ATTACH_SYNC", Encoding.UTF8.GetString(byteBuffer)]);
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
        public async Task ClusterBanListACLsAsync()
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
        public async Task ClusterBeginReplicaRecoverACLsAsync()
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
        public async Task ClusterBumpEpochACLsAsync()
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
        public async Task ClusterCountKeysInSlotACLsAsync()
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
        public async Task ClusterDelKeysInSlotACLsAsync()
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
        public async Task ClusterDelKeysInSlotRangeACLsAsync()
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
        public async Task ClusterDelSlotsACLsAsync()
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
        public async Task ClusterDelSlotsRangeACLsAsync()
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
        public async Task ClusterEndpointACLsAsync()
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
        public async Task ClusterFailoverACLsAsync()
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
        public async Task ClusterFailStopWritesACLsAsync()
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
        public async Task ClusterFlushAllACLsAsync()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER FLUSHALL",
                [DoClusterFlushAllAsync]
            );

            static async Task DoClusterFlushAllAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["FLUSHALL"]);
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
        public async Task ClusterFailReplicationOffsetACLsAsync()
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
        public async Task ClusterForgetACLsAsync()
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
        public async Task ClusterGetKeysInSlotACLsAsync()
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
        public async Task ClusterGossipACLsAsync()
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
        public async Task ClusterHelpACLsAsync()
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
        public async Task ClusterInfoACLsAsync()
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
        public async Task ClusterInitiateReplicaSyncACLsAsync()
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
        public async Task ClusterKeySlotACLsAsync()
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
        public async Task ClusterMeetACLsAsync()
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
        public async Task ClusterMigrateACLsAsync()
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
        public async Task ClusterSyncACLsAsync()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER SYNC",
                [DoClusterMigrateAsync]
            );

            static async Task DoClusterMigrateAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SYNC", "a", "b", "c"]);
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
        public async Task ClusterMTasksACLsAsync()
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
        public async Task ClusterMyIdACLsAsync()
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
        public async Task ClusterMyParentIdACLsAsync()
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
        public async Task ClusterNodesACLsAsync()
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
        public async Task ClusterReplicasACLsAsync()
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
        public async Task ClusterReplicateACLsAsync()
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
        public async Task ClusterResetACLsAsync()
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
        public async Task ClusterSendCkptFileSegmentACLsAsync()
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
        public async Task ClusterSendCkptMetadataACLsAsync()
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
        public async Task ClusterSetConfigEpochACLsAsync()
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
        public async Task ClusterSetSlotACLsAsync()
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
        public async Task ClusterSetSlotsRangeACLsAsync()
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
        public async Task ClusterShardsACLsAsync()
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
        public async Task ClusterSlotsACLsAsync()
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
        public async Task ClusterSlotStateACLsAsync()
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

        [Test]
        public async Task ClusterPublishACLsAsync()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER PUBLISH",
                [DoClusterPublishAsync]
            );

            static async Task DoClusterPublishAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["PUBLISH", "channel", "message"]);
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
        public async Task ClusterSPublishACLsAsync()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER SPUBLISH",
                [DoClusterSPublishAsync]
            );

            static async Task DoClusterSPublishAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SPUBLISH", "channel", "message"]);
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
        public async Task CommandACLsAsync()
        {
            await CheckCommandsAsync(
                "COMMAND",
                [DoCommandAsync],
                skipPermitted: true
            );

            static async Task DoCommandAsync(GarnetClient client)
            {
                // COMMAND returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("COMMAND");
            }
        }

        [Test]
        public async Task CommandCountACLsAsync()
        {
            await CheckCommandsAsync(
                "COMMAND COUNT",
                [DoCommandCountAsync]
            );

            static async Task DoCommandCountAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("COMMAND", ["COUNT"]);
                ClassicAssert.IsTrue(val > 0);
            }
        }

        [Test]
        public async Task CommandInfoACLsAsync()
        {
            await CheckCommandsAsync(
                "COMMAND INFO",
                [DoCommandInfoAsync, DoCommandInfoOneAsync, DoCommandInfoMultiAsync],
                skipPermitted: true
            );

            static async Task DoCommandInfoAsync(GarnetClient client)
            {
                // COMMAND|INFO returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("COMMAND", ["INFO"]);
            }

            static async Task DoCommandInfoOneAsync(GarnetClient client)
            {
                // COMMAND|INFO returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("COMMAND", ["INFO", "GET"]);
            }

            static async Task DoCommandInfoMultiAsync(GarnetClient client)
            {
                // COMMAND|INFO returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("COMMAND", ["INFO", "GET", "SET", "APPEND"]);
            }
        }

        [Test]
        public async Task CommandDocsACLsAsync()
        {
            await CheckCommandsAsync(
                "COMMAND DOCS",
                [DoCommandDocsAsync, DoCommandDocsOneAsync, DoCommandDocsMultiAsync],
                skipPermitted: true
            );

            static async Task DoCommandDocsAsync(GarnetClient client)
            {
                // COMMAND|DOCS returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("COMMAND", ["DOCS"]);
            }

            static async Task DoCommandDocsOneAsync(GarnetClient client)
            {
                // COMMAND|DOCS returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("COMMAND", ["DOCS", "GET"]);
            }

            static async Task DoCommandDocsMultiAsync(GarnetClient client)
            {
                // COMMAND|DOCS returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("COMMAND", ["DOCS", "GET", "SET", "APPEND"]);
            }
        }

        [Test]
        public async Task CommandGetKeysACLsAsync()
        {
            await CheckCommandsAsync(
                "COMMAND GETKEYS",
                [DoCommandGetKeysAsync]
            );

            static async Task DoCommandGetKeysAsync(GarnetClient client)
            {
                string[] res = await client.ExecuteForStringArrayResultAsync("COMMAND", ["GETKEYS", "SET", "mykey", "value"]);
                ClassicAssert.IsNotNull(res);
                ClassicAssert.Contains("mykey", res);
            }
        }

        [Test]
        public async Task CommandGetKeysAndFlagsACLsAsync()
        {
            await CheckCommandsAsync(
                "COMMAND GETKEYSANDFLAGS",
                [DoCommandGetKeysAndFlagsAsync]
            );

            static async Task DoCommandGetKeysAndFlagsAsync(GarnetClient client)
            {
                var res = await client.ExecuteForStringArrayResultAsync("COMMAND", ["GETKEYSANDFLAGS", "EVAL", "return redis.call('TIME')", "0"]);
                ClassicAssert.AreEqual(0, res.Length);
            }
        }

        [Test]
        public async Task CommitAOFACLsAsync()
        {
            await CheckCommandsAsync(
                "COMMITAOF",
                [DoCommitAOFAsync]
            );

            static async Task DoCommitAOFAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("COMMITAOF");
                ClassicAssert.AreEqual("AOF file committed", val);
            }
        }

        [Test]
        public async Task ConfigGetACLsAsync()
        {
            // TODO: CONFIG GET doesn't implement multiple parameters, so that is untested

            await CheckCommandsAsync(
                "CONFIG GET",
                [DoConfigGetOneAsync]
            );

            static async Task DoConfigGetOneAsync(GarnetClient client)
            {
                string[] res = await client.ExecuteForStringArrayResultAsync("CONFIG", ["GET", "timeout"]);

                ClassicAssert.AreEqual(2, res.Length);
                ClassicAssert.AreEqual("timeout", (string)res[0]);
                ClassicAssert.IsTrue(int.Parse(res[1]) >= 0);
            }
        }

        [Test]
        public async Task ConfigRewriteACLsAsync()
        {
            await CheckCommandsAsync(
                "CONFIG REWRITE",
                [DoConfigRewriteAsync]
            );

            static async Task DoConfigRewriteAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("CONFIG", ["REWRITE"]);
                ClassicAssert.AreEqual("OK", res);
            }
        }

        [Test]
        public async Task ConfigSetACLsAsync()
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
        public async Task COScanACLsAsync()
        {
            // TODO: COSCAN parameters are unclear... add more cases later

            await CheckCommandsAsync(
                "COSCAN",
                [DoCOScanAsync],
                skipPermitted: true,
                aclCheckCommandOverride: "CUSTOMOBJECTSCAN"
            );

            static async Task DoCOScanAsync(GarnetClient client)
            {
                // COSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("CUSTOMOBJECTSCAN", ["foo", "0"]);
            }
        }

        [Test]
        public async Task CustomRawStringCmdACLsAsync()
        {
            // TODO: it probably makes sense to expose ACLs for registered commands, but for now just a blanket ACL for all custom commands is all we have

            int count = 0;

            await CheckCommandsAsync(
                "CUSTOMRAWSTRINGCMD",
                [DoSetWpIfPgtAsync],
                knownCategories: ["garnet", "custom", "dangerous"],
                aclCheckCommandOverride: "SETWPIFPGT"
            );

            async Task DoSetWpIfPgtAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("SETWPIFPGT", [$"foo-{count}", "bar", "\0\0\0\0\0\0\0\0"]);
                count++;

                ClassicAssert.AreEqual("OK", (string)res);
            }
        }

        [Test]
        public async Task CustomObjCmdACLsAsync()
        {
            // TODO: it probably makes sense to expose ACLs for registered commands, but for now just a blanket ACL for all custom commands is all we have

            await CheckCommandsAsync(
                "CUSTOMOBJCMD",
                [DoMyDictGetAsync],
                knownCategories: ["garnet", "custom", "dangerous"],
                aclCheckCommandOverride: "MYDICTGET"
            );

            static async Task DoMyDictGetAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("MYDICTGET", ["foo", "bar"]);
                ClassicAssert.IsNull(res);
            }
        }

        [Test]
        public async Task CustomTxnACLsAsync()
        {
            // TODO: it probably makes sense to expose ACLs for registered commands, but for now just a blanket ACL for all custom commands is all we have

            await CheckCommandsAsync(
                "CustomTxn",
                [DoReadWriteTxAsync],
                knownCategories: ["garnet", "custom", "dangerous"],
                aclCheckCommandOverride: "READWRITETX"
            );

            static async Task DoReadWriteTxAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("READWRITETX", ["foo", "bar", "fizz"]);
                ClassicAssert.AreEqual("SUCCESS", res);
            }
        }

        [Test]
        public async Task CustomProcedureACLsAsync()
        {
            // TODO: it probably makes sense to expose ACLs for registered commands, but for now just a blanket ACL for all custom commands is all we have

            await CheckCommandsAsync(
                "CustomProcedure",
                [DoSumAsync],
                knownCategories: ["garnet", "custom", "dangerous"],
                aclCheckCommandOverride: "SUM"
            );

            async Task DoSumAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("SUM", ["key1", "key2", "key3"]);
                ClassicAssert.AreEqual("0", res.ToString());
            }
        }

        [Test]
        public async Task DebugACLsAsync()
        {
            await CheckCommandsAsync(
                "DEBUG",
                [DoDebugAsync]
            );

            async Task DoDebugAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("DEBUG", ["HELP"]);
                ClassicAssert.NotNull(res.ToString());
            }
        }

        [Test]
        public async Task EvalACLsAsync()
        {
            await CheckCommandsAsync(
                "EVAL",
                [DoEvalAsync],
                knownCategories: ["slow", "scripting"]
            );

            async Task DoEvalAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("EVAL", ["return 'OK'", "0"]);
                ClassicAssert.AreEqual("OK", (string)res);
            }
        }

        [Test]
        public async Task EvalShaACLsAsync()
        {
            await CheckCommandsAsync(
                "EVALSHA",
                [DoEvalShaAsync],
                knownCategories: ["slow", "scripting"]
            );

            async Task DoEvalShaAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("EVALSHA", ["57ade87c8731f041ecac85aba56623f8af391fab", "0"]);
                    Assert.Fail("Should be unreachable, script is not loaded");
                }
                catch (Exception e)
                {
                    if (e.Message == "NOSCRIPT No matching script. Please use EVAL.")
                    {
                        return;
                    }

                    throw;
                }
            }
        }

        [Test]
        public async Task ScriptLoadACLsAsync()
        {
            await CheckCommandsAsync(
                "SCRIPT LOAD",
                [DoScriptLoadAsync]
            );

            async Task DoScriptLoadAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("SCRIPT", ["LOAD", "return 'OK'"]);
                ClassicAssert.AreEqual("57ade87c8731f041ecac85aba56623f8af391fab", (string)res);
            }
        }

        [Test]
        public async Task ScriptExistsACLsAsync()
        {
            await CheckCommandsAsync(
                "SCRIPT EXISTS",
                [DoScriptExistsSingleAsync, DoScriptExistsMultiAsync]
            );

            async Task DoScriptExistsSingleAsync(GarnetClient client)
            {
                string[] res = await client.ExecuteForStringArrayResultAsync("SCRIPT", ["EXISTS", "57ade87c8731f041ecac85aba56623f8af391fab"]);
                ClassicAssert.AreEqual(1, res.Length);
                ClassicAssert.IsTrue(res[0] == "1" || res[0] == "0");
            }

            async Task DoScriptExistsMultiAsync(GarnetClient client)
            {
                string[] res = await client.ExecuteForStringArrayResultAsync("SCRIPT", ["EXISTS", "57ade87c8731f041ecac85aba56623f8af391fab", "57ade87c8731f041ecac85aba56623f8af391fab"]);
                ClassicAssert.AreEqual(2, res.Length);
                ClassicAssert.IsTrue(res[0] == "1" || res[0] == "0");
                ClassicAssert.AreEqual(res[0], res[1]);
            }
        }

        [Test]
        public async Task ScriptFlushACLsAsync()
        {
            await CheckCommandsAsync(
                "SCRIPT FLUSH",
                [DoScriptFlushAsync, DoScriptFlushSyncAsync, DoScriptFlushAsyncAsync]
            );

            async Task DoScriptFlushAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("SCRIPT", ["FLUSH"]);
                ClassicAssert.AreEqual("OK", res);
            }

            async Task DoScriptFlushSyncAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("SCRIPT", ["FLUSH", "SYNC"]);
                ClassicAssert.AreEqual("OK", res);
            }

            async Task DoScriptFlushAsyncAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("SCRIPT", ["FLUSH", "ASYNC"]);
                ClassicAssert.AreEqual("OK", res);
            }
        }

        [Test]
        public async Task DBSizeACLsAsync()
        {
            await CheckCommandsAsync(
                "DBSIZE",
                [DoDbSizeAsync]
            );

            static async Task DoDbSizeAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("DBSIZE");
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task DecrACLsAsync()
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
                ClassicAssert.AreEqual(-1, val);
            }
        }

        [Test]
        public async Task DecrByACLsAsync()
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
                ClassicAssert.AreEqual(-2, val);
            }
        }

        [Test]
        public async Task DelACLsAsync()
        {
            await CheckCommandsAsync(
                "DEL",
                [DoDelAsync, DoDelMultiAsync]
            );

            static async Task DoDelAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("DEL", ["foo"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoDelMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("DEL", ["foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task DiscardACLsAsync()
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
        public async Task EchoACLsAsync()
        {
            await CheckCommandsAsync(
                "ECHO",
                [DoEchoAsync]
            );

            static async Task DoEchoAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ECHO", ["hello world"]);
                ClassicAssert.AreEqual("hello world", val);
            }
        }

        [Test]
        public async Task ExecACLsAsync()
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
        public async Task ExistsACLsAsync()
        {
            await CheckCommandsAsync(
                "EXISTS",
                [DoExistsAsync, DoExistsMultiAsync]
            );

            static async Task DoExistsAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXISTS", ["foo"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExistsMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXISTS", ["foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ExpireACLsAsync()
        {
            await CheckCommandsAsync(
                "EXPIRE",
                [DoExpireAsync, DoExpireNXAsync, DoExpireXXAsync, DoExpireGTAsync, DoExpireLTAsync]
            );

            static async Task DoExpireAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXPIRE", ["foo", "10"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireNXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXPIRE", ["foo", "10", "NX"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireXXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXPIRE", ["foo", "10", "XX"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireGTAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXPIRE", ["foo", "10", "GT"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireLTAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXPIRE", ["foo", "10", "LT"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ExpireAtACLsAsync()
        {
            await CheckCommandsAsync(
                "EXPIREAT",
                [DoExpireAsync, DoExpireNXAsync, DoExpireXXAsync, DoExpireGTAsync, DoExpireLTAsync]
            );


            static async Task DoExpireAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();
                long val = await client.ExecuteForLongResultAsync("EXPIREAT", ["foo", expireTimestamp]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireNXAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();
                long val = await client.ExecuteForLongResultAsync("EXPIREAT", ["foo", "10", "NX"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireXXAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();
                long val = await client.ExecuteForLongResultAsync("EXPIREAT", ["foo", "10", "XX"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireGTAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();
                long val = await client.ExecuteForLongResultAsync("EXPIREAT", ["foo", "10", "GT"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireLTAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();
                long val = await client.ExecuteForLongResultAsync("EXPIREAT", ["foo", "10", "LT"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task PExpireAtACLsAsync()
        {
            await CheckCommandsAsync(
                "PEXPIREAT",
                [DoExpireAsync, DoExpireNXAsync, DoExpireXXAsync, DoExpireGTAsync, DoExpireLTAsync]
            );


            static async Task DoExpireAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeMilliseconds().ToString();
                long val = await client.ExecuteForLongResultAsync("PEXPIREAT", ["foo", expireTimestamp]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireNXAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();
                long val = await client.ExecuteForLongResultAsync("PEXPIREAT", ["foo", "10", "NX"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireXXAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();
                long val = await client.ExecuteForLongResultAsync("PEXPIREAT", ["foo", "10", "XX"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireGTAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();
                long val = await client.ExecuteForLongResultAsync("PEXPIREAT", ["foo", "10", "GT"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireLTAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();
                long val = await client.ExecuteForLongResultAsync("PEXPIREAT", ["foo", "10", "LT"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task FailoverACLsAsync()
        {
            // FAILOVER is sufficiently weird that we don't want to test "success"
            //
            // Instead, we only test that we can successful forbid it
            await CheckCommandsAsync(
                "FAILOVER",
                [
                    DoFailoverAsync,
                    DoFailoverToAsync,
                    DoFailoverAbortAsync,
                    DoFailoverToForceAsync,
                    DoFailoverToAbortAsync,
                    DoFailoverToForceAbortAsync,
                    DoFailoverToForceAbortTimeoutAsync,
                ],
                skipPermitted: true
            );

            static async Task DoFailoverAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("FAILOVER");
                    Assert.Fail("Shouldn't be reachable, cluster not enabled");
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

            static async Task DoFailoverToAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("FAILOVER", ["TO", "127.0.0.1", "9999"]);
                    Assert.Fail("Shouldn't be reachable, cluster not enabled");
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

            static async Task DoFailoverAbortAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("FAILOVER", ["ABORT"]);
                    Assert.Fail("Shouldn't be reachable, cluster not enabled");
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

            static async Task DoFailoverToForceAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("FAILOVER", ["TO", "127.0.0.1", "9999", "FORCE"]);
                    Assert.Fail("Shouldn't be reachable, cluster not enabled");
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

            static async Task DoFailoverToAbortAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("FAILOVER", ["TO", "127.0.0.1", "9999", "ABORT"]);
                    Assert.Fail("Shouldn't be reachable, cluster not enabled");
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

            static async Task DoFailoverToForceAbortAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("FAILOVER", ["TO", "127.0.0.1", "9999", "FORCE", "ABORT"]);
                    Assert.Fail("Shouldn't be reachable, cluster not enabled");
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

            static async Task DoFailoverToForceAbortTimeoutAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("FAILOVER", ["TO", "127.0.0.1", "9999", "FORCE", "ABORT", "TIMEOUT", "1"]);
                    Assert.Fail("Shouldn't be reachable, cluster not enabled");
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
        public async Task FlushDBACLsAsync()
        {
            await CheckCommandsAsync(
                "FLUSHDB",
                [DoFlushDBAsync, DoFlushDBAsyncAsync]
            );

            static async Task DoFlushDBAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("FLUSHDB");
                ClassicAssert.AreEqual("OK", val);
            }

            static async Task DoFlushDBAsyncAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("FLUSHDB", ["ASYNC"]);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task FlushAllACLsAsync()
        {
            await CheckCommandsAsync(
                "FLUSHALL",
                [DoFlushAllAsync, DoFlushAllAsyncAsync]
            );

            static async Task DoFlushAllAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("FLUSHALL");
                ClassicAssert.AreEqual("OK", val);
            }

            static async Task DoFlushAllAsyncAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("FLUSHALL", ["ASYNC"]);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task ForceGCACLsAsync()
        {
            await CheckCommandsAsync(
                "FORCEGC",
                [DoForceGCAsync, DoForceGCGenAsync]
            );

            static async Task DoForceGCAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("FORCEGC");
                ClassicAssert.AreEqual("GC completed", val);
            }

            static async Task DoForceGCGenAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("FORCEGC", ["1"]);
                ClassicAssert.AreEqual("GC completed", val);
            }
        }

        [Test]
        public async Task GetACLsAsync()
        {
            await CheckCommandsAsync(
                "GET",
                [DoGetAsync]
            );

            static async Task DoGetAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("GET", ["foo"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task GetEXACLsAsync()
        {
            await CheckCommandsAsync(
                "GETEX",
                [DoGetEXAsync]
            );

            static async Task DoGetEXAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("GETEX", ["foo"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task GetBitACLsAsync()
        {
            await CheckCommandsAsync(
                "GETBIT",
                [DoGetBitAsync]
            );

            static async Task DoGetBitAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GETBIT", ["foo", "4"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task GetDelACLsAsync()
        {
            await CheckCommandsAsync(
                "GETDEL",
                [DoGetDelAsync]
            );

            static async Task DoGetDelAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("GETDEL", ["foo"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task GetSetACLsAsync()
        {
            int keyIx = 0;

            await CheckCommandsAsync(
                "GETSET",
                [DoGetAndSetAsync]
            );

            async Task DoGetAndSetAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("GETSET", [$"foo-{keyIx++}", "bar"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task SubStrACLsAsync()
        {
            await CheckCommandsAsync(
                "SUBSTR",
                [DoSubStringAsync]
            );

            static async Task DoSubStringAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SUBSTR", ["foo", "10", "15"]);
                ClassicAssert.AreEqual("", val);
            }
        }

        [Test]
        public async Task GetRangeACLsAsync()
        {
            await CheckCommandsAsync(
                "GETRANGE",
                [DoGetRangeAsync]
            );

            static async Task DoGetRangeAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("GETRANGE", ["foo", "10", "15"]);
                ClassicAssert.AreEqual("", val);
            }
        }

        [Test]
        public async Task SubStringACLsAsync()
        {
            await CheckCommandsAsync(
                "SUBSTR",
                [DoSubStringAsync]
            );

            static async Task DoSubStringAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SUBSTR", ["foo", "10", "15"]);
                ClassicAssert.AreEqual("", val);
            }
        }

        [Test]
        public async Task HExpireACLsAsync()
        {
            await CheckCommandsAsync(
                "HEXPIRE",
                [DoHExpireAsync]
            );

            static async Task DoHExpireAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("HEXPIRE", ["foo", "1", "FIELDS", "1", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.AreEqual("-2", val[0]);
            }
        }

        [Test]
        public async Task HPExpireACLsAsync()
        {
            await CheckCommandsAsync(
                "HPEXPIRE",
                [DoHPExpireAsync]
            );

            static async Task DoHPExpireAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("HPEXPIRE", ["foo", "1", "FIELDS", "1", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.AreEqual("-2", val[0]);
            }
        }

        [Test]
        public async Task HExpireAtACLsAsync()
        {
            await CheckCommandsAsync(
                "HEXPIREAT",
                [DoHExpireAtAsync]
            );

            static async Task DoHExpireAtAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("HEXPIREAT", ["foo", DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeSeconds().ToString(), "FIELDS", "1", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.AreEqual("-2", val[0]);
            }
        }

        [Test]
        public async Task HPExpireAtACLsAsync()
        {
            await CheckCommandsAsync(
                "HPEXPIREAT",
                [DoHPExpireAtAsync]
            );

            static async Task DoHPExpireAtAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("HPEXPIREAT", ["foo", DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeMilliseconds().ToString(), "FIELDS", "1", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.AreEqual("-2", val[0]);
            }
        }

        [Test]
        public async Task HExpireTimeACLsAsync()
        {
            await CheckCommandsAsync(
                "HEXPIRETIME",
                [DoHExpireTimeAsync]
            );

            static async Task DoHExpireTimeAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("HEXPIRETIME", ["foo", "FIELDS", "1", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.AreEqual("-2", val[0]);
            }
        }

        [Test]
        public async Task HPExpireTimeACLsAsync()
        {
            await CheckCommandsAsync(
                "HPEXPIRETIME",
                [DoHPExpireTimeAsync]
            );

            static async Task DoHPExpireTimeAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("HPEXPIRETIME", ["foo", "FIELDS", "1", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.AreEqual("-2", val[0]);
            }
        }

        [Test]
        public async Task HTTLACLsAsync()
        {
            await CheckCommandsAsync(
                "HTTL",
                [DoHETTLAsync]
            );

            static async Task DoHETTLAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("HTTL", ["foo", "FIELDS", "1", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.AreEqual("-2", val[0]);
            }
        }

        [Test]
        public async Task HPTTLACLsAsync()
        {
            await CheckCommandsAsync(
                "HPTTL",
                [DoHPETTLAsync]
            );

            static async Task DoHPETTLAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("HPTTL", ["foo", "FIELDS", "1", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.AreEqual("-2", val[0]);
            }
        }

        [Test]
        public async Task HPersistACLsAsync()
        {
            await CheckCommandsAsync(
                "HPERSIST",
                [DoHPersistAsync]
            );

            static async Task DoHPersistAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("HPERSIST", ["foo", "FIELDS", "1", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.AreEqual("-2", val[0]);
            }
        }

        [Test]
        public async Task HCollectACLsAsync()
        {
            await CheckCommandsAsync(
                "HCOLLECT",
                [DoHCollectAsync]
            );

            static async Task DoHCollectAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringResultAsync("HCOLLECT", ["foo"]);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task HDelACLsAsync()
        {
            await CheckCommandsAsync(
                "HDEL",
                [DoHDelAsync, DoHDelMultiAsync]
            );

            static async Task DoHDelAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HDEL", ["foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoHDelMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HDEL", ["foo", "bar", "fizz"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task HExistsACLsAsync()
        {
            await CheckCommandsAsync(
                "HEXISTS",
                [DoHDelAsync]
            );

            static async Task DoHDelAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HEXISTS", ["foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task HGetACLsAsync()
        {
            await CheckCommandsAsync(
                "HGET",
                [DoHDelAsync]
            );

            static async Task DoHDelAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("HGET", ["foo", "bar"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task HGetAllACLsAsync()
        {
            await CheckCommandsAsync(
                "HGETALL",
                [DoHDelAsync]
            );

            static async Task DoHDelAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HGETALL", ["foo"]);

                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task HIncrByACLsAsync()
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
                ClassicAssert.AreEqual(cur, val);
            }
        }

        [Test]
        public async Task HIncrByFloatACLsAsync()
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
                ClassicAssert.AreEqual(cur, double.Parse(val, CultureInfo.InvariantCulture));
            }
        }

        [Test]
        public async Task HKeysACLsAsync()
        {
            await CheckCommandsAsync(
                "HKEYS",
                [DoHKeysAsync]
            );

            static async Task DoHKeysAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HKEYS", ["foo"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task HLenACLsAsync()
        {
            await CheckCommandsAsync(
                "HLEN",
                [DoHLenAsync]
            );

            static async Task DoHLenAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HLEN", ["foo"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task HMGetACLsAsync()
        {
            await CheckCommandsAsync(
                "HMGET",
                [DoHMGetAsync, DoHMGetMultiAsync]
            );

            static async Task DoHMGetAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HMGET", ["foo", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.IsNull(val[0]);
            }

            static async Task DoHMGetMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HMGET", ["foo", "bar", "fizz"]);
                ClassicAssert.AreEqual(2, val.Length);
                ClassicAssert.IsNull(val[0]);
                ClassicAssert.IsNull(val[1]);
            }
        }

        [Test]
        public async Task HMSetACLsAsync()
        {
            await CheckCommandsAsync(
                "HMSET",
                [DoHMSetAsync, DoHMSetMultiAsync]
            );

            static async Task DoHMSetAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("HMSET", ["foo", "bar", "fizz"]);
                ClassicAssert.AreEqual("OK", val);
            }

            static async Task DoHMSetMultiAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("HMSET", ["foo", "bar", "fizz", "hello", "world"]);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task HRandFieldACLsAsync()
        {
            await CheckCommandsAsync(
                "HRANDFIELD",
                [DoHRandFieldAsync, DoHRandFieldCountAsync, DoHRandFieldCountWithValuesAsync]
            );

            static async Task DoHRandFieldAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("HRANDFIELD", ["foo"]);
                ClassicAssert.IsNull(val);
            }

            static async Task DoHRandFieldCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HRANDFIELD", ["foo", "1"]);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoHRandFieldCountWithValuesAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HRANDFIELD", ["foo", "1", "WITHVALUES"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task HScanACLsAsync()
        {

            await CheckCommandsAsync(
                "HSCAN",
                [DoHScanAsync, DoHScanMatchAsync, DoHScanCountAsync, DoHScanNoValuesAsync, DoHScanMatchCountAsync, DoHScanMatchNoValuesAsync, DoHScanCountNoValuesAsync, DoHScanMatchCountNoValuesAsync],
                skipPermitted: true
            );

            static async Task DoHScanAsync(GarnetClient client)
            {
                // HSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("HSCAN", ["foo", "0"]);
            }

            static async Task DoHScanMatchAsync(GarnetClient client)
            {
                // HSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("HSCAN", ["foo", "0", "MATCH", "*"]);
            }

            static async Task DoHScanCountAsync(GarnetClient client)
            {
                // HSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("HSCAN", ["foo", "0", "COUNT", "2"]);
            }

            static async Task DoHScanNoValuesAsync(GarnetClient client)
            {
                // HSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("HSCAN", ["foo", "0", "NOVALUES"]);
            }

            static async Task DoHScanMatchCountAsync(GarnetClient client)
            {
                // HSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("HSCAN", ["foo", "0", "MATCH", "*", "COUNT", "2"]);
            }

            static async Task DoHScanMatchNoValuesAsync(GarnetClient client)
            {
                // HSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("HSCAN", ["foo", "0", "MATCH", "*", "NOVALUES"]);
            }

            static async Task DoHScanCountNoValuesAsync(GarnetClient client)
            {
                // HSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("HSCAN", ["foo", "0", "COUNT", "0", "NOVALUES"]);
            }

            static async Task DoHScanMatchCountNoValuesAsync(GarnetClient client)
            {
                // HSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("HSCAN", ["foo", "0", "MATCH", "*", "COUNT", "0", "NOVALUES"]);
            }
        }

        [Test]
        public async Task HSetACLsAsync()
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

                ClassicAssert.AreEqual(1, val);
            }

            async Task DoHSetMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HSET", [$"foo-{keyIx}", "bar", "fizz", "hello", "world"]);
                keyIx++;

                ClassicAssert.AreEqual(2, val);
            }
        }

        [Test]
        public async Task HSetNXACLsAsync()
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

                ClassicAssert.AreEqual(1, val);
            }
        }

        [Test]
        public async Task HStrLenACLsAsync()
        {
            await CheckCommandsAsync(
                "HSTRLEN",
                [DoHStrLenAsync]
            );

            static async Task DoHStrLenAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HSTRLEN", ["foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task HValsACLsAsync()
        {
            await CheckCommandsAsync(
                "HVALS",
                [DoHValsAsync]
            );

            static async Task DoHValsAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HVALS", ["foo"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task IncrACLsAsync()
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
                ClassicAssert.AreEqual(1, val);
            }
        }

        [Test]
        public async Task IncrByACLsAsync()
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
                ClassicAssert.AreEqual(2, val);
            }
        }

        [Test]
        public async Task IncrByFloatACLsAsync()
        {
            int count = 0;

            await CheckCommandsAsync(
                "INCRBYFLOAT",
                [DoIncrByFloatAsync]
            );

            async Task DoIncrByFloatAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringResultAsync("INCRBYFLOAT", [$"foo-{count}", "2"]);
                count++;
                ClassicAssert.AreEqual("2", val);
            }
        }

        [Test]
        public async Task InfoACLsAsync()
        {
            await CheckCommandsAsync(
               "INFO",
               [DoInfoAsync, DoInfoSingleAsync, DoInfoMultiAsync]
            );

            static async Task DoInfoAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("INFO");
                ClassicAssert.IsNotEmpty(val);
            }

            static async Task DoInfoSingleAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("INFO", ["SERVER"]);
                ClassicAssert.IsNotEmpty(val);
            }

            static async Task DoInfoMultiAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("INFO", ["SERVER", "MEMORY"]);
                ClassicAssert.IsNotEmpty(val);
            }
        }

        [Test]
        public async Task RoleACLsAsync()
        {
            await CheckCommandsAsync(
               "ROLE",
               [DoRoleAsync]
            );

            static async Task DoRoleAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ROLE");
                ClassicAssert.IsNotEmpty(val);
            }
        }

        [Test]
        public async Task KeysACLsAsync()
        {
            await CheckCommandsAsync(
               "KEYS",
               [DoKeysAsync]
            );

            static async Task DoKeysAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("KEYS", ["*"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task LastSaveACLsAsync()
        {
            await CheckCommandsAsync(
               "LASTSAVE",
               [DoLastSaveAsync]
            );

            static async Task DoLastSaveAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LASTSAVE");
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task LatencyHelpACLsAsync()
        {
            await CheckCommandsAsync(
               "LATENCY HELP",
               [DoLatencyHelpAsync]
            );

            static async Task DoLatencyHelpAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("LATENCY", ["HELP"]);
                ClassicAssert.IsNotNull(val);
            }
        }

        [Test]
        public async Task LatencyHistogramACLsAsync()
        {
            await CheckCommandsAsync(
               "LATENCY HISTOGRAM",
               [DoLatencyHistogramAsync, DoLatencyHistogramSingleAsync, DoLatencyHistogramMultiAsync]
            );

            static async Task DoLatencyHistogramAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("LATENCY", ["HISTOGRAM"]);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoLatencyHistogramSingleAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("LATENCY", ["HISTOGRAM", "NET_RS_LAT"]);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoLatencyHistogramMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("LATENCY", ["HISTOGRAM", "NET_RS_LAT", "NET_RS_LAT_ADMIN"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task LatencyResetACLsAsync()
        {
            await CheckCommandsAsync(
               "LATENCY RESET",
               [DoLatencyResetAsync, DoLatencyResetSingleAsync, DoLatencyResetMultiAsync]
            );

            static async Task DoLatencyResetAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LATENCY", ["RESET"]);
                ClassicAssert.AreEqual(6, val);
            }

            static async Task DoLatencyResetSingleAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LATENCY", ["RESET", "NET_RS_LAT"]);
                ClassicAssert.AreEqual(1, val);
            }

            static async Task DoLatencyResetMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LATENCY", ["RESET", "NET_RS_LAT", "NET_RS_LAT_ADMIN"]);
                ClassicAssert.AreEqual(2, val);
            }
        }

        [Test]
        public async Task BLMoveACLsAsync()
        {
            await CheckCommandsAsync(
                "BLMOVE",
                [DoBLMoveAsync]
            );

            static async Task DoBLMoveAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("BLMOVE", ["foo", "bar", "RIGHT", "LEFT", "0.1"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task BRPopLPushACLsAsync()
        {
            await CheckCommandsAsync(
                "BRPOPLPUSH",
                [DoBRPopLPushAsync]
            );

            static async Task DoBRPopLPushAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("BRPOPLPUSH", ["foo", "bar", "0.1"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task BLMPopACLsAsync()
        {
            await CheckCommandsAsync(
                "BLMPOP",
                [DoBLMPopAsync]
            );

            static async Task DoBLMPopAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("BLMPOP", ["0.1", "1", "foo", "RIGHT"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task BLPopACLsAsync()
        {
            await CheckCommandsAsync(
                "BLPOP",
                [DoBLPopAsync]
            );

            static async Task DoBLPopAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BLPOP", ["foo", "0.1"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task BRPopACLsAsync()
        {
            await CheckCommandsAsync(
                "BRPOP",
                [DoBRPopAsync]
            );

            static async Task DoBRPopAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BRPOP", ["foo", "0.1"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task BZMPopACLsAsync()
        {
            await CheckCommandsAsync(
                "BZMPOP",
                [DoBZMPopAsync]
            );

            static async Task DoBZMPopAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringResultAsync("BZMPOP", ["0.1", "1", "foo", "MIN"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task BZPopMaxACLsAsync()
        {
            await CheckCommandsAsync(
                "BZPOPMAX",
                [DoBZPopMaxAsync]
            );

            static async Task DoBZPopMaxAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringResultAsync("BZPOPMAX", ["foo", "0.1"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task BZPopMinACLsAsync()
        {
            await CheckCommandsAsync(
                "BZPOPMIN",
                [DoBZPopMinAsync]
            );

            static async Task DoBZPopMinAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringResultAsync("BZPOPMIN", ["foo", "0.1"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task LPopACLsAsync()
        {
            await CheckCommandsAsync(
                "LPOP",
                [DoLPopAsync, DoLPopCountAsync]
            );

            static async Task DoLPopAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LPOP", ["foo"]);
                ClassicAssert.IsNull(val);
            }

            static async Task DoLPopCountAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LPOP", ["foo", "4"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task LPosACLsAsync()
        {
            await CheckCommandsAsync(
                "LPOS",
                [DoLPosAsync]
            );

            static async Task DoLPosAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LPOS", ["foo", "a"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task LPushACLsAsync()
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

                ClassicAssert.AreEqual(count, val);
            }

            async Task DoLPushMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LPUSH", ["foo", "bar", "buzz"]);
                count += 2;

                ClassicAssert.AreEqual(count, val);
            }
        }

        [Test]
        public async Task LPushXACLsAsync()
        {
            await CheckCommandsAsync(
                "LPUSHX",
                [DoLPushXAsync, DoLPushXMultiAsync]
            );

            static async Task DoLPushXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LPUSHX", ["foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }

            async Task DoLPushXMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LPUSHX", ["foo", "bar", "buzz"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task RPopACLsAsync()
        {
            await CheckCommandsAsync(
                "RPOP",
                [DoRPopAsync, DoRPopCountAsync]
            );

            static async Task DoRPopAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("RPOP", ["foo"]);
                ClassicAssert.IsNull(val);
            }

            static async Task DoRPopCountAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("RPOP", ["foo", "4"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task LRushACLsAsync()
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

                ClassicAssert.AreEqual(count, val);
            }

            async Task DoRPushMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("RPUSH", ["foo", "bar", "buzz"]);
                count += 2;

                ClassicAssert.AreEqual(count, val);
            }
        }

        [Test]
        public async Task RPushACLsAsync()
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
                ClassicAssert.AreEqual(1, val);
            }

            async Task DoRPushXMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("RPUSH", [$"foo-{count}", "bar", "buzz"]);
                count++;
                ClassicAssert.AreEqual(2, val);
            }
        }

        [Test]
        public async Task RPushXACLsAsync()
        {
            await CheckCommandsAsync(
                "RPUSHX",
                [DoRPushXAsync, DoRPushXMultiAsync]
            );

            static async Task DoRPushXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("RPUSHX", ["foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoRPushXMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("RPUSHX", ["foo", "bar", "buzz"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task LCSACLsAsync()
        {
            await CheckCommandsAsync(
                "LCS",
                [DoLCSAsync]
            );

            static async Task DoLCSAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LCS", ["foo", "bar"]);
                ClassicAssert.AreEqual("", val);
            }
        }

        [Test]
        public async Task LLenACLsAsync()
        {
            await CheckCommandsAsync(
                "LLEN",
                [DoLLenAsync]
            );

            static async Task DoLLenAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LLEN", ["foo"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task LTrimACLsAsync()
        {
            await CheckCommandsAsync(
                "LTRIM",
                [DoLTrimAsync]
            );

            static async Task DoLTrimAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LTRIM", ["foo", "4", "10"]);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task LRangeACLsAsync()
        {
            await CheckCommandsAsync(
                "LRANGE",
                [DoLRangeAsync]
            );

            static async Task DoLRangeAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("LRANGE", ["foo", "4", "10"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task LIndexACLsAsync()
        {
            await CheckCommandsAsync(
                "LINDEX",
                [DoLIndexAsync]
            );

            static async Task DoLIndexAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LINDEX", ["foo", "4"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task LInsertACLsAsync()
        {
            await CheckCommandsAsync(
                "LINSERT",
                [DoLInsertBeforeAsync, DoLInsertAfterAsync]
            );

            static async Task DoLInsertBeforeAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LINSERT", ["foo", "BEFORE", "hello", "world"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoLInsertAfterAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LINSERT", ["foo", "AFTER", "hello", "world"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task LRemACLsAsync()
        {
            await CheckCommandsAsync(
                "LREM",
                [DoLRemAsync]
            );

            static async Task DoLRemAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LREM", ["foo", "0", "hello"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task RPopLPushACLsAsync()
        {
            await CheckCommandsAsync(
                "RPOPLPUSH",
                [DoLRemAsync]
            );

            static async Task DoLRemAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("RPOPLPUSH", ["foo", "bar"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task LMoveACLsAsync()
        {
            await CheckCommandsAsync(
                "LMOVE",
                [DoLMoveAsync]
            );

            static async Task DoLMoveAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LMOVE", ["foo", "bar", "LEFT", "RIGHT"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task LMPopACLsAsync()
        {
            await CheckCommandsAsync(
                "LMPOP",
                [DoLMPopAsync, DoLMPopCountAsync]
            );

            static async Task DoLMPopAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LMPOP", ["1", "foo", "LEFT"]);
                ClassicAssert.IsNull(val);
            }

            static async Task DoLMPopCountAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LMPOP", ["1", "foo", "LEFT", "COUNT", "1"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task LSetACLsAsync()
        {
            await CheckCommandsAsync(
                "LSET",
                [DoLSetAsync]
            );

            static async Task DoLSetAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("LSET", ["foo", "0", "bar"]);
                    Assert.Fail("Should not be reachable, key does not exist");
                }
                catch (Exception e)
                {
                    if (e.Message != "ERR no such key")
                    {
                        throw;
                    }
                }
            }
        }

        [Test]
        public async Task MemoryUsageACLsAsync()
        {
            await CheckCommandsAsync(
                "MEMORY USAGE",
                [DoMemoryUsageAsync, DoMemoryUsageSamplesAsync]
            );

            static async Task DoMemoryUsageAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("MEMORY", ["USAGE", "foo"]);
                ClassicAssert.IsNull(val);
            }

            static async Task DoMemoryUsageSamplesAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("MEMORY", ["USAGE", "foo", "SAMPLES", "10"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task MGetACLsAsync()
        {
            await CheckCommandsAsync(
                "MGET",
                [DoMemorySingleAsync, DoMemoryMultiAsync]
            );

            static async Task DoMemorySingleAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("MGET", ["foo"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.IsNull(val[0]);
            }

            static async Task DoMemoryMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("MGET", ["foo", "bar"]);
                ClassicAssert.AreEqual(2, val.Length);
                ClassicAssert.IsNull(val[0]);
                ClassicAssert.IsNull(val[1]);
            }
        }

        [Test]
        public async Task MigrateACLsAsync()
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
        public async Task PurgeBPACLsAsync()
        {
            // Uses exceptions for control flow, as we're not setting up replicas here

            await CheckCommandsAsync(
                "PURGEBP",
                [DoPurgeBPClusterAsync, DoPurgeBPAsync]
            );

            static async Task DoPurgeBPClusterAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("PURGEBP", ["MigrationManager"]);
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

            static async Task DoPurgeBPAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("PURGEBP", ["ServerListener"]);
                ClassicAssert.AreEqual("GC completed for ServerListener", val);
            }
        }

        [Test]
        public async Task ModuleLoadCSACLsAsync()
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

        [Test]
        public async Task MonitorACLsAsync()
        {
            // MONITOR is weird, so just check that we can forbid it
            await CheckCommandsAsync(
                "MONITOR",
                [DoMonitorAsync],
                skipPermitted: true
            );

            static async Task DoMonitorAsync(GarnetClient client)
            {
                await client.ExecuteForStringResultAsync("MONITOR");
                Assert.Fail("Should never reach this point");
            }
        }

        [Test]
        public async Task MSetACLsAsync()
        {
            await CheckCommandsAsync(
                "MSET",
                [DoMSetSingleAsync, DoMSetMultiAsync]
            );

            static async Task DoMSetSingleAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("MSET", ["foo", "bar"]);
                ClassicAssert.AreEqual("OK", val);
            }

            static async Task DoMSetMultiAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("MSET", ["foo", "bar", "fizz", "buzz"]);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task MSetNXACLsAsync()
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

                ClassicAssert.AreEqual(1, val);
            }

            async Task DoMSetNXMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("MSETNX", [$"foo-{count}", "bar", $"fizz-{count}", "buzz"]);
                count++;

                ClassicAssert.AreEqual(1, val);
            }
        }

        [Test]
        public async Task MultiACLsAsync()
        {
            await CheckCommandsAsync(
                "MULTI",
                [DoMultiAsync],
                skipPing: true,
                skipAclCheckCmd: true
            );

            static async Task DoMultiAsync(GarnetClient client)
            {
                try
                {
                    string val = await client.ExecuteForStringResultAsync("MULTI");
                    ClassicAssert.AreEqual("OK", val);
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
        public async Task PersistACLsAsync()
        {
            await CheckCommandsAsync(
                "PERSIST",
                [DoPersistAsync]
            );

            static async Task DoPersistAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PERSIST", ["foo"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task PExpireACLsAsync()
        {
            // TODO: pexpire doesn't support combinations of flags (XX GT, XX LT are legal) so those will need to be tested when implemented

            await CheckCommandsAsync(
                "PEXPIRE",
                [DoPExpireAsync, DoPExpireNXAsync, DoPExpireXXAsync, DoPExpireGTAsync, DoPExpireLTAsync]
            );

            static async Task DoPExpireAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PEXPIRE", ["foo", "10"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoPExpireNXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PEXPIRE", ["foo", "10", "NX"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoPExpireXXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PEXPIRE", ["foo", "10", "XX"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoPExpireGTAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PEXPIRE", ["foo", "10", "GT"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoPExpireLTAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PEXPIRE", ["foo", "10", "LT"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task PFAddACLsAsync()
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

                ClassicAssert.AreEqual(1, val);
            }

            async Task DoPFAddMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PFADD", [$"foo-{count}", "bar", "fizz"]);
                count++;

                ClassicAssert.AreEqual(1, val);
            }
        }

        [Test]
        public async Task PFCountACLsAsync()
        {
            await CheckCommandsAsync(
                "PFCOUNT",
                [DoPFCountSingleAsync, DoPFCountMultiAsync]
            );

            static async Task DoPFCountSingleAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PFCOUNT", ["foo"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoPFCountMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PFCOUNT", ["foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task PFMergeACLsAsync()
        {
            await CheckCommandsAsync(
                "PFMERGE",
                [DoPFMergeSingleAsync, DoPFMergeMultiAsync]
            );

            static async Task DoPFMergeSingleAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("PFMERGE", ["foo"]);
                ClassicAssert.AreEqual("OK", val);
            }

            static async Task DoPFMergeMultiAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("PFMERGE", ["foo", "bar"]);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task PingACLsAsync()
        {
            await CheckCommandsAsync(
                "PING",
                [DoPingAsync, DoPingMessageAsync]
            );

            static async Task DoPingAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("PING");
                ClassicAssert.AreEqual("PONG", val);
            }

            static async Task DoPingMessageAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("PING", ["hello"]);
                ClassicAssert.AreEqual("hello", val);
            }
        }

        [Test]
        public async Task PSetEXACLsAsync()
        {
            await CheckCommandsAsync(
                "PSETEX",
                [DoPSetEXAsync]
            );

            static async Task DoPSetEXAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("PSETEX", ["foo", "10", "bar"]);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task PSubscribeACLsAsync()
        {
            // PSUBSCRIBE is sufficient weird that all we care to test is forbidding it
            await CheckCommandsAsync(
                "PSUBSCRIBE",
                [DoPSubscribePatternAsync],
                skipPermitted: true
            );

            static async Task DoPSubscribePatternAsync(GarnetClient client)
            {
                await client.ExecuteForStringResultAsync("PSUBSCRIBE", ["channel"]);
                Assert.Fail("Should not reach this point");
            }
        }

        [Test]
        public async Task PUnsubscribeACLsAsync()
        {
            await CheckCommandsAsync(
                "PUNSUBSCRIBE",
                [DoPUnsubscribePatternAsync]
            );

            static async Task DoPUnsubscribePatternAsync(GarnetClient client)
            {
                string[] res = await client.ExecuteForStringArrayResultAsync("PUNSUBSCRIBE", ["foo"]);
                ClassicAssert.IsNotNull(res);
            }
        }

        [Test]
        public async Task PTTLACLsAsync()
        {
            await CheckCommandsAsync(
                "PTTL",
                [DoPTTLAsync]
            );

            static async Task DoPTTLAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PTTL", ["foo"]);
                ClassicAssert.AreEqual(-2, val);
            }
        }

        [Test]
        public async Task PublishACLsAsync()
        {
            await CheckCommandsAsync(
                "PUBLISH",
                [DoPublishAsync]
            );

            static async Task DoPublishAsync(GarnetClient client)
            {
                long count = await client.ExecuteForLongResultAsync("PUBLISH", ["foo", "bar"]);
                ClassicAssert.AreEqual(0, count);
            }
        }

        [Test]
        public async Task PubSubChannelsACLsAsync()
        {
            await CheckCommandsAsync(
                "PUBSUB CHANNELS",
                [DoPubSubChannelsAsync]
            );

            static async Task DoPubSubChannelsAsync(GarnetClient client)
            {
                var count = await client.ExecuteForStringArrayResultAsync("PUBSUB", ["CHANNELS"]);
                CollectionAssert.IsEmpty(count);
            }
        }

        [Test]
        public async Task PubSubNumPatACLsAsync()
        {
            await CheckCommandsAsync(
                "PUBSUB NUMPAT",
                [DoPubSubNumPatAsync]
            );

            static async Task DoPubSubNumPatAsync(GarnetClient client)
            {
                var count = await client.ExecuteForLongResultAsync("PUBSUB", ["NUMPAT"]);
                ClassicAssert.AreEqual(0, count);
            }
        }

        [Test]
        public async Task PubSubNumSubACLsAsync()
        {
            await CheckCommandsAsync(
                "PUBSUB NUMSUB",
                [DoPubSubNumSubAsync]
            );

            static async Task DoPubSubNumSubAsync(GarnetClient client)
            {
                var count = await client.ExecuteForStringArrayResultAsync("PUBSUB", ["NUMSUB"]);
                CollectionAssert.IsEmpty(count);
            }
        }

        [Test]
        public async Task ReadOnlyACLsAsync()
        {
            await CheckCommandsAsync(
                "READONLY",
                [DoReadOnlyAsync]
            );

            static async Task DoReadOnlyAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("READONLY");
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task ReadWriteACLsAsync()
        {
            await CheckCommandsAsync(
                "READWRITE",
                [DoReadWriteAsync]
            );

            static async Task DoReadWriteAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("READWRITE");
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task RegisterCSACLsAsync()
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
                    if (e.Message == "ERR wrong number of arguments for 'REGISTERCS' command")
                    {
                        return;
                    }

                    throw;
                }
            }
        }

        [Test]
        public async Task ExpireTimeACLsAsync()
        {
            await CheckCommandsAsync(
                "EXPIRETIME",
                [DoExpireTimeAsync]
            );

            static async Task DoExpireTimeAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("EXPIRETIME", ["foo"]);
                ClassicAssert.AreEqual(-2, val);
            }
        }

        [Test]
        public async Task PExpireTimeACLsAsync()
        {
            await CheckCommandsAsync(
                "PEXPIRETIME",
                [DoPExpireTimeAsync]
            );

            static async Task DoPExpireTimeAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("PEXPIRETIME", ["foo"]);
                ClassicAssert.AreEqual(-2, val);
            }
        }

        [Test]
        public async Task RenameACLsAsync()
        {
            await CheckCommandsAsync(
                "RENAME",
                [DoRENAMEAsync]
            );

            static async Task DoRENAMEAsync(GarnetClient client)
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
        public async Task RenameNxACLsAsync()
        {
            await CheckCommandsAsync(
                "RENAMENX",
                [DoRENAMENXAsync]
            );

            static async Task DoRENAMENXAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("RENAMENX", ["foo", "bar"]);
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
        public async Task ReplicaOfACLsAsync()
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

        [Test]
        public async Task RunTxpACLsAsync()
        {
            // TODO: RUNTXP semantics are a bit unclear... expand test later

            // TODO: RUNTXP appears to break the command stream when malformed, so only test that we can forbid it
            await CheckCommandsAsync(
                "RUNTXP",
                [DoRunTxpAsync],
                skipPermitted: true
            );

            static async Task DoRunTxpAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("RUNTXP", ["4"]);
                    Assert.Fail("Should be reachable, command is malformed");
                }
                catch (Exception e)
                {
                    if (e.Message == "ERR Could not get transaction procedure")
                    {
                        return;
                    }

                    throw;
                }
            }
        }

        [Test]
        public async Task SaveACLsAsync()
        {
            await CheckCommandsAsync(
               "SAVE",
               [DoSaveAsync]
           );

            static async Task DoSaveAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SAVE");
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task ScanACLsAsync()
        {
            await CheckCommandsAsync(
                "SCAN",
                [DoScanAsync, DoScanMatchAsync, DoScanCountAsync, DoScanTypeAsync, DoScanMatchCountAsync, DoScanMatchTypeAsync, DoScanCountTypeAsync, DoScanMatchCountTypeAsync],
                skipPermitted: true
            );

            static async Task DoScanAsync(GarnetClient client)
            {
                // SCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SCAN", ["0"]);
            }

            static async Task DoScanMatchAsync(GarnetClient client)
            {
                // SCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SCAN", ["0", "MATCH", "*"]);
            }

            static async Task DoScanCountAsync(GarnetClient client)
            {
                // SCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SCAN", ["0", "COUNT", "5"]);
            }

            static async Task DoScanTypeAsync(GarnetClient client)
            {
                // SCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SCAN", ["0", "TYPE", "zset"]);
            }

            static async Task DoScanMatchCountAsync(GarnetClient client)
            {
                // SCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SCAN", ["0", "MATCH", "*", "COUNT", "5"]);
            }

            static async Task DoScanMatchTypeAsync(GarnetClient client)
            {
                // SCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SCAN", ["0", "MATCH", "*", "TYPE", "zset"]);
            }

            static async Task DoScanCountTypeAsync(GarnetClient client)
            {
                // SCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SCAN", ["0", "COUNT", "5", "TYPE", "zset"]);
            }

            static async Task DoScanMatchCountTypeAsync(GarnetClient client)
            {
                // SCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SCAN", ["0", "MATCH", "*", "COUNT", "5", "TYPE", "zset"]);
            }
        }

        [Test]
        public async Task SecondaryOfACLsAsync()
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
        public async Task SelectACLsAsync()
        {
            await CheckCommandsAsync(
                "SELECT",
                [DoSelectAsync]
            );

            static async Task DoSelectAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SELECT", ["0"]);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task SetACLsAsync()
        {
            // SET doesn't support most extra commands, so this is just key value

            await CheckCommandsAsync(
                "SET",
                [DoSetAsync, DoSetExNxAsync, DoSetXxNxAsync, DoSetKeepTtlAsync, DoSetKeepTtlXxAsync]
            );

            static async Task DoSetAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SET", ["foo", "bar"]);
                ClassicAssert.AreEqual("OK", val);
            }

            static async Task DoSetExNxAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SET", ["foo", "bar", "NX", "EX", "100"]);
                ClassicAssert.IsNull(val);
            }

            static async Task DoSetXxNxAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SET", ["foo", "bar", "XX", "EX", "100"]);
                ClassicAssert.AreEqual("OK", val);
            }

            static async Task DoSetKeepTtlAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SET", ["foo", "bar", "KEEPTTL"]);
                ClassicAssert.AreEqual("OK", val);
            }

            static async Task DoSetKeepTtlXxAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SET", ["foo", "bar", "XX", "KEEPTTL"]);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task GetEtagACLsAsync()
        {
            await CheckCommandsAsync(
                "GETETAG",
                [DoGetEtagAsync]
            );

            static async Task DoGetEtagAsync(GarnetClient client)
            {
                var res = await client.ExecuteForStringResultAsync("GETETAG", ["foo"]);
                ClassicAssert.IsNull(res);
            }
        }

        [Test]
        public async Task SetBitACLsAsync()
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
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SetEXACLsAsync()
        {
            await CheckCommandsAsync(
                "SETEX",
                [DoSetEXAsync]
            );

            static async Task DoSetEXAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SETEX", ["foo", "10", "bar"]);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task SetNXACLsAsync()
        {
            int keyIx = 0;

            await CheckCommandsAsync(
                "SETNX",
                [DoSetIfNotExistAsync]
            );

            async Task DoSetIfNotExistAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("SETNX", [$"foo-{keyIx++}", "bar"]);
                ClassicAssert.AreEqual(1, val);
            }
        }

        [Test]
        public async Task SetRangeACLsAsync()
        {
            await CheckCommandsAsync(
                "SETRANGE",
                [DoSetRangeAsync]
            );

            static async Task DoSetRangeAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SETRANGE", ["foo", "10", "bar"]);
                ClassicAssert.AreEqual(13, val);
            }
        }

        [Test]
        public async Task StrLenACLsAsync()
        {
            await CheckCommandsAsync(
                "STRLEN",
                [DoStrLenAsync]
            );

            static async Task DoStrLenAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("STRLEN", ["foo"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SAddACLsAsync()
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

                ClassicAssert.AreEqual(1, val);
            }

            async Task DoSAddMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SADD", [$"foo-{count}", "bar", "fizz"]);
                count++;

                ClassicAssert.AreEqual(2, val);
            }
        }

        [Test]
        public async Task SRemACLsAsync()
        {
            await CheckCommandsAsync(
                "SREM",
                [DoSRemAsync, DoSRemMultiAsync]
            );

            static async Task DoSRemAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SREM", ["foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoSRemMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SREM", ["foo", "bar", "fizz"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SPopACLsAsync()
        {
            await CheckCommandsAsync(
                "SPOP",
                [DoSPopAsync, DoSPopCountAsync]
            );

            static async Task DoSPopAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SPOP", ["foo"]);
                ClassicAssert.IsNull(val);
            }

            static async Task DoSPopCountAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SPOP", ["foo", "11"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task SMembersACLsAsync()
        {
            await CheckCommandsAsync(
                "SMEMBERS",
                [DoSMembersAsync]
            );

            static async Task DoSMembersAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SMEMBERS", ["foo"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task SCardACLsAsync()
        {
            await CheckCommandsAsync(
                "SCARD",
                [DoSCardAsync]
            );

            static async Task DoSCardAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SCARD", ["foo"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SScanACLsAsync()
        {
            await CheckCommandsAsync(
                "SSCAN",
                [DoSScanAsync, DoSScanMatchAsync, DoSScanCountAsync, DoSScanMatchCountAsync],
                skipPermitted: true
            );

            static async Task DoSScanAsync(GarnetClient client)
            {
                // SSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SSCAN", ["foo", "0"]);
            }

            static async Task DoSScanMatchAsync(GarnetClient client)
            {
                // SSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SSCAN", ["foo", "0", "MATCH", "*"]);
            }

            static async Task DoSScanCountAsync(GarnetClient client)
            {
                // SSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SSCAN", ["foo", "0", "COUNT", "5"]);
            }

            static async Task DoSScanMatchCountAsync(GarnetClient client)
            {
                // SSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SSCAN", ["foo", "0", "MATCH", "*", "COUNT", "5"]);
            }
        }

        [Test]
        public async Task SlaveOfACLsAsync()
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
        public async Task SlowlogGetACLsAsync()
        {
            await CheckCommandsAsync(
                "SLOWLOG GET",
                [DoSlowlogGetAsync]
            );

            static async Task DoSlowlogGetAsync(GarnetClient client)
            {
                string[] res = await client.ExecuteForStringArrayResultAsync("SLOWLOG", ["GET"]);
                ClassicAssert.AreEqual(0, res.Length);
            }
        }

        [Test]
        public async Task SlowlogHelpACLsAsync()
        {
            await CheckCommandsAsync(
                "SLOWLOG HELP",
                [DoSlowlogHelpAsync]
            );

            static async Task DoSlowlogHelpAsync(GarnetClient client)
            {
                string[] res = await client.ExecuteForStringArrayResultAsync("SLOWLOG", ["HELP"]);
                ClassicAssert.AreEqual(12, res.Length);
            }
        }

        [Test]
        public async Task SlowlogLenACLsAsync()
        {
            await CheckCommandsAsync(
                "SLOWLOG LEN",
                [DoSlowlogLenAsync]
            );

            static async Task DoSlowlogLenAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("SLOWLOG", ["LEN"]);
                ClassicAssert.AreEqual("0", res);
            }
        }

        [Test]
        public async Task SlowlogResetACLsAsync()
        {
            await CheckCommandsAsync(
                "SLOWLOG RESET",
                [DoSlowlogResetAsync]
            );

            static async Task DoSlowlogResetAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("SLOWLOG", ["RESET"]);
                ClassicAssert.AreEqual("OK", res);
            }
        }

        [Test]
        public async Task SMoveACLsAsync()
        {
            await CheckCommandsAsync(
                "SMOVE",
                [DoSMoveAsync]
            );

            static async Task DoSMoveAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SMOVE", ["foo", "bar", "fizz"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SRandMemberACLsAsync()
        {
            await CheckCommandsAsync(
                "SRANDMEMBER",
                [DoSRandMemberAsync, DoSRandMemberCountAsync]
            );

            static async Task DoSRandMemberAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SRANDMEMBER", ["foo"]);
                ClassicAssert.IsNull(val);
            }

            static async Task DoSRandMemberCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SRANDMEMBER", ["foo", "5"]);
                ClassicAssert.IsNotNull(val);
            }
        }

        [Test]
        public async Task SIsMemberACLsAsync()
        {
            await CheckCommandsAsync(
                "SISMEMBER",
                [DoSIsMemberAsync]
            );

            static async Task DoSIsMemberAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SISMEMBER", ["foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SMIsMemberACLsAsync()
        {
            await CheckCommandsAsync(
                "SMISMEMBER",
                [DoSMultiIsMemberAsync]
            );

            static async Task DoSMultiIsMemberAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SMISMEMBER", ["foo", "5"]);
                ClassicAssert.IsNotNull(val);
            }
        }

        [Test]
        public async Task SubscribeACLsAsync()
        {
            // SUBSCRIBE is sufficient weird that all we care to test is forbidding it
            await CheckCommandsAsync(
                "SUBSCRIBE",
                [DoSubscribeAsync],
                skipPermitted: true
            );

            static async Task DoSubscribeAsync(GarnetClient client)
            {
                await client.ExecuteForStringResultAsync("SUBSCRIBE", ["channel"]);
                Assert.Fail("Shouldn't reach this point");
            }
        }

        [Test]
        public async Task SUnionACLsAsync()
        {
            await CheckCommandsAsync(
                "SUNION",
                [DoSUnionAsync, DoSUnionMultiAsync]
            );

            static async Task DoSUnionAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SUNION", ["foo"]);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoSUnionMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SUNION", ["foo", "bar"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task SUnionStoreACLsAsync()
        {
            await CheckCommandsAsync(
                "SUNIONSTORE",
                [DoSUnionStoreAsync, DoSUnionStoreMultiAsync]
            );

            static async Task DoSUnionStoreAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SUNIONSTORE", ["dest", "foo"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoSUnionStoreMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SUNIONSTORE", ["dest", "foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SDiffACLsAsync()
        {
            await CheckCommandsAsync(
                "SDIFF",
                [DoSDiffAsync, DoSDiffMultiAsync]
            );

            static async Task DoSDiffAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SDIFF", ["foo"]);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoSDiffMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SDIFF", ["foo", "bar"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task SDiffStoreACLsAsync()
        {
            await CheckCommandsAsync(
                "SDIFFSTORE",
                [DoSDiffStoreAsync, DoSDiffStoreMultiAsync]
            );

            static async Task DoSDiffStoreAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SDIFFSTORE", ["dest", "foo"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoSDiffStoreMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SDIFFSTORE", ["dest", "foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SInterACLsAsync()
        {
            await CheckCommandsAsync(
                "SINTER",
                [DoSDiffAsync, DoSDiffMultiAsync]
            );

            static async Task DoSDiffAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SINTER", ["foo"]);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoSDiffMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SINTER", ["foo", "bar"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task SInterCardACLsAsync()
        {
            await CheckCommandsAsync(
                "SINTERCARD",
                [DoUnionAsync]
            );

            static async Task DoUnionAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("SINTERCARD", ["2", "foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SInterStoreACLsAsync()
        {
            await CheckCommandsAsync(
                "SINTERSTORE",
                [DoSDiffStoreAsync, DoSDiffStoreMultiAsync]
            );

            static async Task DoSDiffStoreAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SINTERSTORE", ["dest", "foo"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoSDiffStoreMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SINTERSTORE", ["dest", "foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task GeoAddACLsAsync()
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

                ClassicAssert.AreEqual(1, val);
            }

            async Task DoGeoAddNXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GEOADD", [$"foo-{count}", "NX", "90", "90", "bar"]);
                count++;

                ClassicAssert.AreEqual(1, val);
            }

            async Task DoGeoAddNXCHAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GEOADD", [$"foo-{count}", "NX", "CH", "90", "90", "bar"]);
                count++;

                ClassicAssert.AreEqual(1, val);
            }

            async Task DoGeoAddMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GEOADD", [$"foo-{count}", "90", "90", "bar", "45", "45", "fizz"]);
                count++;

                ClassicAssert.AreEqual(2, val);
            }

            async Task DoGeoAddNXMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GEOADD", [$"foo-{count}", "NX", "90", "90", "bar", "45", "45", "fizz"]);
                count++;

                ClassicAssert.AreEqual(2, val);
            }

            async Task DoGeoAddNXCHMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GEOADD", [$"foo-{count}", "NX", "CH", "90", "90", "bar", "45", "45", "fizz"]);
                count++;

                ClassicAssert.AreEqual(2, val);
            }
        }

        [Test]
        public async Task GeoHashACLsAsync()
        {
            // TODO: GEOHASH responses do not match Redis when keys are missing.
            // So create some keys to make testing ACLs easier.
            using var outerClient = await CreateGarnetClientAsync(DefaultUser, DefaultPassword);
            ClassicAssert.AreEqual(1, await outerClient.ExecuteForLongResultAsync("GEOADD", ["foo", "10", "10", "bar"]));
            ClassicAssert.AreEqual(1, await outerClient.ExecuteForLongResultAsync("GEOADD", ["foo", "20", "20", "fizz"]));

            await CheckCommandsAsync(
                "GEOHASH",
                [DoGeoHashAsync, DoGeoHashSingleAsync, DoGeoHashMultiAsync]
            );

            static async Task DoGeoHashAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("GEOHASH", ["foo"]);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoGeoHashSingleAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("GEOHASH", ["foo", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.IsNotNull(val[0]);
            }

            static async Task DoGeoHashMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("GEOHASH", ["foo", "bar", "fizz"]);
                ClassicAssert.AreEqual(2, val.Length);
                ClassicAssert.IsNotNull(val[0]);
                ClassicAssert.IsNotNull(val[1]);
            }
        }

        [Test]
        public async Task GeoDistACLsAsync()
        {
            await CheckCommandsAsync(
                "GEODIST",
                [DoGetDistAsync, DoGetDistMAsync]
            );

            static async Task DoGetDistAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("GEODIST", ["foo", "bar", "fizz"]);
                ClassicAssert.IsNull(val);
            }

            static async Task DoGetDistMAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("GEODIST", ["foo", "bar", "fizz", "M"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task GeoPosACLsAsync()
        {
            await CheckCommandsAsync(
                "GEOPOS",
                [DoGeoPosAsync, DoGeoPosMultiAsync],
                skipPermitted: true
            );

            static async Task DoGeoPosAsync(GarnetClient client)
            {
                // GEOPOS replies with an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("GEOPOS", ["foo"]);
            }

            static async Task DoGeoPosMultiAsync(GarnetClient client)
            {
                // GEOPOS replies with an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("GEOPOS", ["foo", "bar"]);
            }
        }

        [Test]
        public async Task GeoRadiusACLsAsync()
        {
            await CheckCommandsAsync(
                "GEORADIUS",
                [DoGeoRadiusAsync],
                skipPermitted: true
            );

            static async Task DoGeoRadiusAsync(GarnetClient client)
            {
                await client.ExecuteForStringResultAsync("GEORADIUS", ["foo", "0", "85", "10", "km"]);
            }
        }

        [Test]
        public async Task GeoRadiusROACLsAsync()
        {
            await CheckCommandsAsync(
                "GEORADIUS_RO",
                [DoGeoRadiusROAsync],
                skipPermitted: true
            );

            static async Task DoGeoRadiusROAsync(GarnetClient client)
            {
                await client.ExecuteForStringResultAsync("GEORADIUS_RO", ["foo", "0", "85", "10", "km"]);
            }
        }

        [Test]
        public async Task GeoRadiusByMemberACLsAsync()
        {
            await CheckCommandsAsync(
                "GEORADIUSBYMEMBER",
                [DoGeoRadiusByMemberAsync],
                skipPermitted: true
            );

            static async Task DoGeoRadiusByMemberAsync(GarnetClient client)
            {
                await client.ExecuteForStringResultAsync("GEORADIUSBYMEMBER", ["foo", "bar", "10", "km"]);
            }
        }

        [Test]
        public async Task GeoRadiusByMemberROACLsAsync()
        {
            await CheckCommandsAsync(
                "GEORADIUSBYMEMBER_RO",
                [DoGeoRadiusByMemberROAsync],
                skipPermitted: true
            );

            static async Task DoGeoRadiusByMemberROAsync(GarnetClient client)
            {
                await client.ExecuteForStringResultAsync("GEORADIUSBYMEMBER_RO", ["foo", "bar", "10", "km"]);
            }
        }

        [Test]
        public async Task GeoSearchACLsAsync()
        {
            await CheckCommandsAsync(
                "GEOSEARCH",
                [DoGeoSearchAsync],
                skipPermitted: true
            );

            static async Task DoGeoSearchAsync(GarnetClient client)
            {
                // GEOSEARCH replies with an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("GEOSEARCH", ["foo", "FROMMEMBER", "bar", "BYBOX", "2", "2", "M"]);
            }
        }

        [Test]
        public async Task GeoSearchStoreACLsAsync()
        {
            await CheckCommandsAsync(
                "GEOSEARCHSTORE",
                [DoGeoSearchStoreAsync],
                skipPermitted: true
            );

            static async Task DoGeoSearchStoreAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("GEOSEARCHSTORE", ["bar", "foo", "FROMMEMBER", "bar", "BYBOX", "2", "2", "M", "STOREDIST"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZAddACLsAsync()
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
                ClassicAssert.AreEqual(1, val);
            }

            async Task DoZAddMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZADD", [$"foo-{count}", "10", "bar", "20", "fizz"]);
                count++;
                ClassicAssert.AreEqual(2, val);
            }
        }

        [Test]
        public async Task ZCardACLsAsync()
        {
            await CheckCommandsAsync(
                "ZCARD",
                [DoZCardAsync]
            );

            static async Task DoZCardAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZCARD", ["foo"]);
                ClassicAssert.AreEqual(0, val);
            }
        }



        [Test]
        public async Task ZMPopACLsAsync()
        {
            await CheckCommandsAsync(
                "ZMPOP",
                [DoZMPopAsync, DoZMPopCountAsync]
            );

            static async Task DoZMPopAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZMPOP", ["2", "foo", "bar", "MIN"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.IsNull(val[0]);
            }

            static async Task DoZMPopCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZMPOP", ["2", "foo", "bar", "MAX", "COUNT", "10"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.IsNull(val[0]);
            }
        }

        [Test]
        public async Task ZPopMaxACLsAsync()
        {
            await CheckCommandsAsync(
                "ZPOPMAX",
                [DoZPopMaxAsync, DoZPopMaxCountAsync]
            );

            static async Task DoZPopMaxAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZPOPMAX", ["foo"]);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZPopMaxCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZPOPMAX", ["foo", "10"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZScoreACLsAsync()
        {
            await CheckCommandsAsync(
                "ZSCORE",
                [DoZScoreAsync]
            );

            static async Task DoZScoreAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ZSCORE", ["foo", "bar"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task ZRemACLsAsync()
        {
            await CheckCommandsAsync(
                "ZREM",
                [DoZRemAsync, DoZRemMultiAsync]
            );

            static async Task DoZRemAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZREM", ["foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoZRemMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZREM", ["foo", "bar", "fizz"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZCountACLsAsync()
        {
            await CheckCommandsAsync(
                "ZCOUNT",
                [DoZCountAsync]
            );

            static async Task DoZCountAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZCOUNT", ["foo", "10", "20"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZIncrByACLsAsync()
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
                ClassicAssert.AreEqual(10, double.Parse(val));
            }
        }

        [Test]
        public async Task ZRankACLsAsync()
        {
            await CheckCommandsAsync(
                "ZRANK",
                [DoZRankAsync]
            );

            static async Task DoZRankAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ZRANK", ["foo", "bar"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task ZRangeACLsAsync()
        {
            // TODO: ZRange has loads of options, come back and test all the different lengths

            await CheckCommandsAsync(
                "ZRANGE",
                [DoZRangeAsync]
            );

            static async Task DoZRangeAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANGE", ["key", "10", "20"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZRevRangeByLexACLsAsync()
        {
            await CheckCommandsAsync(
                "ZREVRANGEBYLEX",
                [DoZRevRangeByLexAsync]
            );

            static async Task DoZRevRangeByLexAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZREVRANGEBYLEX", ["key", "[abc", "[def"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZRangeStoreACLsAsync()
        {
            await CheckCommandsAsync(
                "ZRANGESTORE",
                [DoZRangeStoreAsync]
            );

            static async Task DoZRangeStoreAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("ZRANGESTORE", ["dkey", "key", "0", "-1"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZRangeByLexACLsAsync()
        {
            await CheckCommandsAsync(
                "ZRANGEBYLEX",
                [DoZRangeByLexAsync, DoZRangeByLexLimitAsync]
            );

            static async Task DoZRangeByLexAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANGEBYLEX", ["key", "[abc", "[def"]);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZRangeByLexLimitAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANGEBYLEX", ["key", "[abc", "[def", "LIMIT", "2", "3"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZRangeByScoreACLsAsync()
        {
            await CheckCommandsAsync(
                "ZRANGEBYSCORE",
                [DoZRangeByScoreAsync, DoZRangeByScoreWithScoresAsync, DoZRangeByScoreLimitAsync, DoZRangeByScoreWithScoresLimitAsync]
            );

            static async Task DoZRangeByScoreAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANGEBYSCORE", ["key", "10", "20"]);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZRangeByScoreWithScoresAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANGEBYSCORE", ["key", "10", "20", "WITHSCORES"]);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZRangeByScoreLimitAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANGEBYSCORE", ["key", "10", "20", "LIMIT", "2", "3"]);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZRangeByScoreWithScoresLimitAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANGEBYSCORE", ["key", "10", "20", "WITHSCORES", "LIMIT", "2", "3"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZRevRangeACLsAsync()
        {
            await CheckCommandsAsync(
                "ZREVRANGE",
                [DoZRevRangeAsync, DoZRevRangeWithScoresAsync]
            );

            static async Task DoZRevRangeAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZREVRANGE", ["key", "10", "20"]);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZRevRangeWithScoresAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZREVRANGE", ["key", "10", "20", "WITHSCORES"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZRevRangeByScoreACLsAsync()
        {
            await CheckCommandsAsync(
                "ZREVRANGEBYSCORE",
                [DoZRevRangeByScoreAsync, DoZRevRangeByScoreWithScoresAsync, DoZRevRangeByScoreLimitAsync, DoZRevRangeByScoreWithScoresLimitAsync]
            );

            static async Task DoZRevRangeByScoreAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZREVRANGEBYSCORE", ["key", "10", "20"]);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZRevRangeByScoreWithScoresAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZREVRANGEBYSCORE", ["key", "10", "20", "WITHSCORES"]);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZRevRangeByScoreLimitAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZREVRANGEBYSCORE", ["key", "10", "20", "LIMIT", "2", "3"]);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZRevRangeByScoreWithScoresLimitAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZREVRANGEBYSCORE", ["key", "10", "20", "WITHSCORES", "LIMIT", "2", "3"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZRevRankACLsAsync()
        {
            await CheckCommandsAsync(
                "ZREVRANK",
                [DoZRevRankAsync]
            );

            static async Task DoZRevRankAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ZREVRANK", ["foo", "bar"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task ZRemRangeByLexACLsAsync()
        {
            await CheckCommandsAsync(
                "ZREMRANGEBYLEX",
                [DoZRemRangeByLexAsync]
            );

            static async Task DoZRemRangeByLexAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZREMRANGEBYLEX", ["foo", "[abc", "[def"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZRemRangeByRankACLsAsync()
        {
            await CheckCommandsAsync(
                "ZREMRANGEBYRANK",
                [DoZRemRangeByRankAsync]
            );

            static async Task DoZRemRangeByRankAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZREMRANGEBYRANK", ["foo", "10", "20"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZRemRangeByScoreACLsAsync()
        {
            await CheckCommandsAsync(
                "ZREMRANGEBYSCORE",
                [DoZRemRangeByRankAsync]
            );

            static async Task DoZRemRangeByRankAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZREMRANGEBYSCORE", ["foo", "10", "20"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZLexCountACLsAsync()
        {
            await CheckCommandsAsync(
                "ZLEXCOUNT",
                [DoZLexCountAsync]
            );

            static async Task DoZLexCountAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZLEXCOUNT", ["foo", "[abc", "[def"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZPopMinACLsAsync()
        {
            await CheckCommandsAsync(
                "ZPOPMIN",
                [DoZPopMinAsync, DoZPopMinCountAsync]
            );

            static async Task DoZPopMinAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZPOPMIN", ["foo"]);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZPopMinCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZPOPMIN", ["foo", "10"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZRandMemberACLsAsync()
        {
            await CheckCommandsAsync(
                "ZRANDMEMBER",
                [DoZRandMemberAsync, DoZRandMemberCountAsync, DoZRandMemberCountWithScoresAsync]
            );

            static async Task DoZRandMemberAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ZRANDMEMBER", ["foo"]);
                ClassicAssert.IsNull(val);
            }

            static async Task DoZRandMemberCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANDMEMBER", ["foo", "10"]);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZRandMemberCountWithScoresAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANDMEMBER", ["foo", "10", "WITHSCORES"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZDiffACLsAsync()
        {
            // TODO: ZDIFF doesn't implement WITHSCORES correctly right now - come back and cover when fixed

            await CheckCommandsAsync(
                "ZDIFF",
                [DoZDiffAsync, DoZDiffMultiAsync]
            );

            static async Task DoZDiffAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZDIFF", ["1", "foo"]);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZDiffMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZDIFF", ["2", "foo", "bar"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZDiffStoreACLsAsync()
        {
            await CheckCommandsAsync(
                "ZDIFFSTORE",
                [DoZDiffStoreAsync]
            );

            static async Task DoZDiffStoreAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("ZDIFFSTORE", ["keyZ", "2", "foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZInterACLsAsync()
        {
            await CheckCommandsAsync(
                "ZINTER",
                [DoZInterAsync]
            );

            static async Task DoZInterAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZINTER", ["2", "foo", "bar"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZInterCardACLsAsync()
        {
            await CheckCommandsAsync(
                "ZINTERCARD",
                [DoZInterCardAsync]
            );

            static async Task DoZInterCardAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("ZINTERCARD", ["2", "foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZInterStoreACLsAsync()
        {
            await CheckCommandsAsync(
                "ZINTERSTORE",
                [DoZInterStoreAsync]
            );

            static async Task DoZInterStoreAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("ZINTERSTORE", ["keyZ", "2", "foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZUnionACLsAsync()
        {
            await CheckCommandsAsync(
                "ZUNION",
                [DoZUnionAsync]
            );

            static async Task DoZUnionAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZUNION", ["2", "foo", "bar"]);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZUnionStoreACLsAsync()
        {
            await CheckCommandsAsync(
                "ZUNIONSTORE",
                [DoZUnionStoreAsync]
            );

            static async Task DoZUnionStoreAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("ZUNIONSTORE", ["keyZ", "2", "foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZScanACLsAsync()
        {
            await CheckCommandsAsync(
                "ZSCAN",
                [DoZScanAsync, DoZScanMatchAsync, DoZScanCountAsync, DoZScanNoValuesAsync, DoZScanMatchCountAsync, DoZScanMatchNoValuesAsync, DoZScanCountNoValuesAsync, DoZScanMatchCountNoValuesAsync],
                skipPermitted: true
            );

            static async Task DoZScanAsync(GarnetClient client)
            {
                // ZSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("ZSCAN", ["foo", "0"]);
            }

            static async Task DoZScanMatchAsync(GarnetClient client)
            {
                // ZSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("ZSCAN", ["foo", "0", "MATCH", "*"]);
            }

            static async Task DoZScanCountAsync(GarnetClient client)
            {
                // ZSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("ZSCAN", ["foo", "0", "COUNT", "2"]);
            }

            static async Task DoZScanNoValuesAsync(GarnetClient client)
            {
                // ZSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("ZSCAN", ["foo", "0", "NOVALUES"]);
            }

            static async Task DoZScanMatchCountAsync(GarnetClient client)
            {
                // ZSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("ZSCAN", ["foo", "0", "MATCH", "*", "COUNT", "2"]);
            }

            static async Task DoZScanMatchNoValuesAsync(GarnetClient client)
            {
                // ZSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("ZSCAN", ["foo", "0", "MATCH", "*", "NOVALUES"]);
            }

            static async Task DoZScanCountNoValuesAsync(GarnetClient client)
            {
                // ZSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("ZSCAN", ["foo", "0", "COUNT", "0", "NOVALUES"]);
            }

            static async Task DoZScanMatchCountNoValuesAsync(GarnetClient client)
            {
                // ZSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("ZSCAN", ["foo", "0", "MATCH", "*", "COUNT", "0", "NOVALUES"]);
            }
        }

        [Test]
        public async Task ZMScoreACLsAsync()
        {
            await CheckCommandsAsync(
                "ZMSCORE",
                [DoZDiffAsync, DoZDiffMultiAsync]
            );

            static async Task DoZDiffAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZMSCORE", ["foo", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.IsNull(val[0]);
            }

            static async Task DoZDiffMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZMSCORE", ["foo", "bar", "fizz"]);
                ClassicAssert.AreEqual(2, val.Length);
                ClassicAssert.IsNull(val[0]);
                ClassicAssert.IsNull(val[1]);
            }
        }

        [Test]
        public async Task ZExpireACLsAsync()
        {
            await CheckCommandsAsync(
                "ZEXPIRE",
                [DoZExpireAsync]
            );

            static async Task DoZExpireAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZEXPIRE", ["foo", "1", "MEMBERS", "1", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.AreEqual("-2", val[0]);
            }
        }

        [Test]
        public async Task ZPExpireACLsAsync()
        {
            await CheckCommandsAsync(
                "ZPEXPIRE",
                [DoZPExpireAsync]
            );

            static async Task DoZPExpireAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZPEXPIRE", ["foo", "1", "MEMBERS", "1", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.AreEqual("-2", val[0]);
            }
        }

        [Test]
        public async Task ZExpireAtACLsAsync()
        {
            await CheckCommandsAsync(
                "ZEXPIREAT",
                [DoZExpireAtAsync]
            );

            static async Task DoZExpireAtAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZEXPIREAT", ["foo", DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeSeconds().ToString(), "MEMBERS", "1", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.AreEqual("-2", val[0]);
            }
        }

        [Test]
        public async Task ZPExpireAtACLsAsync()
        {
            await CheckCommandsAsync(
                "ZPEXPIREAT",
                [DoZPExpireAtAsync]
            );

            static async Task DoZPExpireAtAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZPEXPIREAT", ["foo", DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeMilliseconds().ToString(), "MEMBERS", "1", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.AreEqual("-2", val[0]);
            }
        }

        [Test]
        public async Task ZExpireTimeACLsAsync()
        {
            await CheckCommandsAsync(
                "ZEXPIRETIME",
                [DoZExpireTimeAsync]
            );

            static async Task DoZExpireTimeAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZEXPIRETIME", ["foo", "MEMBERS", "1", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.AreEqual("-2", val[0]);
            }
        }

        [Test]
        public async Task ZPExpireTimeACLsAsync()
        {
            await CheckCommandsAsync(
                "ZPEXPIRETIME",
                [DoZPExpireTimeAsync]
            );

            static async Task DoZPExpireTimeAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZPEXPIRETIME", ["foo", "MEMBERS", "1", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.AreEqual("-2", val[0]);
            }
        }

        [Test]
        public async Task ZTTLACLsAsync()
        {
            await CheckCommandsAsync(
                "ZTTL",
                [DoZETTLAsync]
            );

            static async Task DoZETTLAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZTTL", ["foo", "MEMBERS", "1", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.AreEqual("-2", val[0]);
            }
        }

        [Test]
        public async Task ZPTTLACLsAsync()
        {
            await CheckCommandsAsync(
                "ZPTTL",
                [DoZPETTLAsync]
            );

            static async Task DoZPETTLAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZPTTL", ["foo", "MEMBERS", "1", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.AreEqual("-2", val[0]);
            }
        }

        [Test]
        public async Task ZPersistACLsAsync()
        {
            await CheckCommandsAsync(
                "ZPERSIST",
                [DoZPersistAsync]
            );

            static async Task DoZPersistAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZPERSIST", ["foo", "MEMBERS", "1", "bar"]);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.AreEqual("-2", val[0]);
            }
        }

        [Test]
        public async Task ZCollectACLsAsync()
        {
            await CheckCommandsAsync(
                "ZCOLLECT",
                [DoZCollectAsync]
            );

            static async Task DoZCollectAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringResultAsync("ZCOLLECT", ["foo"]);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task TimeACLsAsync()
        {
            await CheckCommandsAsync(
                "TIME",
                [DoTimeAsync]
            );

            static async Task DoTimeAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("TIME");
                ClassicAssert.AreEqual(2, val.Length);
                ClassicAssert.IsTrue(long.Parse(val[0]) > 0);
                ClassicAssert.IsTrue(long.Parse(val[1]) >= 0);
            }
        }

        [Test]
        public async Task TTLACLsAsync()
        {
            await CheckCommandsAsync(
                "TTL",
                [DoTTLAsync]
            );

            static async Task DoTTLAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("TTL", ["foo"]);
                ClassicAssert.AreEqual(-2, val);
            }
        }

        [Test]
        public async Task DumpACLsAsync()
        {
            await CheckCommandsAsync(
                "DUMP",
                [DoDUMPAsync]
            );

            static async Task DoDUMPAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("DUMP", ["foo"]);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task RestoreACLsAsync()
        {
            var count = 0;

            await CheckCommandsAsync(
                "RESTORE",
                [DoRestoreAsync]
            );

            async Task DoRestoreAsync(GarnetClient client)
            {
                var payload = new byte[]
                {
                    0x00, // value type 
                    0x03, // length of payload
                    0x76, 0x61, 0x6C,       // 'v', 'a', 'l'
                    0x0B, 0x00, // RDB version
                    0xDB, 0x82, 0x3C, 0x30, 0x38, 0x78, 0x5A, 0x99 // Crc64
                };

                count++;

                var val = await client.ExecuteForStringResultAsync(
                    "$7\r\nRESTORE\r\n"u8.ToArray(),
                    [
                        Encoding.UTF8.GetBytes($"foo-{count}"),
                        "0"u8.ToArray(),
                        payload
                    ]);

                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task SwapDbACLsAsync()
        {
            await CheckCommandsAsync(
                "SWAPDB",
                [DoSwapDbAsync]
            );

            static async Task DoSwapDbAsync(GarnetClient client)
            {
                try
                {
                    // Currently SWAPDB does not support calling the command when multiple clients are connected to the server.
                    await client.ExecuteForStringResultAsync("SWAPDB", ["0", "1"]);
                    Assert.Fail("Shouldn't reach here, calling SWAPDB should fail.");
                }
                catch (Exception ex)
                {
                    if (ex.Message == Encoding.ASCII.GetString(CmdStrings.RESP_ERR_NOAUTH))
                        throw;

                    ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_SWAPDB_UNSUPPORTED), ex.Message);
                }
            }
        }

        [Test]
        public async Task TypeACLsAsync()
        {
            await CheckCommandsAsync(
                "TYPE",
                [DoTypeAsync]
            );

            static async Task DoTypeAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("TYPE", ["foo"]);
                ClassicAssert.AreEqual("none", val);
            }
        }

        [Test]
        public async Task UnlinkACLsAsync()
        {
            await CheckCommandsAsync(
                "UNLINK",
                [DoUnlinkAsync, DoUnlinkMultiAsync]
            );

            static async Task DoUnlinkAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("UNLINK", ["foo"]);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoUnlinkMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("UNLINK", ["foo", "bar"]);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task UnsubscribeACLsAsync()
        {
            await CheckCommandsAsync(
                "UNSUBSCRIBE",
                [DoUnsubscribePatternAsync]
            );

            static async Task DoUnsubscribePatternAsync(GarnetClient client)
            {
                string[] res = await client.ExecuteForStringArrayResultAsync("UNSUBSCRIBE", ["foo"]);
                ClassicAssert.IsNotNull(res);
            }
        }

        [Test]
        public async Task WatchACLsAsync()
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
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task WatchMSACLsAsync()
        {
            // TODO: should watch fail outside of a transaction?

            await CheckCommandsAsync(
                "WATCHMS",
                [DoWatchMSAsync]
            );

            static async Task DoWatchMSAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("WATCHMS", ["foo"]);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task WatchOSACLsAsync()
        {
            // TODO: should watch fail outside of a transaction?

            await CheckCommandsAsync(
                "WATCHOS",
                [DoWatchOSAsync]
            );

            static async Task DoWatchOSAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("WATCHOS", ["foo"]);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task UnwatchACLsAsync()
        {
            // TODO: should watch fail outside of a transaction?

            await CheckCommandsAsync(
                "UNWATCH",
                [DoUnwatchAsync]
            );

            static async Task DoUnwatchAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("UNWATCH");
                ClassicAssert.AreEqual("OK", val);
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
            bool skipPing = false,
            bool skipPermitted = false,
            string aclCheckCommandOverride = null,
            bool skipAclCheckCmd = false
        )
        {
            const string UserWithAll = "temp-all";
            const string UserWithNone = "temp-none";
            const string TestPassword = "foo";

            ClassicAssert.IsNotEmpty(commands, $"[{command}]: should have delegates to invoke");

            var commandAndSubCommand = (aclCheckCommandOverride ?? command).Split(' ');

            // Figure out the ACL categories that apply to this command
            List<string> categories = knownCategories;
            if (categories == null)
            {
                categories = new();

                RespCommandsInfo info;
                if (!command.Contains(" "))
                {
                    ClassicAssert.True(RespCommandsInfo.TryGetRespCommandInfo(command, out info), $"No RespCommandInfo for {command}, failed to discover categories");
                }
                else
                {
                    string parentCommand = command[..command.IndexOf(' ')];
                    string subCommand = command.Replace(' ', '|');

                    ClassicAssert.True(RespCommandsInfo.TryGetRespCommandInfo(parentCommand, out info), $"No RespCommandInfo for {command}, failed to discover categories");
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

            ClassicAssert.IsNotEmpty(categories, $"[{command}]: should have some ACL categories");

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

                            if (!skipPermitted)
                            {
                                await AssertAllPermittedAsync(defaultUserClient, UserWithAll, allUserClient, commands, $"[{command}]: Denied when should have been permitted (user had +@all)", skipPing, commandAndSubCommand, skipAclCheckCmd);
                            }

                            await SetUserAsync(defaultUserClient, UserWithAll, [$"-@{category}"]);

                            await AssertAllDeniedAsync(defaultUserClient, UserWithAll, allUserClient, commands, $"[{command}]: Permitted when should have been denied (user had -@{category})", skipPing, commandAndSubCommand, skipAclCheckCmd);
                        }

                        // Check adding category works
                        {
                            await ResetUserWithNoneAsync(defaultUserClient);

                            await AssertAllDeniedAsync(defaultUserClient, UserWithNone, noneUserClient, commands, $"[{command}]: Permitted when should have been denied (user had -@all)", skipPing, commandAndSubCommand, skipAclCheckCmd);

                            await SetACLOnUserAsync(defaultUserClient, UserWithNone, [$"+@{category}"]);

                            if (!skipPermitted)
                            {
                                await AssertAllPermittedAsync(defaultUserClient, UserWithNone, noneUserClient, commands, $"[{command}]: Denied when should have been permitted (user had +@{category})", skipPing, commandAndSubCommand, skipAclCheckCmd);
                            }
                        }
                    }

                    // Check (parent) command itself
                    {
                        string commandAcl = command.ToLowerInvariant();
                        if (commandAcl.Contains(" "))
                        {
                            commandAcl = commandAcl[..commandAcl.IndexOf(' ')];
                        }

                        var aclCheckCmdCommand = (aclCheckCommandOverride ?? command).ToLowerInvariant();
                        if (aclCheckCmdCommand.Contains(" "))
                        {
                            aclCheckCmdCommand = aclCheckCmdCommand[..aclCheckCmdCommand.IndexOf(' ')];
                        }

                        // Check removing command works
                        {
                            await ResetUserWithAllAsync(defaultUserClient);

                            await SetACLOnUserAsync(defaultUserClient, UserWithAll, [$"-{commandAcl}"]);

                            await AssertAllDeniedAsync(defaultUserClient, UserWithAll, allUserClient, commands, $"[{command}]: Permitted when should have been denied (user had -{commandAcl})", skipPing, [aclCheckCmdCommand], skipAclCheckCmd);
                        }

                        // Check adding command works
                        {
                            await ResetUserWithNoneAsync(defaultUserClient);

                            await SetACLOnUserAsync(defaultUserClient, UserWithNone, [$"+{commandAcl}"]);

                            if (!skipPermitted)
                            {
                                await AssertAllPermittedAsync(defaultUserClient, UserWithNone, noneUserClient, commands, $"[{command}]: Denied when should have been permitted (user had +{commandAcl})", skipPing, [aclCheckCmdCommand], skipAclCheckCmd);
                            }
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

                            await AssertAllDeniedAsync(defaultUserClient, UserWithAll, allUserClient, commands, $"[{command}]: Permitted when should have been denied (user had -{subCommandAcl})", skipPing, commandAndSubCommand, skipAclCheckCmd);
                        }

                        // Check adding subcommand works
                        {
                            await ResetUserWithNoneAsync(defaultUserClient);

                            await SetACLOnUserAsync(defaultUserClient, UserWithNone, [$"+{subCommandAcl}"]);

                            if (!skipPermitted)
                            {
                                await AssertAllPermittedAsync(defaultUserClient, UserWithNone, noneUserClient, commands, $"[{command}]: Denied when should have been permitted (user had +{subCommandAcl})", skipPing, commandAndSubCommand, skipAclCheckCmd);
                            }
                        }

                        // Checking adding command but removing subcommand works
                        {
                            await ResetUserWithNoneAsync(defaultUserClient);

                            await SetACLOnUserAsync(defaultUserClient, UserWithNone, [$"+{commandAcl}", $"-{subCommandAcl}"]);

                            await AssertAllDeniedAsync(defaultUserClient, UserWithNone, noneUserClient, commands, $"[{command}]: Permitted when should have been denied (user had +{commandAcl} -{subCommandAcl})", skipPing, commandAndSubCommand, skipAclCheckCmd);
                        }

                        // Checking removing command but adding subcommand works
                        {
                            await ResetUserWithAllAsync(defaultUserClient);

                            await SetACLOnUserAsync(defaultUserClient, UserWithAll, [$"-{commandAcl}", $"+{subCommandAcl}"]);

                            if (!skipPermitted)
                            {
                                await AssertAllPermittedAsync(defaultUserClient, UserWithAll, allUserClient, commands, $"[{command}]: Denied when should have been permitted (user had -{commandAcl} +{subCommandAcl})", skipPing, commandAndSubCommand, skipAclCheckCmd);
                            }
                        }
                    }
                }
            }

            // Use default user to update ACL on given user
            static async Task SetACLOnUserAsync(GarnetClient defaultUserClient, string user, string[] aclPatterns)
            {
                string aclRes = await defaultUserClient.ExecuteForStringResultAsync("ACL", ["SETUSER", user, .. aclPatterns]);
                ClassicAssert.AreEqual("OK", aclRes);
            }

            // Create or reset user, with all permissions
            static async Task ResetUserWithAllAsync(GarnetClient defaultUserClient)
            {
                string aclRes = await defaultUserClient.ExecuteForStringResultAsync("ACL", ["SETUSER", UserWithAll, "on", $">{TestPassword}", "+@all"]);
                ClassicAssert.AreEqual("OK", aclRes);
            }

            // Get user that was initialized with -@all
            static async Task ResetUserWithNoneAsync(GarnetClient defaultUserClient)
            {
                string aclRes = await defaultUserClient.ExecuteForStringResultAsync("ACL", ["SETUSER", UserWithNone, "on", $">{TestPassword}", "-@all"]);
                ClassicAssert.AreEqual("OK", aclRes);
            }

            // Check that all commands succeed
            static async Task AssertAllPermittedAsync(GarnetClient defaultUserClient, string currentUserName, GarnetClient currentUserClient, Func<GarnetClient, Task>[] commands, string message, bool skipPing, string[] commandAndSubCommand, bool skipAclCheckCmd)
            {
                foreach (Func<GarnetClient, Task> cmd in commands)
                {
                    ClassicAssert.True(await CheckAuthFailureAsync(() => cmd(currentUserClient)), message);
                }

                if (!skipAclCheckCmd)
                {
                    await AssertRedisAclCheckCmd(true, defaultUserClient, currentUserName, currentUserClient, commandAndSubCommand);
                }

                if (!skipPing)
                {
                    // Check we haven't desynced
                    await PingAsync(defaultUserClient, currentUserName, currentUserClient);
                }
            }

            // Check that a script which calls redis.acl_check_cmd(...) for the given (sub-)command produces the right result
            static async Task AssertRedisAclCheckCmd(
                bool expectedResult,
                GarnetClient defaultUserClient,
                string currentUserName,
                GarnetClient currentUserClient,
                string[] commandAndSubCommand
            )
            {
                var withBar = string.Join("|", commandAndSubCommand);
                if (!RespCommandsInfo.TryGetRespCommandInfo(withBar, out var info, includeSubCommands: true))
                {
                    // Couldn't find info, skip
                    return;
                }

                if (info.Command == RespCommand.EVAL)
                {
                    // Need to be able to EVAL to do this test, so skip
                    return;
                }

                if (info.Command.IsNoAuth())
                {
                    // No point to check these
                    return;
                }

                var aclRes = await defaultUserClient.ExecuteForStringResultAsync("ACL", ["SETUSER", currentUserName, "+eval"]);

                var script = $"return redis.acl_check_cmd({string.Join(", ", commandAndSubCommand.Select(static x => $"'{x}'"))});";

                var canRunStr = await currentUserClient.ExecuteForStringResultAsync("EVAL", [script, "0"]);
                var canRun = canRunStr == "1";

                ClassicAssert.AreEqual(expectedResult, canRun, $"redis.acl_check_cmd(...) return unexpected result for '{withBar}'");
            }

            // Check that all commands fail with NOAUTH
            static async Task AssertAllDeniedAsync(GarnetClient defaultUserClient, string currentUserName, GarnetClient currentUserClient, Func<GarnetClient, Task>[] commands, string message, bool skipPing, string[] commandAndSubCommand, bool skipAclCheckCmd)
            {
                foreach (Func<GarnetClient, Task> cmd in commands)
                {
                    ClassicAssert.False(await CheckAuthFailureAsync(() => cmd(currentUserClient)), message);
                }

                if (!skipAclCheckCmd)
                {
                    await AssertRedisAclCheckCmd(false, defaultUserClient, currentUserName, currentUserClient, commandAndSubCommand);
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
                ClassicAssert.AreEqual("OK", addPingRes);

                // Actually execute the PING
                string pingRes = await currentUserClient.ExecuteForStringResultAsync("PING");
                ClassicAssert.AreEqual("PONG", pingRes);
            }
        }

        /// <summary>
        /// Create a GarnetClient authed as the given user.
        /// </summary>
        private static async Task<GarnetClient> CreateGarnetClientAsync(string username, string password)
        {
            GarnetClient ret = TestUtils.GetGarnetClient();
            await ret.ConnectAsync();

            string authRes = await ret.ExecuteForStringResultAsync("AUTH", [username, password]);
            ClassicAssert.AreEqual("OK", authRes);

            return ret;
        }

        /// <summary>
        /// Create a user with +@all permissions.
        /// </summary>
        private static async Task InitUserAsync(GarnetClient defaultUserClient, string username, string password)
        {
            string res = await defaultUserClient.ExecuteForStringResultAsync("ACL", ["SETUSER", username, "on", $">{password}", "+@all"]);
            ClassicAssert.AreEqual("OK", res);
        }

        /// <summary>
        /// Runs ACL SETUSER default [aclPatterns] and checks that they are reflected in ACL LIST.
        /// </summary>
        private static async Task SetUserAsync(GarnetClient client, string user, params string[] aclPatterns)
        {
            string aclLinePreSet = await GetUserAsync(client, user);

            string setRes = await client.ExecuteForStringResultAsync("ACL", ["SETUSER", user, .. aclPatterns]);
            ClassicAssert.AreEqual("OK", setRes, $"Updating user ({user}) failed");

            string aclLinePostSet = await GetUserAsync(client, user);

            string expectedAclLine = $"{aclLinePreSet} {string.Join(" ", aclPatterns)}";

            CommandPermissionSet actualUserPerms = ACLParser.ParseACLRule(aclLinePostSet).CopyCommandPermissionSet();
            CommandPermissionSet expectedUserPerms = ACLParser.ParseACLRule(expectedAclLine).CopyCommandPermissionSet();

            ClassicAssert.IsTrue(expectedUserPerms.IsEquivalentTo(actualUserPerms), $"User permissions were not equivalent after running SETUSER with {string.Join(" ", aclPatterns)}");

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

                ClassicAssert.IsNotNull(ret, $"Couldn't get user from ACL LIST");

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