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
using Allure.NUnit;
using Garnet.client;
using Garnet.server;
using Garnet.server.ACL;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.Resp.ACL
{
    [AllureNUnit]
    [TestFixture]
    public class RespCommandTests : AllureTestBase
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
                                                  enableModuleCommand: Garnet.server.Auth.Settings.ConnectionProtectionOption.Yes,
                                                  enableRangeIndexPreview: true);

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
            TestUtils.OnTearDown();
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

            // Check tests against RespCommandsInfo
            {
                // Exclude things like ACL, CLIENT, CLUSTER which are "commands" but only their sub commands can be run
                IEnumerable<string> subCommands = allInfo.Where(static x => x.Value.SubCommands != null).SelectMany(static x => x.Value.SubCommands).Select(static x => x.Name);
                var x = advertisedCommands.Except(withOnlySubCommands).Union(subCommands);
                IEnumerable<string> deSubCommanded = advertisedCommands.Except(withOnlySubCommands).Union(subCommands).Select(static x => x.Replace("|", "").Replace("_", "").Replace("-", "").Replace(".", ""));
                IEnumerable<string> notCovered = deSubCommanded.Except(covered, StringComparer.OrdinalIgnoreCase).Except(notCoveredByACLs, StringComparer.OrdinalIgnoreCase);

                ClassicAssert.IsEmpty(notCovered, $"Commands in RespCommandsInfo not covered by ACL Tests:{Environment.NewLine}{string.Join(Environment.NewLine, notCovered.OrderBy(static x => x))}");
            }

            // Check tests against RespCommand
            {
                IEnumerable<RespCommand> allValues = Enum.GetValues<RespCommand>().Select(static x => x.NormalizeForACLs()).Distinct();
                IEnumerable<RespCommand> testableValues =
                    allValues
                    .Except([RespCommand.NONE, RespCommand.INVALID, RespCommand.DELIFEXPIM, RespCommand.RIPROMOTE, RespCommand.RIRESTORE])
                    .Where(cmd => !withOnlySubCommands.Contains(cmd.ToString().Replace("_", ""), StringComparer.OrdinalIgnoreCase))
                    .Where(cmd => !notCoveredByACLs.Contains(cmd.ToString().Replace("_", ""), StringComparer.OrdinalIgnoreCase));
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
            ).ConfigureAwait(false);

            static async Task DoAsyncAsync(GarnetClient server)
            {
                try
                {
                    await server.ExecuteForStringResultAsync("ASYNC", ["BARRIER"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoAclCatAsync(GarnetClient server)
            {
                string[] res = await server.ExecuteForStringArrayResultAsync("ACL", ["CAT"]).ConfigureAwait(false);
                ClassicAssert.IsNotNull(res);
            }
        }

        [Test]
        public async Task AclDelUserACLsAsync()
        {
            await CheckCommandsAsync(
                "ACL DELUSER",
                [DoAclDelUserAsync, DoAclDelUserMultiAsync]
            ).ConfigureAwait(false);

            static async Task DoAclDelUserAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ACL", ["DELUSER", "does-not-exist"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            async Task DoAclDelUserMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ACL", ["DELUSER", "does-not-exist-1", "does-not-exist-2"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task AclGenPassACLsAsync()
        {
            await CheckCommandsAsync(
                "ACL GENPASS",
                [DoAclGenPassAsync]
            ).ConfigureAwait(false);

            static async Task DoAclGenPassAsync(GarnetClient client)
            {
                var result = await client.ExecuteForStringResultAsync("ACL", ["GENPASS"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoAclGetUserAsync(GarnetClient client)
            {
                // ACL GETUSER returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("ACL", ["GETUSER", "default"]).ConfigureAwait(false);
            }
        }

        [Test]
        public async Task AclListACLsAsync()
        {
            await CheckCommandsAsync(
                "ACL LIST",
                [DoAclListAsync]
            ).ConfigureAwait(false);

            static async Task DoAclListAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ACL", ["LIST"]).ConfigureAwait(false);
                ClassicAssert.IsNotNull(val);
            }
        }

        [Test]
        public async Task AclLoadACLsAsync()
        {
            await CheckCommandsAsync(
                "ACL LOAD",
                [DoAclLoadAsync]
            ).ConfigureAwait(false);

            static async Task DoAclLoadAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("ACL", ["LOAD"]).ConfigureAwait(false);

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
            ).ConfigureAwait(false);

            static async Task DoAclSaveAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("ACL", ["SAVE"]).ConfigureAwait(false);

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
            ).ConfigureAwait(false);

            static async Task DoAclSetUserOnAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("ACL", ["SETUSER", "foo", "on"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", res);
            }

            static async Task DoAclSetUserCategoryAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("ACL", ["SETUSER", "foo", "+@read"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", res);
            }

            static async Task DoAclSetUserOnCategoryAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("ACL", ["SETUSER", "foo", "on", "+@read"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", res);
            }
        }

        [Test]
        public async Task AclUsersACLsAsync()
        {
            await CheckCommandsAsync(
                "ACL USERS",
                [DoAclUsersAsync]
            ).ConfigureAwait(false);

            static async Task DoAclUsersAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ACL", ["USERS"]).ConfigureAwait(false);
                ClassicAssert.IsNotNull(val);
            }
        }

        [Test]
        public async Task AclWhoAmIACLsAsync()
        {
            await CheckCommandsAsync(
                "ACL WHOAMI",
                [DoAclWhoAmIAsync]
            ).ConfigureAwait(false);

            static async Task DoAclWhoAmIAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ACL", ["WHOAMI"]).ConfigureAwait(false);
                ClassicAssert.AreNotEqual("", (string)val);
            }
        }

        [Test]
        public async Task ExpDelScanACLsAsync()
        {
            await CheckCommandsAsync(
                "EXPDELSCAN",
                [DoExpDelScanAsync]
            ).ConfigureAwait(false);

            static async Task DoExpDelScanAsync(GarnetClient client)
            {
                await client.ExecuteForStringResultAsync("EXPDELSCAN").ConfigureAwait(false);
            }
        }

        [Test]
        public async Task AppendACLsAsync()
        {
            int count = 0;

            await CheckCommandsAsync(
                "APPEND",
                [DoAppendAsync]
            ).ConfigureAwait(false);

            async Task DoAppendAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("APPEND", [$"key-{count}", "foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoAskingAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ASKING").ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", (string)val);
            }
        }

        [Test]
        public async Task BGSaveACLsAsync()
        {
            await CheckCommandsAsync(
                "BGSAVE",
                [DoBGSaveAsync, DoBGSaveScheduleAsync]
            ).ConfigureAwait(false);

            static async Task DoBGSaveAsync(GarnetClient client)
            {
                try
                {
                    string res = await client.ExecuteForStringResultAsync("BGSAVE").ConfigureAwait(false);

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
                    string res = await client.ExecuteForStringResultAsync("BGSAVE", ["SCHEDULE"]).ConfigureAwait(false);

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
            ).ConfigureAwait(false);

            static async Task DoBitCountAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITCOUNT", ["empty-key"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoBitCountStartEndAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITCOUNT", ["empty-key", "1", "1"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoBitCountStartEndByteAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITCOUNT", ["empty-key", "1", "1", "BYTE"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoBitCountStartEndBitAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITCOUNT", ["empty-key", "1", "1", "BIT"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoBitFieldGetAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "GET", "u4", "0"]).ConfigureAwait(false);
                count++;
                ClassicAssert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldGetWrapAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "GET", "u4", "0", "OVERFLOW", "WRAP"]).ConfigureAwait(false);
                count++;
                ClassicAssert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldGetSatAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "GET", "u4", "0", "OVERFLOW", "SAT"]).ConfigureAwait(false);
                count++;
                ClassicAssert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldGetFailAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "GET", "u4", "0", "OVERFLOW", "FAIL"]).ConfigureAwait(false);
                count++;
                ClassicAssert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldSetAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "SET", "u4", "0", "1"]).ConfigureAwait(false);
                count++;
                ClassicAssert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldSetWrapAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "SET", "u4", "0", "1", "OVERFLOW", "WRAP"]).ConfigureAwait(false);
                count++;
                ClassicAssert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldSetSatAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "SET", "u4", "0", "1", "OVERFLOW", "SAT"]).ConfigureAwait(false);
                count++;
                ClassicAssert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldSetFailAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "SET", "u4", "0", "1", "OVERFLOW", "FAIL"]).ConfigureAwait(false);
                count++;
                ClassicAssert.AreEqual(0, long.Parse(val[0]));
            }

            async Task DoBitFieldIncrByAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "INCRBY", "u4", "0", "4"]).ConfigureAwait(false);
                count++;
                ClassicAssert.AreEqual(4, long.Parse(val[0]));
            }

            async Task DoBitFieldIncrByWrapAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "INCRBY", "u4", "0", "4", "OVERFLOW", "WRAP"]).ConfigureAwait(false);
                count++;
                ClassicAssert.AreEqual(4, long.Parse(val[0]));
            }

            async Task DoBitFieldIncrBySatAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "INCRBY", "u4", "0", "4", "OVERFLOW", "SAT"]).ConfigureAwait(false);
                count++;
                ClassicAssert.AreEqual(4, long.Parse(val[0]));
            }

            async Task DoBitFieldIncrByFailAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "INCRBY", "u4", "0", "4", "OVERFLOW", "FAIL"]).ConfigureAwait(false);
                count++;
                ClassicAssert.AreEqual(4, long.Parse(val[0]));
            }

            async Task DoBitFieldMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD", [$"empty-{count}", "OVERFLOW", "WRAP", "GET", "u4", "1", "SET", "u4", "2", "1", "OVERFLOW", "FAIL", "INCRBY", "u4", "6", "2"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoBitFieldROGetAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD_RO", ["empty-a", "GET", "u4", "0"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, long.Parse(val[0]));
            }

            static async Task DoBitFieldROMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BITFIELD_RO", ["empty-b", "GET", "u4", "0", "GET", "u4", "3"]).ConfigureAwait(false);

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
            ).ConfigureAwait(false);

            static async Task DoBitOpAndAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["AND", "zero", "zero"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoBitOpAndMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["AND", "zero", "zero", "one", "zero"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoBitOpOrAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["OR", "one", "one"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoBitOpOrMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["OR", "one", "one", "one", "one"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoBitOpXorAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["XOR", "one", "zero"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoBitOpXorMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["XOR", "one", "one", "one", "zero"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoBitOpNotAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITOP", ["NOT", "one", "zero"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task BitPosACLsAsync()
        {
            await CheckCommandsAsync(
                "BITPOS",
                [DoBitPosAsync, DoBitPosStartAsync, DoBitPosStartEndAsync, DoBitPosStartEndBitAsync, DoBitPosStartEndByteAsync]
            ).ConfigureAwait(false);

            static async Task DoBitPosAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITPOS", ["empty", "1"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(-1, val);
            }

            static async Task DoBitPosStartAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITPOS", ["empty", "1", "5"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(-1, val);
            }

            static async Task DoBitPosStartEndAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITPOS", ["empty", "1", "5", "7"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(-1, val);
            }

            static async Task DoBitPosStartEndBitAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITPOS", ["empty", "1", "5", "7", "BIT"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(-1, val);
            }

            static async Task DoBitPosStartEndByteAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("BITPOS", ["empty", "1", "5", "7", "BYTE"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(-1, val);
            }
        }

        [Test]
        public async Task ClientIdACLsAsync()
        {
            await CheckCommandsAsync(
                "CLIENT ID",
                [DoClientIdAsync]
            ).ConfigureAwait(false);

            static async Task DoClientIdAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("CLIENT", ["ID"]).ConfigureAwait(false);
                ClassicAssert.AreNotEqual(0, val);
            }
        }

        [Test]
        public async Task ClientInfoACLsAsync()
        {
            await CheckCommandsAsync(
                "CLIENT INFO",
                [DoClientInfoAsync]
            ).ConfigureAwait(false);

            static async Task DoClientInfoAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("CLIENT", ["INFO"]).ConfigureAwait(false);
                ClassicAssert.IsNotEmpty(val);
            }
        }

        [Test]
        public async Task ClientListACLsAsync()
        {
            await CheckCommandsAsync(
                "CLIENT LIST",
                [DoClientListAsync, DoClientListTypeAsync, DoClientListIdAsync, DoClientListIdsAsync]
            ).ConfigureAwait(false);

            static async Task DoClientListAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("CLIENT", ["LIST"]).ConfigureAwait(false);
                ClassicAssert.IsNotEmpty(val);
            }

            static async Task DoClientListTypeAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("CLIENT", ["LIST", "TYPE", "NORMAL"]).ConfigureAwait(false);
                ClassicAssert.IsNotEmpty(val);
            }

            static async Task DoClientListIdAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("CLIENT", ["LIST", "ID", "1"]).ConfigureAwait(false);
                ClassicAssert.IsNotEmpty(val);
            }

            static async Task DoClientListIdsAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("CLIENT", ["LIST", "ID", "1", "2"]).ConfigureAwait(false);
                ClassicAssert.IsNotEmpty(val);
            }
        }

        [Test]
        public async Task ClientKillACLsAsync()
        {
            await CheckCommandsAsync(
                "CLIENT KILL",
                [DoClientKillAsync, DoClientFilterAsync]
            ).ConfigureAwait(false);

            static async Task DoClientKillAsync(GarnetClient client)
            {
                try
                {
                    _ = await client.ExecuteForStringResultAsync("CLIENT", ["KILL", "foo"]).ConfigureAwait(false);
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
                var count = await client.ExecuteForLongResultAsync("CLIENT", ["KILL", "ID", "123"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, count);
            }
        }

        [Test]
        public async Task ClientGetNameACLsAsync()
        {
            await CheckCommandsAsync(
                "CLIENT GETNAME",
                [DoClientGetNameAsync]
            ).ConfigureAwait(false);

            static async Task DoClientGetNameAsync(GarnetClient client)
            {
                var name = await client.ExecuteForStringResultAsync("CLIENT", ["GETNAME"]).ConfigureAwait(false);
                ClassicAssert.IsNotEmpty(name);
            }
        }

        [Test]
        public async Task ClientSetNameACLsAsync()
        {
            await CheckCommandsAsync(
                "CLIENT SETNAME",
                [DoClientSetNameAsync]
            ).ConfigureAwait(false);

            static async Task DoClientSetNameAsync(GarnetClient client)
            {
                var count = await client.ExecuteForStringResultAsync("CLIENT", ["SETNAME", "foo"]).ConfigureAwait(false);
                ClassicAssert.IsNotEmpty(count);
            }
        }

        [Test]
        public async Task ClientSetInfoACLsAsync()
        {
            await CheckCommandsAsync(
                "CLIENT SETINFO",
                [DoClientSetInfoAsync]
            ).ConfigureAwait(false);

            static async Task DoClientSetInfoAsync(GarnetClient client)
            {
                var count = await client.ExecuteForStringResultAsync("CLIENT", ["SETINFO", "LIB-NAME", "foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoSSubscribeAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("SSUBSCRIBE", ["channel"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoSPublishAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("SPUBLISH", ["channel", "message"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClientUnblockAsync(GarnetClient client)
            {
                var count = await client.ExecuteForLongResultAsync("CLIENT", ["UNBLOCK", "123"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterAddSlotsAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["ADDSLOTS", "1"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("CLUSTER", ["ADDSLOTS", "1", "2"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterAddSlotsRangeAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["ADDSLOTSRANGE", "1", "3"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("CLUSTER", ["ADDSLOTSRANGE", "1", "3", "7", "9"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterAppendLogAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["APPENDLOG", "a", "b", "c", "d", "e"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

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
                    await client.ExecuteForStringResultAsync("CLUSTER", ["ATTACH_SYNC", Encoding.UTF8.GetString(byteBuffer)]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterBanListAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["BANLIST"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterBeginReplicaFailoverAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["BEGIN_REPLICA_RECOVER", "1", "2", "3", "4", "5", "6", "7"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterBumpEpochAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["BUMPEPOCH"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterBumpEpochAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["COUNTKEYSINSLOT", "1"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterDelKeysInSlotAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["DELKEYSINSLOT", "1"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterDelKeysInSlotRangeAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["DELKEYSINSLOTRANGE", "1", "3"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("CLUSTER", ["DELKEYSINSLOTRANGE", "1", "3", "5", "9"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterDelSlotsAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["DELSLOTS", "1"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("CLUSTER", ["DELSLOTS", "1", "2"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterDelSlotsRangeAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["DELSLOTSRANGE", "1", "3"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("CLUSTER", ["DELSLOTSRANGE", "1", "3", "9", "11"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterEndpointAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["ENDPOINT", "abcd"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterFailoverAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["FAILOVER"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("CLUSTER", ["FAILOVER", "FORCE"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterFailStopWritesAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["FAILSTOPWRITES", "foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterFlushAllAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["FLUSHALL"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterFailStopWritesAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["FAILREPLICATIONOFFSET", "foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterForgetAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["FORGET", "foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterGetKeysInSlotAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["GETKEYSINSLOT", "foo", "3"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterGossipAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["GOSSIP", "foo", "3"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterHelpAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["HELP"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterInfoAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["INFO"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterInfoAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["INITIATE_REPLICA_SYNC", "1", "2", "3", "4", "5"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterKeySlotAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["KEYSLOT", "foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterMeetAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["MEET", "127.0.0.1", "1234"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("CLUSTER", ["MEET", "127.0.0.1", "1234", "6789"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterMigrateAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["MIGRATE", "a", "b", "c"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterMigrateAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SYNC", "a", "b", "c"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterMTasksAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["MTASKS"]).ConfigureAwait(false);
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
        public async Task ClusterAdvanceTimeACLsAsync()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER ADVANCE_TIME",
                [DoClusterAdvanceTimeAsync]
            );

            static async Task DoClusterAdvanceTimeAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["ADVANCE_TIME"]);
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
            ).ConfigureAwait(false);

            static async Task DoClusterMyIdAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["MYID"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterMyParentIdAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["MYPARENTID"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterNodesAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["NODES"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterReplicasAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["REPLICAS", "foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterReplicateAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["REPLICATE", "foo"]).ConfigureAwait(false);
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
        public async Task ClusterReserveACLsAsync()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER RESERVE",
                [DoClusterReserveAsync]
            ).ConfigureAwait(false);

            static async Task DoClusterReserveAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["RESERVE", "VECTOR_SET_CONTEXTS", "16"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterResetAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["RESET"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("CLUSTER", ["RESET", "HARD"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterSendCkptFileSegmentAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SEND_CKPT_FILE_SEGMENT", "1", "2", "3", "4", "5"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterSendCkptMetadataAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SEND_CKPT_METADATA", "1", "2", "3", "4", "5"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterSetConfigEpochAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SET-CONFIG-EPOCH", "foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterSetSlotAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SETSLOT", "1"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SETSLOT", "1", "STABLE"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SETSLOT", "1", "IMPORTING", "foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterSetSlotsRangeStableAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SETSLOTSRANGE", "STABLE", "1", "5"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SETSLOTSRANGE", "STABLE", "1", "5", "10", "15"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SETSLOTSRANGE", "IMPORTING", "foo", "1", "5"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SETSLOTSRANGE", "IMPORTING", "foo", "1", "5", "10", "15"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterShardsAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SHARDS"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterSlotsAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SLOTS"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterSlotStateAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SLOTSTATE"]).ConfigureAwait(false);
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
        public async Task ClusterMlogKeyTimeACLsAsync()
        {
            // All cluster command "success" is a thrown exception, because clustering is disabled

            await CheckCommandsAsync(
                "CLUSTER MLOG_KEY_TIME",
                [DoClusterMlogKeyTimeAsync, DoClusterMlogKeyTimeFrontierAsync]
            );

            static async Task DoClusterMlogKeyTimeAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["MLOG_KEY_TIME", "key"]);
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

            static async Task DoClusterMlogKeyTimeFrontierAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["MLOG_KEY_TIME", "key", "FRONTIER"]);
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
            ).ConfigureAwait(false);

            static async Task DoClusterPublishAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["PUBLISH", "channel", "message"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoClusterSPublishAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CLUSTER", ["SPUBLISH", "channel", "message"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoCommandAsync(GarnetClient client)
            {
                // COMMAND returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("COMMAND").ConfigureAwait(false);
            }
        }

        [Test]
        public async Task CommandCountACLsAsync()
        {
            await CheckCommandsAsync(
                "COMMAND COUNT",
                [DoCommandCountAsync]
            ).ConfigureAwait(false);

            static async Task DoCommandCountAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("COMMAND", ["COUNT"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoCommandInfoAsync(GarnetClient client)
            {
                // COMMAND|INFO returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("COMMAND", ["INFO"]).ConfigureAwait(false);
            }

            static async Task DoCommandInfoOneAsync(GarnetClient client)
            {
                // COMMAND|INFO returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("COMMAND", ["INFO", "GET"]).ConfigureAwait(false);
            }

            static async Task DoCommandInfoMultiAsync(GarnetClient client)
            {
                // COMMAND|INFO returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("COMMAND", ["INFO", "GET", "SET", "APPEND"]).ConfigureAwait(false);
            }
        }

        [Test]
        public async Task CommandDocsACLsAsync()
        {
            await CheckCommandsAsync(
                "COMMAND DOCS",
                [DoCommandDocsAsync, DoCommandDocsOneAsync, DoCommandDocsMultiAsync],
                skipPermitted: true
            ).ConfigureAwait(false);

            static async Task DoCommandDocsAsync(GarnetClient client)
            {
                // COMMAND|DOCS returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("COMMAND", ["DOCS"]).ConfigureAwait(false);
            }

            static async Task DoCommandDocsOneAsync(GarnetClient client)
            {
                // COMMAND|DOCS returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("COMMAND", ["DOCS", "GET"]).ConfigureAwait(false);
            }

            static async Task DoCommandDocsMultiAsync(GarnetClient client)
            {
                // COMMAND|DOCS returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("COMMAND", ["DOCS", "GET", "SET", "APPEND"]).ConfigureAwait(false);
            }
        }

        [Test]
        public async Task CommandGetKeysACLsAsync()
        {
            await CheckCommandsAsync(
                "COMMAND GETKEYS",
                [DoCommandGetKeysAsync]
            ).ConfigureAwait(false);

            static async Task DoCommandGetKeysAsync(GarnetClient client)
            {
                string[] res = await client.ExecuteForStringArrayResultAsync("COMMAND", ["GETKEYS", "SET", "mykey", "value"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoCommandGetKeysAndFlagsAsync(GarnetClient client)
            {
                var res = await client.ExecuteForStringArrayResultAsync("COMMAND", ["GETKEYSANDFLAGS", "EVAL", "return redis.call('TIME')", "0"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, res.Length);
            }
        }

        [Test]
        public async Task CommitAOFACLsAsync()
        {
            await CheckCommandsAsync(
                "COMMITAOF",
                [DoCommitAOFAsync]
            ).ConfigureAwait(false);

            static async Task DoCommitAOFAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("COMMITAOF").ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoConfigGetOneAsync(GarnetClient client)
            {
                string[] res = await client.ExecuteForStringArrayResultAsync("CONFIG", ["GET", "timeout"]).ConfigureAwait(false);

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
            ).ConfigureAwait(false);

            static async Task DoConfigRewriteAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("CONFIG", ["REWRITE"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoConfigSetOneAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("CONFIG", ["SET", "foo", "bar"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("CONFIG", ["SET", "foo", "bar", "fizz", "buzz"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoCOScanAsync(GarnetClient client)
            {
                // COSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("CUSTOMOBJECTSCAN", ["foo", "0"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoSetWpIfPgtAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("SETWPIFPGT", [$"foo-{count}", "bar", "\0\0\0\0\0\0\0\0"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoMyDictGetAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("MYDICTGET", ["foo", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoReadWriteTxAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("READWRITETX", ["foo", "bar", "fizz"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoSumAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("SUM", ["key1", "key2", "key3"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("0", res.ToString());
            }
        }

        [Test]
        public async Task DebugACLsAsync()
        {
            await CheckCommandsAsync(
                "DEBUG",
                [DoDebugAsync]
            ).ConfigureAwait(false);

            async Task DoDebugAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("DEBUG", ["HELP"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoEvalAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("EVAL", ["return 'OK'", "0"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoEvalShaAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("EVALSHA", ["57ade87c8731f041ecac85aba56623f8af391fab", "0"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoScriptLoadAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("SCRIPT", ["LOAD", "return 'OK'"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("57ade87c8731f041ecac85aba56623f8af391fab", (string)res);
            }
        }

        [Test]
        public async Task ScriptExistsACLsAsync()
        {
            await CheckCommandsAsync(
                "SCRIPT EXISTS",
                [DoScriptExistsSingleAsync, DoScriptExistsMultiAsync]
            ).ConfigureAwait(false);

            async Task DoScriptExistsSingleAsync(GarnetClient client)
            {
                string[] res = await client.ExecuteForStringArrayResultAsync("SCRIPT", ["EXISTS", "57ade87c8731f041ecac85aba56623f8af391fab"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(1, res.Length);
                ClassicAssert.IsTrue(res[0] == "1" || res[0] == "0");
            }

            async Task DoScriptExistsMultiAsync(GarnetClient client)
            {
                string[] res = await client.ExecuteForStringArrayResultAsync("SCRIPT", ["EXISTS", "57ade87c8731f041ecac85aba56623f8af391fab", "57ade87c8731f041ecac85aba56623f8af391fab"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoScriptFlushAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("SCRIPT", ["FLUSH"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", res);
            }

            async Task DoScriptFlushSyncAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("SCRIPT", ["FLUSH", "SYNC"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", res);
            }

            async Task DoScriptFlushAsyncAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("SCRIPT", ["FLUSH", "ASYNC"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", res);
            }
        }

        [Test]
        public async Task DBSizeACLsAsync()
        {
            await CheckCommandsAsync(
                "DBSIZE",
                [DoDbSizeAsync]
            ).ConfigureAwait(false);

            static async Task DoDbSizeAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("DBSIZE").ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoDecrAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("DECR", [$"foo-{count}"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoDecrByAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("DECRBY", [$"foo-{count}", "2"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoDelAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("DEL", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoDelMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("DEL", ["foo", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoDiscardAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("DISCARD").ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoEchoAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ECHO", ["hello world"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoExecAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("EXEC").ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoExistsAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXISTS", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExistsMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXISTS", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ExpireACLsAsync()
        {
            await CheckCommandsAsync(
                "EXPIRE",
                [DoExpireAsync, DoExpireNXAsync, DoExpireXXAsync, DoExpireGTAsync, DoExpireLTAsync]
            ).ConfigureAwait(false);

            static async Task DoExpireAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXPIRE", ["foo", "10"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireNXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXPIRE", ["foo", "10", "NX"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireXXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXPIRE", ["foo", "10", "XX"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireGTAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXPIRE", ["foo", "10", "GT"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireLTAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("EXPIRE", ["foo", "10", "LT"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ExpireAtACLsAsync()
        {
            await CheckCommandsAsync(
                "EXPIREAT",
                [DoExpireAsync, DoExpireNXAsync, DoExpireXXAsync, DoExpireGTAsync, DoExpireLTAsync]
            ).ConfigureAwait(false);


            static async Task DoExpireAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();
                long val = await client.ExecuteForLongResultAsync("EXPIREAT", ["foo", expireTimestamp]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireNXAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();
                long val = await client.ExecuteForLongResultAsync("EXPIREAT", ["foo", "10", "NX"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireXXAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();
                long val = await client.ExecuteForLongResultAsync("EXPIREAT", ["foo", "10", "XX"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireGTAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();
                long val = await client.ExecuteForLongResultAsync("EXPIREAT", ["foo", "10", "GT"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireLTAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();
                long val = await client.ExecuteForLongResultAsync("EXPIREAT", ["foo", "10", "LT"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task PExpireAtACLsAsync()
        {
            await CheckCommandsAsync(
                "PEXPIREAT",
                [DoExpireAsync, DoExpireNXAsync, DoExpireXXAsync, DoExpireGTAsync, DoExpireLTAsync]
            ).ConfigureAwait(false);


            static async Task DoExpireAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeMilliseconds().ToString();
                long val = await client.ExecuteForLongResultAsync("PEXPIREAT", ["foo", expireTimestamp]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireNXAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();
                long val = await client.ExecuteForLongResultAsync("PEXPIREAT", ["foo", "10", "NX"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireXXAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();
                long val = await client.ExecuteForLongResultAsync("PEXPIREAT", ["foo", "10", "XX"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireGTAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();
                long val = await client.ExecuteForLongResultAsync("PEXPIREAT", ["foo", "10", "GT"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoExpireLTAsync(GarnetClient client)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();
                long val = await client.ExecuteForLongResultAsync("PEXPIREAT", ["foo", "10", "LT"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoFailoverAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("FAILOVER").ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("FAILOVER", ["TO", "127.0.0.1", "9999"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("FAILOVER", ["ABORT"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("FAILOVER", ["TO", "127.0.0.1", "9999", "FORCE"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("FAILOVER", ["TO", "127.0.0.1", "9999", "ABORT"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("FAILOVER", ["TO", "127.0.0.1", "9999", "FORCE", "ABORT"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("FAILOVER", ["TO", "127.0.0.1", "9999", "FORCE", "ABORT", "TIMEOUT", "1"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoFlushDBAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("FLUSHDB").ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }

            static async Task DoFlushDBAsyncAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("FLUSHDB", ["ASYNC"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task FlushAllACLsAsync()
        {
            await CheckCommandsAsync(
                "FLUSHALL",
                [DoFlushAllAsync, DoFlushAllAsyncAsync]
            ).ConfigureAwait(false);

            static async Task DoFlushAllAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("FLUSHALL").ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }

            static async Task DoFlushAllAsyncAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("FLUSHALL", ["ASYNC"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task ForceGCACLsAsync()
        {
            await CheckCommandsAsync(
                "FORCEGC",
                [DoForceGCAsync, DoForceGCGenAsync]
            ).ConfigureAwait(false);

            static async Task DoForceGCAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("FORCEGC").ConfigureAwait(false);
                ClassicAssert.AreEqual("GC completed", val);
            }

            static async Task DoForceGCGenAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("FORCEGC", ["1"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("GC completed", val);
            }
        }

        [Test]
        public async Task GetACLsAsync()
        {
            await CheckCommandsAsync(
                "GET",
                [DoGetAsync]
            ).ConfigureAwait(false);

            static async Task DoGetAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("GET", ["foo"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task GetEXACLsAsync()
        {
            await CheckCommandsAsync(
                "GETEX",
                [DoGetEXAsync]
            ).ConfigureAwait(false);

            static async Task DoGetEXAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("GETEX", ["foo"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task GetBitACLsAsync()
        {
            await CheckCommandsAsync(
                "GETBIT",
                [DoGetBitAsync]
            ).ConfigureAwait(false);

            static async Task DoGetBitAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GETBIT", ["foo", "4"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task GetDelACLsAsync()
        {
            await CheckCommandsAsync(
                "GETDEL",
                [DoGetDelAsync]
            ).ConfigureAwait(false);

            static async Task DoGetDelAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("GETDEL", ["foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoGetAndSetAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("GETSET", [$"foo-{keyIx++}", "bar"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task SubStrACLsAsync()
        {
            await CheckCommandsAsync(
                "SUBSTR",
                [DoSubStringAsync]
            ).ConfigureAwait(false);

            static async Task DoSubStringAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SUBSTR", ["foo", "10", "15"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("", val);
            }
        }

        [Test]
        public async Task GetRangeACLsAsync()
        {
            await CheckCommandsAsync(
                "GETRANGE",
                [DoGetRangeAsync]
            ).ConfigureAwait(false);

            static async Task DoGetRangeAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("GETRANGE", ["foo", "10", "15"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("", val);
            }
        }

        [Test]
        public async Task SubStringACLsAsync()
        {
            await CheckCommandsAsync(
                "SUBSTR",
                [DoSubStringAsync]
            ).ConfigureAwait(false);

            static async Task DoSubStringAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SUBSTR", ["foo", "10", "15"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("", val);
            }
        }

        [Test]
        public async Task HExpireACLsAsync()
        {
            await CheckCommandsAsync(
                "HEXPIRE",
                [DoHExpireAsync]
            ).ConfigureAwait(false);

            static async Task DoHExpireAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("HEXPIRE", ["foo", "1", "FIELDS", "1", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoHPExpireAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("HPEXPIRE", ["foo", "1", "FIELDS", "1", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoHExpireAtAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("HEXPIREAT", ["foo", DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeSeconds().ToString(), "FIELDS", "1", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoHPExpireAtAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("HPEXPIREAT", ["foo", DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeMilliseconds().ToString(), "FIELDS", "1", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoHExpireTimeAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("HEXPIRETIME", ["foo", "FIELDS", "1", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoHPExpireTimeAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("HPEXPIRETIME", ["foo", "FIELDS", "1", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoHETTLAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("HTTL", ["foo", "FIELDS", "1", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoHPETTLAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("HPTTL", ["foo", "FIELDS", "1", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoHPersistAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("HPERSIST", ["foo", "FIELDS", "1", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoHCollectAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringResultAsync("HCOLLECT", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task HDelACLsAsync()
        {
            await CheckCommandsAsync(
                "HDEL",
                [DoHDelAsync, DoHDelMultiAsync]
            ).ConfigureAwait(false);

            static async Task DoHDelAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HDEL", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoHDelMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HDEL", ["foo", "bar", "fizz"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task HExistsACLsAsync()
        {
            await CheckCommandsAsync(
                "HEXISTS",
                [DoHDelAsync]
            ).ConfigureAwait(false);

            static async Task DoHDelAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HEXISTS", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task HGetACLsAsync()
        {
            await CheckCommandsAsync(
                "HGET",
                [DoHDelAsync]
            ).ConfigureAwait(false);

            static async Task DoHDelAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("HGET", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task HGetAllACLsAsync()
        {
            await CheckCommandsAsync(
                "HGETALL",
                [DoHDelAsync]
            ).ConfigureAwait(false);

            static async Task DoHDelAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HGETALL", ["foo"]).ConfigureAwait(false);

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
            ).ConfigureAwait(false);

            async Task DoHIncrByAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HINCRBY", ["foo", "bar", "2"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoHIncrByFloatAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("HINCRBYFLOAT", ["foo", "bar", "1.0"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoHKeysAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HKEYS", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task HLenACLsAsync()
        {
            await CheckCommandsAsync(
                "HLEN",
                [DoHLenAsync]
            ).ConfigureAwait(false);

            static async Task DoHLenAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HLEN", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task HMGetACLsAsync()
        {
            await CheckCommandsAsync(
                "HMGET",
                [DoHMGetAsync, DoHMGetMultiAsync]
            ).ConfigureAwait(false);

            static async Task DoHMGetAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HMGET", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.IsNull(val[0]);
            }

            static async Task DoHMGetMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HMGET", ["foo", "bar", "fizz"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoHMSetAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("HMSET", ["foo", "bar", "fizz"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }

            static async Task DoHMSetMultiAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("HMSET", ["foo", "bar", "fizz", "hello", "world"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task HRandFieldACLsAsync()
        {
            await CheckCommandsAsync(
                "HRANDFIELD",
                [DoHRandFieldAsync, DoHRandFieldCountAsync, DoHRandFieldCountWithValuesAsync]
            ).ConfigureAwait(false);

            static async Task DoHRandFieldAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("HRANDFIELD", ["foo"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }

            static async Task DoHRandFieldCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HRANDFIELD", ["foo", "1"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoHRandFieldCountWithValuesAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HRANDFIELD", ["foo", "1", "WITHVALUES"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoHScanAsync(GarnetClient client)
            {
                // HSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("HSCAN", ["foo", "0"]).ConfigureAwait(false);
            }

            static async Task DoHScanMatchAsync(GarnetClient client)
            {
                // HSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("HSCAN", ["foo", "0", "MATCH", "*"]).ConfigureAwait(false);
            }

            static async Task DoHScanCountAsync(GarnetClient client)
            {
                // HSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("HSCAN", ["foo", "0", "COUNT", "2"]).ConfigureAwait(false);
            }

            static async Task DoHScanNoValuesAsync(GarnetClient client)
            {
                // HSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("HSCAN", ["foo", "0", "NOVALUES"]).ConfigureAwait(false);
            }

            static async Task DoHScanMatchCountAsync(GarnetClient client)
            {
                // HSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("HSCAN", ["foo", "0", "MATCH", "*", "COUNT", "2"]).ConfigureAwait(false);
            }

            static async Task DoHScanMatchNoValuesAsync(GarnetClient client)
            {
                // HSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("HSCAN", ["foo", "0", "MATCH", "*", "NOVALUES"]).ConfigureAwait(false);
            }

            static async Task DoHScanCountNoValuesAsync(GarnetClient client)
            {
                // HSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("HSCAN", ["foo", "0", "COUNT", "0", "NOVALUES"]).ConfigureAwait(false);
            }

            static async Task DoHScanMatchCountNoValuesAsync(GarnetClient client)
            {
                // HSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("HSCAN", ["foo", "0", "MATCH", "*", "COUNT", "0", "NOVALUES"]).ConfigureAwait(false);
            }
        }

        [Test]
        public async Task HSetACLsAsync()
        {
            int keyIx = 0;

            await CheckCommandsAsync(
                "HSET",
                [DoHSetAsync, DoHSetMultiAsync]
            ).ConfigureAwait(false);

            async Task DoHSetAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HSET", [$"foo-{keyIx}", "bar", "fizz"]).ConfigureAwait(false);
                keyIx++;

                ClassicAssert.AreEqual(1, val);
            }

            async Task DoHSetMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HSET", [$"foo-{keyIx}", "bar", "fizz", "hello", "world"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoHSetNXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HSETNX", [$"foo-{keyIx}", "bar", "fizz"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoHStrLenAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("HSTRLEN", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task HValsACLsAsync()
        {
            await CheckCommandsAsync(
                "HVALS",
                [DoHValsAsync]
            ).ConfigureAwait(false);

            static async Task DoHValsAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("HVALS", ["foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoIncrAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("INCR", [$"foo-{count}"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoIncrByAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("INCRBY", [$"foo-{count}", "2"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoIncrByFloatAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringResultAsync("INCRBYFLOAT", [$"foo-{count}", "2"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoInfoAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("INFO").ConfigureAwait(false);
                ClassicAssert.IsNotEmpty(val);
            }

            static async Task DoInfoSingleAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("INFO", ["SERVER"]).ConfigureAwait(false);
                ClassicAssert.IsNotEmpty(val);
            }

            static async Task DoInfoMultiAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("INFO", ["SERVER", "MEMORY"]).ConfigureAwait(false);
                ClassicAssert.IsNotEmpty(val);
            }
        }

        [Test]
        public async Task RoleACLsAsync()
        {
            await CheckCommandsAsync(
               "ROLE",
               [DoRoleAsync]
            ).ConfigureAwait(false);

            static async Task DoRoleAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ROLE").ConfigureAwait(false);
                ClassicAssert.IsNotEmpty(val);
            }
        }

        [Test]
        public async Task KeysACLsAsync()
        {
            await CheckCommandsAsync(
               "KEYS",
               [DoKeysAsync]
            ).ConfigureAwait(false);

            static async Task DoKeysAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("KEYS", ["*"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task LastSaveACLsAsync()
        {
            await CheckCommandsAsync(
               "LASTSAVE",
               [DoLastSaveAsync]
            ).ConfigureAwait(false);

            static async Task DoLastSaveAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LASTSAVE").ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task LatencyHelpACLsAsync()
        {
            await CheckCommandsAsync(
               "LATENCY HELP",
               [DoLatencyHelpAsync]
            ).ConfigureAwait(false);

            static async Task DoLatencyHelpAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("LATENCY", ["HELP"]).ConfigureAwait(false);
                ClassicAssert.IsNotNull(val);
            }
        }

        [Test]
        public async Task LatencyHistogramACLsAsync()
        {
            await CheckCommandsAsync(
               "LATENCY HISTOGRAM",
               [DoLatencyHistogramAsync, DoLatencyHistogramSingleAsync, DoLatencyHistogramMultiAsync]
            ).ConfigureAwait(false);

            static async Task DoLatencyHistogramAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("LATENCY", ["HISTOGRAM"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoLatencyHistogramSingleAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("LATENCY", ["HISTOGRAM", "NET_RS_LAT"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoLatencyHistogramMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("LATENCY", ["HISTOGRAM", "NET_RS_LAT", "NET_RS_LAT_ADMIN"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task LatencyResetACLsAsync()
        {
            await CheckCommandsAsync(
               "LATENCY RESET",
               [DoLatencyResetAsync, DoLatencyResetSingleAsync, DoLatencyResetMultiAsync]
            ).ConfigureAwait(false);

            static async Task DoLatencyResetAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LATENCY", ["RESET"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(6, val);
            }

            static async Task DoLatencyResetSingleAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LATENCY", ["RESET", "NET_RS_LAT"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(1, val);
            }

            static async Task DoLatencyResetMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LATENCY", ["RESET", "NET_RS_LAT", "NET_RS_LAT_ADMIN"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(2, val);
            }
        }

        [Test]
        public async Task BLMoveACLsAsync()
        {
            await CheckCommandsAsync(
                "BLMOVE",
                [DoBLMoveAsync]
            ).ConfigureAwait(false);

            static async Task DoBLMoveAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("BLMOVE", ["foo", "bar", "RIGHT", "LEFT", "0.1"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task BRPopLPushACLsAsync()
        {
            await CheckCommandsAsync(
                "BRPOPLPUSH",
                [DoBRPopLPushAsync]
            ).ConfigureAwait(false);

            static async Task DoBRPopLPushAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("BRPOPLPUSH", ["foo", "bar", "0.1"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task BLMPopACLsAsync()
        {
            await CheckCommandsAsync(
                "BLMPOP",
                [DoBLMPopAsync]
            ).ConfigureAwait(false);

            static async Task DoBLMPopAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("BLMPOP", ["0.1", "1", "foo", "RIGHT"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task BLPopACLsAsync()
        {
            await CheckCommandsAsync(
                "BLPOP",
                [DoBLPopAsync]
            ).ConfigureAwait(false);

            static async Task DoBLPopAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BLPOP", ["foo", "0.1"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task BRPopACLsAsync()
        {
            await CheckCommandsAsync(
                "BRPOP",
                [DoBRPopAsync]
            ).ConfigureAwait(false);

            static async Task DoBRPopAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("BRPOP", ["foo", "0.1"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task BZMPopACLsAsync()
        {
            await CheckCommandsAsync(
                "BZMPOP",
                [DoBZMPopAsync]
            ).ConfigureAwait(false);

            static async Task DoBZMPopAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringResultAsync("BZMPOP", ["0.1", "1", "foo", "MIN"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task BZPopMaxACLsAsync()
        {
            await CheckCommandsAsync(
                "BZPOPMAX",
                [DoBZPopMaxAsync]
            ).ConfigureAwait(false);

            static async Task DoBZPopMaxAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringResultAsync("BZPOPMAX", ["foo", "0.1"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task BZPopMinACLsAsync()
        {
            await CheckCommandsAsync(
                "BZPOPMIN",
                [DoBZPopMinAsync]
            ).ConfigureAwait(false);

            static async Task DoBZPopMinAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringResultAsync("BZPOPMIN", ["foo", "0.1"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task LPopACLsAsync()
        {
            await CheckCommandsAsync(
                "LPOP",
                [DoLPopAsync, DoLPopCountAsync]
            ).ConfigureAwait(false);

            static async Task DoLPopAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LPOP", ["foo"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }

            static async Task DoLPopCountAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LPOP", ["foo", "4"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task LPosACLsAsync()
        {
            await CheckCommandsAsync(
                "LPOS",
                [DoLPosAsync]
            ).ConfigureAwait(false);

            static async Task DoLPosAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LPOS", ["foo", "a"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoLPushAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LPUSH", ["foo", "bar"]).ConfigureAwait(false);
                count++;

                ClassicAssert.AreEqual(count, val);
            }

            async Task DoLPushMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LPUSH", ["foo", "bar", "buzz"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoLPushXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LPUSHX", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            async Task DoLPushXMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LPUSHX", ["foo", "bar", "buzz"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task RPopACLsAsync()
        {
            await CheckCommandsAsync(
                "RPOP",
                [DoRPopAsync, DoRPopCountAsync]
            ).ConfigureAwait(false);

            static async Task DoRPopAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("RPOP", ["foo"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }

            static async Task DoRPopCountAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("RPOP", ["foo", "4"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoRPushAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("RPUSH", ["foo", "bar"]).ConfigureAwait(false);
                count++;

                ClassicAssert.AreEqual(count, val);
            }

            async Task DoRPushMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("RPUSH", ["foo", "bar", "buzz"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoRPushXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("RPUSH", [$"foo-{count}", "bar"]).ConfigureAwait(false);
                count++;
                ClassicAssert.AreEqual(1, val);
            }

            async Task DoRPushXMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("RPUSH", [$"foo-{count}", "bar", "buzz"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoRPushXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("RPUSHX", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoRPushXMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("RPUSHX", ["foo", "bar", "buzz"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task LCSACLsAsync()
        {
            await CheckCommandsAsync(
                "LCS",
                [DoLCSAsync]
            ).ConfigureAwait(false);

            static async Task DoLCSAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LCS", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("", val);
            }
        }

        [Test]
        public async Task LLenACLsAsync()
        {
            await CheckCommandsAsync(
                "LLEN",
                [DoLLenAsync]
            ).ConfigureAwait(false);

            static async Task DoLLenAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LLEN", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task LTrimACLsAsync()
        {
            await CheckCommandsAsync(
                "LTRIM",
                [DoLTrimAsync]
            ).ConfigureAwait(false);

            static async Task DoLTrimAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LTRIM", ["foo", "4", "10"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task LRangeACLsAsync()
        {
            await CheckCommandsAsync(
                "LRANGE",
                [DoLRangeAsync]
            ).ConfigureAwait(false);

            static async Task DoLRangeAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("LRANGE", ["foo", "4", "10"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task LIndexACLsAsync()
        {
            await CheckCommandsAsync(
                "LINDEX",
                [DoLIndexAsync]
            ).ConfigureAwait(false);

            static async Task DoLIndexAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LINDEX", ["foo", "4"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task LInsertACLsAsync()
        {
            await CheckCommandsAsync(
                "LINSERT",
                [DoLInsertBeforeAsync, DoLInsertAfterAsync]
            ).ConfigureAwait(false);

            static async Task DoLInsertBeforeAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LINSERT", ["foo", "BEFORE", "hello", "world"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoLInsertAfterAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LINSERT", ["foo", "AFTER", "hello", "world"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task LRemACLsAsync()
        {
            await CheckCommandsAsync(
                "LREM",
                [DoLRemAsync]
            ).ConfigureAwait(false);

            static async Task DoLRemAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("LREM", ["foo", "0", "hello"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task RPopLPushACLsAsync()
        {
            await CheckCommandsAsync(
                "RPOPLPUSH",
                [DoLRemAsync]
            ).ConfigureAwait(false);

            static async Task DoLRemAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("RPOPLPUSH", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task LMoveACLsAsync()
        {
            await CheckCommandsAsync(
                "LMOVE",
                [DoLMoveAsync]
            ).ConfigureAwait(false);

            static async Task DoLMoveAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LMOVE", ["foo", "bar", "LEFT", "RIGHT"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task LMPopACLsAsync()
        {
            await CheckCommandsAsync(
                "LMPOP",
                [DoLMPopAsync, DoLMPopCountAsync]
            ).ConfigureAwait(false);

            static async Task DoLMPopAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LMPOP", ["1", "foo", "LEFT"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }

            static async Task DoLMPopCountAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("LMPOP", ["1", "foo", "LEFT", "COUNT", "1"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task LSetACLsAsync()
        {
            await CheckCommandsAsync(
                "LSET",
                [DoLSetAsync]
            ).ConfigureAwait(false);

            static async Task DoLSetAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("LSET", ["foo", "0", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoMemoryUsageAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("MEMORY", ["USAGE", "foo"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }

            static async Task DoMemoryUsageSamplesAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("MEMORY", ["USAGE", "foo", "SAMPLES", "10"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task MGetACLsAsync()
        {
            await CheckCommandsAsync(
                "MGET",
                [DoMemorySingleAsync, DoMemoryMultiAsync]
            ).ConfigureAwait(false);

            static async Task DoMemorySingleAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("MGET", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.IsNull(val[0]);
            }

            static async Task DoMemoryMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("MGET", ["foo", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoMigrateAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("MIGRATE", ["127.0.0.1", "9999", "KEY", "0", "1000"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoPurgeBPClusterAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("PURGEBP", ["MigrationManager"]).ConfigureAwait(false);
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
                string val = await client.ExecuteForStringResultAsync("PURGEBP", ["ServerListener"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoModuleLoadAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("MODULE", ["LOADCS", "nonexisting.dll"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoMonitorAsync(GarnetClient client)
            {
                await client.ExecuteForStringResultAsync("MONITOR").ConfigureAwait(false);
                Assert.Fail("Should never reach this point");
            }
        }

        [Test]
        public async Task MSetACLsAsync()
        {
            await CheckCommandsAsync(
                "MSET",
                [DoMSetSingleAsync, DoMSetMultiAsync]
            ).ConfigureAwait(false);

            static async Task DoMSetSingleAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("MSET", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }

            static async Task DoMSetMultiAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("MSET", ["foo", "bar", "fizz", "buzz"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoMSetNXSingleAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("MSETNX", [$"foo-{count}", "bar"]).ConfigureAwait(false);
                count++;

                ClassicAssert.AreEqual(1, val);
            }

            async Task DoMSetNXMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("MSETNX", [$"foo-{count}", "bar", $"fizz-{count}", "buzz"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoMultiAsync(GarnetClient client)
            {
                try
                {
                    string val = await client.ExecuteForStringResultAsync("MULTI").ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoPersistAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PERSIST", ["foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoPExpireAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PEXPIRE", ["foo", "10"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoPExpireNXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PEXPIRE", ["foo", "10", "NX"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoPExpireXXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PEXPIRE", ["foo", "10", "XX"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoPExpireGTAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PEXPIRE", ["foo", "10", "GT"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoPExpireLTAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PEXPIRE", ["foo", "10", "LT"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoPFAddSingleAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PFADD", [$"foo-{count}", "bar"]).ConfigureAwait(false);
                count++;

                ClassicAssert.AreEqual(1, val);
            }

            async Task DoPFAddMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PFADD", [$"foo-{count}", "bar", "fizz"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoPFCountSingleAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PFCOUNT", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoPFCountMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PFCOUNT", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task PFMergeACLsAsync()
        {
            await CheckCommandsAsync(
                "PFMERGE",
                [DoPFMergeSingleAsync, DoPFMergeMultiAsync]
            ).ConfigureAwait(false);

            static async Task DoPFMergeSingleAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("PFMERGE", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }

            static async Task DoPFMergeMultiAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("PFMERGE", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task PingACLsAsync()
        {
            await CheckCommandsAsync(
                "PING",
                [DoPingAsync, DoPingMessageAsync]
            ).ConfigureAwait(false);

            static async Task DoPingAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("PING").ConfigureAwait(false);
                ClassicAssert.AreEqual("PONG", val);
            }

            static async Task DoPingMessageAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("PING", ["hello"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("hello", val);
            }
        }

        [Test]
        public async Task PSetEXACLsAsync()
        {
            await CheckCommandsAsync(
                "PSETEX",
                [DoPSetEXAsync]
            ).ConfigureAwait(false);

            static async Task DoPSetEXAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("PSETEX", ["foo", "10", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoPSubscribePatternAsync(GarnetClient client)
            {
                await client.ExecuteForStringResultAsync("PSUBSCRIBE", ["channel"]).ConfigureAwait(false);
                Assert.Fail("Should not reach this point");
            }
        }

        [Test]
        public async Task PUnsubscribeACLsAsync()
        {
            await CheckCommandsAsync(
                "PUNSUBSCRIBE",
                [DoPUnsubscribePatternAsync]
            ).ConfigureAwait(false);

            static async Task DoPUnsubscribePatternAsync(GarnetClient client)
            {
                string[] res = await client.ExecuteForStringArrayResultAsync("PUNSUBSCRIBE", ["foo"]).ConfigureAwait(false);
                ClassicAssert.IsNotNull(res);
            }
        }

        [Test]
        public async Task PTTLACLsAsync()
        {
            await CheckCommandsAsync(
                "PTTL",
                [DoPTTLAsync]
            ).ConfigureAwait(false);

            static async Task DoPTTLAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("PTTL", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(-2, val);
            }
        }

        [Test]
        public async Task PublishACLsAsync()
        {
            await CheckCommandsAsync(
                "PUBLISH",
                [DoPublishAsync]
            ).ConfigureAwait(false);

            static async Task DoPublishAsync(GarnetClient client)
            {
                long count = await client.ExecuteForLongResultAsync("PUBLISH", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, count);
            }
        }

        [Test]
        public async Task PubSubChannelsACLsAsync()
        {
            await CheckCommandsAsync(
                "PUBSUB CHANNELS",
                [DoPubSubChannelsAsync]
            ).ConfigureAwait(false);

            static async Task DoPubSubChannelsAsync(GarnetClient client)
            {
                var count = await client.ExecuteForStringArrayResultAsync("PUBSUB", ["CHANNELS"]).ConfigureAwait(false);
                CollectionAssert.IsEmpty(count);
            }
        }

        [Test]
        public async Task PubSubNumPatACLsAsync()
        {
            await CheckCommandsAsync(
                "PUBSUB NUMPAT",
                [DoPubSubNumPatAsync]
            ).ConfigureAwait(false);

            static async Task DoPubSubNumPatAsync(GarnetClient client)
            {
                var count = await client.ExecuteForLongResultAsync("PUBSUB", ["NUMPAT"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, count);
            }
        }

        [Test]
        public async Task PubSubNumSubACLsAsync()
        {
            await CheckCommandsAsync(
                "PUBSUB NUMSUB",
                [DoPubSubNumSubAsync]
            ).ConfigureAwait(false);

            static async Task DoPubSubNumSubAsync(GarnetClient client)
            {
                var count = await client.ExecuteForStringArrayResultAsync("PUBSUB", ["NUMSUB"]).ConfigureAwait(false);
                CollectionAssert.IsEmpty(count);
            }
        }

        [Test]
        public async Task ReadOnlyACLsAsync()
        {
            await CheckCommandsAsync(
                "READONLY",
                [DoReadOnlyAsync]
            ).ConfigureAwait(false);

            static async Task DoReadOnlyAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("READONLY").ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task ReadWriteACLsAsync()
        {
            await CheckCommandsAsync(
                "READWRITE",
                [DoReadWriteAsync]
            ).ConfigureAwait(false);

            static async Task DoReadWriteAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("READWRITE").ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoRegisterCSAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("REGISTERCS").ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoExpireTimeAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("EXPIRETIME", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(-2, val);
            }
        }

        [Test]
        public async Task PExpireTimeACLsAsync()
        {
            await CheckCommandsAsync(
                "PEXPIRETIME",
                [DoPExpireTimeAsync]
            ).ConfigureAwait(false);

            static async Task DoPExpireTimeAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("PEXPIRETIME", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(-2, val);
            }
        }

        [Test]
        public async Task RenameACLsAsync()
        {
            await CheckCommandsAsync(
                "RENAME",
                [DoRENAMEAsync]
            ).ConfigureAwait(false);

            static async Task DoRENAMEAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("RENAME", ["foo", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoRENAMENXAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("RENAMENX", ["foo", "bar"]).ConfigureAwait(false);
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
        public async Task RICreateACLsAsync()
        {
            int count = 0;

            await CheckCommandsAsync(
                "RI.CREATE",
                [DoRICreateAsync]
            ).ConfigureAwait(false);

            async Task DoRICreateAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringResultAsync("RI.CREATE", [$"myindex-{count}", "MEMORY", "CACHESIZE", "65536"]).ConfigureAwait(false);
                count++;
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task RIDelACLsAsync()
        {
            // Pre-create the index using default user
            using var setupClient = await CreateGarnetClientAsync(DefaultUser, DefaultPassword).ConfigureAwait(false);
            await setupClient.ExecuteForStringResultAsync("RI.CREATE", ["ridel-acl-idx", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8"]).ConfigureAwait(false);

            int count = 0;

            await CheckCommandsAsync(
                "RI.DEL",
                [DoRIDelAsync]
            ).ConfigureAwait(false);

            async Task DoRIDelAsync(GarnetClient client)
            {
                // Insert a field as default user, then delete it as the test user
                await setupClient.ExecuteForStringResultAsync("RI.SET", ["ridel-acl-idx", $"field-{count}", "val"]).ConfigureAwait(false);
                var val = await client.ExecuteForStringResultAsync("RI.DEL", ["ridel-acl-idx", $"field-{count}"]).ConfigureAwait(false);
                count++;
                ClassicAssert.AreEqual("1", val);
            }
        }

        [Test]
        public async Task RIGetACLsAsync()
        {
            // Pre-create the index and insert a field using default user
            using var setupClient = await CreateGarnetClientAsync(DefaultUser, DefaultPassword).ConfigureAwait(false);
            await setupClient.ExecuteForStringResultAsync("RI.CREATE", ["riget-acl-idx", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8"]).ConfigureAwait(false);
            await setupClient.ExecuteForStringResultAsync("RI.SET", ["riget-acl-idx", "field1", "value1"]).ConfigureAwait(false);

            await CheckCommandsAsync(
                "RI.GET",
                [DoRIGetAsync]
            ).ConfigureAwait(false);

            static async Task DoRIGetAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringResultAsync("RI.GET", ["riget-acl-idx", "field1"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("value1", val);
            }
        }

        [Test]
        public async Task RISetACLsAsync()
        {
            // Pre-create the index using default user
            using var setupClient = await CreateGarnetClientAsync(DefaultUser, DefaultPassword).ConfigureAwait(false);
            await setupClient.ExecuteForStringResultAsync("RI.CREATE", ["riset-acl-idx", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8"]).ConfigureAwait(false);

            await CheckCommandsAsync(
                "RI.SET",
                [DoRISetAsync]
            ).ConfigureAwait(false);

            static async Task DoRISetAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringResultAsync("RI.SET", ["riset-acl-idx", "field1", "value1"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task RIRangeACLsAsync()
        {
            // Pre-create the index and insert fields using default user (DISK mode required for scan)
            using var setupClient = await CreateGarnetClientAsync(DefaultUser, DefaultPassword).ConfigureAwait(false);
            await setupClient.ExecuteForStringResultAsync("RI.CREATE", ["rirange-acl-idx", "DISK", "CACHESIZE", "65536", "MINRECORD", "8"]).ConfigureAwait(false);
            await setupClient.ExecuteForStringResultAsync("RI.SET", ["rirange-acl-idx", "aaa", "val-a"]).ConfigureAwait(false);
            await setupClient.ExecuteForStringResultAsync("RI.SET", ["rirange-acl-idx", "bbb", "val-b"]).ConfigureAwait(false);

            await CheckCommandsAsync(
                "RI.RANGE",
                [DoRIRangeAsync]
            ).ConfigureAwait(false);

            static async Task DoRIRangeAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("RI.RANGE", ["rirange-acl-idx", "aaa", "bbb", "FIELDS", "KEY"]).ConfigureAwait(false);
                ClassicAssert.IsNotNull(val);
                ClassicAssert.AreEqual(2, val.Length);
            }
        }

        [Test]
        public async Task RIScanACLsAsync()
        {
            // Pre-create the index and insert a field using default user (DISK mode required for scan)
            using var setupClient = await CreateGarnetClientAsync(DefaultUser, DefaultPassword).ConfigureAwait(false);
            await setupClient.ExecuteForStringResultAsync("RI.CREATE", ["riscan-acl-idx", "DISK", "CACHESIZE", "65536", "MINRECORD", "8"]).ConfigureAwait(false);
            await setupClient.ExecuteForStringResultAsync("RI.SET", ["riscan-acl-idx", "aaa", "val-a"]).ConfigureAwait(false);

            await CheckCommandsAsync(
                "RI.SCAN",
                [DoRIScanAsync]
            ).ConfigureAwait(false);

            static async Task DoRIScanAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("RI.SCAN", ["riscan-acl-idx", "aaa", "COUNT", "10", "FIELDS", "KEY"]).ConfigureAwait(false);
                ClassicAssert.IsNotNull(val);
                ClassicAssert.IsTrue(val.Length >= 1);
            }
        }

        [Test]
        public async Task RIExistsACLsAsync()
        {
            // Pre-create the index using default user
            using var setupClient = await CreateGarnetClientAsync(DefaultUser, DefaultPassword).ConfigureAwait(false);
            await setupClient.ExecuteForStringResultAsync("RI.CREATE", ["riexists-acl-idx", "MEMORY", "CACHESIZE", "65536"]).ConfigureAwait(false);

            await CheckCommandsAsync(
                "RI.EXISTS",
                [DoRIExistsAsync]
            ).ConfigureAwait(false);

            static async Task DoRIExistsAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringResultAsync("RI.EXISTS", ["riexists-acl-idx"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("1", val);
            }
        }

        [Test]
        public async Task RIConfigACLsAsync()
        {
            // Pre-create the index using default user
            using var setupClient = await CreateGarnetClientAsync(DefaultUser, DefaultPassword).ConfigureAwait(false);
            await setupClient.ExecuteForStringResultAsync("RI.CREATE", ["riconfig-acl-idx", "MEMORY", "CACHESIZE", "65536"]).ConfigureAwait(false);

            await CheckCommandsAsync(
                "RI.CONFIG",
                [DoRIConfigAsync]
            ).ConfigureAwait(false);

            static async Task DoRIConfigAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("RI.CONFIG", ["riconfig-acl-idx"]).ConfigureAwait(false);
                ClassicAssert.IsNotNull(val);
                ClassicAssert.AreEqual(12, val.Length);
            }
        }

        [Test]
        public async Task RIMetricsACLsAsync()
        {
            // Pre-create the index using default user
            using var setupClient = await CreateGarnetClientAsync(DefaultUser, DefaultPassword).ConfigureAwait(false);
            await setupClient.ExecuteForStringResultAsync("RI.CREATE", ["rimetrics-acl-idx", "MEMORY", "CACHESIZE", "65536"]).ConfigureAwait(false);

            await CheckCommandsAsync(
                "RI.METRICS",
                [DoRIMetricsAsync]
            ).ConfigureAwait(false);

            static async Task DoRIMetricsAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("RI.METRICS", ["rimetrics-acl-idx"]).ConfigureAwait(false);
                ClassicAssert.IsNotNull(val);
                ClassicAssert.AreEqual(8, val.Length);
            }
        }

        [Test]
        public async Task ReplicaOfACLsAsync()
        {
            // Uses exceptions as control flow, since clustering is disabled in these tests

            await CheckCommandsAsync(
                "REPLICAOF",
                [DoReplicaOfAsync, DoReplicaOfNoOneAsync]
            ).ConfigureAwait(false);

            static async Task DoReplicaOfAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("REPLICAOF", ["127.0.0.1", "9999"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("REPLICAOF", ["NO", "ONE"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoRunTxpAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("RUNTXP", ["4"]).ConfigureAwait(false);
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
           ).ConfigureAwait(false);

            static async Task DoSaveAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SAVE").ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoScanAsync(GarnetClient client)
            {
                // SCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SCAN", ["0"]).ConfigureAwait(false);
            }

            static async Task DoScanMatchAsync(GarnetClient client)
            {
                // SCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SCAN", ["0", "MATCH", "*"]).ConfigureAwait(false);
            }

            static async Task DoScanCountAsync(GarnetClient client)
            {
                // SCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SCAN", ["0", "COUNT", "5"]).ConfigureAwait(false);
            }

            static async Task DoScanTypeAsync(GarnetClient client)
            {
                // SCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SCAN", ["0", "TYPE", "zset"]).ConfigureAwait(false);
            }

            static async Task DoScanMatchCountAsync(GarnetClient client)
            {
                // SCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SCAN", ["0", "MATCH", "*", "COUNT", "5"]).ConfigureAwait(false);
            }

            static async Task DoScanMatchTypeAsync(GarnetClient client)
            {
                // SCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SCAN", ["0", "MATCH", "*", "TYPE", "zset"]).ConfigureAwait(false);
            }

            static async Task DoScanCountTypeAsync(GarnetClient client)
            {
                // SCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SCAN", ["0", "COUNT", "5", "TYPE", "zset"]).ConfigureAwait(false);
            }

            static async Task DoScanMatchCountTypeAsync(GarnetClient client)
            {
                // SCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SCAN", ["0", "MATCH", "*", "COUNT", "5", "TYPE", "zset"]).ConfigureAwait(false);
            }
        }

        [Test]
        public async Task SecondaryOfACLsAsync()
        {
            // Uses exceptions as control flow, since clustering is disabled in these tests

            await CheckCommandsAsync(
                "SECONDARYOF",
                [DoSecondaryOfAsync, DoSecondaryOfNoOneAsync]
            ).ConfigureAwait(false);

            static async Task DoSecondaryOfAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("SECONDARYOF", ["127.0.0.1", "9999"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("SECONDARYOF", ["NO", "ONE"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoSelectAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SELECT", ["0"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoSetAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SET", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }

            static async Task DoSetExNxAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SET", ["foo", "bar", "NX", "EX", "100"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }

            static async Task DoSetXxNxAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SET", ["foo", "bar", "XX", "EX", "100"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }

            static async Task DoSetKeepTtlAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SET", ["foo", "bar", "KEEPTTL"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }

            static async Task DoSetKeepTtlXxAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SET", ["foo", "bar", "XX", "KEEPTTL"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task SetIfMatchACLsAsync()
        {
            await CheckCommandsAsync(
               "SETIFMATCH",
               [DoSetIfMatchAsync]
           ).ConfigureAwait(false);

            static async Task DoSetIfMatchAsync(GarnetClient client)
            {
                var res = await client.ExecuteForStringArrayResultAsync("SETIFMATCH", ["foo", "rizz", "0"]).ConfigureAwait(false);
                ClassicAssert.IsNotNull(res);
            }
        }

        [Test]
        public async Task SetIfGreaterACLsAsync()
        {
            await CheckCommandsAsync(
               "SETIFGREATER",
               [DoSetIfGreaterAsync]
           ).ConfigureAwait(false);

            static async Task DoSetIfGreaterAsync(GarnetClient client)
            {
                var res = await client.ExecuteForStringArrayResultAsync("SETIFGREATER", ["foo", "rizz", "0"]).ConfigureAwait(false);
                ClassicAssert.IsNotNull(res);
            }
        }

        [Test]
        public async Task SetWithEtagACLsAsync()
        {
            int count = 0;

            await CheckCommandsAsync(
               "SETWITHETAG",
               [DoSetWithEtagAsync]
           ).ConfigureAwait(false);

            async Task DoSetWithEtagAsync(GarnetClient client)
            {
                var res = await client.ExecuteForStringResultAsync("SETWITHETAG", [$"key-{count}", "value"]).ConfigureAwait(false);
                count++;
                ClassicAssert.IsNotNull(res);
            }
        }

        [Test]
        public async Task DelIfGreaterACLsAsync()
        {
            await CheckCommandsAsync(
               "DELIFGREATER",
               [DoDelIfGreaterAsync]
           ).ConfigureAwait(false);

            static async Task DoDelIfGreaterAsync(GarnetClient client)
            {
                var res = await client.ExecuteForStringArrayResultAsync("DELIFGREATER", ["foo", "1"]).ConfigureAwait(false);
                ClassicAssert.IsNotNull(res);
            }
        }

        [Test]
        public async Task GetIfNotMatchACLsAsync()
        {
            await CheckCommandsAsync(
               "GETIFNOTMATCH",
               [DoGetIfNotMatchAsync]
           ).ConfigureAwait(false);

            static async Task DoGetIfNotMatchAsync(GarnetClient client)
            {
                var res = await client.ExecuteForStringResultAsync("GETIFNOTMATCH", ["foo", "0"]).ConfigureAwait(false);
                ClassicAssert.IsNull(res);
            }
        }

        [Test]
        public async Task GetWithEtagACLsAsync()
        {
            await CheckCommandsAsync(
               "GETWITHETAG",
               [DoGetWithEtagAsync]
           ).ConfigureAwait(false);

            static async Task DoGetWithEtagAsync(GarnetClient client)
            {
                var res = await client.ExecuteForStringResultAsync("GETWITHETAG", ["foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoSetBitAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SETBIT", [$"foo-{count}", "10", "1"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoSetEXAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SETEX", ["foo", "10", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoSetIfNotExistAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("SETNX", [$"foo-{keyIx++}", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(1, val);
            }
        }

        [Test]
        public async Task SetRangeACLsAsync()
        {
            await CheckCommandsAsync(
                "SETRANGE",
                [DoSetRangeAsync]
            ).ConfigureAwait(false);

            static async Task DoSetRangeAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SETRANGE", ["foo", "10", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(13, val);
            }
        }

        [Test]
        public async Task StrLenACLsAsync()
        {
            await CheckCommandsAsync(
                "STRLEN",
                [DoStrLenAsync]
            ).ConfigureAwait(false);

            static async Task DoStrLenAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("STRLEN", ["foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoSAddAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SADD", [$"foo-{count}", "bar"]).ConfigureAwait(false);
                count++;

                ClassicAssert.AreEqual(1, val);
            }

            async Task DoSAddMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SADD", [$"foo-{count}", "bar", "fizz"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoSRemAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SREM", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoSRemMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SREM", ["foo", "bar", "fizz"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SPopACLsAsync()
        {
            await CheckCommandsAsync(
                "SPOP",
                [DoSPopAsync, DoSPopCountAsync]
            ).ConfigureAwait(false);

            static async Task DoSPopAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SPOP", ["foo"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }

            static async Task DoSPopCountAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SPOP", ["foo", "11"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task SMembersACLsAsync()
        {
            await CheckCommandsAsync(
                "SMEMBERS",
                [DoSMembersAsync]
            ).ConfigureAwait(false);

            static async Task DoSMembersAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SMEMBERS", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task SCardACLsAsync()
        {
            await CheckCommandsAsync(
                "SCARD",
                [DoSCardAsync]
            ).ConfigureAwait(false);

            static async Task DoSCardAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SCARD", ["foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoSScanAsync(GarnetClient client)
            {
                // SSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SSCAN", ["foo", "0"]).ConfigureAwait(false);
            }

            static async Task DoSScanMatchAsync(GarnetClient client)
            {
                // SSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SSCAN", ["foo", "0", "MATCH", "*"]).ConfigureAwait(false);
            }

            static async Task DoSScanCountAsync(GarnetClient client)
            {
                // SSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SSCAN", ["foo", "0", "COUNT", "5"]).ConfigureAwait(false);
            }

            static async Task DoSScanMatchCountAsync(GarnetClient client)
            {
                // SSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("SSCAN", ["foo", "0", "MATCH", "*", "COUNT", "5"]).ConfigureAwait(false);
            }
        }

        [Test]
        public async Task SlaveOfACLsAsync()
        {
            // Uses exceptions as control flow, since clustering is disabled in these tests

            await CheckCommandsAsync(
                "SLAVEOF",
                [DoSlaveOfAsync, DoSlaveOfNoOneAsync]
            ).ConfigureAwait(false);

            static async Task DoSlaveOfAsync(GarnetClient client)
            {
                try
                {
                    await client.ExecuteForStringResultAsync("SLAVEOF", ["127.0.0.1", "9999"]).ConfigureAwait(false);
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
                    await client.ExecuteForStringResultAsync("SLAVEOF", ["NO", "ONE"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoSlowlogGetAsync(GarnetClient client)
            {
                string[] res = await client.ExecuteForStringArrayResultAsync("SLOWLOG", ["GET"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, res.Length);
            }
        }

        [Test]
        public async Task SlowlogHelpACLsAsync()
        {
            await CheckCommandsAsync(
                "SLOWLOG HELP",
                [DoSlowlogHelpAsync]
            ).ConfigureAwait(false);

            static async Task DoSlowlogHelpAsync(GarnetClient client)
            {
                string[] res = await client.ExecuteForStringArrayResultAsync("SLOWLOG", ["HELP"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(12, res.Length);
            }
        }

        [Test]
        public async Task SlowlogLenACLsAsync()
        {
            await CheckCommandsAsync(
                "SLOWLOG LEN",
                [DoSlowlogLenAsync]
            ).ConfigureAwait(false);

            static async Task DoSlowlogLenAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("SLOWLOG", ["LEN"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("0", res);
            }
        }

        [Test]
        public async Task SlowlogResetACLsAsync()
        {
            await CheckCommandsAsync(
                "SLOWLOG RESET",
                [DoSlowlogResetAsync]
            ).ConfigureAwait(false);

            static async Task DoSlowlogResetAsync(GarnetClient client)
            {
                string res = await client.ExecuteForStringResultAsync("SLOWLOG", ["RESET"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", res);
            }
        }

        [Test]
        public async Task SMoveACLsAsync()
        {
            await CheckCommandsAsync(
                "SMOVE",
                [DoSMoveAsync]
            ).ConfigureAwait(false);

            static async Task DoSMoveAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SMOVE", ["foo", "bar", "fizz"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SRandMemberACLsAsync()
        {
            await CheckCommandsAsync(
                "SRANDMEMBER",
                [DoSRandMemberAsync, DoSRandMemberCountAsync]
            ).ConfigureAwait(false);

            static async Task DoSRandMemberAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("SRANDMEMBER", ["foo"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }

            static async Task DoSRandMemberCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SRANDMEMBER", ["foo", "5"]).ConfigureAwait(false);
                ClassicAssert.IsNotNull(val);
            }
        }

        [Test]
        public async Task SIsMemberACLsAsync()
        {
            await CheckCommandsAsync(
                "SISMEMBER",
                [DoSIsMemberAsync]
            ).ConfigureAwait(false);

            static async Task DoSIsMemberAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SISMEMBER", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SMIsMemberACLsAsync()
        {
            await CheckCommandsAsync(
                "SMISMEMBER",
                [DoSMultiIsMemberAsync]
            ).ConfigureAwait(false);

            static async Task DoSMultiIsMemberAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SMISMEMBER", ["foo", "5"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoSubscribeAsync(GarnetClient client)
            {
                await client.ExecuteForStringResultAsync("SUBSCRIBE", ["channel"]).ConfigureAwait(false);
                Assert.Fail("Shouldn't reach this point");
            }
        }

        [Test]
        public async Task SUnionACLsAsync()
        {
            await CheckCommandsAsync(
                "SUNION",
                [DoSUnionAsync, DoSUnionMultiAsync]
            ).ConfigureAwait(false);

            static async Task DoSUnionAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SUNION", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoSUnionMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SUNION", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task SUnionStoreACLsAsync()
        {
            await CheckCommandsAsync(
                "SUNIONSTORE",
                [DoSUnionStoreAsync, DoSUnionStoreMultiAsync]
            ).ConfigureAwait(false);

            static async Task DoSUnionStoreAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SUNIONSTORE", ["dest", "foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoSUnionStoreMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SUNIONSTORE", ["dest", "foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SDiffACLsAsync()
        {
            await CheckCommandsAsync(
                "SDIFF",
                [DoSDiffAsync, DoSDiffMultiAsync]
            ).ConfigureAwait(false);

            static async Task DoSDiffAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SDIFF", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoSDiffMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SDIFF", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task SDiffStoreACLsAsync()
        {
            await CheckCommandsAsync(
                "SDIFFSTORE",
                [DoSDiffStoreAsync, DoSDiffStoreMultiAsync]
            ).ConfigureAwait(false);

            static async Task DoSDiffStoreAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SDIFFSTORE", ["dest", "foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoSDiffStoreMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SDIFFSTORE", ["dest", "foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SInterACLsAsync()
        {
            await CheckCommandsAsync(
                "SINTER",
                [DoSDiffAsync, DoSDiffMultiAsync]
            ).ConfigureAwait(false);

            static async Task DoSDiffAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SINTER", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoSDiffMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("SINTER", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task SInterCardACLsAsync()
        {
            await CheckCommandsAsync(
                "SINTERCARD",
                [DoUnionAsync]
            ).ConfigureAwait(false);

            static async Task DoUnionAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("SINTERCARD", ["2", "foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task SInterStoreACLsAsync()
        {
            await CheckCommandsAsync(
                "SINTERSTORE",
                [DoSDiffStoreAsync, DoSDiffStoreMultiAsync]
            ).ConfigureAwait(false);

            static async Task DoSDiffStoreAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SINTERSTORE", ["dest", "foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoSDiffStoreMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("SINTERSTORE", ["dest", "foo", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoGeoAddAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GEOADD", [$"foo-{count}", "90", "90", "bar"]).ConfigureAwait(false);
                count++;

                ClassicAssert.AreEqual(1, val);
            }

            async Task DoGeoAddNXAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GEOADD", [$"foo-{count}", "NX", "90", "90", "bar"]).ConfigureAwait(false);
                count++;

                ClassicAssert.AreEqual(1, val);
            }

            async Task DoGeoAddNXCHAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GEOADD", [$"foo-{count}", "NX", "CH", "90", "90", "bar"]).ConfigureAwait(false);
                count++;

                ClassicAssert.AreEqual(1, val);
            }

            async Task DoGeoAddMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GEOADD", [$"foo-{count}", "90", "90", "bar", "45", "45", "fizz"]).ConfigureAwait(false);
                count++;

                ClassicAssert.AreEqual(2, val);
            }

            async Task DoGeoAddNXMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GEOADD", [$"foo-{count}", "NX", "90", "90", "bar", "45", "45", "fizz"]).ConfigureAwait(false);
                count++;

                ClassicAssert.AreEqual(2, val);
            }

            async Task DoGeoAddNXCHMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("GEOADD", [$"foo-{count}", "NX", "CH", "90", "90", "bar", "45", "45", "fizz"]).ConfigureAwait(false);
                count++;

                ClassicAssert.AreEqual(2, val);
            }
        }

        [Test]
        public async Task GeoHashACLsAsync()
        {
            // TODO: GEOHASH responses do not match Redis when keys are missing.
            // So create some keys to make testing ACLs easier.
            using var outerClient = await CreateGarnetClientAsync(DefaultUser, DefaultPassword).ConfigureAwait(false);
            ClassicAssert.AreEqual(1, await outerClient.ExecuteForLongResultAsync("GEOADD", ["foo", "10", "10", "bar"]).ConfigureAwait(false));
            ClassicAssert.AreEqual(1, await outerClient.ExecuteForLongResultAsync("GEOADD", ["foo", "20", "20", "fizz"]).ConfigureAwait(false));

            await CheckCommandsAsync(
                "GEOHASH",
                [DoGeoHashAsync, DoGeoHashSingleAsync, DoGeoHashMultiAsync]
            ).ConfigureAwait(false);

            static async Task DoGeoHashAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("GEOHASH", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoGeoHashSingleAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("GEOHASH", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.IsNotNull(val[0]);
            }

            static async Task DoGeoHashMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("GEOHASH", ["foo", "bar", "fizz"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoGetDistAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("GEODIST", ["foo", "bar", "fizz"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }

            static async Task DoGetDistMAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("GEODIST", ["foo", "bar", "fizz", "M"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoGeoPosAsync(GarnetClient client)
            {
                // GEOPOS replies with an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("GEOPOS", ["foo"]).ConfigureAwait(false);
            }

            static async Task DoGeoPosMultiAsync(GarnetClient client)
            {
                // GEOPOS replies with an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("GEOPOS", ["foo", "bar"]).ConfigureAwait(false);
            }
        }

        [Test]
        public async Task GeoRadiusACLsAsync()
        {
            await CheckCommandsAsync(
                "GEORADIUS",
                [DoGeoRadiusAsync],
                skipPermitted: true
            ).ConfigureAwait(false);

            static async Task DoGeoRadiusAsync(GarnetClient client)
            {
                await client.ExecuteForStringResultAsync("GEORADIUS", ["foo", "0", "85", "10", "km"]).ConfigureAwait(false);
            }
        }

        [Test]
        public async Task GeoRadiusROACLsAsync()
        {
            await CheckCommandsAsync(
                "GEORADIUS_RO",
                [DoGeoRadiusROAsync],
                skipPermitted: true
            ).ConfigureAwait(false);

            static async Task DoGeoRadiusROAsync(GarnetClient client)
            {
                await client.ExecuteForStringResultAsync("GEORADIUS_RO", ["foo", "0", "85", "10", "km"]).ConfigureAwait(false);
            }
        }

        [Test]
        public async Task GeoRadiusByMemberACLsAsync()
        {
            await CheckCommandsAsync(
                "GEORADIUSBYMEMBER",
                [DoGeoRadiusByMemberAsync],
                skipPermitted: true
            ).ConfigureAwait(false);

            static async Task DoGeoRadiusByMemberAsync(GarnetClient client)
            {
                await client.ExecuteForStringResultAsync("GEORADIUSBYMEMBER", ["foo", "bar", "10", "km"]).ConfigureAwait(false);
            }
        }

        [Test]
        public async Task GeoRadiusByMemberROACLsAsync()
        {
            await CheckCommandsAsync(
                "GEORADIUSBYMEMBER_RO",
                [DoGeoRadiusByMemberROAsync],
                skipPermitted: true
            ).ConfigureAwait(false);

            static async Task DoGeoRadiusByMemberROAsync(GarnetClient client)
            {
                await client.ExecuteForStringResultAsync("GEORADIUSBYMEMBER_RO", ["foo", "bar", "10", "km"]).ConfigureAwait(false);
            }
        }

        [Test]
        public async Task GeoSearchACLsAsync()
        {
            await CheckCommandsAsync(
                "GEOSEARCH",
                [DoGeoSearchAsync],
                skipPermitted: true
            ).ConfigureAwait(false);

            static async Task DoGeoSearchAsync(GarnetClient client)
            {
                // GEOSEARCH replies with an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("GEOSEARCH", ["foo", "FROMMEMBER", "bar", "BYBOX", "2", "2", "M"]).ConfigureAwait(false);
            }
        }

        [Test]
        public async Task GeoSearchStoreACLsAsync()
        {
            await CheckCommandsAsync(
                "GEOSEARCHSTORE",
                [DoGeoSearchStoreAsync],
                skipPermitted: true
            ).ConfigureAwait(false);

            static async Task DoGeoSearchStoreAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("GEOSEARCHSTORE", ["bar", "foo", "FROMMEMBER", "bar", "BYBOX", "2", "2", "M", "STOREDIST"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoZAddAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZADD", [$"foo-{count}", "10", "bar"]).ConfigureAwait(false);
                count++;
                ClassicAssert.AreEqual(1, val);
            }

            async Task DoZAddMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZADD", [$"foo-{count}", "10", "bar", "20", "fizz"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoZCardAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZCARD", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }



        [Test]
        public async Task ZMPopACLsAsync()
        {
            await CheckCommandsAsync(
                "ZMPOP",
                [DoZMPopAsync, DoZMPopCountAsync]
            ).ConfigureAwait(false);

            static async Task DoZMPopAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZMPOP", ["2", "foo", "bar", "MIN"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.IsNull(val[0]);
            }

            static async Task DoZMPopCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZMPOP", ["2", "foo", "bar", "MAX", "COUNT", "10"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoZPopMaxAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZPOPMAX", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZPopMaxCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZPOPMAX", ["foo", "10"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZScoreACLsAsync()
        {
            await CheckCommandsAsync(
                "ZSCORE",
                [DoZScoreAsync]
            ).ConfigureAwait(false);

            static async Task DoZScoreAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ZSCORE", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task ZRemACLsAsync()
        {
            await CheckCommandsAsync(
                "ZREM",
                [DoZRemAsync, DoZRemMultiAsync]
            ).ConfigureAwait(false);

            static async Task DoZRemAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZREM", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoZRemMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZREM", ["foo", "bar", "fizz"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZCountACLsAsync()
        {
            await CheckCommandsAsync(
                "ZCOUNT",
                [DoZCountAsync]
            ).ConfigureAwait(false);

            static async Task DoZCountAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZCOUNT", ["foo", "10", "20"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            async Task DoZIncrByAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ZINCRBY", [$"foo-{count}", "10", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoZRankAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ZRANK", ["foo", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoZRangeAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANGE", ["key", "10", "20"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZRevRangeByLexACLsAsync()
        {
            await CheckCommandsAsync(
                "ZREVRANGEBYLEX",
                [DoZRevRangeByLexAsync]
            ).ConfigureAwait(false);

            static async Task DoZRevRangeByLexAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZREVRANGEBYLEX", ["key", "10", "20"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZRangeStoreACLsAsync()
        {
            await CheckCommandsAsync(
                "ZRANGESTORE",
                [DoZRangeStoreAsync]
            ).ConfigureAwait(false);

            static async Task DoZRangeStoreAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("ZRANGESTORE", ["dkey", "key", "0", "-1"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZRangeByLexACLsAsync()
        {
            await CheckCommandsAsync(
                "ZRANGEBYLEX",
                [DoZRangeByLexAsync, DoZRangeByLexLimitAsync]
            ).ConfigureAwait(false);

            static async Task DoZRangeByLexAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANGEBYLEX", ["key", "10", "20"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZRangeByLexLimitAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANGEBYLEX", ["key", "10", "20", "LIMIT", "2", "3"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZRangeByScoreACLsAsync()
        {
            await CheckCommandsAsync(
                "ZRANGEBYSCORE",
                [DoZRangeByScoreAsync, DoZRangeByScoreWithScoresAsync, DoZRangeByScoreLimitAsync, DoZRangeByScoreWithScoresLimitAsync]
            ).ConfigureAwait(false);

            static async Task DoZRangeByScoreAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANGEBYSCORE", ["key", "10", "20"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZRangeByScoreWithScoresAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANGEBYSCORE", ["key", "10", "20", "WITHSCORES"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZRangeByScoreLimitAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANGEBYSCORE", ["key", "10", "20", "LIMIT", "2", "3"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZRangeByScoreWithScoresLimitAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANGEBYSCORE", ["key", "10", "20", "WITHSCORES", "LIMIT", "2", "3"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZRevRangeACLsAsync()
        {
            await CheckCommandsAsync(
                "ZREVRANGE",
                [DoZRevRangeAsync, DoZRevRangeWithScoresAsync]
            ).ConfigureAwait(false);

            static async Task DoZRevRangeAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZREVRANGE", ["key", "10", "20"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZRevRangeWithScoresAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZREVRANGE", ["key", "10", "20", "WITHSCORES"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZRevRangeByScoreACLsAsync()
        {
            await CheckCommandsAsync(
                "ZREVRANGEBYSCORE",
                [DoZRevRangeByScoreAsync, DoZRevRangeByScoreWithScoresAsync, DoZRevRangeByScoreLimitAsync, DoZRevRangeByScoreWithScoresLimitAsync]
            ).ConfigureAwait(false);

            static async Task DoZRevRangeByScoreAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZREVRANGEBYSCORE", ["key", "10", "20"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZRevRangeByScoreWithScoresAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZREVRANGEBYSCORE", ["key", "10", "20", "WITHSCORES"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZRevRangeByScoreLimitAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZREVRANGEBYSCORE", ["key", "10", "20", "LIMIT", "2", "3"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZRevRangeByScoreWithScoresLimitAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZREVRANGEBYSCORE", ["key", "10", "20", "WITHSCORES", "LIMIT", "2", "3"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZRevRankACLsAsync()
        {
            await CheckCommandsAsync(
                "ZREVRANK",
                [DoZRevRankAsync]
            ).ConfigureAwait(false);

            static async Task DoZRevRankAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ZREVRANK", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }
        }

        [Test]
        public async Task ZRemRangeByLexACLsAsync()
        {
            await CheckCommandsAsync(
                "ZREMRANGEBYLEX",
                [DoZRemRangeByLexAsync]
            ).ConfigureAwait(false);

            static async Task DoZRemRangeByLexAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZREMRANGEBYLEX", ["foo", "abc", "def"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZRemRangeByRankACLsAsync()
        {
            await CheckCommandsAsync(
                "ZREMRANGEBYRANK",
                [DoZRemRangeByRankAsync]
            ).ConfigureAwait(false);

            static async Task DoZRemRangeByRankAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZREMRANGEBYRANK", ["foo", "10", "20"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZRemRangeByScoreACLsAsync()
        {
            await CheckCommandsAsync(
                "ZREMRANGEBYSCORE",
                [DoZRemRangeByRankAsync]
            ).ConfigureAwait(false);

            static async Task DoZRemRangeByRankAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZREMRANGEBYSCORE", ["foo", "10", "20"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZLexCountACLsAsync()
        {
            await CheckCommandsAsync(
                "ZLEXCOUNT",
                [DoZLexCountAsync]
            ).ConfigureAwait(false);

            static async Task DoZLexCountAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("ZLEXCOUNT", ["foo", "abc", "def"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZPopMinACLsAsync()
        {
            await CheckCommandsAsync(
                "ZPOPMIN",
                [DoZPopMinAsync, DoZPopMinCountAsync]
            ).ConfigureAwait(false);

            static async Task DoZPopMinAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZPOPMIN", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZPopMinCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZPOPMIN", ["foo", "10"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZRandMemberACLsAsync()
        {
            await CheckCommandsAsync(
                "ZRANDMEMBER",
                [DoZRandMemberAsync, DoZRandMemberCountAsync, DoZRandMemberCountWithScoresAsync]
            ).ConfigureAwait(false);

            static async Task DoZRandMemberAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("ZRANDMEMBER", ["foo"]).ConfigureAwait(false);
                ClassicAssert.IsNull(val);
            }

            static async Task DoZRandMemberCountAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANDMEMBER", ["foo", "10"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZRandMemberCountWithScoresAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZRANDMEMBER", ["foo", "10", "WITHSCORES"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoZDiffAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZDIFF", ["1", "foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }

            static async Task DoZDiffMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZDIFF", ["2", "foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZDiffStoreACLsAsync()
        {
            await CheckCommandsAsync(
                "ZDIFFSTORE",
                [DoZDiffStoreAsync]
            ).ConfigureAwait(false);

            static async Task DoZDiffStoreAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("ZDIFFSTORE", ["keyZ", "2", "foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZInterACLsAsync()
        {
            await CheckCommandsAsync(
                "ZINTER",
                [DoZInterAsync]
            ).ConfigureAwait(false);

            static async Task DoZInterAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZINTER", ["2", "foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZInterCardACLsAsync()
        {
            await CheckCommandsAsync(
                "ZINTERCARD",
                [DoZInterCardAsync]
            ).ConfigureAwait(false);

            static async Task DoZInterCardAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("ZINTERCARD", ["2", "foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZInterStoreACLsAsync()
        {
            await CheckCommandsAsync(
                "ZINTERSTORE",
                [DoZInterStoreAsync]
            ).ConfigureAwait(false);

            static async Task DoZInterStoreAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("ZINTERSTORE", ["keyZ", "2", "foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task ZUnionACLsAsync()
        {
            await CheckCommandsAsync(
                "ZUNION",
                [DoZUnionAsync]
            ).ConfigureAwait(false);

            static async Task DoZUnionAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZUNION", ["2", "foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task ZUnionStoreACLsAsync()
        {
            await CheckCommandsAsync(
                "ZUNIONSTORE",
                [DoZUnionStoreAsync]
            ).ConfigureAwait(false);

            static async Task DoZUnionStoreAsync(GarnetClient client)
            {
                var val = await client.ExecuteForLongResultAsync("ZUNIONSTORE", ["keyZ", "2", "foo", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoZScanAsync(GarnetClient client)
            {
                // ZSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("ZSCAN", ["foo", "0"]).ConfigureAwait(false);
            }

            static async Task DoZScanMatchAsync(GarnetClient client)
            {
                // ZSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("ZSCAN", ["foo", "0", "MATCH", "*"]).ConfigureAwait(false);
            }

            static async Task DoZScanCountAsync(GarnetClient client)
            {
                // ZSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("ZSCAN", ["foo", "0", "COUNT", "2"]).ConfigureAwait(false);
            }

            static async Task DoZScanNoValuesAsync(GarnetClient client)
            {
                // ZSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("ZSCAN", ["foo", "0", "NOVALUES"]).ConfigureAwait(false);
            }

            static async Task DoZScanMatchCountAsync(GarnetClient client)
            {
                // ZSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("ZSCAN", ["foo", "0", "MATCH", "*", "COUNT", "2"]).ConfigureAwait(false);
            }

            static async Task DoZScanMatchNoValuesAsync(GarnetClient client)
            {
                // ZSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("ZSCAN", ["foo", "0", "MATCH", "*", "NOVALUES"]).ConfigureAwait(false);
            }

            static async Task DoZScanCountNoValuesAsync(GarnetClient client)
            {
                // ZSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("ZSCAN", ["foo", "0", "COUNT", "0", "NOVALUES"]).ConfigureAwait(false);
            }

            static async Task DoZScanMatchCountNoValuesAsync(GarnetClient client)
            {
                // ZSCAN returns an array of arrays, which GarnetClient doesn't deal with
                await client.ExecuteForStringResultAsync("ZSCAN", ["foo", "0", "MATCH", "*", "COUNT", "0", "NOVALUES"]).ConfigureAwait(false);
            }
        }

        [Test]
        public async Task ZMScoreACLsAsync()
        {
            await CheckCommandsAsync(
                "ZMSCORE",
                [DoZDiffAsync, DoZDiffMultiAsync]
            ).ConfigureAwait(false);

            static async Task DoZDiffAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZMSCORE", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(1, val.Length);
                ClassicAssert.IsNull(val[0]);
            }

            static async Task DoZDiffMultiAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("ZMSCORE", ["foo", "bar", "fizz"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoZExpireAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZEXPIRE", ["foo", "1", "MEMBERS", "1", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoZPExpireAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZPEXPIRE", ["foo", "1", "MEMBERS", "1", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoZExpireAtAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZEXPIREAT", ["foo", DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeSeconds().ToString(), "MEMBERS", "1", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoZPExpireAtAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZPEXPIREAT", ["foo", DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeMilliseconds().ToString(), "MEMBERS", "1", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoZExpireTimeAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZEXPIRETIME", ["foo", "MEMBERS", "1", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoZPExpireTimeAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZPEXPIRETIME", ["foo", "MEMBERS", "1", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoZETTLAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZTTL", ["foo", "MEMBERS", "1", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoZPETTLAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZPTTL", ["foo", "MEMBERS", "1", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoZPersistAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringArrayResultAsync("ZPERSIST", ["foo", "MEMBERS", "1", "bar"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoZCollectAsync(GarnetClient client)
            {
                var val = await client.ExecuteForStringResultAsync("ZCOLLECT", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task TimeACLsAsync()
        {
            await CheckCommandsAsync(
                "TIME",
                [DoTimeAsync]
            ).ConfigureAwait(false);

            static async Task DoTimeAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("TIME").ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoTTLAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("TTL", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(-2, val);
            }
        }

        [Test]
        public async Task DumpACLsAsync()
        {
            await CheckCommandsAsync(
                "DUMP",
                [DoDUMPAsync]
            ).ConfigureAwait(false);

            static async Task DoDUMPAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("DUMP", ["foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

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
                    ]).ConfigureAwait(false);

                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task SwapDbACLsAsync()
        {
            await CheckCommandsAsync(
                "SWAPDB",
                [DoSwapDbAsync]
            ).ConfigureAwait(false);

            static async Task DoSwapDbAsync(GarnetClient client)
            {
                try
                {
                    // Currently SWAPDB does not support calling the command when multiple clients are connected to the server.
                    await client.ExecuteForStringResultAsync("SWAPDB", ["0", "1"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoTypeAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("TYPE", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("none", val);
            }
        }

        [Test]
        public async Task UnlinkACLsAsync()
        {
            await CheckCommandsAsync(
                "UNLINK",
                [DoUnlinkAsync, DoUnlinkMultiAsync]
            ).ConfigureAwait(false);

            static async Task DoUnlinkAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("UNLINK", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }

            static async Task DoUnlinkMultiAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("UNLINK", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task UnsubscribeACLsAsync()
        {
            await CheckCommandsAsync(
                "UNSUBSCRIBE",
                [DoUnsubscribePatternAsync]
            ).ConfigureAwait(false);

            static async Task DoUnsubscribePatternAsync(GarnetClient client)
            {
                string[] res = await client.ExecuteForStringArrayResultAsync("UNSUBSCRIBE", ["foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoWatchAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("WATCH", ["foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoWatchMSAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("WATCHMS", ["foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoWatchOSAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("WATCHOS", ["foo"]).ConfigureAwait(false);
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
            ).ConfigureAwait(false);

            static async Task DoUnwatchAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("UNWATCH").ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task VAddACLsAsync()
        {
            await CheckCommandsAsync(
                "VADD",
                [DoVAddAsync]
            ).ConfigureAwait(false);

            static async Task DoVAddAsync(GarnetClient client)
            {
                var elem = Encoding.ASCII.GetString("\x0\x1\x2\x3"u8);

                long val = await client.ExecuteForLongResultAsync("VADD", ["foo", "REDUCE", "50", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", elem, "CAS", "NOQUANT", "EF", "16", "SETATTR", "{ 'hello': 'world' }", "M", "32"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(1, val);
            }
        }

        [Test]
        public async Task VCardACLsAsync()
        {
            await CheckCommandsAsync(
                "VCARD",
                [DoVCardAsync]
            ).ConfigureAwait(false);

            static async Task DoVCardAsync(GarnetClient client)
            {
                // TODO: this is a placeholder implementation

                string val = await client.ExecuteForStringResultAsync("VCARD", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task VDimACLsAsync()
        {
            await CheckCommandsAsync(
                "VDIM",
                [DoVDimAsync]
            ).ConfigureAwait(false);

            static async Task DoVDimAsync(GarnetClient client)
            {
                try
                {
                    _ = await client.ExecuteForStringResultAsync("VDIM", ["foo"]).ConfigureAwait(false);
                    ClassicAssert.Fail("Shouldn't be reachable");
                }
                catch (Exception e) when (e.Message.Equals("ERR Key not found"))
                {
                    // Excepted
                }
            }
        }

        [Test]
        public async Task VEmbACLsAsync()
        {
            await CheckCommandsAsync(
                "VEMB",
                [DoVEmbAsync]
            ).ConfigureAwait(false);

            static async Task DoVEmbAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("VEMB", ["foo", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
            }
        }

        [Test]
        public async Task VGetAttrACLsAsync()
        {
            await CheckCommandsAsync(
                "VGETATTR",
                [DoVGetAttrAsync]
            ).ConfigureAwait(false);

            static async Task DoVGetAttrAsync(GarnetClient client)
            {
                string val = await client.ExecuteForStringResultAsync("VGETATTR", ["foo", "wololo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(null, val);
            }
        }

        [Test]
        public async Task VInfoACLsAsync()
        {
            await CheckCommandsAsync(
                "VINFO",
                [DoVInfoAsync]
            ).ConfigureAwait(false);

            static async Task DoVInfoAsync(GarnetClient client)
            {
                var res = await client.ExecuteForStringArrayResultAsync("VINFO", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(res, null);
            }
        }

        [Test]
        public async Task VIsMemberACLsAsync()
        {
            await CheckCommandsAsync(
                "VISMEMBER",
                [DoVIsMemberAsync]
            ).ConfigureAwait(false);

            static async Task DoVIsMemberAsync(GarnetClient client)
            {
                // TODO: this is a placeholder implementation

                string val = await client.ExecuteForStringResultAsync("VISMEMBER", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task VLinksACLsAsync()
        {
            await CheckCommandsAsync(
                "VLINKS",
                [DoVLinksAsync]
            ).ConfigureAwait(false);

            static async Task DoVLinksAsync(GarnetClient client)
            {
                // TODO: this is a placeholder implementation

                string val = await client.ExecuteForStringResultAsync("VLINKS", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task VRandMemberACLsAsync()
        {
            await CheckCommandsAsync(
                "VRANDMEMBER",
                [DoVRandMemberAsync]
            ).ConfigureAwait(false);

            static async Task DoVRandMemberAsync(GarnetClient client)
            {
                // TODO: this is a placeholder implementation

                string val = await client.ExecuteForStringResultAsync("VRANDMEMBER", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task VRemACLsAsync()
        {
            await CheckCommandsAsync(
                "VREM",
                [DoVRemAsync]
            ).ConfigureAwait(false);

            static async Task DoVRemAsync(GarnetClient client)
            {
                long val = await client.ExecuteForLongResultAsync("VREM", ["foo", Encoding.UTF8.GetString("\0\0\0\0"u8)]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val);
            }
        }

        [Test]
        public async Task VSetAttrACLsAsync()
        {
            await CheckCommandsAsync(
                "VSETATTR",
                [DoVSetAttrAsync]
            ).ConfigureAwait(false);

            static async Task DoVSetAttrAsync(GarnetClient client)
            {
                // TODO: this is a placeholder implementation

                string val = await client.ExecuteForStringResultAsync("VSETATTR", ["foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", val);
            }
        }

        [Test]
        public async Task VSimACLsAsync()
        {
            await CheckCommandsAsync(
                "VSIM",
                [DoVSimAsync]
            ).ConfigureAwait(false);

            static async Task DoVSimAsync(GarnetClient client)
            {
                string[] val = await client.ExecuteForStringArrayResultAsync("VSIM", ["foo", "ELE", "bar"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(0, val.Length);
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
            using (GarnetClient defaultUserClient = await CreateGarnetClientAsync(DefaultUser, DefaultPassword).ConfigureAwait(false))
            {
                // Spin up test users, with all permissions so we can spin up connections without issue
                await InitUserAsync(defaultUserClient, UserWithAll, TestPassword).ConfigureAwait(false);
                await InitUserAsync(defaultUserClient, UserWithNone, TestPassword).ConfigureAwait(false);

                // Spin up two connections for users that we'll use as starting points for different ACL changes
                using (GarnetClient allUserClient = await CreateGarnetClientAsync(UserWithAll, TestPassword).ConfigureAwait(false))
                using (GarnetClient noneUserClient = await CreateGarnetClientAsync(UserWithNone, TestPassword).ConfigureAwait(false))
                {
                    // Check categories
                    foreach (string category in categories)
                    {
                        // Check removing category works
                        {
                            await ResetUserWithAllAsync(defaultUserClient).ConfigureAwait(false);

                            if (!skipPermitted)
                            {
                                await AssertAllPermittedAsync(defaultUserClient, UserWithAll, allUserClient, commands, $"[{command}]: Denied when should have been permitted (user had +@all)", skipPing, commandAndSubCommand, skipAclCheckCmd).ConfigureAwait(false);
                            }

                            await SetUserAsync(defaultUserClient, UserWithAll, [$"-@{category}"]).ConfigureAwait(false);

                            await AssertAllDeniedAsync(defaultUserClient, UserWithAll, allUserClient, commands, $"[{command}]: Permitted when should have been denied (user had -@{category})", skipPing, commandAndSubCommand, skipAclCheckCmd).ConfigureAwait(false);
                        }

                        // Check adding category works
                        {
                            await ResetUserWithNoneAsync(defaultUserClient).ConfigureAwait(false);

                            await AssertAllDeniedAsync(defaultUserClient, UserWithNone, noneUserClient, commands, $"[{command}]: Permitted when should have been denied (user had -@all)", skipPing, commandAndSubCommand, skipAclCheckCmd).ConfigureAwait(false);

                            await SetACLOnUserAsync(defaultUserClient, UserWithNone, [$"+@{category}"]).ConfigureAwait(false);

                            if (!skipPermitted)
                            {
                                await AssertAllPermittedAsync(defaultUserClient, UserWithNone, noneUserClient, commands, $"[{command}]: Denied when should have been permitted (user had +@{category})", skipPing, commandAndSubCommand, skipAclCheckCmd).ConfigureAwait(false);
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
                            await ResetUserWithAllAsync(defaultUserClient).ConfigureAwait(false);

                            await SetACLOnUserAsync(defaultUserClient, UserWithAll, [$"-{commandAcl}"]).ConfigureAwait(false);

                            await AssertAllDeniedAsync(defaultUserClient, UserWithAll, allUserClient, commands, $"[{command}]: Permitted when should have been denied (user had -{commandAcl})", skipPing, [aclCheckCmdCommand], skipAclCheckCmd).ConfigureAwait(false);
                        }

                        // Check adding command works
                        {
                            await ResetUserWithNoneAsync(defaultUserClient).ConfigureAwait(false);

                            await SetACLOnUserAsync(defaultUserClient, UserWithNone, [$"+{commandAcl}"]).ConfigureAwait(false);

                            if (!skipPermitted)
                            {
                                await AssertAllPermittedAsync(defaultUserClient, UserWithNone, noneUserClient, commands, $"[{command}]: Denied when should have been permitted (user had +{commandAcl})", skipPing, [aclCheckCmdCommand], skipAclCheckCmd).ConfigureAwait(false);
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
                            await ResetUserWithAllAsync(defaultUserClient).ConfigureAwait(false);

                            await SetACLOnUserAsync(defaultUserClient, UserWithAll, [$"-{subCommandAcl}"]).ConfigureAwait(false);

                            await AssertAllDeniedAsync(defaultUserClient, UserWithAll, allUserClient, commands, $"[{command}]: Permitted when should have been denied (user had -{subCommandAcl})", skipPing, commandAndSubCommand, skipAclCheckCmd).ConfigureAwait(false);
                        }

                        // Check adding subcommand works
                        {
                            await ResetUserWithNoneAsync(defaultUserClient).ConfigureAwait(false);

                            await SetACLOnUserAsync(defaultUserClient, UserWithNone, [$"+{subCommandAcl}"]).ConfigureAwait(false);

                            if (!skipPermitted)
                            {
                                await AssertAllPermittedAsync(defaultUserClient, UserWithNone, noneUserClient, commands, $"[{command}]: Denied when should have been permitted (user had +{subCommandAcl})", skipPing, commandAndSubCommand, skipAclCheckCmd).ConfigureAwait(false);
                            }
                        }

                        // Checking adding command but removing subcommand works
                        {
                            await ResetUserWithNoneAsync(defaultUserClient).ConfigureAwait(false);

                            await SetACLOnUserAsync(defaultUserClient, UserWithNone, [$"+{commandAcl}", $"-{subCommandAcl}"]).ConfigureAwait(false);

                            await AssertAllDeniedAsync(defaultUserClient, UserWithNone, noneUserClient, commands, $"[{command}]: Permitted when should have been denied (user had +{commandAcl} -{subCommandAcl})", skipPing, commandAndSubCommand, skipAclCheckCmd).ConfigureAwait(false);
                        }

                        // Checking removing command but adding subcommand works
                        {
                            await ResetUserWithAllAsync(defaultUserClient).ConfigureAwait(false);

                            await SetACLOnUserAsync(defaultUserClient, UserWithAll, [$"-{commandAcl}", $"+{subCommandAcl}"]).ConfigureAwait(false);

                            if (!skipPermitted)
                            {
                                await AssertAllPermittedAsync(defaultUserClient, UserWithAll, allUserClient, commands, $"[{command}]: Denied when should have been permitted (user had -{commandAcl} +{subCommandAcl})", skipPing, commandAndSubCommand, skipAclCheckCmd).ConfigureAwait(false);
                            }
                        }
                    }
                }
            }

            // Use default user to update ACL on given user
            static async Task SetACLOnUserAsync(GarnetClient defaultUserClient, string user, string[] aclPatterns)
            {
                string aclRes = await defaultUserClient.ExecuteForStringResultAsync("ACL", ["SETUSER", user, .. aclPatterns]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", aclRes);
            }

            // Create or reset user, with all permissions
            static async Task ResetUserWithAllAsync(GarnetClient defaultUserClient)
            {
                string aclRes = await defaultUserClient.ExecuteForStringResultAsync("ACL", ["SETUSER", UserWithAll, "on", $">{TestPassword}", "+@all"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", aclRes);
            }

            // Get user that was initialized with -@all
            static async Task ResetUserWithNoneAsync(GarnetClient defaultUserClient)
            {
                string aclRes = await defaultUserClient.ExecuteForStringResultAsync("ACL", ["SETUSER", UserWithNone, "on", $">{TestPassword}", "-@all"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", aclRes);
            }

            // Check that all commands succeed
            static async Task AssertAllPermittedAsync(GarnetClient defaultUserClient, string currentUserName, GarnetClient currentUserClient, Func<GarnetClient, Task>[] commands, string message, bool skipPing, string[] commandAndSubCommand, bool skipAclCheckCmd)
            {
                foreach (Func<GarnetClient, Task> cmd in commands)
                {
                    ClassicAssert.True(await CheckAuthFailureAsync(() => cmd(currentUserClient)).ConfigureAwait(false), message);
                }

                if (!skipAclCheckCmd)
                {
                    await AssertRedisAclCheckCmd(true, defaultUserClient, currentUserName, currentUserClient, commandAndSubCommand).ConfigureAwait(false);
                }

                if (!skipPing)
                {
                    // Check we haven't desynced
                    await PingAsync(defaultUserClient, currentUserName, currentUserClient).ConfigureAwait(false);
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

                var aclRes = await defaultUserClient.ExecuteForStringResultAsync("ACL", ["SETUSER", currentUserName, "+eval"]).ConfigureAwait(false);

                var script = $"return redis.acl_check_cmd({string.Join(", ", commandAndSubCommand.Select(static x => $"'{x}'"))});";

                var canRunStr = await currentUserClient.ExecuteForStringResultAsync("EVAL", [script, "0"]).ConfigureAwait(false);
                var canRun = canRunStr == "1";

                ClassicAssert.AreEqual(expectedResult, canRun, $"redis.acl_check_cmd(...) return unexpected result for '{withBar}'");
            }

            // Check that all commands fail with NOAUTH
            static async Task AssertAllDeniedAsync(GarnetClient defaultUserClient, string currentUserName, GarnetClient currentUserClient, Func<GarnetClient, Task>[] commands, string message, bool skipPing, string[] commandAndSubCommand, bool skipAclCheckCmd)
            {
                foreach (Func<GarnetClient, Task> cmd in commands)
                {
                    ClassicAssert.False(await CheckAuthFailureAsync(() => cmd(currentUserClient)).ConfigureAwait(false), message);
                }

                if (!skipAclCheckCmd)
                {
                    await AssertRedisAclCheckCmd(false, defaultUserClient, currentUserName, currentUserClient, commandAndSubCommand).ConfigureAwait(false);
                }

                if (!skipPing)
                {
                    // Check we haven't desynced
                    await PingAsync(defaultUserClient, currentUserName, currentUserClient).ConfigureAwait(false);
                }
            }

            // Enable PING on user and issue PING on connection
            static async Task PingAsync(GarnetClient defaultUserClient, string currentUserName, GarnetClient currentUserClient)
            {
                // Have to add PING because it'll be denied by reset of test in many cases
                // since we do this towards the end of our asserts, it shouldn't invalidate
                // the rest of the test.
                string addPingRes = await defaultUserClient.ExecuteForStringResultAsync("ACL", ["SETUSER", currentUserName, "on", "+ping"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", addPingRes);

                // Actually execute the PING
                string pingRes = await currentUserClient.ExecuteForStringResultAsync("PING").ConfigureAwait(false);
                ClassicAssert.AreEqual("PONG", pingRes);
            }
        }

        /// <summary>
        /// Create a GarnetClient authed as the given user.
        /// </summary>
        private static async Task<GarnetClient> CreateGarnetClientAsync(string username, string password)
        {
            GarnetClient ret = TestUtils.GetGarnetClient();
            await ret.ConnectAsync().ConfigureAwait(false);

            string authRes = await ret.ExecuteForStringResultAsync("AUTH", [username, password]).ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", authRes);

            return ret;
        }

        /// <summary>
        /// Create a user with +@all permissions.
        /// </summary>
        private static async Task InitUserAsync(GarnetClient defaultUserClient, string username, string password)
        {
            string res = await defaultUserClient.ExecuteForStringResultAsync("ACL", ["SETUSER", username, "on", $">{password}", "+@all"]).ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", res);
        }

        /// <summary>
        /// Runs ACL SETUSER default [aclPatterns] and checks that they are reflected in ACL LIST.
        /// </summary>
        private static async Task SetUserAsync(GarnetClient client, string user, params string[] aclPatterns)
        {
            string aclLinePreSet = await GetUserAsync(client, user).ConfigureAwait(false);

            string setRes = await client.ExecuteForStringResultAsync("ACL", ["SETUSER", user, .. aclPatterns]).ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", setRes, $"Updating user ({user}) failed");

            string aclLinePostSet = await GetUserAsync(client, user).ConfigureAwait(false);

            string expectedAclLine = $"{aclLinePreSet} {string.Join(" ", aclPatterns)}";

            CommandPermissionSet actualUserPerms = ACLParser.ParseACLRule(aclLinePostSet).CopyCommandPermissionSet();
            CommandPermissionSet expectedUserPerms = ACLParser.ParseACLRule(expectedAclLine).CopyCommandPermissionSet();

            ClassicAssert.IsTrue(expectedUserPerms.IsEquivalentTo(actualUserPerms), $"User permissions were not equivalent after running SETUSER with {string.Join(" ", aclPatterns)}");

            // TODO: if and when ACL GETUSER is implemented, just use that
            static async Task<string> GetUserAsync(GarnetClient client, string user)
            {
                string ret = null;
                string[] resArr = await client.ExecuteForStringArrayResultAsync("ACL", ["LIST"]).ConfigureAwait(false);
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
                await act().ConfigureAwait(false);
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