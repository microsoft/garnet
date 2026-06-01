// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Garnet.server.ACL;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.Resp.ACL
{
    /// <summary>
    /// Tests for per-name ACLs on custom (extension) commands.
    /// Exercises the parser fallback, deny-precedence semantics, SETUSER strict validation,
    /// case-insensitivity, name validation, the per-user cap, and ACL save/load round-trip.
    /// </summary>
    [TestFixture]
    public class CustomCommandACLTests : TestBase
    {
        private const string DefaultPassword = nameof(CustomCommandACLTests);
        private const string DefaultUser = "default";

        private GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, defaultPassword: DefaultPassword,
                useAcl: true, enableLua: false,
                enableModuleCommand: Garnet.server.Auth.Settings.ConnectionProtectionOption.Yes);

            ClassicAssert.IsTrue(TestUtils.TryGetCustomCommandsInfo(out var respCustomCommandsInfo));
            ClassicAssert.IsNotNull(respCustomCommandsInfo);

            // Two raw-string commands so we can test deny-precedence (allow one, deny another in the same category).
            server.Register.NewCommand("SETWPIFPGT", CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand(), respCustomCommandsInfo["SETWPIFPGT"]);
            server.Register.NewCommand("MYDICTGET", CommandType.Read, new MyDictFactory(), new MyDictGet(), respCustomCommandsInfo["MYDICTGET"]);

            // Minimal no-op transaction proc + procedure registered so we can cover the
            // CustomTxn and CustomProcedure dispatch branches of CheckACLPermissions
            // (the bitmap-only fallthrough path is exercised by SETWPIFPGT/MYDICTGET).
            server.Register.NewTransactionProc("NOOPTXN", () => new AclNoOpTxn(), new RespCommandsInfo { Arity = -1 });
            server.Register.NewProcedure("NOOPPROC", () => new AclNoOpProc(), new RespCommandsInfo { Arity = -1 });

            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
        }

        // ----- Connection helpers ---------------------------------------------------------------

        private static async Task<GarnetClient> ConnectAsync(string user, string password)
        {
            var c = TestUtils.GetGarnetClient();
            await c.ConnectAsync().ConfigureAwait(false);
            var auth = await c.ExecuteForStringResultAsync("AUTH", [user, password]).ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", auth);
            return c;
        }

        private static async Task<string> SetUserAsync(GarnetClient admin, string user, params string[] ops)
        {
            return await admin.ExecuteForStringResultAsync("ACL", new[] { "SETUSER", user, "on", ">pw" }.Concat(ops).ToArray()).ConfigureAwait(false);
        }

        private static async Task<Exception> CaptureExceptionAsync(Func<Task> action)
        {
            try
            {
                await action().ConfigureAwait(false);
                return null;
            }
            catch (Exception ex)
            {
                return ex;
            }
        }

        // ----- Parser-level (unit) tests -------------------------------------------------------

        [Test]
        public void Parser_AcceptsUnknownNameAsCustomCommand()
        {
            // Unknown names that pass the format check should land on the custom-name path
            // rather than throwing AclCommandDoesNotExistException.
            var user = new User("alice");
            ACLParser.ApplyACLOpToUser(ref user, "+json.get");

            CollectionAssert.Contains(user.CustomCommandsAllowed, "JSON.GET");
            CollectionAssert.DoesNotContain(user.CustomCommandsDenied, "JSON.GET");
        }

        [Test]
        public void Parser_RejectsNameWithWhitespace()
        {
            // Tokens are split by whitespace at the file/wire level, but a name token that
            // somehow carries embedded whitespace (or other forbidden chars) must still be rejected.
            var user = new User("alice");
            Assert.Throws<AclCommandDoesNotExistException>(() => ACLParser.ApplyACLOpToUser(ref user, "+bad\tname"));
        }

        [Test]
        public void Parser_RejectsNonAsciiName()
        {
            var user = new User("alice");
            Assert.Throws<AclCommandDoesNotExistException>(() => ACLParser.ApplyACLOpToUser(ref user, "+jsön.get"));
        }

        [Test]
        public void Parser_RejectsNameOverLengthLimit()
        {
            var user = new User("alice");
            string longName = new string('a', ACLParser.MaxCustomCommandNameLength + 1);
            Assert.Throws<AclCommandDoesNotExistException>(() => ACLParser.ApplyACLOpToUser(ref user, "+" + longName));
        }

        [Test]
        public void Parser_RejectsNameWithLeadingNonAlphanumeric()
        {
            // Real RESP command names always begin with [A-Za-z0-9]. Names like "-foo",
            // ".foo", "_foo", "|foo" must be rejected so the unknown-name fallback can't
            // absorb tokens that look like punctuation or stray separators.
            var user = new User("alice");
            Assert.Throws<AclCommandDoesNotExistException>(() => ACLParser.ApplyACLOpToUser(ref user, "+-foo"));
            Assert.Throws<AclCommandDoesNotExistException>(() => ACLParser.ApplyACLOpToUser(ref user, "+.foo"));
            Assert.Throws<AclCommandDoesNotExistException>(() => ACLParser.ApplyACLOpToUser(ref user, "+_foo"));
            Assert.Throws<AclCommandDoesNotExistException>(() => ACLParser.ApplyACLOpToUser(ref user, "+|foo"));
        }

        [Test]
        public void Parser_AcceptsNameAtLengthLimit()
        {
            var user = new User("alice");
            string maxName = new string('a', ACLParser.MaxCustomCommandNameLength);
            ACLParser.ApplyACLOpToUser(ref user, "+" + maxName);
            CollectionAssert.Contains(user.CustomCommandsAllowed, maxName.ToUpperInvariant());
        }

        [Test]
        public void Parser_DenyPrecedenceOnSameName()
        {
            // +name then -name → name lives in deny set only (deny wins).
            var user = new User("alice");
            ACLParser.ApplyACLOpToUser(ref user, "+@custom");
            ACLParser.ApplyACLOpToUser(ref user, "+json.set");
            ACLParser.ApplyACLOpToUser(ref user, "-json.set");

            CollectionAssert.DoesNotContain(user.CustomCommandsAllowed, "JSON.SET");
            CollectionAssert.Contains(user.CustomCommandsDenied, "JSON.SET");
        }

        [Test]
        public void Parser_LastWriteWins_AllowAfterDeny()
        {
            // -name then +name → name lives in allow set only.
            var user = new User("alice");
            ACLParser.ApplyACLOpToUser(ref user, "-json.set");
            ACLParser.ApplyACLOpToUser(ref user, "+json.set");

            CollectionAssert.Contains(user.CustomCommandsAllowed, "JSON.SET");
            CollectionAssert.DoesNotContain(user.CustomCommandsDenied, "JSON.SET");
        }

        [Test]
        public void Parser_CaseInsensitiveNormalization()
        {
            var user1 = new User("alice");
            ACLParser.ApplyACLOpToUser(ref user1, "+JSON.GET");

            var user2 = new User("bob");
            ACLParser.ApplyACLOpToUser(ref user2, "+json.get");

            var user3 = new User("carol");
            ACLParser.ApplyACLOpToUser(ref user3, "+Json.Get");

            // All three normalize to the same uppercase entry so dispatch-time lookup matches NameStr.
            CollectionAssert.Contains(user1.CustomCommandsAllowed, "JSON.GET");
            CollectionAssert.Contains(user2.CustomCommandsAllowed, "JSON.GET");
            CollectionAssert.Contains(user3.CustomCommandsAllowed, "JSON.GET");
        }

        [Test]
        public void Parser_DescribeUserRoundTripPreservesCustomSets()
        {
            // Regression for ACL file persistence: writing a user out via DescribeUser and parsing
            // the resulting line back must produce an equivalent permission set. If rationalization
            // drops -name / +name tokens (or the parser fails to re-absorb them), saved ACL files
            // would silently change semantics on reload.
            var u = new User("alice");
            u.AddCategory(RespAclCategories.Custom);
            u.AddCustomCommand("MYDICTGET");
            u.RemoveCustomCommand("SETWPIFPGT");

            string desc = u.DescribeUser();
            var u2 = ACLParser.ParseACLRule(desc);

            ClassicAssert.IsTrue(u.CopyCommandPermissionSet().IsEquivalentTo(u2.CopyCommandPermissionSet()),
                $"Round-tripped user should be equivalent. Description: {desc}");
            CollectionAssert.Contains(u2.CustomCommandsAllowed, "MYDICTGET");
            CollectionAssert.Contains(u2.CustomCommandsDenied, "SETWPIFPGT");
        }

        [Test]
        public void User_AddCustomCommand_NullThrows()
        {
            // Public API must not NRE on null input.
            var user = new User("alice");
            Assert.Throws<ArgumentNullException>(() => user.AddCustomCommand(null));
            Assert.Throws<ArgumentNullException>(() => user.RemoveCustomCommand(null));
        }

        [Test]
        public void User_AddCustomCommand_RejectsMalformedNames()
        {
            // Public API must reject names ACLParser would reject; otherwise the persisted
            // description can be poisoned (e.g. "foo +@all" re-parsing as two tokens on reload).
            var user = new User("alice");

            Assert.Throws<ACLException>(() => user.AddCustomCommand("foo bar"));
            Assert.Throws<ACLException>(() => user.RemoveCustomCommand("foo bar"));
            Assert.Throws<ACLException>(() => user.AddCustomCommand("foo +@all"));
            Assert.Throws<ACLException>(() => user.RemoveCustomCommand("foo +@all"));
            Assert.Throws<ACLException>(() => user.AddCustomCommand(""));
            Assert.Throws<ACLException>(() => user.AddCustomCommand(".leading_dot"));

            Assert.DoesNotThrow(() => user.AddCustomCommand("json.set"));
        }

        [Test]
        public void User_AddCustomCommand_LengthBoundary()
        {
            // Pin the MaxCustomCommandNameLength contract so a future bump doesn't silently
            // change what the parser will accept on reload.
            var user = new User("alice");
            string maxLen = new string('a', Garnet.server.ACL.ACLParser.MaxCustomCommandNameLength);
            string tooLong = maxLen + "a";

            Assert.DoesNotThrow(() => user.AddCustomCommand(maxLen));
            Assert.Throws<ACLException>(() => user.AddCustomCommand(tooLong));
        }

        [Test]
        public void User_DescribeUser_RoundTripsCustomAllowAndDeny()
        {
            // Per-name allow/deny must survive ACL save -> reload. DescribeUser produces the on-disk
            // form; feeding it back through ParseACLRule must reconstruct the same custom sets.
            var original = new User("alice");
            original.IsEnabled = true;
            original.AddCustomCommand("json.set");
            original.AddCustomCommand("foo.bar");
            original.RemoveCustomCommand("json.get");

            string described = original.DescribeUser();

            var reparsed = Garnet.server.ACL.ACLParser.ParseACLRule(described);

            var allow = reparsed.CopyCommandPermissionSet().CustomAllowed;
            var deny = reparsed.CopyCommandPermissionSet().CustomDenied;

            ClassicAssert.IsTrue(allow.Contains("JSON.SET"));
            ClassicAssert.IsTrue(allow.Contains("FOO.BAR"));
            ClassicAssert.IsFalse(allow.Contains("JSON.GET"));
            ClassicAssert.IsTrue(deny.Contains("JSON.GET"));
        }

        [Test]
        public void PermissionSet_CustomSetsAreCaseInsensitive()
        {
            // Regression for the comparer hardening: callers must not have to uppercase
            // before checking Contains. Add via the User-level API (which uppercases),
            // then verify Contains hits regardless of caller casing.
            var user = new User("alice");
            user.AddCustomCommand("Json.Get");

            var perms = user.CopyCommandPermissionSet();
            ClassicAssert.IsTrue(perms.CustomAllowed.Contains("JSON.GET"));
            ClassicAssert.IsTrue(perms.CustomAllowed.Contains("json.get"));
            ClassicAssert.IsTrue(perms.CustomAllowed.Contains("Json.Get"));

            // Deny set should behave the same way.
            user.RemoveCustomCommand("Json.Get");
            perms = user.CopyCommandPermissionSet();
            ClassicAssert.IsFalse(perms.CustomAllowed.Contains("JSON.GET"));
            ClassicAssert.IsTrue(perms.CustomDenied.Contains("JSON.GET"));
            ClassicAssert.IsTrue(perms.CustomDenied.Contains("json.get"));
        }

        // ----- CommandPermissionSet (unit) -----------------------------------------------------

        [Test]
        public void PermissionSet_DenyBeatsBitmap()
        {
            // User with +@custom (sets generic CustomRawStringCmd bit) and -setwpifpgt (per-name deny).
            // CanAccessCustomCommand(CustomRawStringCmd, "SETWPIFPGT") must return false because deny wins.
            var user = new User("alice");
            user.AddCategory(RespAclCategories.Custom);
            user.RemoveCustomCommand("SETWPIFPGT");

            ClassicAssert.IsFalse(user.CanAccessCustomCommand(RespCommand.CustomRawStringCmd, "SETWPIFPGT"));
            ClassicAssert.IsTrue(user.CanAccessCustomCommand(RespCommand.CustomRawStringCmd, "MYDICTGET"));
        }

        [Test]
        public void PermissionSet_AllowOverridesMissingBitmap()
        {
            // User with -@all but +setwpifpgt: bitmap bit not set for CustomRawStringCmd, but per-name allow grants access.
            var user = new User("alice");
            user.RemoveCategory(RespAclCategories.All);
            user.AddCustomCommand("SETWPIFPGT");

            ClassicAssert.IsTrue(user.CanAccessCustomCommand(RespCommand.CustomRawStringCmd, "SETWPIFPGT"));
            ClassicAssert.IsFalse(user.CanAccessCustomCommand(RespCommand.CustomRawStringCmd, "MYDICTGET"));
        }

        [Test]
        public void PermissionSet_AllSentinelAllowsEverything()
        {
            var user = new User("alice");
            user.AddCategory(RespAclCategories.All);

            ClassicAssert.IsTrue(user.CanAccessCustomCommand(RespCommand.CustomRawStringCmd, "ANYTHING.GOES"));
            ClassicAssert.IsTrue(user.CanAccessCustomCommand(RespCommand.CustomProcedure, "WHATEVER"));
        }

        [Test]
        public void PermissionSet_AllSentinelMinusName_DeniesThatName()
        {
            // +@all -setwpifpgt: must transition off the sentinel via Copy() and honor the deny.
            var user = new User("alice");
            user.AddCategory(RespAclCategories.All);
            user.RemoveCustomCommand("SETWPIFPGT");

            ClassicAssert.IsFalse(user.CanAccessCustomCommand(RespCommand.CustomRawStringCmd, "SETWPIFPGT"));
            // Everything else still allowed because the (non-sentinel) bitmap has all bits set.
            ClassicAssert.IsTrue(user.CanAccessCustomCommand(RespCommand.CustomRawStringCmd, "OTHER"));
        }

        [Test]
        public void PermissionSet_DefaultFailsClosed()
        {
            // Fresh user with no permissions must deny custom commands.
            var user = new User("alice");
            ClassicAssert.IsFalse(user.CanAccessCustomCommand(RespCommand.CustomRawStringCmd, "ANYTHING"));
        }

        [Test]
        public void PermissionSet_IsEquivalentToConsidersCustomSets()
        {
            // Two users with the same bitmap but different per-name sets must NOT be equivalent.
            // (Regression test for description rationalization not dropping deny tokens.)
            var u1 = new User("a");
            u1.AddCategory(RespAclCategories.Custom);

            var u2 = new User("b");
            u2.AddCategory(RespAclCategories.Custom);
            u2.RemoveCustomCommand("SETWPIFPGT");

            ClassicAssert.IsFalse(u1.CopyCommandPermissionSet().IsEquivalentTo(u2.CopyCommandPermissionSet()));
        }

        // ----- ACL SETUSER strict validation (integration) -------------------------------------

        [Test]
        public async Task SetUser_RejectsUnknownCustomName()
        {
            using var admin = await ConnectAsync(DefaultUser, DefaultPassword);

            var ex = await CaptureExceptionAsync(async () =>
            {
                await SetUserAsync(admin, "alice", "+definitely_not_registered");
            });

            ClassicAssert.IsNotNull(ex, "Expected SETUSER to fail for unknown custom command name");
            StringAssert.Contains("Unknown custom command", ex.Message);
        }

        [Test]
        public async Task SetUser_AcceptsRegisteredCustomName()
        {
            using var admin = await ConnectAsync(DefaultUser, DefaultPassword);
            var res = await SetUserAsync(admin, "alice", "+setwpifpgt");
            ClassicAssert.AreEqual("OK", res);
        }

        [Test]
        public async Task SetUser_RejectsInvalidNameSyntax()
        {
            using var admin = await ConnectAsync(DefaultUser, DefaultPassword);

            // Non-ASCII character in name — must be rejected before the strict registration check.
            var ex = await CaptureExceptionAsync(async () =>
            {
                await SetUserAsync(admin, "alice", "+jsön.get");
            });
            ClassicAssert.IsNotNull(ex);
        }

        [Test]
        public async Task SetUser_AcceptsCommandRegisteredWithoutCommandInfo()
        {
            // Regression: IsCustomCommandRegistered must succeed for commands registered without
            // RespCommandsInfo. Before the polish pass it consulted the info-only dict and would
            // false-reject. We register an extra command here with null commandInfo and verify
            // SETUSER's strict validation accepts it.
            const string noInfoName = "NOINFOCMD";
            server.Register.NewCommand(noInfoName, CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand(),
                commandInfo: null, commandDocs: null);

            using var admin = await ConnectAsync(DefaultUser, DefaultPassword);
            var res = await SetUserAsync(admin, "bob", "+" + noInfoName.ToLowerInvariant());
            ClassicAssert.AreEqual("OK", res);
        }

        // Reflects out the live AccessControlList from a running server. `GarnetServer.Provider`
        // is internal to Garnet.host (no InternalsVisibleTo for Garnet.test.acl), so reflection
        // is the least-invasive escape hatch — used only to seed loose-loaded state for these
        // two regression tests.
        private static AccessControlList GetAcl(GarnetServer s)
        {
            var providerProp = typeof(GarnetServer).GetField("Provider",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public);
            var provider = providerProp.GetValue(s);
            var storeWrapperProp = provider.GetType().GetProperty("StoreWrapper",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public);
            var sw = storeWrapperProp.GetValue(provider);
            var aclField = sw.GetType().GetField("accessControlList");
            return (AccessControlList)aclField.GetValue(sw);
        }

        [Test]
        public async Task SetUser_AllowsSwapOfLooseLoadedName()
        {
            // Regression: an ACL file may carry +name for a module that hasn't registered yet
            // (loose-by-default at file load). A later SETUSER toggling that name allow->deny
            // must succeed — the name is already on the user, so SETUSER is not introducing
            // a brand-new unresolved reference. Previously this threw "Unknown custom command".
            using var admin = await ConnectAsync(DefaultUser, DefaultPassword);

            var ok = await SetUserAsync(admin, "alice", "on");
            ClassicAssert.AreEqual("OK", ok);

            const string LooseName = "LOOSELOADED_PROBE";
            var acl = GetAcl(server);
            ClassicAssert.IsNotNull(acl);
            var aliceHandle = acl.GetUserHandle("alice");
            ClassicAssert.IsNotNull(aliceHandle);

            // Inject a loose-loaded name via the parser path (bypassing SETUSER's strict check,
            // which is exactly what AccessControlList.Load does at startup for ACL-file entries).
            var injected = new User(aliceHandle.User);
            ACLParser.ApplyACLOpToUser(ref injected, "+" + LooseName.ToLowerInvariant());
            CollectionAssert.Contains(injected.CustomCommandsAllowed, LooseName);
            ClassicAssert.IsTrue(aliceHandle.TrySetUser(injected, aliceHandle.User));

            // Toggle allow->deny over the wire. Pre-fix: throws. Post-fix: succeeds.
            var toggle = await SetUserAsync(admin, "alice", "-" + LooseName.ToLowerInvariant());
            ClassicAssert.AreEqual("OK", toggle);

            var aliceAfter = acl.GetUserHandle("alice").User;
            CollectionAssert.Contains(aliceAfter.CustomCommandsDenied, LooseName);
            CollectionAssert.DoesNotContain(aliceAfter.CustomCommandsAllowed, LooseName);
        }

        [Test]
        public async Task SetUser_AllowsSwapOfLooseLoadedName_DenyToAllow()
        {
            // Mirror of the above for the other direction (deny -> allow).
            using var admin = await ConnectAsync(DefaultUser, DefaultPassword);
            var ok = await SetUserAsync(admin, "alice", "on");
            ClassicAssert.AreEqual("OK", ok);

            const string LooseName = "LOOSELOADED_PROBE2";
            var acl = GetAcl(server);
            var aliceHandle = acl.GetUserHandle("alice");

            var injected = new User(aliceHandle.User);
            ACLParser.ApplyACLOpToUser(ref injected, "-" + LooseName.ToLowerInvariant());
            CollectionAssert.Contains(injected.CustomCommandsDenied, LooseName);
            ClassicAssert.IsTrue(aliceHandle.TrySetUser(injected, aliceHandle.User));

            var toggle = await SetUserAsync(admin, "alice", "+" + LooseName.ToLowerInvariant());
            ClassicAssert.AreEqual("OK", toggle);

            var aliceAfter = acl.GetUserHandle("alice").User;
            CollectionAssert.Contains(aliceAfter.CustomCommandsAllowed, LooseName);
            CollectionAssert.DoesNotContain(aliceAfter.CustomCommandsDenied, LooseName);
        }

        // ----- Dispatch-level deny / allow (integration) ---------------------------------------

        [Test]
        public async Task Dispatch_DenyPrecedence_PlusCategoryMinusName()
        {
            using var admin = await ConnectAsync(DefaultUser, DefaultPassword);

            await SetUserAsync(admin, "alice", "+@custom", "-setwpifpgt");

            using var alice = await ConnectAsync("alice", "pw");

            // SETWPIFPGT must be denied (explicit -name beats +@custom).
            var denied = await CaptureExceptionAsync(async () =>
            {
                await alice.ExecuteForStringResultAsync("SETWPIFPGT", ["k", "v", "\0\0\0\0\0\0\0\0"]).ConfigureAwait(false);
            });
            ClassicAssert.IsNotNull(denied, "SETWPIFPGT should have been denied by -setwpifpgt");
            StringAssert.Contains("NOAUTH", denied.Message.ToUpperInvariant());

            // MYDICTGET is still permitted (only SETWPIFPGT was denied).
            var mydict = await alice.ExecuteForStringResultAsync("MYDICTGET", ["foo", "bar"]).ConfigureAwait(false);
            // Returns nil/null on miss; just confirms the call wasn't rejected by ACL.
            ClassicAssert.IsNull(mydict);
        }

        [Test]
        public async Task Dispatch_PlusNameFromNone_AllowsOnlyThatCommand()
        {
            using var admin = await ConnectAsync(DefaultUser, DefaultPassword);

            // Strip everything, then add only SETWPIFPGT.
            await SetUserAsync(admin, "alice", "-@all", "+ping", "+auth", "+setwpifpgt");

            using var alice = await ConnectAsync("alice", "pw");

            // Permitted command works.
            var ok = await alice.ExecuteForStringResultAsync("SETWPIFPGT", ["k", "v", "\0\0\0\0\0\0\0\0"]).ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", ok);

            // Sibling custom command is denied.
            var denied = await CaptureExceptionAsync(async () =>
            {
                await alice.ExecuteForStringResultAsync("MYDICTGET", ["foo", "bar"]).ConfigureAwait(false);
            });
            ClassicAssert.IsNotNull(denied, "MYDICTGET should not be reachable without +mydictget or +@custom");
        }

        [Test]
        public async Task Dispatch_CaseInsensitiveOnTheWire()
        {
            using var admin = await ConnectAsync(DefaultUser, DefaultPassword);

            // Mixed-case in the SETUSER token; dispatch-time lookup is against uppercased NameStr.
            await SetUserAsync(admin, "alice", "-@all", "+ping", "+auth", "+SeTwPiFpGt");

            using var alice = await ConnectAsync("alice", "pw");
            var ok = await alice.ExecuteForStringResultAsync("SETWPIFPGT", ["k", "v", "\0\0\0\0\0\0\0\0"]).ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", ok);
        }

        // ----- ACL GETUSER / LIST round-trip ---------------------------------------------------

        [Test]
        public async Task GetUser_IncludesCustomNameTokens()
        {
            using var admin = await ConnectAsync(DefaultUser, DefaultPassword);
            await SetUserAsync(admin, "alice", "+@custom", "-setwpifpgt");

            // ACL LIST returns a description per user; alice's row must contain -setwpifpgt.
            var lines = await admin.ExecuteForStringArrayResultAsync("ACL", ["LIST"]).ConfigureAwait(false);
            ClassicAssert.IsNotNull(lines);
            string aliceLine = lines.FirstOrDefault(l => l != null && l.StartsWith("user alice "));
            ClassicAssert.IsNotNull(aliceLine, "alice should appear in ACL LIST");
            StringAssert.Contains("-setwpifpgt", aliceLine.ToLowerInvariant());
        }

        [Test]
        public async Task Dispatch_CustomTxn_DenyPrecedence()
        {
            // Covers the CustomTxn branch of CheckACLPermissions.
            using var admin = await ConnectAsync(DefaultUser, DefaultPassword);

            // +@custom would grant the txn via the bitmap; -NOOPTXN must still beat it.
            await SetUserAsync(admin, "alice", "+@custom", "-NOOPTXN");
            using var alice = await ConnectAsync("alice", "pw");

            var denied = await CaptureExceptionAsync(async () =>
            {
                await alice.ExecuteForStringResultAsync("NOOPTXN").ConfigureAwait(false);
            });
            ClassicAssert.IsNotNull(denied, "NOOPTXN should have been denied by -NOOPTXN");
            StringAssert.Contains("NOAUTH", denied.Message.ToUpperInvariant());
        }

        [Test]
        public async Task Dispatch_CustomTxn_AllowFromMinusAll()
        {
            // Covers the CustomTxn allow path when the generic bitmap bit is clear.
            using var admin = await ConnectAsync(DefaultUser, DefaultPassword);
            await SetUserAsync(admin, "alice", "-@all", "+ping", "+auth", "+NOOPTXN");
            using var alice = await ConnectAsync("alice", "pw");

            var ok = await alice.ExecuteForStringResultAsync("NOOPTXN").ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", ok);
        }

        [Test]
        public async Task Dispatch_CustomProcedure_DenyPrecedence()
        {
            // Covers the CustomProcedure branch of CheckACLPermissions.
            using var admin = await ConnectAsync(DefaultUser, DefaultPassword);
            await SetUserAsync(admin, "alice", "+@custom", "-NOOPPROC");
            using var alice = await ConnectAsync("alice", "pw");

            var denied = await CaptureExceptionAsync(async () =>
            {
                await alice.ExecuteForStringResultAsync("NOOPPROC").ConfigureAwait(false);
            });
            ClassicAssert.IsNotNull(denied, "NOOPPROC should have been denied by -NOOPPROC");
            StringAssert.Contains("NOAUTH", denied.Message.ToUpperInvariant());
        }

        [Test]
        public async Task Dispatch_CustomProcedure_AllowFromMinusAll()
        {
            // Covers the CustomProcedure allow path when the generic bitmap bit is clear.
            using var admin = await ConnectAsync(DefaultUser, DefaultPassword);
            await SetUserAsync(admin, "alice", "-@all", "+ping", "+auth", "+NOOPPROC");
            using var alice = await ConnectAsync("alice", "pw");

            var ok = await alice.ExecuteForStringResultAsync("NOOPPROC").ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", ok);
        }

        // ----- Strict-mode startup validation (integration) ------------------------------------

        [Test]
        public void StrictMode_StartupFails_WhenAclFileReferencesUnregisteredCustomCommand()
        {
            // ValidateCustomCommandACLs runs in the GarnetServer constructor, so strict mode
            // has to be set on opts before construction (the shared SetUp server isn't useful here).
            server.Dispose();

            var aclFile = Path.Join(TestUtils.MethodTestDir, "strict.acl");
            File.WriteAllText(aclFile, "user alice on >pw +unregistered_probe\n");

            var ex = Assert.Throws<GarnetException>(() =>
            {
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                    defaultPassword: DefaultPassword, useAcl: true, aclFile: aclFile,
                    aclStrictCustomCommands: true, enableLua: false,
                    enableModuleCommand: Garnet.server.Auth.Settings.ConnectionProtectionOption.Yes);
            });
            StringAssert.Contains("ACL strict mode", ex.Message);
            StringAssert.Contains("UNREGISTERED_PROBE", ex.Message.ToUpperInvariant());
            StringAssert.Contains("ALICE", ex.Message.ToUpperInvariant());

            // Constructor threw before assignment; let TearDown's null-safe Dispose no-op.
            server = null;
        }

        [Test]
        public void LenientMode_StartupSucceeds_WhenAclFileReferencesUnregisteredCustomCommand()
        {
            // Default (lenient) mode must boot cleanly despite the unresolved reference.
            server.Dispose();

            var aclFile = Path.Join(TestUtils.MethodTestDir, "lenient.acl");
            File.WriteAllText(aclFile, "user alice on >pw +unregistered_probe\n");

            Assert.DoesNotThrow(() =>
            {
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                    defaultPassword: DefaultPassword, useAcl: true, aclFile: aclFile,
                    aclStrictCustomCommands: false, enableLua: false,
                    enableModuleCommand: Garnet.server.Auth.Settings.ConnectionProtectionOption.Yes);
            });

            Assert.DoesNotThrow(() => server.Start());
        }
    }

    // No-op transaction proc / procedure used only by the ACL dispatch tests above.
    // Kept inline so the test suite stays self-contained and doesn't pull in NoOpModule.
    internal sealed class AclNoOpTxn : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
            => true;

        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            WriteSimpleString(ref output, "OK");
        }
    }

    internal sealed class AclNoOpProc : CustomProcedure
    {
        public override bool Execute<TGarnetApi>(TGarnetApi garnetApi, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            WriteSimpleString(ref output, "OK");
            return true;
        }
    }
}