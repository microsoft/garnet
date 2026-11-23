// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server.ACL;
using Garnet.server.Auth;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.Resp.ACL
{
    /// <summary>
    /// Tests for ACL Configuration file related operations.
    /// </summary>
    [TestFixture]
    class AclConfigurationFileTests : AclTest
    {
        sealed class DelayedAclAuthenticator(AccessControlList acl, Action beforePublication) : GarnetACLAuthenticator(acl, null)
        {
            protected override bool AuthenticateInternal(UserHandle userHandle, ReadOnlySpan<byte> username, ReadOnlySpan<byte> password)
            {
                beforePublication();
                _userHandle = userHandle;
                return true;
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        public void LateAuthenticationCannotRestoreRevokedHandle(bool reload)
        {
            var acl = new AccessControlList();
            ACLParser.ParseACLRule($"user {TestUserA} on nopass +@all", acl);
            var configurationFile = Path.Join(TestUtils.MethodTestDir, "users.acl");
            File.WriteAllText(configurationFile, $"user {TestUserA} on nopass +get");
            var username = Encoding.ASCII.GetBytes(TestUserA);
            var auth = new DelayedAclAuthenticator(acl, () =>
            {
                if (reload)
                {
                    acl.Load("", configurationFile);
                }
                else
                {
                    ClassicAssert.IsTrue(acl.DeleteUserHandle(TestUserA));
                    ACLParser.ParseACLRule($"user {TestUserA} on nopass +get", acl);
                }
            });

            ClassicAssert.IsTrue(auth.Authenticate(""u8, username));
            ClassicAssert.IsFalse(auth.IsAuthenticated);
            var fresh = new GarnetAclWithPasswordAuthenticator(acl, null);
            ClassicAssert.IsTrue(fresh.Authenticate(""u8, username));
            ClassicAssert.IsTrue(fresh.IsAuthenticated);
        }

        [Test]
        public void RevokedHandleCannotBeReadded()
        {
            var acl = new AccessControlList();
            ACLParser.ParseACLRule($"user {TestUserA} on nopass +@all", acl);
            var handle = acl.GetUserHandle(TestUserA);
            ClassicAssert.IsTrue(acl.DeleteUserHandle(TestUserA));
            Assert.Throws<ACLException>(() => acl.AddUserHandle(handle));
            ClassicAssert.IsNull(acl.GetUserHandle(TestUserA));
        }

        [Test]
        public void ClearUsersRevokesExistingAuthentication()
        {
            var acl = new AccessControlList();
            var auth = new GarnetAclWithPasswordAuthenticator(acl, null);
            ClassicAssert.IsTrue(auth.Authenticate(""u8, "default"u8));
            acl.ClearUsers();
            ClassicAssert.IsFalse(auth.IsAuthenticated);
        }

        /// <summary>
        /// Ensures an empty input file will result in just the default user being present.
        /// </summary>
        [Test]
        public async Task EmptyInput()
        {
            // Create an empty input file
            var configurationFile = Path.Join(TestUtils.MethodTestDir, "users.acl");
            File.CreateText(configurationFile).Close();

            // Ensure Garnet starts up with default user only
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: configurationFile);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            string[] users = await c.ExecuteForArrayAsync("ACL", "USERS").ConfigureAwait(false);
            ClassicAssert.IsTrue(1 == users.Length);
            ClassicAssert.Contains("default", users);
        }

        /// <summary>
        /// Ensure that when no default user is defined in the configuration, it is automatically created.
        /// </summary>
        [Test]
        public async Task NoDefaultRule()
        {
            // Create a simple input configuration file
            var configurationFile = Path.Join(TestUtils.MethodTestDir, "users.acl");
            File.WriteAllText(configurationFile, "user testA on >password123 +@admin\r\nuser testB on >passw0rd >password +@admin ");

            // Start up Garnet
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: configurationFile);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Ensure Garnet started up with three users:
            // the 2 specified users and the automatically created default user
            string[] users = await c.ExecuteForArrayAsync("ACL", "USERS").ConfigureAwait(false);
            ClassicAssert.IsTrue(3 == users.Length);
            ClassicAssert.Contains("default", users);
            ClassicAssert.Contains("testA", users);
            ClassicAssert.Contains("testB", users);
        }

        /// <summary>
        /// Test that when the default is defined in the configuration file, that configuration takes precedence
        /// </summary>
        [Test]
        public async Task WithDefaultRule()
        {
            // Create an input with 3 user definitions (including default)
            var configurationFile = Path.Join(TestUtils.MethodTestDir, "users.acl");
            File.WriteAllText(configurationFile, "user testA on >password123 +@admin +@slow\r\nuser testB on >passw0rd >password +@admin\r\nuser default on nopass +@admin +@slow");

            // Start up Garnet with a defined default user password
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: configurationFile, defaultPassword: DummyPassword);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Ensure all three users are defined
            string[] users = await c.ExecuteForArrayAsync("ACL", "USERS").ConfigureAwait(false);
            ClassicAssert.IsTrue(3 == users.Length);
            ClassicAssert.Contains("default", users);
            ClassicAssert.Contains("testA", users);
            ClassicAssert.Contains("testB", users);

            // Ensure that the default password used to create Garnet was ignored
            users = await c.ExecuteForArrayAsync("ACL", "LIST").ConfigureAwait(false);
            foreach (string user in users)
            {
                if (user.StartsWith("user default"))
                {
                    // No password should have been defined
                    ClassicAssert.IsTrue(!user.Contains('#'));
                }
            }
        }

        /// <summary>
        /// Test that ACL LOAD correctly replaces the contents of the ACL
        /// </summary>
        [Test]
        public async Task AclLoad()
        {
            // Create a modified ACL that (1) removes two users, (2) adds one user, (3) removes one password and (4) removes the default user
            string originalConfigurationFile = "user testA on >password123 +@admin +@slow\r\nuser testB on >passw0rd >password +@admin +@slow\r\nuser testC on >passw0rd\r\nuser default on nopass +@admin +@slow";
            string modifiedConfigurationFile = "user testD on >password123\r\nuser testB on >passw0rd +@admin +@slow";

            var configurationFile = Path.Join(TestUtils.MethodTestDir, "users.acl");

            File.WriteAllText(configurationFile, originalConfigurationFile);

            // Start up Garnet
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: configurationFile, defaultPassword: DummyPassword);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Ensure Garnet started up 4 users: testA, testB, testC and default
            string[] users = await c.ExecuteForArrayAsync("ACL", "USERS").ConfigureAwait(false);
            ClassicAssert.IsTrue(4 == users.Length);
            ClassicAssert.Contains("default", users);
            ClassicAssert.Contains("testA", users);
            ClassicAssert.Contains("testB", users);
            ClassicAssert.Contains("testC", users);

            // Check that (1) testB contains two passwords and (2) default user has no password
            users = await c.ExecuteForArrayAsync("ACL", "LIST").ConfigureAwait(false);
            foreach (var user in users)
            {
                if (user.StartsWith("user testB"))
                {
                    ClassicAssert.AreEqual(2, user.Count(x => x == '#'));
                }
                else if (user.StartsWith("user default"))
                {
                    ClassicAssert.AreEqual(0, user.Count(x => x == '#'));
                }
            }

            // Update the configuration file and reload
            File.WriteAllText(configurationFile, modifiedConfigurationFile);
            ClassicAssert.AreEqual("OK", await c.ExecuteAsync("ACL", "LOAD").ConfigureAwait(false));
            ClassicAssert.AreEqual("OK", await c.ExecuteAsync("AUTH", "default", DummyPassword).ConfigureAwait(false));

            // Check the integrity of the list
            users = await c.ExecuteForArrayAsync("ACL", "USERS").ConfigureAwait(false);

            // Verify the ACL now contains only three users
            ClassicAssert.IsTrue(3 == users.Length);
            ClassicAssert.Contains("default", users);
            ClassicAssert.Contains("testD", users);
            ClassicAssert.Contains("testB", users);

            // Ensure that (1) one password was removed from testB and (2) defaut password was set
            users = await c.ExecuteForArrayAsync("ACL", "LIST").ConfigureAwait(false);
            foreach (var user in users)
            {
                if (user.StartsWith("user testB"))
                {
                    ClassicAssert.AreEqual(1, user.Count(x => x == '#'));
                }
                else if (user.StartsWith("user default"))
                {
                    ClassicAssert.IsTrue(user.Contains(DummyPasswordHash));
                }
            }
        }

        /// <summary>
        /// Test that ACL LOAD does not apply any changes when the ACL file contains errors
        /// </summary>
        [Test]
        public async Task AclLoadErrors()
        {
            // Create a modified ACL that (1) adds a new user, (2) modifies an existing user and (3) fails.
            string originalConfigurationFile = "user testA on >password123 +@admin";
            string modifiedConfigurationFile = $"user testB on nopass\r\nuser testA on >password123 >{DummyPassword} +@admin\r\nuser badinput testC on >passw0rd +@admin";

            var configurationFile = Path.Join(TestUtils.MethodTestDir, "users.acl");

            File.WriteAllText(configurationFile, originalConfigurationFile);

            // Start up Garnet
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: configurationFile);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            using var authenticatedUser = TestUtils.GetGarnetClientSession();
            authenticatedUser.Connect();
            ClassicAssert.AreEqual("OK", await authenticatedUser.ExecuteAsync("AUTH", "testA", "password123").ConfigureAwait(false));
            Assert.That(await authenticatedUser.ExecuteForArrayAsync("ACL", "USERS").ConfigureAwait(false), Does.Contain("testA"));

            // Ensure Garnet started up 2 users: testA and default
            string[] users = await c.ExecuteForArrayAsync("ACL", "USERS").ConfigureAwait(false);
            ClassicAssert.IsTrue(2 == users.Length);
            ClassicAssert.Contains("default", users);
            ClassicAssert.Contains("testA", users);

            // Update and reload the configuration file
            // Ensure the command fails and that the user list has not changed
            File.WriteAllText(configurationFile, modifiedConfigurationFile);

            try
            {
                var response = await c.ExecuteAsync("ACL", "LOAD").ConfigureAwait(false);
                Assert.Fail("Loading a malformed ACL file should result in an error.");
            }
            catch (Exception exception)
            {
                ClassicAssert.IsTrue(exception.Message.StartsWith("ERR"));
            }

            Assert.That(await authenticatedUser.ExecuteForArrayAsync("ACL", "USERS").ConfigureAwait(false), Does.Contain("testA"));
            users = await c.ExecuteForArrayAsync("ACL", "USERS").ConfigureAwait(false);

            // Check that we still only know testA and default
            ClassicAssert.IsTrue(2 == users.Length);
            ClassicAssert.Contains("default", users);
            ClassicAssert.Contains("testA", users);

            // Ensure that testA does not contain the dummy password
            users = await c.ExecuteForArrayAsync("ACL", "LIST").ConfigureAwait(false);
            foreach (var user in users)
            {
                if (user.StartsWith("user testA"))
                {
                    ClassicAssert.AreEqual(1, user.Count(x => x == '#'));
                    ClassicAssert.IsFalse(user.Contains(DummyPasswordHash));
                }
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task AclLoadInvalidatesExistingSessionAsync(bool removeUser)
        {
            var configurationFile = Path.Join(TestUtils.MethodTestDir, "users.acl");
            var defaultRule = $"user default on >{DummyPasswordB} +@all\r\n";
            File.WriteAllText(configurationFile, defaultRule + $"user {TestUserA} on >{DummyPassword} +@all");
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: configurationFile);
            server.Start();

            using var admin = TestUtils.GetGarnetClientSession();
            admin.Connect();
            ClassicAssert.AreEqual("OK", await admin.ExecuteAsync("AUTH", "default", DummyPasswordB).ConfigureAwait(false));
            ClassicAssert.AreEqual("OK", await admin.ExecuteAsync("SET", "marker", "value").ConfigureAwait(false));

            using var user = TestUtils.GetGarnetClientSession();
            user.Connect();
            ClassicAssert.AreEqual("OK", await user.ExecuteAsync("AUTH", TestUserA, DummyPassword).ConfigureAwait(false));
            ClassicAssert.AreEqual("value", await user.ExecuteAsync("GET", "marker").ConfigureAwait(false));

            File.WriteAllText(configurationFile,
                defaultRule + (removeUser ? "" : $"user {TestUserA} on >{DummyPasswordB} +get"));
            ClassicAssert.AreEqual("OK", await admin.ExecuteAsync("ACL", "LOAD").ConfigureAwait(false));

            var error = Assert.ThrowsAsync<Exception>(
                async () => await user.ExecuteAsync("GET", "marker").ConfigureAwait(false));
            ClassicAssert.AreEqual("NOAUTH Authentication required.", error.Message);
            error = Assert.ThrowsAsync<Exception>(
                async () => await user.ExecuteAsync("ACL", "SETUSER", TestUserA, "on", "nopass", "+@all").ConfigureAwait(false));
            ClassicAssert.AreEqual("NOAUTH Authentication required.", error.Message);

            using var fresh = TestUtils.GetGarnetClientSession();
            fresh.Connect();
            error = Assert.ThrowsAsync<Exception>(
                async () => await fresh.ExecuteAsync("AUTH", TestUserA, DummyPassword).ConfigureAwait(false));
            Assert.That(error.Message, Does.StartWith("WRONGPASS"));

            if (!removeUser)
            {
                ClassicAssert.AreEqual("OK", await user.ExecuteAsync("AUTH", TestUserA, DummyPasswordB).ConfigureAwait(false));
                ClassicAssert.AreEqual("value", await user.ExecuteAsync("GET", "marker").ConfigureAwait(false));
                error = Assert.ThrowsAsync<Exception>(
                    async () => await user.ExecuteAsync("SET", "marker", "changed").ConfigureAwait(false));
                Assert.That(error.Message, Does.StartWith("NOPERM"));
            }
        }

        [Test]
        public async Task AclLoadInvalidatesPasswordlessDefaultSessionAsync()
        {
            var configurationFile = Path.Join(TestUtils.MethodTestDir, "users.acl");
            File.WriteAllText(configurationFile, "user default on nopass +@all");
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: configurationFile);
            server.Start();

            using var user = TestUtils.GetGarnetClientSession();
            user.Connect();
            ClassicAssert.AreEqual("OK", await user.ExecuteAsync("SET", "marker", "value").ConfigureAwait(false));

            File.WriteAllText(configurationFile, $"user default on resetpass >{DummyPassword} +get");
            ClassicAssert.AreEqual("OK", await user.ExecuteAsync("ACL", "LOAD").ConfigureAwait(false));

            var error = Assert.ThrowsAsync<Exception>(
                async () => await user.ExecuteAsync("GET", "marker").ConfigureAwait(false));
            ClassicAssert.AreEqual("NOAUTH Authentication required.", error.Message);
            error = Assert.ThrowsAsync<Exception>(
                async () => await user.ExecuteAsync("ACL", "SETUSER", "default", "nopass", "+@all").ConfigureAwait(false));
            ClassicAssert.AreEqual("NOAUTH Authentication required.", error.Message);
            error = Assert.ThrowsAsync<Exception>(
                async () => await user.ExecuteAsync("ACL", "SAVE").ConfigureAwait(false));
            ClassicAssert.AreEqual("NOAUTH Authentication required.", error.Message);

            using var fresh = TestUtils.GetGarnetClientSession();
            fresh.Connect();
            error = Assert.ThrowsAsync<Exception>(
                async () => await fresh.ExecuteAsync("GET", "marker").ConfigureAwait(false));
            ClassicAssert.AreEqual("NOAUTH Authentication required.", error.Message);

            ClassicAssert.AreEqual("OK", await user.ExecuteAsync("AUTH", "default", DummyPassword).ConfigureAwait(false));
            ClassicAssert.AreEqual("value", await user.ExecuteAsync("GET", "marker").ConfigureAwait(false));
            error = Assert.ThrowsAsync<Exception>(
                async () => await user.ExecuteAsync("SET", "marker", "changed").ConfigureAwait(false));
            Assert.That(error.Message, Does.StartWith("NOPERM"));
        }

        [TestCase("LOAD", false, "SET", false)]
        [TestCase("LOAD", true, "SET", false)]
        [TestCase("DELUSER", true, "SET", false)]
        [TestCase("SETUSER", true, "SET", false)]
        [TestCase("SETUSEREXEC", true, "SET", false)]
        [TestCase("LOAD", true, "SET", true)]
        [TestCase("LOAD", true, "RI.SET", true)]
        [TestCase("LOAD", true, "VADD", true)]
        public async Task AclChangesDuringTransactionReleaseResourcesAsync(string operation, bool queueAfterChange, string queuedCommand, bool restrictBeforeExec)
        {
            var configurationFile = Path.Join(TestUtils.MethodTestDir, "users.acl");
            File.WriteAllText(configurationFile,
                $"user default on >{DummyPasswordB} +@all\r\nuser {TestUserA} on >{DummyPassword} +@all");
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: configurationFile);
            server.Start();

            using var client = new TcpClient();
            client.Client.Connect(TestUtils.EndPoint);
            using var stream = client.GetStream();
            stream.ReadTimeout = 5000;
            using var reader = new StreamReader(stream, Encoding.ASCII, leaveOpen: true);

            static string Request(params string[] args)
                => $"*{args.Length}\r\n" + string.Concat(args.Select(arg => $"${arg.Length}\r\n{arg}\r\n"));

            void Expect(string expected)
            {
                foreach (var line in expected.Split("\r\n", StringSplitOptions.RemoveEmptyEntries))
                    ClassicAssert.AreEqual(line, reader.ReadLine());
            }

            stream.Write(Encoding.ASCII.GetBytes(Request("AUTH", TestUserA, DummyPassword)));
            Expect("+OK\r\n");

            string[] aclCommand = operation switch
            {
                "LOAD" => ["ACL", "LOAD"],
                "DELUSER" => ["ACL", "DELUSER", TestUserA],
                "SETUSEREXEC" => ["ACL", "SETUSER", TestUserA, "-exec"],
                _ => ["ACL", "SETUSER", TestUserA, "-exec", "-set"]
            };
            var workStillAllowed = operation == "SETUSEREXEC";
            var denied = operation.StartsWith("SETUSER", StringComparison.Ordinal)
                ? "-NOPERM this user has no permissions to run the command\r\n"
                : "-NOAUTH Authentication required.\r\n";
            var transaction = Request("MULTI") + Request("SET", "locked-key", "value") + Request(aclCommand);
            var queuedReplies = "+OK\r\n+QUEUED\r\n+QUEUED\r\n";
            if (queueAfterChange)
            {
                string[] command = queuedCommand switch
                {
                    "RI.SET" => ["RI.SET", "later-key", "field", "value"],
                    "VADD" => ["VADD", "later-key", "VALUES", "2", "1", "0", "element"],
                    _ => ["SET", "later-key", "value"]
                };
                transaction += Request(command);
                queuedReplies += "+QUEUED\r\n";
            }
            const string NoPermission = "-NOPERM this user has no permissions to run the command\r\n";
            var firstWriteDenied = restrictBeforeExec && queuedCommand == "SET";
            var executedReplies = $"*{(queueAfterChange ? 3 : 2)}\r\n";
            executedReplies += firstWriteDenied ? NoPermission : "+OK\r\n";
            executedReplies += operation == "DELUSER" ? ":1\r\n" : "+OK\r\n";
            if (queueAfterChange)
                executedReplies += workStillAllowed ? "+OK\r\n" : denied;

            try
            {
                if (restrictBeforeExec)
                {
                    stream.Write(Encoding.ASCII.GetBytes(transaction));
                    Expect(queuedReplies);
                    using var admin = TestUtils.GetGarnetClientSession();
                    admin.Connect();
                    ClassicAssert.AreEqual("OK", await admin.ExecuteAsync("AUTH", "default", DummyPasswordB).ConfigureAwait(false));
                    ClassicAssert.AreEqual("OK",
                        await admin.ExecuteAsync("ACL", "SETUSER", TestUserA, $"-{queuedCommand.ToLowerInvariant()}").ConfigureAwait(false));
                    transaction = "";
                    queuedReplies = "";
                }
                transaction += Request("EXEC") + Request("SET", "after-boundary", "value");
                stream.Write(Encoding.ASCII.GetBytes(transaction));
                Expect(queuedReplies + executedReplies + (workStillAllowed ? "+OK\r\n" : denied));

                // Subsequent top-level commands use the current ACL, even in the same receive batch.
                stream.Write(Encoding.ASCII.GetBytes(Request("EXEC")));
                Expect(denied);
                stream.Write(Encoding.ASCII.GetBytes(Request("AUTH", "default", DummyPasswordB) + Request("PING")));
                Expect("+OK\r\n+PONG\r\n");

                using var other = TestUtils.GetGarnetClientSession();
                other.Connect();
                var timeout = TimeSpan.FromSeconds(5);
                ClassicAssert.AreEqual("OK",
                    await other.ExecuteAsync("AUTH", "default", DummyPasswordB).WaitAsync(timeout).ConfigureAwait(false));
                ClassicAssert.AreEqual(firstWriteDenied ? null : "value",
                    await other.ExecuteAsync("GET", "locked-key").WaitAsync(timeout).ConfigureAwait(false));
                ClassicAssert.AreEqual(workStillAllowed ? "value" : null,
                    await other.ExecuteAsync("GET", "later-key").WaitAsync(timeout).ConfigureAwait(false));
                ClassicAssert.AreEqual(workStillAllowed ? "value" : null,
                    await other.ExecuteAsync("GET", "after-boundary").WaitAsync(timeout).ConfigureAwait(false));
                ClassicAssert.AreEqual("OK",
                    await other.ExecuteAsync("SET", "locked-key", "updated").WaitAsync(timeout).ConfigureAwait(false));
                ClassicAssert.AreEqual("OK", await other.ExecuteAsync("SAVE").WaitAsync(timeout).ConfigureAwait(false));
            }
            finally
            {
                // Release a transaction left running by a failing assertion before disposing the server.
                stream.Write(Encoding.ASCII.GetBytes(
                    Request("AUTH", "default", DummyPasswordB) + Request("EXEC") + Request("ECHO", "txn-cleanup")));
                while (reader.ReadLine() is { } line && line != "txn-cleanup")
                {
                }
            }
        }

        [Test]
        public async Task RevokedUserCannotStartQueuedTransactionAsync()
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();

            using var admin = TestUtils.GetGarnetClientSession();
            admin.Connect();
            ClassicAssert.AreEqual("OK",
                await admin.ExecuteAsync("ACL", "SETUSER", TestUserA, "on", $">{DummyPassword}", "+@all").ConfigureAwait(false));

            using var user = TestUtils.GetGarnetClientSession();
            user.Connect();
            ClassicAssert.AreEqual("OK", await user.ExecuteAsync("AUTH", TestUserA, DummyPassword).ConfigureAwait(false));
            ClassicAssert.AreEqual("OK", await user.ExecuteAsync("MULTI").ConfigureAwait(false));
            ClassicAssert.AreEqual("QUEUED", await user.ExecuteAsync("SET", "locked-key", "value").ConfigureAwait(false));
            ClassicAssert.AreEqual("1", await admin.ExecuteAsync("ACL", "DELUSER", TestUserA).ConfigureAwait(false));

            var error = Assert.ThrowsAsync<Exception>(
                async () => await user.ExecuteAsync("EXEC").ConfigureAwait(false));
            ClassicAssert.AreEqual("NOAUTH Authentication required.", error.Message);

            var timeout = TimeSpan.FromSeconds(5);
            ClassicAssert.IsNull(await admin.ExecuteAsync("GET", "locked-key").WaitAsync(timeout).ConfigureAwait(false));
            ClassicAssert.AreEqual("OK", await admin.ExecuteAsync("SAVE").WaitAsync(timeout).ConfigureAwait(false));
        }

        [Test]
        public async Task EmptyTransactionFinalizesAsync()
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();
            using var client = TestUtils.GetGarnetClientSession();
            client.Connect();
            var timeout = TimeSpan.FromSeconds(5);
            ClassicAssert.AreEqual("OK", await client.ExecuteAsync("MULTI").ConfigureAwait(false));
            ClassicAssert.IsEmpty(await client.ExecuteForArrayAsync("EXEC").WaitAsync(timeout).ConfigureAwait(false));
            ClassicAssert.AreEqual("PONG", await client.ExecuteAsync("PING").WaitAsync(timeout).ConfigureAwait(false));
            ClassicAssert.AreEqual("OK", await client.ExecuteAsync("SAVE").WaitAsync(timeout).ConfigureAwait(false));
        }

        [TestCase(false, false)]
        [TestCase(false, true)]
        [TestCase(true, false)]
        [TestCase(true, true)]
        public async Task ScriptExecCannotFinalizeLuaTransactionAsync(bool allowExec, bool usePcall)
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true,
                enableLua: true, luaTransactionMode: true);
            server.Start();

            using var admin = TestUtils.GetGarnetClientSession();
            admin.Connect();
            ClassicAssert.AreEqual("OK",
                await admin.ExecuteAsync("ACL", "SETUSER", TestUserA, "on", $">{DummyPassword}", "+@all").ConfigureAwait(false));
            if (!allowExec)
                ClassicAssert.AreEqual("OK", await admin.ExecuteAsync("ACL", "SETUSER", TestUserA, "-exec").ConfigureAwait(false));

            using var user = TestUtils.GetGarnetClientSession();
            user.Connect();
            ClassicAssert.AreEqual("OK", await user.ExecuteAsync("AUTH", TestUserA, DummyPassword).ConfigureAwait(false));
            var timeout = TimeSpan.FromSeconds(5);
            string error;
            if (usePcall)
            {
                error = await user.ExecuteAsync("EVAL", "return redis.pcall('EXEC').err", "1", "lua-key")
                    .WaitAsync(timeout).ConfigureAwait(false);
            }
            else
            {
                var exception = Assert.ThrowsAsync<Exception>(async () =>
                    await user.ExecuteAsync("EVAL", "return redis.call('EXEC')", "1", "lua-key")
                        .WaitAsync(timeout).ConfigureAwait(false));
                error = exception.Message;
            }
            Assert.That(error, Does.Contain(allowExec ? "not allowed from script" : "permission").IgnoreCase);
            ClassicAssert.AreEqual("OK",
                await user.ExecuteAsync("SET", "lua-key", "value").WaitAsync(timeout).ConfigureAwait(false));
            ClassicAssert.AreEqual("OK", await admin.ExecuteAsync("SAVE").WaitAsync(timeout).ConfigureAwait(false));
        }

        /// <summary>
        /// Ensures that duplicate user definitions in configuration files are flagged as errors.
        /// </summary>
        [Test]
        public async Task DuplicateUserNames()
        {
            // Create a file with two users with name "test"
            var configurationFile = Path.Join(TestUtils.MethodTestDir, "users.acl");
            File.WriteAllText(configurationFile, $"user test on >{DummyPassword} +@admin\r\nuser test off");

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: configurationFile);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Ensure correct users were created
            string[] users = await c.ExecuteForArrayAsync("ACL", "USERS").ConfigureAwait(false);
            ClassicAssert.IsTrue(2 == users.Length);
            ClassicAssert.Contains("default", users);
            ClassicAssert.Contains("test", users);

            // Expected behavior: the second rule modifies the first one
            foreach (string user in users)
            {
                if (user.StartsWith($"user test"))
                {
                    // Should contain exactly 1 password (from first rule)
                    ClassicAssert.IsTrue(user.Count(x => x == '#') == 1);
                    ClassicAssert.IsTrue(user.Contains(DummyPasswordHash));

                    // Should be set to off (second rule)
                    ClassicAssert.IsTrue(user.Contains("off"));
                }
            }
        }

        /// <summary>
        /// Ensure Garnet handles non-existing configuration file paths by throwing an appropriate exception
        /// </summary>
        [Test]
        public void BadInputNonexistingFile()
        {
            // NOTE: Do not create the configuration file
            var configurationFile = Path.Join(TestUtils.MethodTestDir, "users.acl");

            // Garnet should ignore the non-existing configuration file and start up with default user
            Assert.Throws<ACLException>(() => TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: configurationFile));
        }

        /// <summary>
        /// Ensures that malformed statements in configuration files throw an appropriate exception
        /// </summary>
        [Test]
        public void BadInputMalformedStatement()
        {
            // Test badinput
            {
                var configurationFile = Path.Join(TestUtils.MethodTestDir, "users.acl");
                File.WriteAllText(configurationFile, "user test on >password123 +@admin\r\nuser testB badinput on >passw0rd >password +@admin ");

                // Ensure Garnet starts up and just ignores the malformed statement
                Assert.Throws<ACLException>(() => TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: configurationFile));
            }

            // Test numeric RespCommand rejected
            //{
            //    var configurationFile = Path.Join(TestUtils.MethodTestDir, "users2.acl");
            //    File.WriteAllText(configurationFile, "user test on >password123 +1");

            //    // Ensure Garnet starts up and just ignores the malformed statement
            //    Assert.Throws<ACLException>(() => TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: configurationFile));
            //}

            // Test None rejected by strict mode
            {
                var configurationFile = Path.Join(TestUtils.MethodTestDir, "users3.acl");
                File.WriteAllText(configurationFile, "user test on >password123 +none");

                // Ensure Garnet refuses to start and surfaces the strict-mode diagnostic
                var ex = Assert.Throws<GarnetException>(() => TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: configurationFile));
                StringAssert.Contains("ACL strict mode", ex.Message);
            }

            // Test Invalid rejected by strict mode
            {
                var configurationFile = Path.Join(TestUtils.MethodTestDir, "users4.acl");
                File.WriteAllText(configurationFile, "user test on >password123 +invalid");

                // Ensure Garnet refuses to start and surfaces the strict-mode diagnostic
                var ex = Assert.Throws<GarnetException>(() => TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: configurationFile));
                StringAssert.Contains("ACL strict mode", ex.Message);
            }
        }

        [Test]
        public void AclSave()
        {
            // Create a modified ACL that (1) removes two users, (2) adds one user, (3) removes one password and (4) removes the default user
            var originalConfigurationFile =
                "user testA on >password123 +@admin\r\n" +
                "user testB on >passw0rd >password +@admin\r\n" +
                "user testC on >passw0rd\r\nuser default on nopass +@all";
            var configurationFile = Path.Join(TestUtils.MethodTestDir, "users.acl");
            var updateUser = "testD";
            var updatePass = "placeholder";

            File.WriteAllText(configurationFile, originalConfigurationFile);

            // Start up Garnet
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: configurationFile, defaultPassword: DummyPassword);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Update existing user
            var resp = (string)db.Execute("ACL", ["SETUSER", updateUser, $">{updatePass}"]);
            ClassicAssert.AreEqual("OK", resp);

            // List users
            var users = (string[])db.Execute("ACL", "USERS");
            ClassicAssert.IsTrue(users.ToHashSet().Contains(updateUser));

            // Reload acl config
            resp = (string)db.Execute("ACL", "LOAD");
            ClassicAssert.AreEqual("OK", resp);
            ClassicAssert.AreEqual("OK", (string)db.Execute("AUTH", "default", ""));

            // Retrieve users after reload and ensure user does not exist
            users = (string[])db.Execute("ACL", "USERS");
            ClassicAssert.IsFalse(users.ToHashSet().Contains(updateUser));

            // Update existing user
            resp = (string)db.Execute("ACL", ["SETUSER", updateUser, $">{updatePass}"]);
            ClassicAssert.AreEqual("OK", resp);

            // Save in memory ACL
            resp = (string)db.Execute("ACL", "SAVE");
            ClassicAssert.AreEqual("OK", resp);

            // Reload acl config
            resp = (string)db.Execute("ACL", "LOAD");
            ClassicAssert.AreEqual("OK", resp);
            ClassicAssert.AreEqual("OK", (string)db.Execute("AUTH", "default", ""));

            // Retrieve users after save and ensure user does exist
            users = (string[])db.Execute("ACL", "USERS");
            ClassicAssert.IsTrue(users.ToHashSet().Contains(updateUser));
        }
    }
}