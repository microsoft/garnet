// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.Resp.ACL
{
    /// <summary>
    /// Tests for Resp ACL commands that don't have subcommands
    /// </summary>
    [TestFixture]
    internal class BasicTests : AclTest
    {
        /// <summary>
        /// Creates and starts the Garnet test server
        /// </summary>
        [SetUp]
        public virtual void Setup()
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();
        }

        /// <summary>
        /// Tests that ACL WHOAMI shows the currently authenticated user
        /// </summary>
        [Test]
        public async Task BasicWhoamiTest()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Check user is authenticated to default
            var response = await c.ExecuteAsync("ACL", "WHOAMI");
            ClassicAssert.AreEqual("default", response);

            // Add the testuser and password
            response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, "on", "nopass", "+@admin", "+@slow");
            ClassicAssert.IsTrue(response.StartsWith("OK"));

            // Change users and verify whoami changes
            response = await c.ExecuteAsync("AUTH", TestUserA, "password");
            ClassicAssert.IsTrue(response.StartsWith("OK"));
            response = await c.ExecuteAsync("ACL", "WHOAMI");
            ClassicAssert.AreEqual(TestUserA, response);

            // Change users back to default and verify whoami changes
            response = await c.ExecuteAsync("AUTH", "default", "password");
            ClassicAssert.IsTrue(response.StartsWith("OK"));
            response = await c.ExecuteAsync("ACL", "WHOAMI");
            ClassicAssert.AreEqual("default", response);
        }

        /// <summary>
        /// Ensures that ACL LIST adapts to changing user sets
        /// </summary>
        [Test]
        public async Task BasicListTest()
        {
            const string ExpectedDefaultRule = "user default on nopass +@all";
            const string ExpectedTestUserRule = $"user {TestUserA} off";

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Right now only the default user should exist
            string[] users = await c.ExecuteForArrayAsync("ACL", "LIST");
            ClassicAssert.IsTrue(1 == users.Length);
            ClassicAssert.Contains(ExpectedDefaultRule, users);

            // Add the test user and makes sure the list gets extended
            var response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA);
            ClassicAssert.AreEqual("OK", response);
            users = await c.ExecuteForArrayAsync("ACL", "LIST");
            ClassicAssert.IsTrue(2 == users.Length);
            ClassicAssert.Contains(ExpectedDefaultRule, users);
            ClassicAssert.Contains(ExpectedTestUserRule, users);

            // Remove the test user and make sure the list gets trimmed again
            response = await c.ExecuteAsync("ACL", "DELUSER", TestUserA);
            ClassicAssert.AreEqual("1", response);
            users = await c.ExecuteForArrayAsync("ACL", "LIST");
            ClassicAssert.IsTrue(1 == users.Length);
            ClassicAssert.Contains(ExpectedDefaultRule, users);
        }

        /// <summary>
        /// Ensure that ACL USERS results change when new users are added
        /// </summary>
        [Test]
        public async Task BasicUsersTest()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Right now only the default user should exist
            string[] users = await c.ExecuteForArrayAsync("ACL", "USERS");
            ClassicAssert.IsTrue(1 == users.Length);
            ClassicAssert.Contains("default", users);

            // Add a second user and makes sure the list gets longer.
            var response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA);
            ClassicAssert.AreEqual("OK", response);
            users = await c.ExecuteForArrayAsync("ACL", "USERS");
            ClassicAssert.IsTrue(2 == users.Length);
            ClassicAssert.Contains(TestUserA, users);
            ClassicAssert.Contains("default", users);
        }

        [Test]
        public async Task BasicGenPassTest()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            var response = await c.ExecuteAsync("ACL", "GENPASS");
            ClassicAssert.AreEqual(64, response.Length);

            response = await c.ExecuteAsync("ACL", "GENPASS", "5");
            ClassicAssert.AreEqual(2, response.Length);

            Assert.ThrowsAsync<Exception>(async () => await c.ExecuteAsync("ACL", "GENPASS", "abcd"),
                                          "ERR value is not an integer or out of range.");

            var error = "ERR ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096";

            Assert.ThrowsAsync<Exception>(async () => await c.ExecuteAsync("ACL", "GENPASS", "4097"), error);
            Assert.ThrowsAsync<Exception>(async () => await c.ExecuteAsync("ACL", "GENPASS", "0"), error);
        }

        /// <summary>
        /// Tests that an error is returned when an invalid subcommand is specified
        /// </summary>
        [Test]
        public void InvalidSubcommand()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ACL subcommand");
            ClassicAssert.IsTrue(response.AsSpan().StartsWith("-ERR"u8));
        }

        /// <summary>
        /// Test that our check for "has subcommands" matches reality.
        /// </summary>
        [Test]
        public void NoAuthValidation()
        {
            foreach (var cmd in Enum.GetValues<RespCommand>())
            {
                if (cmd == RespCommand.NONE || cmd == RespCommand.INVALID)
                {
                    continue;
                }

                if (RespCommandsInfo.TryGetRespCommandInfo(cmd, out var info))
                {
                    var infoIsNoAuth = info.Flags.HasFlag(RespCommandFlags.NoAuth);
                    var cmdIsNoAuth = cmd.IsNoAuth();

                    ClassicAssert.AreEqual(infoIsNoAuth, cmdIsNoAuth, $"Mismatch for command {cmd}");
                }
            }
        }
    }
}