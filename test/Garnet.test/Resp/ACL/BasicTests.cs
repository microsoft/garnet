// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Internal;

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
            Assert.AreEqual("default", response);

            // Add the testuser and password
            response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, "on", "nopass", "+@admin");
            Assert.IsTrue(response.StartsWith("OK"));

            // Change users and verify whoami changes
            response = await c.ExecuteAsync("AUTH", TestUserA, "password");
            Assert.IsTrue(response.StartsWith("OK"));
            response = await c.ExecuteAsync("ACL", "WHOAMI");
            Assert.AreEqual(TestUserA, response);

            // Change users back to default and verify whoami changes
            response = await c.ExecuteAsync("AUTH", "default", "password");
            Assert.IsTrue(response.StartsWith("OK"));
            response = await c.ExecuteAsync("ACL", "WHOAMI");
            Assert.AreEqual("default", response);
        }

        /// <summary>
        /// Ensures that ACL LIST adapts to changing user sets
        /// </summary>
        [Test]
        public async Task BasicListTest()
        {
            const string ExpectedDefaultRule = "user default on nopass +@admin";
            const string ExpectedTestUserRule = $"user {TestUserA} off";

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Right now only the default user should exist
            string[] users = await c.ExecuteForArrayAsync("ACL", "LIST");
            Assert.IsTrue(1 == users.Length);
            Assert.Contains(ExpectedDefaultRule, users);

            // Add the test user and makes sure the list gets extended
            var response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA);
            Assert.AreEqual("OK", response);
            users = await c.ExecuteForArrayAsync("ACL", "LIST");
            Assert.IsTrue(2 == users.Length);
            Assert.Contains(ExpectedDefaultRule, users);
            Assert.Contains(ExpectedTestUserRule, users);

            // Remove the test user and make sure the list gets trimmed again
            response = await c.ExecuteAsync("ACL", "DELUSER", TestUserA);
            Assert.AreEqual("1", response);
            users = await c.ExecuteForArrayAsync("ACL", "LIST");
            Assert.IsTrue(1 == users.Length);
            Assert.Contains(ExpectedDefaultRule, users);
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
            Assert.IsTrue(1 == users.Length);
            Assert.Contains("default", users);

            // Add a second user and makes sure the list gets longer.
            var response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA);
            Assert.AreEqual("OK", response);
            users = await c.ExecuteForArrayAsync("ACL", "USERS");
            Assert.IsTrue(2 == users.Length);
            Assert.Contains(TestUserA, users);
            Assert.Contains("default", users);
        }

        /// <summary>
        /// Tests that an error is returned when an invalid subcommand is specified
        /// </summary>
        [Test]
        public void InvalidSubcommand()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ACL subcommand");
            Assert.IsTrue(Encoding.ASCII.GetString(response).StartsWith("-ERR"));
        }
    }
}