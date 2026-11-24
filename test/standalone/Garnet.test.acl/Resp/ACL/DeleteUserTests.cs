// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.Resp.ACL
{
    /// <summary>
    /// Tests for ACL DELUSER operations.
    /// </summary>
    [TestFixture]
    class DeleteUserTests : AclTest
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
        /// Executes deluser command with a single user
        /// </summary>
        [Test]
        public async Task DeleteSingleUser()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Add two new users
            var response = await c.ExecuteAsync("ACL", "setuser", TestUserA, ">passwd").ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", response);

            response = await c.ExecuteAsync("ACL", "setuser", TestUserB, ">passwd").ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", response);

            // Verify that both users exist
            string[] usernames = await c.ExecuteForArrayAsync("ACL", "users").ConfigureAwait(false);
            Assert.That(usernames, Does.Contain(TestUserA));
            Assert.That(usernames, Does.Contain(TestUserB));

            // Try to delete test user A
            response = await c.ExecuteAsync("ACL", "deluser", TestUserA).ConfigureAwait(false);
            ClassicAssert.AreEqual("1", response);

            // Ensure test user A is not listed, but test user B still exists
            usernames = await c.ExecuteForArrayAsync("ACL", "users").ConfigureAwait(false);
            Assert.That(usernames, Does.Not.Contain(TestUserA));
            Assert.That(usernames, Does.Contain(TestUserB));
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task DeleteUserInvalidatesExistingSessionAsync(bool recreateBeforeCommand)
        {
            using var admin = TestUtils.GetGarnetClientSession();
            admin.Connect();
            ClassicAssert.AreEqual("OK",
                await admin.ExecuteAsync("ACL", "SETUSER", TestUserA, "on", $">{DummyPassword}", "+@all").ConfigureAwait(false));
            ClassicAssert.AreEqual("OK", await admin.ExecuteAsync("SET", "marker", "value").ConfigureAwait(false));

            using var user = TestUtils.GetGarnetClientSession();
            user.Connect();
            ClassicAssert.AreEqual("OK", await user.ExecuteAsync("AUTH", TestUserA, DummyPassword).ConfigureAwait(false));
            ClassicAssert.AreEqual("value", await user.ExecuteAsync("GET", "marker").ConfigureAwait(false));

            ClassicAssert.AreEqual("1", await admin.ExecuteAsync("ACL", "DELUSER", TestUserA).ConfigureAwait(false));
            if (recreateBeforeCommand)
            {
                ClassicAssert.AreEqual("OK",
                    await admin.ExecuteAsync("ACL", "SETUSER", TestUserA, "on", $">{DummyPasswordB}", "+get").ConfigureAwait(false));
            }

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

            if (recreateBeforeCommand)
            {
                ClassicAssert.AreEqual("OK", await user.ExecuteAsync("AUTH", TestUserA, DummyPasswordB).ConfigureAwait(false));
                ClassicAssert.AreEqual("value", await user.ExecuteAsync("GET", "marker").ConfigureAwait(false));
                error = Assert.ThrowsAsync<Exception>(
                    async () => await user.ExecuteAsync("SET", "marker", "changed").ConfigureAwait(false));
                Assert.That(error.Message, Does.StartWith("NOPERM"));
            }
        }

        /// <summary>
        /// Executes deluser command with multiple users
        /// </summary>
        [Test]
        public async Task DeleteMultipleUser()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Add two new users
            var response = await c.ExecuteAsync("ACL", "setuser", TestUserA, ">passwd").ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", response);

            response = await c.ExecuteAsync("ACL", "setuser", TestUserB, ">passwd").ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", response);

            // Verify that both users exist
            string[] usernames = await c.ExecuteForArrayAsync("ACL", "users").ConfigureAwait(false);
            Assert.That(usernames, Does.Contain(TestUserA));
            Assert.That(usernames, Does.Contain(TestUserB));

            // Try to delete both users in a single command
            response = await c.ExecuteAsync("ACL", "deluser", TestUserA, TestUserB).ConfigureAwait(false);
            ClassicAssert.AreEqual("2", response);

            // Ensure both users have been deleted
            usernames = await c.ExecuteForArrayAsync("ACL", "users").ConfigureAwait(false);
            Assert.That(usernames, Does.Not.Contain(TestUserA));
            Assert.That(usernames, Does.Not.Contain(TestUserB));
        }

        /// <summary>
        /// Attempts to delete a non-existing user. Garnet should just ignore the command
        /// </summary>
        [Test]
        public async Task DeleteNonexistingUser()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Add two new users
            var response = await c.ExecuteAsync("ACL", "setuser", TestUserA, ">passwd").ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", response);

            response = await c.ExecuteAsync("ACL", "setuser", TestUserB, ">passwd").ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", response);

            // Verify that both users exist
            string[] usernames = await c.ExecuteForArrayAsync("ACL", "users").ConfigureAwait(false);
            Assert.That(usernames, Does.Contain(TestUserA));
            Assert.That(usernames, Does.Contain(TestUserB));

            // Try to delete both users in a single command
            response = await c.ExecuteAsync("ACL", "deluser", TestUserUnknown).ConfigureAwait(false);
            ClassicAssert.AreEqual("0", response);

            // Ensure both users still exist
            usernames = await c.ExecuteForArrayAsync("ACL", "users").ConfigureAwait(false);
            Assert.That(usernames, Does.Contain(TestUserA));
            Assert.That(usernames, Does.Contain(TestUserB));
        }

        /// <summary>
        /// Tests that the default user cannot be deleted but is replaced by an implicit default user
        /// </summary>
        [Test]
        public async Task DeleteDefaultUser()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Verify that default user exists
            string[] usernames = await c.ExecuteForArrayAsync("ACL", "users").ConfigureAwait(false);
            Assert.That(usernames, Does.Contain("default"));

            // Try to delete the default user
            try
            {
                var response = await c.ExecuteAsync("ACL", "deluser", "default").ConfigureAwait(false);
                Assert.Fail("Attempting to delete the default user should raise an error.");
            }
            catch (Exception exception)
            {
                ClassicAssert.IsTrue(exception.Message.StartsWith("ERR"));
            }

            // Verify that default user still exists
            usernames = await c.ExecuteForArrayAsync("ACL", "users").ConfigureAwait(false);
            Assert.That(usernames, Does.Contain("default"));
        }

        /// <summary>
        /// Attempts to call deluser without any usernames.
        /// </summary>
        [Test]
        public async Task DeleteNoUser()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Add two new users
            var response = await c.ExecuteAsync("ACL", "setuser", TestUserA, ">passwd").ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", response);

            response = await c.ExecuteAsync("ACL", "setuser", TestUserB, ">passwd").ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", response);

            // Verify that both users exist
            string[] usernames = await c.ExecuteForArrayAsync("ACL", "users").ConfigureAwait(false);
            Assert.That(usernames, Does.Contain(TestUserA));
            Assert.That(usernames, Does.Contain(TestUserB));

            // Try to delete both users in a single command
            try
            {
                response = await c.ExecuteAsync("ACL", "deluser").ConfigureAwait(false);
                Assert.Fail("Shouldn't succeed, DELUSER requires arguments");
            }
            catch (Exception e)
            {
                ClassicAssert.AreEqual("ERR wrong number of arguments for 'acl|deluser' command", e.Message);
            }

            // Ensure both users still exist
            usernames = await c.ExecuteForArrayAsync("ACL", "users").ConfigureAwait(false);
            Assert.That(usernames, Does.Contain(TestUserA));
            Assert.That(usernames, Does.Contain(TestUserB));
        }
    }
}