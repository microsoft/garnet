// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using NUnit.Framework;

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
            var response = await c.ExecuteAsync("ACL", "setuser", TestUserA, ">passwd");
            Assert.AreEqual("OK", response);

            response = await c.ExecuteAsync("ACL", "setuser", TestUserB, ">passwd");
            Assert.AreEqual("OK", response);

            // Verify that both users exist
            string[] usernames = await c.ExecuteForArrayAsync("ACL", "users");
            Assert.That(usernames, Does.Contain(TestUserA));
            Assert.That(usernames, Does.Contain(TestUserB));

            // Try to delete test user A
            response = await c.ExecuteAsync("ACL", "deluser", TestUserA);
            Assert.AreEqual("1", response);

            // Ensure test user A is not listed, but test user B still exists
            usernames = await c.ExecuteForArrayAsync("ACL", "users");
            Assert.That(usernames, Does.Not.Contain(TestUserA));
            Assert.That(usernames, Does.Contain(TestUserB));
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
            var response = await c.ExecuteAsync("ACL", "setuser", TestUserA, ">passwd");
            Assert.AreEqual("OK", response);

            response = await c.ExecuteAsync("ACL", "setuser", TestUserB, ">passwd");
            Assert.AreEqual("OK", response);

            // Verify that both users exist
            string[] usernames = await c.ExecuteForArrayAsync("ACL", "users");
            Assert.That(usernames, Does.Contain(TestUserA));
            Assert.That(usernames, Does.Contain(TestUserB));

            // Try to delete both users in a single command
            response = await c.ExecuteAsync("ACL", "deluser", TestUserA, TestUserB);
            Assert.AreEqual("2", response);

            // Ensure both users have been deleted
            usernames = await c.ExecuteForArrayAsync("ACL", "users");
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
            var response = await c.ExecuteAsync("ACL", "setuser", TestUserA, ">passwd");
            Assert.AreEqual("OK", response);

            response = await c.ExecuteAsync("ACL", "setuser", TestUserB, ">passwd");
            Assert.AreEqual("OK", response);

            // Verify that both users exist
            string[] usernames = await c.ExecuteForArrayAsync("ACL", "users");
            Assert.That(usernames, Does.Contain(TestUserA));
            Assert.That(usernames, Does.Contain(TestUserB));

            // Try to delete both users in a single command
            response = await c.ExecuteAsync("ACL", "deluser", TestUserUnknown);
            Assert.AreEqual("0", response);

            // Ensure both users still exist
            usernames = await c.ExecuteForArrayAsync("ACL", "users");
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
            string[] usernames = await c.ExecuteForArrayAsync("ACL", "users");
            Assert.That(usernames, Does.Contain("default"));

            // Try to delete the default user
            try
            {
                var response = await c.ExecuteAsync("ACL", "deluser", "default");
                Assert.Fail("Attempting to delete the default user should raise an error.");
            }
            catch (Exception exception)
            {
                Assert.IsTrue(exception.Message.StartsWith("ERR"));
            }

            // Verify that default user still exists
            usernames = await c.ExecuteForArrayAsync("ACL", "users");
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
            var response = await c.ExecuteAsync("ACL", "setuser", TestUserA, ">passwd");
            Assert.AreEqual("OK", response);

            response = await c.ExecuteAsync("ACL", "setuser", TestUserB, ">passwd");
            Assert.AreEqual("OK", response);

            // Verify that both users exist
            string[] usernames = await c.ExecuteForArrayAsync("ACL", "users");
            Assert.That(usernames, Does.Contain(TestUserA));
            Assert.That(usernames, Does.Contain(TestUserB));

            // Try to delete both users in a single command
            response = await c.ExecuteAsync("ACL", "deluser");
            Assert.AreEqual("0", response);

            // Ensure both users still exist
            usernames = await c.ExecuteForArrayAsync("ACL", "users");
            Assert.That(usernames, Does.Contain(TestUserA));
            Assert.That(usernames, Does.Contain(TestUserB));
        }
    }
}