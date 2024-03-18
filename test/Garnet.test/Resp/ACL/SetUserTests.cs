// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Garnet.test.Resp.ACL
{
    /// <summary>
    /// Tests for ACL SETUSER operations.
    /// </summary>
    [TestFixture]
    class SetUserTests : AclTest
    {
        /// <summary>
        /// Tests that new connections start with default user when no users are defined
        /// </summary>
        [Test]
        public void PasswordlessDefaultUserTest()
        {
            // Create a new test server without password - should automatically login default user
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();

            // Check user is authenticated as default
            using var lightClientRequest = TestUtils.CreateRequest();
            var expectedResponse = "+default\r\n";
            var response = lightClientRequest.SendCommand("ACL WHOAMI");
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);

            // Correctness check
            Assert.AreEqual(expectedResponse, actualValue);
        }

        /// <summary>
        /// Tests that new connections do not start with default user when default user is password protected
        /// </summary>
        [Test]
        public async Task ProtectedDefaultUserErrorHandlingTest()
        {
            // Create a new test server with password - should disallow any operation
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, defaultPassword: DummyPassword);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Ensure that we connect as an unauthenticated session
            try
            {
                await c.ExecuteAsync("ACL", "LIST");
                Assert.Fail("Unauthenticated user should not be able to execute ACL commands.");
            }
            catch (Exception exception)
            {
                Assert.IsTrue(exception.Message.StartsWith("NOAUTH"));
            }
        }

        /// <summary>
        /// Tests that login with password only logs in default user
        /// </summary>
        [Test]
        public async Task ProtectedDefaultUserLoginImplicitTest()
        {
            // Create a new test server with password - should disallow any operation
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, defaultPassword: DummyPassword);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Authenticate user with password only
            var response = await c.ExecuteAsync("AUTH", DummyPassword);
            Assert.IsTrue(response.StartsWith("OK"));

            // Check user is authenticated as default user
            response = await c.ExecuteAsync("ACL", "WHOAMI");
            Assert.AreEqual("default", response);
        }

        /// <summary>
        /// Tests that automatically defined default user can be logged in with explicit user name
        /// </summary>
        [Test]
        public async Task ProtectedDefaultUserLoginExplicitTest()
        {
            // Create a new test server with password - should disallow any operation
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, defaultPassword: DummyPassword);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Authenticate user with username AND password
            var response = await c.ExecuteAsync("AUTH", "default", DummyPassword);
            Assert.IsTrue(response.StartsWith("OK"));

            // Check user is authenticated as default user
            response = await c.ExecuteAsync("ACL", "WHOAMI");
            Assert.AreEqual("default", response);
        }

        /// <summary>
        /// Tests disabling/enabling of users
        /// </summary>
        /// <returns></returns>
        [Test]
        public async Task EnableAndDisableUsers()
        {
            // Create a new test server without default password
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Add the testuser and password
            var response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, $">{DummyPassword}");
            Assert.IsTrue(response.StartsWith("OK"));

            // Ensure the user cannot be authenticated
            try
            {
                response = await c.ExecuteAsync("AUTH", TestUserA, DummyPassword);
                Assert.Fail("Attempting to authenticate a new user that is not specified as 'on' should result in an error.");
            }
            catch (Exception exception)
            {
                Assert.IsTrue(exception.Message.StartsWith("WRONGPASS"));
            }

            // Explicitly enable the user
            response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, "on");
            Assert.IsTrue(response.StartsWith("OK"));

            // User should be able to log in now
            response = await c.ExecuteAsync("AUTH", TestUserA, DummyPassword);
            Assert.IsTrue(response.StartsWith("OK"));

            // Switch to default user and explicitly disable user
            response = await c.ExecuteAsync("AUTH", "default");
            Assert.IsTrue(response.StartsWith("OK"));

            response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, "off");
            Assert.IsTrue(response.StartsWith("OK"));

            // Ensure the user cannot be authenticated anymore
            try
            {
                response = await c.ExecuteAsync("AUTH", TestUserA, DummyPassword);
                Assert.Fail("Attempting to authenticate a new user that is not specified as 'on' should result in an error.");
            }
            catch (Exception exception)
            {
                Assert.IsTrue(exception.Message.StartsWith("WRONGPASS"));
            }
        }

        /// <summary>
        /// Tests that passwords can be added in cleartext
        /// </summary>
        [Test]
        public async Task AddPasswordFromCleartextTest()
        {
            // Create a new test server with password - should disallow any operation
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Add the password and verify it is set correctly
            var response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, $">{DummyPassword}");
            Assert.IsTrue(response.StartsWith("OK"));
            string[] users = await c.ExecuteForArrayAsync("ACL", "list");
            foreach (string user in users)
            {
                if (user.StartsWith($"user {TestUserA}"))
                {
                    Assert.IsTrue(user.Contains(DummyPasswordHash));
                }
                else
                {
                    Assert.IsFalse(user.Contains(DummyPasswordHash));
                }
            }
        }

        /// <summary>
        /// Tests that passwords can be added in hexstrings
        /// </summary>
        [Test]
        public async Task AddPasswordFromHashTest()
        {
            // Create a new test server with password - should disallow any operation
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Add the password and verify it is set correctly
            var response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, $"#{DummyPasswordHash}");
            Assert.IsTrue(response.StartsWith("OK"));
            string[] users = await c.ExecuteForArrayAsync("ACL", "list");
            foreach (string user in users)
            {
                if (user.StartsWith($"user {TestUserA}"))
                {
                    Assert.IsTrue(user.Contains(DummyPasswordHash));
                }
                else
                {
                    Assert.IsFalse(user.Contains(DummyPasswordHash));
                }
            }
        }

        /// <summary>
        /// Tests that passwords can be removed using the clearstring password
        /// </summary>
        [Test]
        public async Task RemovePasswordFromCleartextTest()
        {
            // Create a new test server with password - should disallow any operation
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Add the password
            var response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, $">{DummyPassword}");
            Assert.IsTrue(response.StartsWith("OK"));

            // Try to delete the password
            response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, $"<{DummyPassword}");
            Assert.IsTrue(response.StartsWith("OK"));

            // Ensure the password is not set for the testuser
            string[] users = await c.ExecuteForArrayAsync("ACL", "list");
            foreach (string user in users)
            {
                if (user.StartsWith($"user {TestUserA}"))
                {
                    Assert.IsFalse(user.Contains(DummyPasswordHash));
                }
            }
        }

        /// <summary>
        /// Tests that passwords can be removed using the password hash
        /// </summary>
        [Test]
        public async Task RemovePasswordFromHashTest()
        {
            // Create a new test server with password - should disallow any operation
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Add the password
            var response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, $"#{DummyPasswordHash}");
            Assert.IsTrue(response.StartsWith("OK"));

            // Try to delete the password
            response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, $"!{DummyPasswordHash}");
            Assert.IsTrue(response.StartsWith("OK"));

            // Ensure the password is not set for the testuser
            string[] users = await c.ExecuteForArrayAsync("ACL", "list");
            foreach (string user in users)
            {
                if (user.StartsWith($"user {TestUserA}"))
                {
                    Assert.IsFalse(user.Contains(DummyPasswordHash));
                }
            }
        }

        /// <summary>
        /// Attempt to add a duplicate password
        /// </summary>
        [Test]
        public async Task AddDuplicatePasswordTest()
        {
            // Create a new test server with password - should disallow any operation
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Add the password
            var response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, $">{DummyPassword}");
            Assert.IsTrue(response.StartsWith("OK"));

            // Try to add the same password again
            response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, $">{DummyPassword}");
            Assert.IsTrue(response.StartsWith("OK"));

            // Ensure the test user still has only one valid password hash listed
            string[] users = await c.ExecuteForArrayAsync("ACL", "list");
            foreach (string user in users)
            {
                if (user.StartsWith($"user {TestUserA}"))
                {
                    Assert.IsTrue(user.Count(x => x == '#') == 1);
                }
            }
        }

        /// <summary>
        /// Checks that passwordless users can login with any password
        /// </summary>
        /// <returns></returns>
        [Test]
        public async Task PasswordlessUserTest()
        {
            // Create a new test server with password - should disallow any operation
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Create the test user with a password
            var response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, "on", $">{DummyPassword}", "nopass");
            Assert.IsTrue(response.StartsWith("OK"));

            // Attempt to authenticate the user with a wrong password
            response = await c.ExecuteAsync("AUTH", TestUserA, DummyPasswordB);
            Assert.IsTrue(response.StartsWith("OK"));
        }

        /// <summary>
        /// Tests that ACL op 'resetpass' resets all password
        /// </summary>
        /// <returns></returns>
        [Test]
        public async Task ResetPasswordsTest()
        {
            // Create a new test server with password - should disallow any operation
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Add the two passwords
            var response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, $">{DummyPassword}", $">{DummyPasswordB}");
            Assert.IsTrue(response.StartsWith("OK"));

            // Ensure the test user has two passwords registered
            string[] users = await c.ExecuteForArrayAsync("ACL", "list");
            foreach (string user in users)
            {
                if (user.StartsWith($"user {TestUserA}"))
                {
                    Assert.IsTrue(user.Count(x => x == '#') == 2);
                }
            }

            // Reset passwords
            response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, "resetpass");
            Assert.IsTrue(response.StartsWith("OK"));

            // Ensure the test user has no passwords registered
            users = await c.ExecuteForArrayAsync("ACL", "list");
            foreach (string user in users)
            {
                if (user.StartsWith($"user {TestUserA}"))
                {
                    Assert.IsTrue(user.Count(x => x == '#') == 0);
                }
            }
        }

        /// <summary>
        /// Ensures that categories can be added and remove from users
        /// </summary>
        [Test]
        public async Task AddAndRemoveCategoryTest()
        {
            const string TestCategory = "admin";

            // Create a new test server with password - should disallow any operation
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Add test user without the test category
            var response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, "on", $">{DummyPassword}");
            Assert.IsTrue(response.StartsWith("OK"));

            // Ensure the user IS NOT assigned the category
            string[] users = await c.ExecuteForArrayAsync("ACL", "list");
            foreach (string user in users)
            {
                if (user.StartsWith($"user {TestUserA}"))
                {
                    Assert.IsFalse(user.Contains($"+@{TestCategory}"));
                }
            }

            // Add test category to test user
            response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, $"+@{TestCategory}");
            Assert.IsTrue(response.StartsWith("OK"));

            // Ensure the user IS assigned the category
            users = await c.ExecuteForArrayAsync("ACL", "list");
            foreach (string user in users)
            {
                if (user.StartsWith($"user {TestUserA}"))
                {
                    Assert.IsTrue(user.Contains($"+@{TestCategory}"));
                }
            }

            // Remove test category to test user
            response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, $"-@{TestCategory}");
            Assert.IsTrue(response.StartsWith("OK"));

            // Ensure the user IS NOT assigned the category anymore
            users = await c.ExecuteForArrayAsync("ACL", "list");
            foreach (string user in users)
            {
                if (user.StartsWith($"user {TestUserA}"))
                {
                    Assert.IsFalse(user.Contains($"+@{TestCategory}"));
                }
            }
        }

        /// <summary>
        /// Tests the 'reset' ACL operation
        /// </summary>
        [Test]
        public async Task ResetUser()
        {
            // Create a new test server
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Add an enabled test user with a password and a category assigned
            var response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, "on", $">{DummyPassword}", "+@admin");
            Assert.IsTrue(response.StartsWith("OK"));

            // Verify the values have been set
            string[] users = await c.ExecuteForArrayAsync("ACL", "list");
            foreach (string user in users)
            {
                if (user.StartsWith($"user {TestUserA}"))
                {
                    Assert.IsTrue(user.Contains(DummyPasswordHash));
                    Assert.IsTrue(user.Contains("on"));
                    Assert.IsTrue(user.Contains("+@admin"));
                }
            }

            // Resets the user
            response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, "reset");
            Assert.IsTrue(response.StartsWith("OK"));

            // Verify the values have been reset to their original default
            users = await c.ExecuteForArrayAsync("ACL", "list");
            foreach (string user in users)
            {
                // Ensure the user user has been fully reset and disabled
                if (user.StartsWith($"user {TestUserA}"))
                {
                    Assert.AreEqual(user, $"user {TestUserA} off");
                }
            }
        }

        /// <summary>
        /// Tests that empty subcommands fail gracefully
        /// </summary>
        [Test]
        public async Task BadInputEmpty()
        {
            // Create a new test server
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Ensure missing tokens will cause an error
            try
            {
                var response = await c.ExecuteAsync("ACL", "SETUSER");
                Assert.Fail("Calling ACL setuser without any arguments should raise an error.");
            }
            catch (Exception exception)
            {
                Assert.IsTrue(exception.Message.StartsWith("ERR"));
            }
        }

        /// <summary>
        /// Tests that empty subcommands fail gracefully
        /// </summary>
        [Test]
        public async Task BadInputUnknownOperation()
        {
            // Create a new test server
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Ensure unknown ACL operations are raised
            try
            {
                var response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, "qwerty");
                Assert.Fail("Calling ACL with an invalid operation should raise an error.");
            }
            catch (Exception exception)
            {
                Assert.IsTrue(exception.Message.StartsWith("ERR Unknown operation"));
            }
        }

        /// <summary>
        /// Tests that wildcard key patterns parse
        /// </summary>
        [Test]
        public async Task KeyPatternsWildcard()
        {
            // Create a new test server
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Add a user with wildcard key permissions
            var response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, "~*");
            Assert.IsTrue(response.StartsWith("OK"));
        }
    }
}