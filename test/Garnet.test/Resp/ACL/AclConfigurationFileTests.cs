// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.server.ACL;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.Resp.ACL
{
    /// <summary>
    /// Tests for ACL Configuration file related operations.
    /// </summary>
    [AllureNUnit]
    [TestFixture]
    class AclConfigurationFileTests : AclTest
    {
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

            string[] users = await c.ExecuteForArrayAsync("ACL", "USERS");
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
            string[] users = await c.ExecuteForArrayAsync("ACL", "USERS");
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
            string[] users = await c.ExecuteForArrayAsync("ACL", "USERS");
            ClassicAssert.IsTrue(3 == users.Length);
            ClassicAssert.Contains("default", users);
            ClassicAssert.Contains("testA", users);
            ClassicAssert.Contains("testB", users);

            // Ensure that the default password used to create Garnet was ignored
            users = await c.ExecuteForArrayAsync("ACL", "LIST");
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
            string[] users = await c.ExecuteForArrayAsync("ACL", "USERS");
            ClassicAssert.IsTrue(4 == users.Length);
            ClassicAssert.Contains("default", users);
            ClassicAssert.Contains("testA", users);
            ClassicAssert.Contains("testB", users);
            ClassicAssert.Contains("testC", users);

            // Check that (1) testB contains two passwords and (2) default user has no password
            users = await c.ExecuteForArrayAsync("ACL", "LIST");
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
            c.Execute("ACL", "LOAD");

            // Check the integrity of the list
            users = await c.ExecuteForArrayAsync("ACL", "USERS");

            // Verify the ACL now contains only three users
            ClassicAssert.IsTrue(3 == users.Length);
            ClassicAssert.Contains("default", users);
            ClassicAssert.Contains("testD", users);
            ClassicAssert.Contains("testB", users);

            // Ensure that (1) one password was removed from testB and (2) defaut password was set
            users = await c.ExecuteForArrayAsync("ACL", "LIST");
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

            // Ensure Garnet started up 2 users: testA and default
            string[] users = await c.ExecuteForArrayAsync("ACL", "USERS");
            ClassicAssert.IsTrue(2 == users.Length);
            ClassicAssert.Contains("default", users);
            ClassicAssert.Contains("testA", users);

            // Update and reload the configuration file
            // Ensure the command fails and that the user list has not changed
            File.WriteAllText(configurationFile, modifiedConfigurationFile);

            try
            {
                var response = await c.ExecuteAsync("ACL", "LOAD");
                Assert.Fail("Loading a malformed ACL file should result in an error.");
            }
            catch (Exception exception)
            {
                ClassicAssert.IsTrue(exception.Message.StartsWith("ERR"));
            }

            users = await c.ExecuteForArrayAsync("ACL", "USERS");

            // Check that we still only know testA and default
            ClassicAssert.IsTrue(2 == users.Length);
            ClassicAssert.Contains("default", users);
            ClassicAssert.Contains("testA", users);

            // Ensure that testA does not contain the dummy password
            users = await c.ExecuteForArrayAsync("ACL", "LIST");
            foreach (var user in users)
            {
                if (user.StartsWith("user testA"))
                {
                    ClassicAssert.AreEqual(1, user.Count(x => x == '#'));
                    ClassicAssert.IsFalse(user.Contains(DummyPasswordHash));
                }
            }
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
            string[] users = await c.ExecuteForArrayAsync("ACL", "USERS");
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

            // Test None rejected
            {
                var configurationFile = Path.Join(TestUtils.MethodTestDir, "users3.acl");
                File.WriteAllText(configurationFile, "user test on >password123 +none");

                // Ensure Garnet starts up and just ignores the malformed statement
                Assert.Throws<ACLException>(() => TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: configurationFile));
            }


            // Test Invalid rejected
            {
                var configurationFile = Path.Join(TestUtils.MethodTestDir, "users4.acl");
                File.WriteAllText(configurationFile, "user test on >password123 +invalid");

                // Ensure Garnet starts up and just ignores the malformed statement
                Assert.Throws<ACLException>(() => TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: configurationFile));
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

            // Retrieve users after save and ensure user does exist
            users = (string[])db.Execute("ACL", "USERS");
            ClassicAssert.IsTrue(users.ToHashSet().Contains(updateUser));
        }
    }
}