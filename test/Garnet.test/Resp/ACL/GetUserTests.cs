// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Allure.NUnit;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.Resp.ACL
{
    /// <summary>
    /// Tests for ACL GETUSER command.
    /// </summary>
    [AllureNUnit]
    [TestFixture]
    internal class GetUserTests : AclTest
    {
        private const string DefaultUserName = "default";

        private const string MultiCommandList = "+get +set +setex +decr +decrby +incr +incrby +del +unlink +flushdb +latency";

        /// <summary>
        /// Tests that ACL GETUSER shows the default user.
        /// </summary>
        [Test]
        public async Task GetUserTest()
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            UserAclResult commandResult = new UserAclResult(await db.ExecuteAsync("ACL", "GETUSER", DefaultUserName));

            ClassicAssert.NotNull(commandResult);
            ClassicAssert.IsTrue(commandResult.IsWellStructured());
            ClassicAssert.IsTrue(commandResult.IsEnabled);
            ClassicAssert.AreEqual(0, commandResult.PasswordResult.Length);
            ClassicAssert.AreEqual("+@all", commandResult.PermittedCommands);
        }

        /// <summary>
        /// Tests that ACL GETUSER returns null when user is not found.
        /// </summary>
        [Test]
        public async Task GetUserNotFoundTest()
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            ClassicAssert.IsTrue((await db.ExecuteAsync("ACL", "GETUSER", $"!{DefaultUserName}")).IsNull);
        }

        /// <summary>
        /// Tests that ACL GETUSER shows various users and their appropriate ACL information.
        /// Note: Multi-segment ACLs are consolidated, ex: -@all +get becomes +get. Matching ACL LIST behavior.
        /// </summary>
        [Test, Sequential]
        [TestCase(TestUserA, "on", DummyPassword, "+@admin", "+@admin")]
        [TestCase(TestUserA, "off", "nopass", "+get", "+get")]
        [TestCase(TestUserA, "", "", "+@all", "+@all")]
        [TestCase(TestUserA, "on", "nopass", "-@all +get", "+get")]
        public async Task GetUserAclTest(
            string userName,
            string enabled,
            string credential,
            string commands,
            string expectedCommands)
        {
            if (!string.IsNullOrWhiteSpace(credential) && credential != "nopass")
            {
                credential = $">{credential}";
            }

            StringBuilder sb = new StringBuilder($"user default on nopass +@all {Environment.NewLine}");
            sb.AppendLine($"user {userName} {enabled} {credential} {commands}");

            // Create an input with user definition (including default)
            var configurationFile = Path.Join(TestUtils.MethodTestDir, "users.acl");
            File.WriteAllText(configurationFile, sb.ToString());

            // Start up Garnet with a defined default user password
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: configurationFile, defaultPassword: DummyPassword);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(authUsername: DefaultUserName));
            var db = redis.GetDatabase(0);

            UserAclResult commandResult = new UserAclResult(await db.ExecuteAsync("ACL", "GETUSER", userName));

            ClassicAssert.NotNull(commandResult);
            ClassicAssert.IsTrue(commandResult.IsWellStructured());

            ClassicAssert.AreEqual(enabled == "on", commandResult.IsEnabled);

            bool expectPasswordMatch = !string.IsNullOrWhiteSpace(credential) && credential != "nopass";
            ClassicAssert.AreEqual(expectPasswordMatch, commandResult.ContainsPasswordHash($"#{DummyPasswordHash}"));

            ClassicAssert.AreEqual(expectedCommands, commandResult.PermittedCommands);
        }

        /// <summary>
        /// Tests that ACL GETUSER retrieves correct user and their appropriate ACL information when multiple users
        /// are present.
        /// </summary>
        [Test, Sequential]
        public async Task GetUserMultiUserTest()
        {
            StringBuilder sb = new StringBuilder($"user {DefaultUserName} on nopass +@all {Environment.NewLine}");
            sb.AppendLine($"user {TestUserA} on >{DummyPassword} {MultiCommandList} {Environment.NewLine}");
            sb.AppendLine($"user {TestUserB} on >{DummyPasswordB} +set {Environment.NewLine}");

            // Create an input with 3 user definitions (including default)
            var configurationFile = Path.Join(TestUtils.MethodTestDir, "users.acl");
            File.WriteAllText(configurationFile, sb.ToString());

            // Start up Garnet with a defined default user password
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true, aclFile: configurationFile, defaultPassword: DummyPassword);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(authUsername: DefaultUserName));
            var db = redis.GetDatabase(0);

            UserAclResult commandResult = new UserAclResult(await db.ExecuteAsync("ACL", "GETUSER", TestUserA));

            ClassicAssert.NotNull(commandResult);
            ClassicAssert.IsTrue(commandResult.IsWellStructured());
            ClassicAssert.IsTrue(commandResult.IsEnabled);
            ClassicAssert.IsTrue(commandResult.ContainsPasswordHash($"#{DummyPasswordHash}"));
            ClassicAssert.AreEqual(MultiCommandList, commandResult.PermittedCommands);
        }
    }
}