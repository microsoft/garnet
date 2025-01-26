// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.Resp.ACL
{
    /// <summary>
    /// Tests for ACL GETUSER command.
    /// </summary>
    [TestFixture]
    internal class GetUserTests : AclTest
    {
        private const string DefaultUserName = "default";

        private const string MultiCommandList = "+get +set +setex +decr +decrby +incr +incrby +del +unlink +flushdb +latency";

        private const int CommandResultArrayLength = 6;

        /*
         * Use ordinal values when retrieving command results to ensure consistency of client contract.
         * Changes requiring modifications to these ordinal values are likely breaking for clients.
        */
        private const int FlagsPropertyNameIndex = 0;

        private const int FlagsPropertyValueIndex = 1;

        private const int PasswordPropertyNameIndex = 2;

        private const int PasswordPropertyValueIndex = 3;

        private const int CommandsPropertyNameIndex = 4;

        private const int CommandsPropertyValueIndex = 5;

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

            RedisResult commandResult = await db.ExecuteAsync("ACL", "GETUSER", DefaultUserName);

            ClassicAssert.NotNull(commandResult);
            ClassicAssert.AreEqual(CommandResultArrayLength, commandResult.Length);

            ClassicAssert.AreEqual("flags", commandResult[FlagsPropertyNameIndex].ToString());
            ClassicAssert.AreEqual("on", commandResult[FlagsPropertyValueIndex][0].ToString());

            ClassicAssert.AreEqual("passwords", commandResult[PasswordPropertyNameIndex].ToString());
            ClassicAssert.AreEqual(0, commandResult[PasswordPropertyValueIndex].Length);

            ClassicAssert.AreEqual("commands", commandResult[CommandsPropertyNameIndex].ToString());
            ClassicAssert.AreEqual("+@all", commandResult[CommandsPropertyValueIndex].ToString());
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
            if(!string.IsNullOrWhiteSpace(credential) && credential != "nopass")
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

            RedisResult commandResult = await db.ExecuteAsync("ACL", "GETUSER", userName);

            ClassicAssert.NotNull(commandResult);
            ClassicAssert.AreEqual(CommandResultArrayLength, commandResult.Length);

            ClassicAssert.AreEqual("flags", commandResult[FlagsPropertyNameIndex].ToString());

            if (string.IsNullOrWhiteSpace(enabled))
            {
                enabled = "off";
            }

            ClassicAssert.AreEqual(enabled, commandResult[FlagsPropertyValueIndex][0].ToString());

            ClassicAssert.AreEqual("passwords", commandResult[PasswordPropertyNameIndex].ToString());

            bool found = ContainsDefaultPassword(commandResult);

            if (!found && !string.IsNullOrWhiteSpace(credential) && credential != "nopass")
            {
                ClassicAssert.Fail("Credential not found");
            }

            ClassicAssert.AreEqual("commands", commandResult[CommandsPropertyNameIndex].ToString());
            ClassicAssert.AreEqual(expectedCommands, commandResult[CommandsPropertyValueIndex].ToString());
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

            RedisResult commandResult = await db.ExecuteAsync("ACL", "GETUSER", TestUserA);

            ClassicAssert.NotNull(commandResult);
            ClassicAssert.AreEqual(CommandResultArrayLength, commandResult.Length);

            ClassicAssert.AreEqual("flags", commandResult[FlagsPropertyNameIndex].ToString());

            ClassicAssert.AreEqual("on", commandResult[FlagsPropertyValueIndex][0].ToString());

            ClassicAssert.AreEqual("passwords", commandResult[PasswordPropertyNameIndex].ToString());
            var found = ContainsDefaultPassword(commandResult);

            if (!found)
            {
                ClassicAssert.Fail("Credential not found");
            }

            ClassicAssert.AreEqual("commands", commandResult[CommandsPropertyNameIndex].ToString());
            ClassicAssert.AreEqual(MultiCommandList, commandResult[CommandsPropertyValueIndex].ToString());
        }

        private static bool ContainsDefaultPassword(RedisResult commandResult)
        {
            bool found = false;
            foreach (RedisResult hash in (RedisResult[])commandResult[PasswordPropertyValueIndex])
            {
                if (hash.ToString() == $"#{DummyPasswordHash}")
                {
                    found = true;
                }
            }

            return found;
        }
    }
}
