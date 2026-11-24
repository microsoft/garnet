// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// A command a script issues is checked against the slots the node serves, whichever path the script
    /// runner takes to execute it.
    /// </summary>
    [TestFixture]
    public class LuaClusterModeTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableCluster: true, enableLua: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// The node serves only the slot of the declared key <c>k</c> (7629). The scripts operate on <c>x</c>
        /// (slot 16287), which the node does not serve, so the commands they issue must be refused.
        /// </summary>
        [Test]
        [TestCase("SET", "return redis.call('SET', 'x', 'v')")]
        [TestCase("GET", "return redis.call('GET', 'x')")]
        [TestCase("HSET", "return redis.call('HSET', 'x', 'f', 'v')")]
        [TestCase("HGET", "return redis.call('HGET', 'x', 'f')")]
        [TestCase("EXPIRE", "return redis.call('EXPIRE', 'x', 60)")]
        public void ACommandOnAKeyTheNodeDoesNotServeIsRefused(string command, string script)
        {
            using var lightClient = TestUtils.CreateRequest();

            var addSlot = Encoding.ASCII.GetString(lightClient.SendCommand("CLUSTER ADDSLOTS 7629")).TrimEnd('\0');
            StringAssert.StartsWith("+OK", addSlot);

            var request = Encoding.ASCII.GetBytes($"*4\r\n$4\r\nEVAL\r\n${script.Length}\r\n{script}\r\n$1\r\n1\r\n$1\r\nk\r\n");
            var reply = Encoding.ASCII.GetString(lightClient.SendCommand(request)).TrimEnd('\0');

            StringAssert.StartsWith("-", reply, $"{command} on a key the node does not serve was executed");
            StringAssert.Contains("CLUSTERDOWN", reply);
        }

        [Test]
        public void CommandsOnAKeyTheNodeServesAreExecuted()
        {
            using var lightClient = TestUtils.CreateRequest();

            var addSlot = Encoding.ASCII.GetString(lightClient.SendCommand("CLUSTER ADDSLOTS 7629")).TrimEnd('\0');
            StringAssert.StartsWith("+OK", addSlot);

            const string script = "redis.call('SET', KEYS[1], 'v'); redis.call('HSET', '{k}h', 'f', 'w'); return redis.call('GET', KEYS[1])";
            var request = Encoding.ASCII.GetBytes($"*4\r\n$4\r\nEVAL\r\n${script.Length}\r\n{script}\r\n$1\r\n1\r\n$1\r\nk\r\n");
            var reply = Encoding.ASCII.GetString(lightClient.SendCommand(request)).TrimEnd('\0');

            Assert.That(reply, Is.EqualTo("$1\r\nv\r\n"));
        }
    }
}