// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class ClusterETagTests : TestBase
    {
        private ClusterTestContext context;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown() => context?.TearDown();

        [TestCase("a")]
        [TestCase("b")]
        [Category("CLUSTER")]
        public void StableSlotCommandsExecuteOnlyOnOwner(string key)
        {
            CreateCluster(2);
            int slot = ClusterTestUtils.HashSlot(Encoding.ASCII.GetBytes(key));
            int owner = slot < 8192 ? 0 : 1;
            int other = 1 - owner;
            string redirect = $"MOVED {slot} {context.clusterTestUtils.GetEndPoint(owner)}";

            Assert.That(Execute(owner, "GETWITHETAG", key).IsNull, Is.True);
            AssertRedirect(other, "GETWITHETAG", [key], redirect);
            AssertRedirect(other, "SETWITHETAG", [key, "rejected"], redirect);
            AssertRedirect(other, "SETIFMATCH", [key, "rejected", 0], redirect);
            Assert.That(Execute(owner, "GETWITHETAG", key).IsNull, Is.True);

            AssertInteger(Execute(owner, "SETWITHETAG", key, "first"), 1);
            AssertPair(Execute(owner, "GETWITHETAG", key), 1, "first");
            AssertRedirect(other, "GETWITHETAG", [key], redirect);
            AssertRedirect(other, "SETWITHETAG", [key, "rejected"], redirect);
            AssertRedirect(other, "SETIFMATCH", [key, "rejected", 1], redirect);
            AssertPair(Execute(owner, "GETWITHETAG", key), 1, "first");

            AssertPair(Execute(owner, "SETIFMATCH", key, "second", 1), 2, null);
            AssertPair(Execute(owner, "GETWITHETAG", key), 2, "second");
            AssertInteger(Execute(owner, "SETWITHETAG", key, "third"), 3);
            AssertPair(Execute(owner, "GETWITHETAG", key), 3, "third");
        }

        [TestCase("GETWITHETAG")]
        [TestCase("SETWITHETAG")]
        [TestCase("SETIFMATCH")]
        [Category("CLUSTER")]
        public async Task MigratingSlotsServeExistingKeysAndRequireAskingOnImporter(string command)
        {
            CreateCluster(2);
            const string ExistingKey = "{b}existing";
            const string MissingKey = "{b}missing";
            int slot = ClusterTestUtils.HashSlot(Encoding.ASCII.GetBytes(ExistingKey));
            Assert.That(slot, Is.LessThan(8192));
            Assert.That(ClusterTestUtils.HashSlot(Encoding.ASCII.GetBytes(MissingKey)), Is.EqualTo(slot));
            AssertInteger(Execute(0, "SETWITHETAG", ExistingKey, "original"), 1);

            string sourceId = context.clusterTestUtils.GetNodeIdFromNode(0, context.logger);
            string targetId = context.clusterTestUtils.GetNodeIdFromNode(1, context.logger);
            Assert.That(context.clusterTestUtils.SetSlot(1, slot, "IMPORTING", sourceId), Is.EqualTo("OK"));
            Assert.That(context.clusterTestUtils.SetSlot(0, slot, "MIGRATING", targetId), Is.EqualTo("OK"));

            object[] existingArguments = command switch
            {
                "GETWITHETAG" => [ExistingKey],
                "SETWITHETAG" => [ExistingKey, "updated"],
                _ => [ExistingKey, "updated", 1]
            };
            RedisResult existingResult = Execute(0, command, existingArguments);
            if (command == "GETWITHETAG")
                AssertPair(existingResult, 1, "original");
            else if (command == "SETWITHETAG")
                AssertInteger(existingResult, 2);
            else
                AssertPair(existingResult, 2, null);

            object[] missingArguments = command switch
            {
                "GETWITHETAG" => [MissingKey],
                "SETWITHETAG" => [MissingKey, "created"],
                _ => [MissingKey, "created", 0]
            };
            string ask = $"ASK {slot} {context.clusterTestUtils.GetEndPoint(1)}";
            string moved = $"MOVED {slot} {context.clusterTestUtils.GetEndPoint(0)}";
            AssertRedirect(0, command, missingArguments, ask);
            AssertRedirect(1, command, missingArguments, moved);

            using GarnetClientSession importer = context.clusterTestUtils.CreateGarnetClientSession(1);
            importer.Connect();
            if (command == "GETWITHETAG")
            {
                Assert.That(await importer.ExecuteAsync("ASKING").WaitAsync(context.cts.Token), Is.EqualTo("OK"));
                Assert.That(await importer.ExecuteAsync("GETWITHETAG", MissingKey).WaitAsync(context.cts.Token), Is.Null);
                Assert.That(await importer.ExecuteAsync("ASKING").WaitAsync(context.cts.Token), Is.EqualTo("OK"));
                Assert.That(await importer.ExecuteAsync("SETWITHETAG", MissingKey, "created").WaitAsync(context.cts.Token), Is.EqualTo("1"));
            }

            Assert.That(await importer.ExecuteAsync("ASKING").WaitAsync(context.cts.Token), Is.EqualTo("OK"));
            // ASKING belongs to this connection and permits only its next command.
            AssertRedirect(1, command, missingArguments, moved);
            if (command == "GETWITHETAG")
                Assert.That(await importer.ExecuteForArrayAsync(command, MissingKey).WaitAsync(context.cts.Token), Is.EqualTo(["1", "created"]));
            else if (command == "SETWITHETAG")
                Assert.That(await importer.ExecuteAsync(command, MissingKey, "created").WaitAsync(context.cts.Token), Is.EqualTo("1"));
            else
                Assert.That(await importer.ExecuteForArrayAsync(command, MissingKey, "created", "0").WaitAsync(context.cts.Token), Is.EqualTo(["1", null]));

            string[] request = command switch
            {
                "GETWITHETAG" => [command, MissingKey],
                "SETWITHETAG" => [command, MissingKey, "rejected"],
                _ => [command, MissingKey, "rejected", "1"]
            };
            Exception exception = Assert.ThrowsAsync<Exception>(async () =>
                await importer.ExecuteAsync(request).WaitAsync(context.cts.Token));
            Assert.That(exception.Message, Is.EqualTo(moved));
            AssertRedirect(0, command, missingArguments, ask);
            AssertPair(Execute(0, "GETWITHETAG", ExistingKey), command == "GETWITHETAG" ? 1 : 2,
                command == "GETWITHETAG" ? "original" : "updated");
        }

        [Test]
        [Category("CLUSTER")]
        public void ConditionalResponsesDistinguishSuccessMismatchAndNoGet()
        {
            CreateCluster();
            const string Key = "etag-responses";

            AssertPair(Execute(0, "SETIFMATCH", "etag-missing", "created", 91), 92, null);
            AssertPair(Execute(0, "GETWITHETAG", "etag-missing"), 92, "created");
            AssertPair(Execute(0, "SETIFMATCH", Key, "first", 0), 1, null);
            AssertPair(Execute(0, "SETIFMATCH", Key, "rejected", 0), 1, "first");
            AssertPair(Execute(0, "GETWITHETAG", Key), 1, "first");
            AssertPair(Execute(0, "SETIFMATCH", Key, "second", 1), 2, null);
            AssertPair(Execute(0, "SETIFMATCH", Key, "rejected", 1, "NOGET"), 2, null);
            AssertPair(Execute(0, "GETWITHETAG", Key), 2, "second");
            AssertPair(Execute(0, "SETIFMATCH", Key, "third", 2, "NOGET"), 3, null);
            AssertPair(Execute(0, "GETWITHETAG", Key), 3, "third");
        }

        [Test]
        [Category("CLUSTER")]
        public async Task ReplicaReadsRequireReadOnlyAndWritesAlwaysRedirect()
        {
            CreateCluster(1, 1);
            const string Key = "etag-replica";
            AssertInteger(Execute(0, "SETWITHETAG", Key, "first"), 1);
            AssertPair(Execute(0, "SETIFMATCH", Key, "second", 1), 2, null);
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger, context.cts.Token);

            using GarnetClientSession replica = context.clusterTestUtils.CreateGarnetClientSession(1);
            replica.Connect();
            int slot = ClusterTestUtils.HashSlot(Encoding.ASCII.GetBytes(Key));
            string redirect = $"MOVED {slot} {context.clusterTestUtils.GetEndPoint(0)}";

            AssertReplicaRedirect("GETWITHETAG", Key);
            AssertReplicaRedirect("SETWITHETAG", Key, "rejected");
            AssertReplicaRedirect("SETIFMATCH", Key, "rejected", "2");

            Assert.That(await replica.ExecuteAsync("READONLY"), Is.EqualTo("OK"));
            Assert.That(await replica.ExecuteForArrayAsync("GETWITHETAG", Key), Is.EqualTo(["2", "second"]));
            AssertReplicaRedirect("SETWITHETAG", Key, "rejected");
            AssertReplicaRedirect("SETIFMATCH", Key, "rejected", "2");
            Assert.That(await replica.ExecuteForArrayAsync("GETWITHETAG", Key), Is.EqualTo(["2", "second"]));
            AssertPair(Execute(0, "GETWITHETAG", Key), 2, "second");

            Assert.That(await replica.ExecuteAsync("READWRITE"), Is.EqualTo("OK"));
            AssertReplicaRedirect("GETWITHETAG", Key);

            void AssertReplicaRedirect(params string[] command)
            {
                Exception exception = Assert.ThrowsAsync<Exception>(async () =>
                    await replica.ExecuteAsync(command).WaitAsync(context.cts.Token));
                Assert.That(exception.Message, Is.EqualTo(redirect));
            }
        }

        [Test]
        [Category("CLUSTER")]
        public void SynchronizedFailoverPreservesEtagAndConditionalUpdates()
        {
            CreateCluster(1, 1);
            const string Key = "etag-failover";
            AssertInteger(Execute(0, "SETWITHETAG", Key, "first", "EX", 300), 1);
            AssertPair(Execute(0, "SETIFMATCH", Key, "second", 1), 2, null);
            AssertPair(Execute(0, "SETIFMATCH", Key, "before-failover", 2), 3, null);
            long expiration = (long)Execute(0, "PEXPIRETIME", Key);
            Assert.That(expiration, Is.GreaterThan(0));
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger, context.cts.Token);
            AssertPair(Execute(1, "GETWITHETAG", Key), 3, "before-failover");

            Assert.That(context.clusterTestUtils.ClusterFailover(1, logger: context.logger), Is.EqualTo("OK"));
            context.clusterTestUtils.WaitForNoFailover(1, context.logger);
            context.clusterTestUtils.WaitForFailoverCompleted(1, context.logger);
            context.clusterTestUtils.WaitForReplicaRecovery(0, context.logger);
            context.clusterTestUtils.WaitForPrimaryRole(1, context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(1, 0, context.logger, context.cts.Token);
            string newPrimaryId = context.clusterTestUtils.GetNodeIdFromNode(1, context.logger);
            context.clusterTestUtils.WaitForSlotOwnership(0, newPrimaryId, [0, 16383], context.logger);

            AssertPair(Execute(1, "GETWITHETAG", Key), 3, "before-failover");
            Assert.That((long)Execute(1, "PEXPIRETIME", Key), Is.EqualTo(expiration));
            AssertPair(Execute(1, "SETIFMATCH", Key, "stale", 2), 3, "before-failover");
            AssertPair(Execute(1, "SETIFMATCH", Key, "stale", 2, "NOGET"), 3, null);
            AssertPair(Execute(1, "GETWITHETAG", Key), 3, "before-failover");
            AssertPair(Execute(1, "SETIFMATCH", Key, "after-failover", 3), 4, null);
            AssertPair(Execute(1, "GETWITHETAG", Key), 4, "after-failover");
            Assert.That((long)Execute(1, "PEXPIRETIME", Key), Is.EqualTo(expiration));
            AssertInteger(Execute(1, "SETWITHETAG", Key, "unconditional"), 5);
            AssertPair(Execute(1, "SETIFMATCH", Key, "final", 5, "NOGET"), 6, null);
            AssertPair(Execute(1, "GETWITHETAG", Key), 6, "final");
            AssertInteger(Execute(1, "PTTL", Key), -1);

            int slot = ClusterTestUtils.HashSlot(Encoding.ASCII.GetBytes(Key));
            string redirect = $"MOVED {slot} {context.clusterTestUtils.GetEndPoint(1)}";
            AssertRedirect(0, "SETWITHETAG", [Key, "rejected"], redirect);
            AssertRedirect(0, "SETIFMATCH", [Key, "rejected", 6], redirect);
            context.clusterTestUtils.WaitForReplicaAofSync(1, 0, context.logger, context.cts.Token);
            Assert.That(Execute(0, "READONLY").ToString(), Is.EqualTo("OK"));
            AssertPair(Execute(0, "GETWITHETAG", Key), 6, "final");
            AssertInteger(Execute(0, "PTTL", Key), -1);
        }

        [Test]
        [Category("CLUSTER")]
        public async Task ConcurrentConditionalUpdatesHaveExactlyOneWinner()
        {
            CreateCluster();
            using GarnetClientSession first = context.clusterTestUtils.CreateGarnetClientSession(0);
            using GarnetClientSession second = context.clusterTestUtils.CreateGarnetClientSession(0);
            first.Connect();
            second.Connect();

            for (int iteration = 0; iteration < 32; iteration++)
            {
                string key = $"etag-race-{iteration}";
                AssertInteger(Execute(0, "SETWITHETAG", key, "initial"), 1);
                using Barrier start = new(2);

                Task<string[]> firstUpdate = Update(first, "first");
                Task<string[]> secondUpdate = Update(second, "second");
                string[][] responses = await Task.WhenAll(firstUpdate, secondUpdate).WaitAsync(context.cts.Token);

                Assert.That(responses[0], Has.Length.EqualTo(2));
                Assert.That(responses[1], Has.Length.EqualTo(2));
                Assert.That(responses[0][0], Is.EqualTo("2"));
                Assert.That(responses[1][0], Is.EqualTo("2"));
                int winner = responses[0][1] == null ? 0 : 1;
                int loser = 1 - winner;
                string winningValue = winner == 0 ? "first" : "second";
                Assert.That(responses[winner][1], Is.Null);
                Assert.That(responses[loser][1], Is.EqualTo(winningValue));
                AssertPair(Execute(0, "GETWITHETAG", key), 2, winningValue);

                Task<string[]> Update(GarnetClientSession client, string value) => Task.Run(async () =>
                {
                    start.SignalAndWait(context.cts.Token);
                    return await client.ExecuteForArrayAsync("SETIFMATCH", key, value, "1");
                });
            }
        }

        [TestCase("EX", 60)]
        [TestCase("PX", 60000)]
        [Category("CLUSTER")]
        public void ConditionalUpdatesPreserveExpiryAndUnconditionalUpdatesClearIt(string expiryOption, int expiry)
        {
            CreateCluster();
            const string Key = "etag-expiry";
            AssertInteger(Execute(0, "SETWITHETAG", Key, "first", expiryOption, expiry), 1);
            long originalExpiry = (long)Execute(0, "PEXPIRETIME", Key);
            Assert.That(originalExpiry, Is.GreaterThan(0));

            AssertPair(Execute(0, "SETIFMATCH", Key, "rejected", 0, "PX", 120000), 1, "first");
            Assert.That((long)Execute(0, "PEXPIRETIME", Key), Is.EqualTo(originalExpiry));
            AssertPair(Execute(0, "SETIFMATCH", Key, "second", 1), 2, null);
            Assert.That((long)Execute(0, "PEXPIRETIME", Key), Is.EqualTo(originalExpiry));

            AssertPair(Execute(0, "SETIFMATCH", Key, "third", 2, expiryOption, expiry * 2), 3, null);
            Assert.That((long)Execute(0, "PEXPIRETIME", Key), Is.GreaterThanOrEqualTo(originalExpiry + 60000));
            AssertInteger(Execute(0, "SETWITHETAG", Key, "fourth"), 4);
            AssertInteger(Execute(0, "PTTL", Key), -1);
            AssertPair(Execute(0, "GETWITHETAG", Key), 4, "fourth");
        }

        [TestCase("SETWITHETAG", "EX", 1)]
        [TestCase("SETWITHETAG", "PX", 50)]
        [TestCase("SETIFMATCH", "EX", 1)]
        [TestCase("SETIFMATCH", "PX", 50)]
        [Category("CLUSTER")]
        public void ExpiredKeysAreMissingAndRecreationUsesCommandEtag(string command, string expiryOption, int expiry)
        {
            CreateCluster();
            const string Key = "etag-expired";
            AssertInteger(Execute(0, "SETWITHETAG", Key, "first"), 1);
            if (command == "SETWITHETAG")
                AssertInteger(Execute(0, command, Key, "expires", expiryOption, expiry), 2);
            else
                AssertPair(Execute(0, command, Key, "expires", 1, expiryOption, expiry), 2, null);

            Assert.That(() => Execute(0, "GETWITHETAG", Key).IsNull, Is.True.After(5000, 25));
            AssertPair(Execute(0, "SETIFMATCH", Key, "recreated", 2), 3, null);
            AssertPair(Execute(0, "GETWITHETAG", Key), 3, "recreated");
            AssertInteger(Execute(0, "DEL", Key), 1);
            AssertInteger(Execute(0, "SETWITHETAG", Key, "reset"), 1);
            AssertPair(Execute(0, "GETWITHETAG", Key), 1, "reset");
        }

        [Test]
        [Category("CLUSTER")]
        public void DeletedKeysUseCommandEtagOnRecreation()
        {
            CreateCluster();
            const string Key = "etag-deleted";
            AssertInteger(Execute(0, "SETWITHETAG", Key, "first"), 1);
            AssertInteger(Execute(0, "SETWITHETAG", Key, "second"), 2);
            AssertInteger(Execute(0, "DEL", Key), 1);
            Assert.That(Execute(0, "GETWITHETAG", Key).IsNull, Is.True);
            AssertPair(Execute(0, "SETIFMATCH", Key, "recreated", 2), 3, null);
            AssertPair(Execute(0, "GETWITHETAG", Key), 3, "recreated");
            AssertInteger(Execute(0, "DEL", Key), 1);
            AssertInteger(Execute(0, "SETWITHETAG", Key, "reset"), 1);
            AssertPair(Execute(0, "GETWITHETAG", Key), 1, "reset");
        }

        private void CreateCluster(int primaryCount = 1, int replicaCount = 0)
        {
            context.CreateInstances(primaryCount * (replicaCount + 1), enableAOF: true);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(primaryCount, replicaCount, logger: context.logger);
        }

        private RedisResult Execute(int nodeIndex, string command, params object[] arguments)
            => context.clusterTestUtils.GetServer(nodeIndex).Execute(0, command, arguments, CommandFlags.NoRedirect);

        private void AssertRedirect(int nodeIndex, string command, object[] arguments, string redirect)
        {
            using GarnetClientSession client = context.clusterTestUtils.CreateGarnetClientSession(nodeIndex);
            client.Connect();
            string[] request = new string[arguments.Length + 1];
            request[0] = command;
            for (int index = 0; index < arguments.Length; index++)
                request[index + 1] = Convert.ToString(arguments[index], CultureInfo.InvariantCulture);
            Exception exception = Assert.ThrowsAsync<Exception>(async () =>
                await client.ExecuteAsync(request).WaitAsync(context.cts.Token));
            Assert.That(exception.Message, Is.EqualTo(redirect));
        }

        private static void AssertInteger(RedisResult result, long expected)
        {
            Assert.That(result.Resp2Type, Is.EqualTo(ResultType.Integer));
            Assert.That((long)result, Is.EqualTo(expected));
        }

        private static void AssertPair(RedisResult result, long etag, string value)
        {
            Assert.That(result.Resp2Type, Is.EqualTo(ResultType.Array));
            RedisResult[] pair = (RedisResult[])result;
            Assert.That(pair, Has.Length.EqualTo(2));
            AssertInteger(pair[0], etag);
            if (value == null)
                Assert.That(pair[1].IsNull, Is.True);
            else
            {
                Assert.That(pair[1].Resp2Type, Is.EqualTo(ResultType.BulkString));
                Assert.That(pair[1].ToString(), Is.EqualTo(value));
            }
        }
    }
}