// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture]
    public class RenameTests : TestBase
    {
        private ClusterTestContext context;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown()
        {
            context.TearDown();
        }

        [Test]
        public Task RenameStringsAsync()
        {
            const string StringValue = nameof(RenameStringsAsync) + "_Value";
            return RunCommonRenameTestAsync(
                static async (primary, originalKey) => ClassicAssert.IsTrue(await primary.StringSetAsync(originalKey, StringValue).ConfigureAwait(false)),
                static async (replica, targetKey) => ClassicAssert.AreEqual(StringValue, (string)await replica.ExecuteAsync(database: 0, "GET", [targetKey]).ConfigureAwait(false))
            );
        }

        [Test]
        public Task RenameObjectsAsync()
        {
            const string ListValue = nameof(RenameObjectsAsync) + "_Value";
            return RunCommonRenameTestAsync(
                static async (primary, originalKey) => ClassicAssert.AreEqual(1, await primary.ListLeftPushAsync(originalKey, ListValue).ConfigureAwait(false)),
                static async (replica, targetKey) =>
                {
                    ClassicAssert.AreEqual(1, (int)await replica.ExecuteAsync(database: 0, "LLEN", [targetKey]).ConfigureAwait(false));
                    ClassicAssert.AreEqual(ListValue, (string)await replica.ExecuteAsync(database: 0, "LINDEX", [targetKey, "0"]).ConfigureAwait(false));
                }
            );
        }

        // Needs fixes from: https://github.com/microsoft/garnet/pull/2236 due to transaction dependency
        // TODO: Restore and fix once ^ landed
        //[Test]
        //public Task RenameVectorSetAsync()
        //{
        //    const string ElementName = nameof(RenameVectorSetAsync) + "_ElementName";
        //    const float ElementValue = 123.4f;
        //    return RunCommonRenameTestAsync(
        //        static async (primary, originalKey) => ClassicAssert.IsTrue(await primary.VectorSetAddAsync(originalKey, VectorSetAddRequest.Member(ElementName, new[] { ElementValue })).ConfigureAwait(false)),
        //        static async (replica, targetKey) =>
        //        {
        //            ClassicAssert.AreEqual(1, (int)await replica.ExecuteAsync(database: 0, "VCARD", [targetKey]).ConfigureAwait(false));
        //            ClassicAssert.AreEqual(ElementValue, (float)((double[])await replica.ExecuteAsync(database: 0, "VEMB", [targetKey, ElementValue]).ConfigureAwait(false)).Single());
        //        }
        //    );
        //}

        private async Task RunCommonRenameTestAsync(
            Func<IDatabase, string, Task> createOriginalValue,
            Func<IServer, string, Task> validateRenamedValue,
            [CallerMemberName] string caller = null
        )
        {
            const int PrimaryIndex = 0;
            const int ReplicaIndex = 1;

            var rootKey = caller + "{sameslot}";
            var originalKey = rootKey + "_Original";
            var targetKey = rootKey + "_Target";
            var blockingKey = rootKey + "_Blocking";

            var blockingValue = caller + "_BlockingValue";

            await SimpleSetupClusterAsync().ConfigureAwait(false);

            await using var redis = await ConnectionMultiplexer.ConnectAsync(context.clusterTestUtils.GetRedisConfig(context.endpoints)).ConfigureAwait(false);
            var db = redis.GetDatabase();

            ClassicAssert.IsTrue(await db.StringSetAsync(blockingKey, blockingValue).ConfigureAwait(false));

            var replica = redis.GetServer(context.endpoints[ReplicaIndex]);
            ClassicAssert.AreEqual("OK", (string)await replica.ExecuteAsync("READONLY").ConfigureAwait(false));

            // RENAME replicates
            {
                await createOriginalValue(db, originalKey).ConfigureAwait(false);

                ClassicAssert.AreEqual("OK", (string)await db.ExecuteAsync("RENAME", [originalKey, targetKey]).ConfigureAwait(false));
                ClassicAssert.IsNull(await db.KeyTimeToLiveAsync(targetKey).ConfigureAwait(false));

                context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex);

                ClassicAssert.IsFalse((bool)await replica.ExecuteAsync(database: 0, "EXISTS", [originalKey]).ConfigureAwait(false));
                ClassicAssert.IsTrue((bool)await replica.ExecuteAsync(database: 0, "EXISTS", [targetKey]).ConfigureAwait(false));
                await validateRenamedValue(replica, targetKey).ConfigureAwait(false);
                ClassicAssert.AreEqual(-1, (int)await replica.ExecuteAsync(database: 0, "TTL", [targetKey]).ConfigureAwait(false));

                ClassicAssert.IsTrue(await db.KeyDeleteAsync(targetKey).ConfigureAwait(false));
            }

            // RENAME with TTL replicates
            {
                await createOriginalValue(db, originalKey).ConfigureAwait(false);
                ClassicAssert.True(await db.KeyExpireAsync(originalKey, TimeSpan.FromSeconds(60)).ConfigureAwait(false));

                ClassicAssert.AreEqual("OK", (string)await db.ExecuteAsync("RENAME", [originalKey, targetKey]).ConfigureAwait(false));
                ClassicAssert.IsNotNull(await db.KeyTimeToLiveAsync(targetKey).ConfigureAwait(false));

                context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex);

                ClassicAssert.IsFalse((bool)await replica.ExecuteAsync(database: 0, "EXISTS", [originalKey]).ConfigureAwait(false));
                ClassicAssert.IsTrue((bool)await replica.ExecuteAsync(database: 0, "EXISTS", [targetKey]).ConfigureAwait(false));
                await validateRenamedValue(replica, targetKey).ConfigureAwait(false);
                ClassicAssert.IsTrue((int)await replica.ExecuteAsync(database: 0, "TTL", [targetKey]).ConfigureAwait(false) is > 0 and <= 60);

                ClassicAssert.IsTrue(await db.KeyDeleteAsync(targetKey).ConfigureAwait(false));
            }

            // RENAMENX replicates
            {
                await createOriginalValue(db, originalKey).ConfigureAwait(false);

                ClassicAssert.AreEqual(0, (int)await db.ExecuteAsync("RENAMENX", [originalKey, blockingKey]).ConfigureAwait(false));

                ClassicAssert.AreEqual(1, (int)await db.ExecuteAsync("RENAMENX", [originalKey, targetKey]).ConfigureAwait(false));
                ClassicAssert.IsNull(await db.KeyTimeToLiveAsync(targetKey).ConfigureAwait(false));

                context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex);

                ClassicAssert.IsFalse((bool)await replica.ExecuteAsync(database: 0, "EXISTS", [originalKey]).ConfigureAwait(false));
                ClassicAssert.IsTrue((bool)await replica.ExecuteAsync(database: 0, "EXISTS", [targetKey]).ConfigureAwait(false));
                ClassicAssert.IsTrue((bool)await replica.ExecuteAsync(database: 0, "EXISTS", [blockingKey]).ConfigureAwait(false));
                await validateRenamedValue(replica, targetKey).ConfigureAwait(false);
                ClassicAssert.AreEqual(blockingValue, (string)await replica.ExecuteAsync(database: 0, "GET", [blockingKey]).ConfigureAwait(false));
                ClassicAssert.AreEqual(-1, (int)await replica.ExecuteAsync(database: 0, "TTL", [targetKey]).ConfigureAwait(false));

                ClassicAssert.IsTrue(await db.KeyDeleteAsync(targetKey).ConfigureAwait(false));
            }

            // RENAMENX with TTL replicates
            {
                await createOriginalValue(db, originalKey).ConfigureAwait(false);
                ClassicAssert.True(await db.KeyExpireAsync(originalKey, TimeSpan.FromSeconds(60)).ConfigureAwait(false));

                ClassicAssert.AreEqual(0, (int)await db.ExecuteAsync("RENAMENX", [originalKey, blockingKey]).ConfigureAwait(false));

                ClassicAssert.AreEqual(1, (int)await db.ExecuteAsync("RENAMENX", [originalKey, targetKey]).ConfigureAwait(false));
                ClassicAssert.IsNotNull(await db.KeyTimeToLiveAsync(targetKey).ConfigureAwait(false));

                context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex);

                ClassicAssert.IsFalse((bool)await replica.ExecuteAsync(database: 0, "EXISTS", [originalKey]).ConfigureAwait(false));
                ClassicAssert.IsTrue((bool)await replica.ExecuteAsync(database: 0, "EXISTS", [targetKey]).ConfigureAwait(false));
                ClassicAssert.IsTrue((bool)await replica.ExecuteAsync(database: 0, "EXISTS", [blockingKey]).ConfigureAwait(false));
                await validateRenamedValue(replica, targetKey).ConfigureAwait(false);
                ClassicAssert.AreEqual(blockingValue, (string)await replica.ExecuteAsync(database: 0, "GET", [blockingKey]).ConfigureAwait(false));
                ClassicAssert.IsTrue((int)await replica.ExecuteAsync(database: 0, "TTL", [targetKey]).ConfigureAwait(false) is > 0 and <= 60);

                ClassicAssert.IsTrue(await db.KeyDeleteAsync(targetKey).ConfigureAwait(false));
            }
        }

        private async Task SimpleSetupClusterAsync()
        {
            const int InstanceCount = 2;

            context.CreateInstances(InstanceCount, useTLS: true, enableAOF: true);
            context.CreateConnection(useTLS: true);
            _ = await context.clusterTestUtils.SimpleSetupClusterAsync(primary_count: InstanceCount / 2, replica_count: 1).ConfigureAwait(false);
        }
    }
}