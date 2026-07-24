// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespMemoryWriterOverflowTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableReadCache: false, enableAOF: false, lowMemory: false);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        public async Task HashAsync()
        {
            const string Key = nameof(HashAsync);
            const int NumFields = 250_000;
            const int FieldLength = 20_000;

            using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig()).ConfigureAwait(false);
            var db = redis.GetDatabase();

            var fieldValue = new string('h', FieldLength);

            var writeTasks = new Task<bool>[NumFields];

            for (var i = 0; i < NumFields; i++)
            {
                writeTasks[i] = db.HashSetAsync(Key, $"field:{i}", fieldValue);
            }

            var writeReses = await Task.WhenAll(writeTasks).ConfigureAwait(false);
            ClassicAssert.IsTrue(writeReses.All(static x => x));

            var x = await db.HashGetAllAsync(Key).ConfigureAwait(false);

            var exc = ClassicAssert.ThrowsAsync<RedisServerException>(() => db.HashGetAllAsync(Key));
            ClassicAssert.AreEqual($"ERR Exceeded maximum response size of ({Array.MaxLength:N0}) bytes", exc.Message);
        }

        [Test]
        public async Task ListAsync()
        {
            const string Key = nameof(ListAsync);
            const int NumElements = 250_000;
            const int ElementLength = 40_000;

            using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig()).ConfigureAwait(false);
            var db = redis.GetDatabase();

            var elementValue = new string('l', ElementLength);

            var writeTasks = new Task<long>[NumElements];

            for (var i = 0; i < NumElements; i++)
            {
                writeTasks[i] = db.ListLeftPushAsync(Key, elementValue);
            }

            _ = await Task.WhenAll(writeTasks).ConfigureAwait(false);

            var x = await db.ListRangeAsync(Key).ConfigureAwait(false);

            var exc = ClassicAssert.ThrowsAsync<RedisServerException>(() => db.HashGetAllAsync(Key));
            ClassicAssert.AreEqual($"ERR Exceeded maximum response size of ({Array.MaxLength:N0}) bytes", exc.Message);
        }

        [Test]
        public async Task SetAsync()
        {
            const string Key = nameof(SetAsync);
            const int NumMembers = 250_000;
            const int MemberLength = 40_000;

            using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig()).ConfigureAwait(false);
            var db = redis.GetDatabase();

            var writeTasks = new Task<bool>[NumMembers];

            for (var i = 0; i < NumMembers; i++)
            {
                var memberName = $"{i}_{new string('s', MemberLength)}";
                writeTasks[i] = db.SetAddAsync(Key, memberName);
            }

            var writeReses = await Task.WhenAll(writeTasks).ConfigureAwait(false);
            ClassicAssert.IsTrue(writeReses.All(static x => x));

            var x = await db.SetMembersAsync(Key).ConfigureAwait(false);

            var exc = ClassicAssert.ThrowsAsync<RedisServerException>(() => db.HashGetAllAsync(Key));
            ClassicAssert.AreEqual($"ERR Exceeded maximum response size of ({Array.MaxLength:N0}) bytes", exc.Message);
        }

        [Test]
        public async Task SortedSetAsync()
        {
            const string Key = nameof(SortedSetAsync);
            const int NumMembers = 250_000;
            const int MemberLength = 40_000;

            using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig()).ConfigureAwait(false);
            var db = redis.GetDatabase();

            var writeTasks = new Task<bool>[NumMembers];

            for (var i = 0; i < NumMembers; i++)
            {
                var memberName = $"{i}_{new string('z', MemberLength)}";
                writeTasks[i] = db.SortedSetAddAsync(Key, memberName, i);
            }

            var writeReses = await Task.WhenAll(writeTasks).ConfigureAwait(false);
            ClassicAssert.IsTrue(writeReses.All(static x => x));

            var x = await db.SortedSetRangeByScoreWithScoresAsync(Key).ConfigureAwait(false);

            var exc = ClassicAssert.ThrowsAsync<RedisServerException>(() => db.HashGetAllAsync(Key));
            ClassicAssert.AreEqual($"ERR Exceeded maximum response size of ({Array.MaxLength:N0}) bytes", exc.Message);
        }
    }
}