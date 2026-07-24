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
        private const int BatchSize = 1_000;

        private GarnetServer server;

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
            TestUtils.OnTearDown(waitForDelete: true);
        }

        [Test]
        public async Task HashAsync()
        {
            const string Key = nameof(HashAsync);
            const int NumFields = 60_000;
            const int FieldLength = 40_000;

            using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig()).ConfigureAwait(false);
            var db = redis.GetDatabase();

            var fieldValue = new string('h', FieldLength);

            for (var i = 0; i < NumFields; i += BatchSize)
            {
                var writeTasks = new Task<bool>[BatchSize];
                writeTasks.AsSpan().Fill(Task.FromResult(true));

                for (var j = 0; j < writeTasks.Length; j++)
                {
                    writeTasks[j] = db.HashSetAsync(Key, $"field:{(i + j)}", fieldValue);
                }

                var writeReses = await Task.WhenAll(writeTasks).ConfigureAwait(false);
                ClassicAssert.IsTrue(writeReses.All(static x => x));
            }

            var exc = ClassicAssert.ThrowsAsync<RedisServerException>(() => db.HashGetAllAsync(Key));
            ClassicAssert.AreEqual($"ERR Garnet Exception: Exceeded maximum response size of ({Array.MaxLength:N0}) bytes", exc.Message);
        }

        [Test]
        public async Task ListAsync()
        {
            const string Key = nameof(ListAsync);
            const int NumElements = 60_000;
            const int ElementLength = 40_000;

            using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig()).ConfigureAwait(false);
            var db = redis.GetDatabase();

            var elementValue = new string('l', ElementLength);

            for (var i = 0; i < NumElements; i += BatchSize)
            {
                var writeTasks = new Task[BatchSize];
                writeTasks.AsSpan().Fill(Task.CompletedTask);

                for (var j = 0; j < writeTasks.Length; j++)
                {
                    writeTasks[j] = db.ListRightPushAsync(Key, elementValue);
                }

                await Task.WhenAll(writeTasks).ConfigureAwait(false);
            }

            var exc = ClassicAssert.ThrowsAsync<RedisServerException>(() => db.ListRangeAsync(Key));
            ClassicAssert.AreEqual($"ERR Garnet Exception: Exceeded maximum response size of ({Array.MaxLength:N0}) bytes", exc.Message);
        }

        [Test]
        public async Task SetAsync()
        {
            const string Key = nameof(SetAsync);
            const int NumMembers = 60_000;
            const int MemberLength = 40_000;

            using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig()).ConfigureAwait(false);
            var db = redis.GetDatabase();

            var longValue = new string('s', MemberLength);

            for (var i = 0; i < NumMembers; i += BatchSize)
            {
                var writeTasks = new Task<bool>[BatchSize];
                writeTasks.AsSpan().Fill(Task.FromResult(true));

                for (var j = 0; j < writeTasks.Length; j++)
                {
                    var memberName = $"{(i + j)}_{longValue}";
                    writeTasks[j] = db.SetAddAsync(Key, memberName);
                }

                var writeReses = await Task.WhenAll(writeTasks).ConfigureAwait(false);
                ClassicAssert.IsTrue(writeReses.All(static x => x));
            }

            var exc = ClassicAssert.ThrowsAsync<RedisServerException>(() => db.SetMembersAsync(Key));
            ClassicAssert.AreEqual($"ERR Garnet Exception: Exceeded maximum response size of ({Array.MaxLength:N0}) bytes", exc.Message);
        }

        [Test]
        public async Task SortedSetAsync()
        {
            const string Key = nameof(SortedSetAsync);
            const int NumMembers = 60_000;
            const int MemberLength = 40_000;

            using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig()).ConfigureAwait(false);
            var db = redis.GetDatabase();

            var longValue = new string('z', MemberLength);

            for (var i = 0; i < NumMembers; i += BatchSize)
            {
                var writeTasks = new Task<bool>[BatchSize];
                writeTasks.AsSpan().Fill(Task.FromResult(true));

                for (var j = 0; j < writeTasks.Length; j++)
                {
                    var memberName = $"{(i + j)}_{longValue}";
                    writeTasks[j] = db.SortedSetAddAsync(Key, memberName, (i + j));
                }

                var writeReses = await Task.WhenAll(writeTasks).ConfigureAwait(false);
                ClassicAssert.IsTrue(writeReses.All(static x => x));
            }

            var exc = ClassicAssert.ThrowsAsync<RedisServerException>(() => db.SortedSetRangeByScoreWithScoresAsync(Key));
            ClassicAssert.AreEqual($"ERR Garnet Exception: Exceeded maximum response size of ({Array.MaxLength:N0}) bytes", exc.Message);
        }
    }
}