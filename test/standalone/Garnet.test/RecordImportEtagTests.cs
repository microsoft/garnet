// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.server;
using NUnit.Framework;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    [TestFixture]
    public class RecordImportEtagTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
        }

        unsafe void ImportRecord(string source, string destination)
        {
            StoreWrapper storeWrapper = server.Provider.StoreWrapper;
            FunctionsState state = storeWrapper.CreateFunctionsState();
            using var session = storeWrapper.store.NewSession<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions>(new UnifiedSessionFunctions(state));
            var context = session.BasicContext;
            UnifiedInput readInput = new(RespCommand.RENAME);
            UnifiedOutput output = new();
            try
            {
                Status readStatus = context.Read((FixedSpanByteKey)Encoding.UTF8.GetBytes(source), ref readInput, ref output);
                Assert.That(readStatus.Found, Is.True);
                fixed (byte* recordPtr = output.SpanByteAndMemory.ReadOnlySpan)
                {
                    LogRecord record = new(recordPtr, state.transientObjectIdMap);
                    UnifiedInput input = new();
                    Status status = context.Upsert((FixedSpanByteKey)Encoding.UTF8.GetBytes(destination), ref input, in record);
                    Assert.That(status.IsCompletedSuccessfully, Is.True);
                }
            }
            finally
            {
                output.Dispose();
            }
        }

        static void AssertTaggedValue(IDatabase database, string key, long tag, string value)
        {
            RedisResult[] result = (RedisResult[])database.Execute("GETWITHETAG", key);
            Assert.That((long)result[0], Is.EqualTo(tag));
            Assert.That((string)result[1], Is.EqualTo(value));
        }

        [Test]
        public void ImportPreservesTagAndExpiry([Values] bool replace, [Values] bool expiry, [Values(32, 8192)] int valueLength)
        {
            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase database = redis.GetDatabase();
            string value = new('v', valueLength);
            database.Execute("SETWITHETAG", "source", "first");
            database.Execute("SETWITHETAG", "source", "second");
            if (expiry)
                database.Execute("SETWITHETAG", "source", value, "EX", 300);
            else
                database.Execute("SETWITHETAG", "source", value);
            if (replace)
                database.StringSet("destination", new string('x', valueLength * 2), TimeSpan.FromMinutes(10));

            ImportRecord("source", "destination");

            AssertTaggedValue(database, "destination", 3, value);
            Assert.That(database.KeyTimeToLive("destination").HasValue, Is.EqualTo(expiry));
            RedisResult[] stale = (RedisResult[])database.Execute("SETIFMATCH", "destination", "stale", 2);
            Assert.That((long)stale[0], Is.EqualTo(3));
            Assert.That((string)stale[1], Is.EqualTo(value));
            RedisResult[] matching = (RedisResult[])database.Execute("SETIFMATCH", "destination", "updated", 3);
            Assert.That((long)matching[0], Is.EqualTo(4));
            Assert.That(matching[1].IsNull, Is.True);
            AssertTaggedValue(database, "destination", 4, "updated");
            database.StringSet("destination", "plain");
            AssertTaggedValue(database, "destination", 0, "plain");
        }

        [Test]
        public void RenamePreservesTagAndExpiry([Values] bool replace, [Values] bool expiry)
        {
            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase database = redis.GetDatabase();
            database.Execute("SETWITHETAG", "source", "first");
            database.Execute("SETWITHETAG", "source", "value");
            if (expiry)
                database.KeyExpire("source", TimeSpan.FromMinutes(5));
            if (replace)
                database.StringSet("destination", new string('x', 128));

            Assert.That(database.KeyRename("source", "destination"), Is.True);
            AssertTaggedValue(database, "destination", 2, "value");
            Assert.That(database.KeyExists("source"), Is.False);
            Assert.That(database.KeyTimeToLive("destination").HasValue, Is.EqualTo(expiry));
        }

        [Test]
        public void UntaggedImportClearsDestinationTagAndExpiry()
        {
            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase database = redis.GetDatabase();
            database.StringSet("source", "plain");
            database.Execute("SETWITHETAG", "destination", new string('x', 128), "EX", 300);

            ImportRecord("source", "destination");

            AssertTaggedValue(database, "destination", 0, "plain");
            Assert.That(database.KeyTimeToLive("destination"), Is.Null);
        }
    }
}
