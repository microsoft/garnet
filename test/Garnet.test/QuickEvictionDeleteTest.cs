// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test;

[TestFixture]
public class QuickEvictionDeleteTest
{
    GarnetServer server;

    [SetUp]
    public void Setup()
    {
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
        server.Start();
    }

    [TearDown]
    public void TearDown()
    {
        server.Dispose();
        TestUtils.OnTearDown();
    }

    [Test]
    public void DeleteInMemoryKeyReturnsOne()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        var db = redis.GetDatabase(0);

        db.StringSet("mykey", "myvalue");
        var result = db.KeyDelete("mykey");
        ClassicAssert.IsTrue(result, "DEL of in-memory key should return true");
    }

    [Test]
    public void DeleteEvictedKeyReturnsOne()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        var db = redis.GetDatabase(0);

        // Write a key early
        db.StringSet("early_key", "early_value");

        // Verify it exists
        var val = db.StringGet("early_key");
        ClassicAssert.AreEqual("early_value", (string)val);

        // Fill the log to push early_key below HeadAddress
        for (var i = 0; i < 500; i++)
            db.StringSet($"filler{i:D4}", $"data{i:D4}");

        // DELETE the evicted key — should still return true per Redis semantics
        var result = db.KeyDelete("early_key");
        ClassicAssert.IsTrue(result, "DEL of evicted-but-existing key should return true");
    }

    [Test]
    public void DeleteNonExistentKeyReturnsZero()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        var db = redis.GetDatabase(0);

        var result = db.KeyDelete("never_existed");
        ClassicAssert.IsFalse(result, "DEL of non-existent key should return false");
    }
}
