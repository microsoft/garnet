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

    /// <summary>
    /// DEL returns true when the key is still in memory, but returns false when the
    /// same key has been evicted below HeadAddress. A GET with CopyReadsToTail brings
    /// the record back into the mutable region, making DEL work again.
    /// </summary>
    [Test]
    [TestCase(false, false, TestName = "DeleteKey_InMemory_ReturnsTrue")]
    [TestCase(true, false, TestName = "DeleteKey_Evicted_ReturnsFalse_KnownLimitation")]
    [TestCase(true, true, TestName = "DeleteKey_EvictedThenGet_ReturnsTrue")]
    public void DeleteKeyReturnsDependsOnEviction(bool evictBeforeDelete, bool getBeforeDelete)
    {
        if (getBeforeDelete)
        {
            // Need CopyReadsToTail so GET brings the record back to the mutable region.
            // Without it, GET reads from disk but does NOT copy back — DEL still fails.
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, copyReadsToTail: true);
            server.Start();
        }

        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        var db = redis.GetDatabase(0);

        db.StringSet("target_key", "target_value");

        // Verify the key exists
        var val = db.StringGet("target_key");
        ClassicAssert.AreEqual("target_value", (string)val);

        if (evictBeforeDelete)
        {
            // Fill the log to push target_key below HeadAddress
            for (var i = 0; i < 500; i++)
                db.StringSet($"filler{i:D4}", $"data{i:D4}");
        }

        if (getBeforeDelete)
        {
            // With CopyReadsToTail, GET brings the record back into the mutable
            // region (CopyToTail), so the subsequent DEL finds it in memory.
            val = db.StringGet("target_key");
            ClassicAssert.AreEqual("target_value", (string)val);
        }

        var result = db.KeyDelete("target_key");

        if (evictBeforeDelete && !getBeforeDelete)
        {
            // Known limitation: Tsavorite's Delete returns NOTFOUND for records
            // below HeadAddress because it cannot confirm the key existed without
            // disk I/O. A tombstone IS written, but the status doesn't reflect it.
            ClassicAssert.IsFalse(result, "DEL of evicted key returns false (known limitation)");
        }
        else
        {
            ClassicAssert.IsTrue(result, "DEL should return true when key is in memory");
        }
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
