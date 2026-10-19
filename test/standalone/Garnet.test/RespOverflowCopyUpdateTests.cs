// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Read-copy-update of overflow string values. A copy-update copies the value into a new record and then edits it there,
    /// so a command that grows the value must write into an array sized for the new length.
    /// </summary>
    /// <remarks>
    /// The low-memory server uses 4 KB pages, so string values above the 2 KB inline limit are overflow. A record in the
    /// mutable region is updated in place; a read-only or on-disk record takes the copy-update path. A value of unchanged
    /// length is shared with the source record, while a value that grows is copied into an array sized for the new length.
    /// </remarks>
    [TestFixture]
    public class RespOverflowCopyUpdateTests : TestBase
    {
        const int ValueSize = 3000;
        const int Growth = 500;

        public enum SourceLocation
        {
            Mutable,
            ReadOnly,
            OnDisk,
        }

        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            // A 50% mutable region leaves half the in-memory log read-only, so a record can be observed read-only while resident.
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, mutablePercent: 50);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        public void AppendGrowsOverflowValue([Values] SourceLocation location)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var (db, original) = SetAndPlace(redis, location);

            ClassicAssert.AreEqual(ValueSize + 3, db.StringAppend("key", "bbb"));
            AssertValue(db, [.. original, .. "bbb"u8.ToArray()]);
        }

        [Test]
        public void SetRangePastEndGrowsOverflowValue([Values] SourceLocation location)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var (db, original) = SetAndPlace(redis, location);

            ClassicAssert.AreEqual(ValueSize + Growth + 3, (long)db.StringSetRange("key", ValueSize + Growth, "bbb"));
            AssertValue(db, [.. original, .. new byte[Growth], .. "bbb"u8.ToArray()]);
        }

        [Test]
        public void SetBitPastEndGrowsOverflowValue([Values] SourceLocation location)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var (db, original) = SetAndPlace(redis, location);

            ClassicAssert.IsFalse(db.StringSetBit("key", (ValueSize + Growth) * 8L, true));
            AssertValue(db, [.. original, .. new byte[Growth], 0x80]);
        }

        [Test]
        public void BitFieldPastEndGrowsOverflowValue([Values] SourceLocation location)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var (db, original) = SetAndPlace(redis, location);

            var result = (RedisResult[])db.Execute("BITFIELD", "key", "SET", "u8", $"#{ValueSize + Growth}", 255);
            ClassicAssert.AreEqual(0, (long)result[0]);
            AssertValue(db, [.. original, .. new byte[Growth], 0xFF]);
        }

        [Test]
        public void SetBitWithinOverflowValue([Values] SourceLocation location)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var (db, original) = SetAndPlace(redis, location);

            ClassicAssert.IsFalse(db.StringSetBit("key", 10 * 8L, true));
            var expected = (byte[])original.Clone();
            expected[10] |= 0x80;
            AssertValue(db, expected);
        }

        /// <summary>Sets "key" to an overflow value, then moves its record to <paramref name="location"/> by writing filler records.</summary>
        private static (IDatabase db, byte[] original) SetAndPlace(ConnectionMultiplexer redis, SourceLocation location)
        {
            var db = redis.GetDatabase(0);
            var iServer = redis.GetServer(TestUtils.EndPoint);

            var original = new byte[ValueSize];
            for (var i = 0; i < original.Length; i++)
                original[i] = (byte)((i * 31) + 7);
            ClassicAssert.AreEqual(0, original[10] & 0x80, "SetBitWithinOverflowValue expects bit 80 to start clear");

            var recordStart = TestUtils.GetStoreAddressInfo(iServer).TailAddress;
            ClassicAssert.IsTrue(db.StringSet("key", original));
            var recordEnd = TestUtils.GetStoreAddressInfo(iServer).TailAddress;

            if (location != SourceLocation.Mutable)
            {
                for (var filler = 0; ; filler++)
                {
                    var info = TestUtils.GetStoreAddressInfo(iServer);
                    if (location == SourceLocation.ReadOnly && info.ReadOnlyAddress >= recordEnd)
                    {
                        ClassicAssert.LessOrEqual(info.HeadAddress, recordStart, "The record was evicted before it was observed read-only");
                        break;
                    }
                    if (location == SourceLocation.OnDisk && info.HeadAddress >= recordEnd)
                        break;
                    ClassicAssert.Less(filler, 100_000, $"The record never reached {location}");
                    _ = db.StringSet($"filler{filler:D6}", "x");
                }
            }

            return (db, original);
        }

        private static void AssertValue(IDatabase db, byte[] expected)
        {
            var actual = (byte[])db.StringGet("key");
            ClassicAssert.IsNotNull(actual);
            ClassicAssert.AreEqual(expected.Length, actual.Length, "Value length");
            ClassicAssert.IsTrue(expected.AsSpan().SequenceEqual(actual), "Value content");
        }
    }
}