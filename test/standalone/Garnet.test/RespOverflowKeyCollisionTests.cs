// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Reads of a key whose on-disk hash chain contains a record for another key, in the same index bucket and tag, that is too long
    /// to be stored inline. Such a key is compared only after it is read from the object log, and a mismatch must continue to the
    /// previous record in the chain or report the key as not found.
    /// </summary>
    [TestFixture]
    public class RespOverflowKeyCollisionTests : TestBase
    {
        // 1,024-byte keys (above the 1,022-byte inline key limit) that share an index bucket and tag with the default 1m test index.
        static readonly string KeyPrefix = "synthetic:" + new string('x', 982);
        static readonly string StoredKey = KeyPrefix + "87b7bf4eda4f44ccda64f1f5c40c5811";
        static readonly string CollidingKey = KeyPrefix + "fe6295bee3767698743c3edcf341e7cc";

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
            server.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        public void GetMissingKeyCollidingWithDiskOverflowKey([Values] bool storedIsHash)
        {
            AssertKeysShareBucketAndTag();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            if (storedIsHash)
                ClassicAssert.IsTrue(db.HashSet(StoredKey, "field", "value"));
            else
                ClassicAssert.IsTrue(db.StringSet(StoredKey, "value"));
            FlushAndEvict(db);

            // Each read goes pending to the stored key's record, whose key does not match; it is the only record in the chain.
            ClassicAssert.IsTrue(db.StringGet(CollidingKey).IsNull);
            ClassicAssert.IsFalse(db.KeyExists(CollidingKey));

            var storedValue = storedIsHash ? db.HashGet(StoredKey, "field") : db.StringGet(StoredKey);
            ClassicAssert.AreEqual("value", (string)storedValue);
        }

        [Test]
        public void GetKeyBehindCollidingDiskOverflowKey([Values] bool newerIsHash)
        {
            AssertKeysShareBucketAndTag();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            // CollidingKey is written first, so the newer StoredKey record precedes it in the hash chain.
            ClassicAssert.IsTrue(db.StringSet(CollidingKey, "older"));
            if (newerIsHash)
                ClassicAssert.IsTrue(db.HashSet(StoredKey, "field", "newer"));
            else
                ClassicAssert.IsTrue(db.StringSet(StoredKey, "newer"));
            FlushAndEvict(db);

            // Each read first reads the StoredKey record, whose key does not match, then continues to its PreviousAddress.
            ClassicAssert.AreEqual("older", (string)db.StringGet(CollidingKey));
            ClassicAssert.IsTrue(db.KeyExists(CollidingKey));
        }

        static void FlushAndEvict(IDatabase db)
            => StringAssert.StartsWith("OK", (string)db.Execute("DEBUG", "FLUSHANDEVICT"));

        void AssertKeysShareBucketAndTag()
        {
            var storedHash = GetKeyHash(StoredKey);
            var collidingHash = GetKeyHash(CollidingKey);
            var bucketMask = server.Provider.StoreWrapper.store.IndexSize - 1;
            ClassicAssert.AreEqual(storedHash & bucketMask, collidingHash & bucketMask,
                "The test keys must share an index bucket; regenerate them if the key hash or the test index size changed");
            ClassicAssert.AreEqual(HashBucketEntry.GetTag(storedHash), HashBucketEntry.GetTag(collidingHash),
                "The test keys must share a tag; regenerate them if the key hash changed");
        }

        static long GetKeyHash(string key)
            => GarnetKeyComparer.StaticGetHashCode64((FixedSpanByteKey)new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes(key)));
    }
}