// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Allure.NUnit;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class RespLowMemoryTests : AllureTestBase
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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        void MakeReadOnly(long untilAddress, IServer server, IDatabase db)
        {
            var i = 1000;
            var info = TestUtils.GetStoreAddressInfo(server);

            // Add keys so that the first record enters the read-only region
            // Each record is 40 bytes here, because they do not have expirations
            while (info.ReadOnlyAddress < untilAddress)
            {
                var key = $"key{i++:00000}";
                _ = db.StringSet(key, key);
                info = TestUtils.GetStoreAddressInfo(server);
            }
        }

        [Test]
        public void PersistCopyUpdateTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var server = redis.GetServer(TestUtils.EndPoint);
            var info = TestUtils.GetStoreAddressInfo(server);

            // Start at tail address of PageHeader.Size (64)
            ClassicAssert.AreEqual(PageHeader.Size, info.TailAddress);

            var expire = 100;
            var key0 = $"key{0:00000}";
            _ = db.StringSet(key0, key0, TimeSpan.FromSeconds(expire));

            // Record size for key0 is RecordInfo.Size (8) + MinLengthMetadataBytes (5) + 2 * 8 bytes (key and value) + 8 bytes expiry = 13 + 24 = 37 bytes rounded up to 40
            // so the new tail address should be 64 + 40 = 104 (that is, the record for key0 is located at [64, 104)).
            info = TestUtils.GetStoreAddressInfo(server);
            ClassicAssert.AreEqual(104, info.TailAddress);

            // Make the record read-only by adding more records
            MakeReadOnly(info.TailAddress, server, db);

            info = TestUtils.GetStoreAddressInfo(server);
            var previousTail = info.TailAddress;

            // The first record inserted (key0) is now read-only
            ClassicAssert.IsTrue(info.ReadOnlyAddress >= 104);

            // Persist the key, which should cause RMW to CopyUpdate to tail
            var response = db.KeyPersist(key0);
            ClassicAssert.IsTrue(response);

            // Now key0 is only 32 bytes, as we are removing the expiration
            // That is, key0 is now moved to [previousTail, previousTail + 32)
            info = TestUtils.GetStoreAddressInfo(server);
            ClassicAssert.AreEqual(previousTail + 32, info.TailAddress);

            // Verify that key0 exists with correct value
            ClassicAssert.AreEqual(key0, (string)db.StringGet(key0));
        }
    }
}