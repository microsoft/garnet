// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    struct StoreAddressInfo
    {
        public long BeginAddress;
        public long HeadAddress;
        public long ReadOnlyAddress;
        public long TailAddress;
    }

    [TestFixture]
    public class RespLowMemoryTests
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

        StoreAddressInfo GetStoreAddressInfo(IServer server)
        {
            StoreAddressInfo result = default;
            var info = server.Info("STORE");
            foreach (var section in info)
            {
                foreach (var entry in section)
                {
                    if (entry.Key.Equals("Log.BeginAddress"))
                        result.BeginAddress = long.Parse(entry.Value);
                    else if (entry.Key.Equals("Log.HeadAddress"))
                        result.HeadAddress = long.Parse(entry.Value);
                    else if (entry.Key.Equals("Log.SafeReadOnlyAddress"))
                        result.ReadOnlyAddress = long.Parse(entry.Value);
                    else if (entry.Key.Equals("Log.TailAddress"))
                        result.TailAddress = long.Parse(entry.Value);
                }
            }
            return result;
        }

        void MakeReadOnly(long untilAddress, IServer server, IDatabase db)
        {
            var i = 1000;
            var info = GetStoreAddressInfo(server);

            // Add keys so that the first record enters the read-only region
            // Each record is 40 bytes here, because they do not have expirations
            while (info.ReadOnlyAddress < untilAddress)
            {
                var key = $"key{i++:00000}";
                _ = db.StringSet(key, key);
                info = GetStoreAddressInfo(server);
            }
        }

        [Test]
        public void PersistCopyUpdateTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var server = redis.GetServer(TestUtils.Address, TestUtils.Port);
            var info = GetStoreAddressInfo(server);

            // Start at tail address of 64
            ClassicAssert.AreEqual(64, info.TailAddress);

            var expire = 100;
            var i = 0;
            var key0 = $"key{i++:00000}";
            _ = db.StringSet(key0, key0, TimeSpan.FromSeconds(expire));

            // Record size for key0 is 8 bytes header + 16 bytes key + 16 bytes value + 8 bytes expiry = 48 bytes
            // so the new tail address should be 64 + 48 = 112
            // That is, key0 is located at [64, 112)
            info = GetStoreAddressInfo(server);
            ClassicAssert.AreEqual(112, info.TailAddress);

            // Make the record read-only by adding more records
            MakeReadOnly(info.TailAddress, server, db);

            info = GetStoreAddressInfo(server);
            var previousTail = info.TailAddress;

            // The first record inserted (key0) is now read-only
            ClassicAssert.IsTrue(info.ReadOnlyAddress >= 112);

            // Persist the key, which should cause RMW to CopyUpdate to tail
            var response = db.KeyPersist(key0);
            ClassicAssert.IsTrue(response);

            // Now key0 is only 40 bytes, as we are removing the expiration
            // That is, key0 is now moved to [previousTail, previousTail + 40)
            info = GetStoreAddressInfo(server);
            ClassicAssert.AreEqual(previousTail + 40, info.TailAddress);

            // Verify that key0 exists with correct value
            ClassicAssert.AreEqual(key0, (string)db.StringGet(key0));
        }
    }
}