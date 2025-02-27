// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    public class DisableObjectStoreTests
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disableObjects: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test, Order(1)]
        [Category("SETBIT")]
        public void GarnetObjectStoreDisabled()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var mykey = "mykey";
            try
            {
                _ = db.ListLength(mykey);
            }
            catch (Exception ex)
            {
                var msg = ex.Message;
                ClassicAssert.AreEqual("ERR Garnet Exception: Object store is disabled", msg);
            }

            // Ensure connection is still healthy
            for (var i = 0; i < 100; i++)
            {
                var myvalue = "myvalue" + i;
                var result = db.StringSet(mykey, myvalue);
                ClassicAssert.IsTrue(result);
                var returned = (string)db.StringGet(mykey);
                ClassicAssert.AreEqual(myvalue, returned);
            }
        }
    }
}
