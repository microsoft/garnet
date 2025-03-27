// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Embedded.server;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    [TestFixture]
    public class RespStreamTests
    {
        protected GarnetServer server;
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random;
        static ulong N = 5;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();
            random = new Random();

            // write to one stream to test for range scans
            var streamKey = "rangeScan";
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            for (ulong i = 0; i < N; i++)
            {
                var entryKey = GenerateRandomString(4); // generate random ascii string of length 4
                var entryValue = GenerateRandomString(4); // generate random ascii string of length 4
                var retId = db.StreamAdd(streamKey, entryKey, entryValue);
            }
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        public string GenerateRandomString(int length)
        {
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        #region STREAMIDTests
        [Test]
        public void StreamAddAutoGenIdTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var streamKey = "add";
            var entryKey = GenerateRandomString(4); // generate random ascii string of length 4
            var entryValue = GenerateRandomString(4); // generate random ascii string of length 4
            var retId = db.StreamAdd(streamKey, entryKey, entryValue);
            ClassicAssert.IsTrue(retId.ToString().Contains("-"));
        }

        [Test]
        public void StreamAddUserDefinedTsTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var streamKey = "addTs";
            var entryKey = GenerateRandomString(4); // generate random ascii string of length 4
            var entryValue = GenerateRandomString(4); // generate random ascii string of length 4
            var retId = db.StreamAdd(streamKey, entryKey, entryValue, $"{1}");
            ClassicAssert.IsTrue(retId.ToString().Contains("-"));
        }

        [Test]
        public void StreamAddUserDefinedIdTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var streamKey = "addId";
            var entryKey = GenerateRandomString(4); // generate random ascii string of length 4
            var entryValue = GenerateRandomString(4); // generate random ascii string of length 4
            var retId = db.StreamAdd(streamKey, entryKey, entryValue, $"{1}-0");
            ClassicAssert.IsTrue(retId.ToString().Contains("-"));
        }
        #endregion

        #region STREAMOperationsTests
        [Test]
        public void StreamAddAndLengthTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var streamKey = "length";
            var count = 0;
            for (ulong i = 0; i < N; i++)
            {
                var entryKey = GenerateRandomString(4); // generate random ascii string of length 4
                var entryValue = GenerateRandomString(4); // generate random ascii string of length 4
                var retId = db.StreamAdd(streamKey, entryKey, entryValue);
                count++;
            }
            ClassicAssert.AreEqual(count, N);

            var length = db.StreamLength(streamKey);
            ClassicAssert.AreEqual(length, N);
        }

        [Test]
        public void StreamRangeExistingTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var streamKey = "rangeScan";
            var range = db.StreamRange(streamKey, "-", "+");
            ClassicAssert.AreEqual(range.Length, N);
        }

        [Test]
        public void StreamRangeNonExistingTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var streamKey = "nonExistingRangeScan";
            var range = db.StreamRange(streamKey, "-", "+");
            ClassicAssert.AreEqual(range.Length, 0);
        }

        [Test]
        public void StreamRangeWithCountTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var streamKey = "rangeScan";
            int limit = 2;
            var range = db.StreamRange(streamKey, "-", "+", limit);
            ClassicAssert.AreEqual(range.Length, limit);
        }

        [Test]
        public void StreamDeleteSingleTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var streamKey = "delOne";
            var entryKey = GenerateRandomString(4); // generate random ascii string of length 4
            var entryValue = GenerateRandomString(4); // generate random ascii string of length 4
            var retId = db.StreamAdd(streamKey, entryKey, entryValue, $"{1}-0");

            var delCount = db.StreamDelete(streamKey, [retId]);
            ClassicAssert.AreEqual(delCount, 1);
        }

        [Test]
        [Category("Delete")]
        public void StreamDeleteMultipleTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var streamKey = "delMultiple";
            var count = 0;
            for (ulong i = 0; i < N; i++)
            {
                var entryKey = GenerateRandomString(4); // generate random ascii string of length 4
                var entryValue = GenerateRandomString(4); // generate random ascii string of length 4
                var retId = db.StreamAdd(streamKey, entryKey, entryValue, $"{i + 1}-0");
                count++;
            }
            ClassicAssert.AreEqual(count, N);

            // Pick arbitrary 2 unique indices between 0 and N and store each index in a set
            int numToDelete = 2;
            var indices = new HashSet<int>();
            while (indices.Count < numToDelete)
            {
                indices.Add(random.Next(0, (int)N));
            }

            var eIds = new RedisValue[numToDelete];
            int c = 0;
            foreach (var idx in indices)
            {
                eIds[c++] = $"{idx + 1}-0";
            }

            var delCount = db.StreamDelete(streamKey, eIds);
            ClassicAssert.AreEqual(delCount, indices.Count);
        }


        #endregion
    }
}