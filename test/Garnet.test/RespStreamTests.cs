// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

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
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, enableStreams: true);
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

        [Test]
        [Category("Trim")]
        public void StreamTrimMaxLenTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var streamKey = "trimByMaxLen";
            long count = 500;
            for (long i = 0; i < count; i++)
            {
                var entryKey = GenerateRandomString(4); // generate random ascii string of length 4
                var entryValue = GenerateRandomString(4); // generate random ascii string of length 4
                var retId = db.StreamAdd(streamKey, entryKey, entryValue, $"{i + 1}-0");
            }
            var maxLen = 100;
            var trimCount = db.StreamTrim(streamKey, maxLen);
            ClassicAssert.GreaterOrEqual(trimCount, 1);
            ClassicAssert.GreaterOrEqual(count - trimCount, maxLen);
        }


        #endregion


        #region StreamCompatabilityTests
        // check if common things like KEYS, and SCAN work with streams

        [Test]
        public void StreamKeysCommandTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var server = redis.GetServer(TestUtils.GetConfig().EndPoints[0]);

            // Create some streams and other types
            db.StreamAdd("stream:1", "field1", "value1");
            db.StreamAdd("stream:2", "field2", "value2");
            db.StreamAdd("stream:3", "field3", "value3");
            db.StringSet("string:1", "value");
            db.HashSet("hash:1", "field", "value");

            // Test KEYS with pattern matching all
            var allKeys = server.Keys(pattern: "*").ToArray();
            ClassicAssert.GreaterOrEqual(allKeys.Length, 5);

            // Test KEYS with stream pattern
            var streamKeys = server.Keys(pattern: "stream:*").ToArray();
            ClassicAssert.AreEqual(3, streamKeys.Length);
            ClassicAssert.IsTrue(streamKeys.Any(k => k == "stream:1"));
            ClassicAssert.IsTrue(streamKeys.Any(k => k == "stream:2"));
            ClassicAssert.IsTrue(streamKeys.Any(k => k == "stream:3"));
        }

        [Test]
        public void StreamScanCommandTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var server = redis.GetServer(TestUtils.GetConfig().EndPoints[0]);

            // Create multiple streams
            for (int i = 0; i < 20; i++)
            {
                db.StreamAdd($"scan:stream:{i}", "field", "value");
            }

            // Scan and collect all keys
            var scannedKeys = new HashSet<string>();
            var cursor = 0L;
            var iterations = 0;
            var maxIterations = 100; // Safety limit

            do
            {
                var result = server.Execute("SCAN", cursor.ToString(), "MATCH", "scan:stream:*", "COUNT", "5");
                var scanResult = (RedisResult[])result;
                
                cursor = long.Parse((string)scanResult[0]);
                var keys = (RedisResult[])scanResult[1];
                
                foreach (var key in keys)
                {
                    scannedKeys.Add((string)key);
                }
                
                iterations++;
            } while (cursor != 0 && iterations < maxIterations);

            // Verify cursor eventually returns to 0
            ClassicAssert.AreEqual(0, cursor, "SCAN cursor should eventually return to 0");
            
            // Verify all keys were found
            ClassicAssert.AreEqual(20, scannedKeys.Count, "All stream keys should be returned by SCAN");
            
            // Verify specific keys exist
            for (int i = 0; i < 20; i++)
            {
                ClassicAssert.IsTrue(scannedKeys.Contains($"scan:stream:{i}"), $"Key scan:stream:{i} should be in results");
            }
        }

        [Test]
        public void StreamScanWithCountTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var server = redis.GetServer(TestUtils.GetConfig().EndPoints[0]);

            // Create streams
            for (int i = 0; i < 50; i++)
            {
                db.StreamAdd($"count:stream:{i}", "field", "value");
            }

            var cursor = 0L;
            var result = server.Execute("SCAN", cursor.ToString(), "MATCH", "count:stream:*", "COUNT", "10");
            var scanResult = (RedisResult[])result;
            
            var newCursor = long.Parse((string)scanResult[0]);
            var keys = (RedisResult[])scanResult[1];

            // COUNT is a hint, not a guarantee, but we should get some results
            ClassicAssert.GreaterOrEqual(keys.Length, 1, "Should return at least one key");
            // With 50 keys and COUNT 10, we shouldn't return all keys in one scan
            ClassicAssert.Less(keys.Length, 50, "Should not return all keys in single scan with small COUNT");
        }

        [Test]
        public void StreamScanByTypeTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var server = redis.GetServer(TestUtils.GetConfig().EndPoints[0]);

            // Create streams and other types with same prefix
            db.StreamAdd("type:stream:1", "field", "value");
            db.StreamAdd("type:stream:2", "field", "value");
            db.StringSet("type:string:1", "value");
            db.HashSet("type:hash:1", "field", "value");
            db.ListRightPush("type:list:1", "value");

            // Scan for all keys with type prefix
            var allKeys = new HashSet<string>();
            var cursor = 0L;
            
            do
            {
                var result = server.Execute("SCAN", cursor.ToString(), "MATCH", "type:*");
                var scanResult = (RedisResult[])result;
                cursor = long.Parse((string)scanResult[0]);
                var keys = (RedisResult[])scanResult[1];
                
                foreach (var key in keys)
                {
                    allKeys.Add((string)key);
                }
            } while (cursor != 0);

            // Should find all 5 keys
            ClassicAssert.AreEqual(5, allKeys.Count);

            // Now scan with TYPE filter for streams only
            var streamKeys = new HashSet<string>();
            cursor = 0L;
            
            do
            {
                var result = server.Execute("SCAN", cursor.ToString(), "MATCH", "type:*", "TYPE", "stream");
                var scanResult = (RedisResult[])result;
                cursor = long.Parse((string)scanResult[0]);
                var keys = (RedisResult[])scanResult[1];
                
                foreach (var key in keys)
                {
                    streamKeys.Add((string)key);
                }
            } while (cursor != 0);

            // Should only find the 2 stream keys
            ClassicAssert.AreEqual(2, streamKeys.Count);
            ClassicAssert.IsTrue(streamKeys.Contains("type:stream:1"));
            ClassicAssert.IsTrue(streamKeys.Contains("type:stream:2"));
            ClassicAssert.IsFalse(streamKeys.Contains("type:string:1"));
            ClassicAssert.IsFalse(streamKeys.Contains("type:hash:1"));

            // Verify TYPE string filter doesn't return streams
            var stringKeys = new HashSet<string>();
            cursor = 0L;
            
            do
            {
                var result = server.Execute("SCAN", cursor.ToString(), "MATCH", "type:*", "TYPE", "string");
                var scanResult = (RedisResult[])result;
                cursor = long.Parse((string)scanResult[0]);
                var keys = (RedisResult[])scanResult[1];
                
                foreach (var key in keys)
                {
                    stringKeys.Add((string)key);
                }
            } while (cursor != 0);

            // Should only find the string key
            ClassicAssert.AreEqual(1, stringKeys.Count);
            ClassicAssert.IsTrue(stringKeys.Contains("type:string:1"));
            ClassicAssert.IsFalse(stringKeys.Contains("type:stream:1"));
            ClassicAssert.IsFalse(stringKeys.Contains("type:stream:2"));

            // Do a full scan without type and see if all keys are present when using cursor
            var fullScanKeys = new HashSet<string>();
            cursor = 0L;
            do
            {
                var result = server.Execute("SCAN", cursor.ToString(), "MATCH", "type:*", "COUNT", "2");
                var scanResult = (RedisResult[])result;
                cursor = long.Parse((string)scanResult[0]);
                var keys = (RedisResult[])scanResult[1];
                
                foreach (var key in keys)
                {
                    fullScanKeys.Add((string)key);
                }
            } while (cursor != 0);

            ClassicAssert.AreEqual(5, fullScanKeys.Count);
        
            ClassicAssert.IsTrue(fullScanKeys.Contains("type:string:1"));
            ClassicAssert.IsTrue(fullScanKeys.Contains("type:hash:1"));
            ClassicAssert.IsTrue(fullScanKeys.Contains("type:list:1"));
            ClassicAssert.IsTrue(fullScanKeys.Contains("type:stream:1"));
            ClassicAssert.IsTrue(fullScanKeys.Contains("type:stream:2"));
        }

        #endregion

    }
}