// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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
        public void StreamMultipleValuesTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var streamKey = "x1";
            var entryKey = GenerateRandomString(4); // generate random ascii string of length 4
            var entryValue = GenerateRandomString(4); // generate random ascii string of length 4
            var retId = db.StreamAdd(streamKey, entryKey, entryValue, $"{1}-0"); // currently all 3 pairs are put on the log. each pair is ctrl metadata followed by string atm

            var res = db.StreamRange(streamKey, "-", "+");
            ClassicAssert.AreEqual(res.Length, 1);
            foreach (var entry in res)
            {
                ClassicAssert.AreEqual(entry.Id.ToString(), retId.ToString());
                ClassicAssert.AreEqual(entry.Values.Length, 1);
                ClassicAssert.AreEqual(entry.Values[0].Name.ToString(), entryKey);
                ClassicAssert.AreEqual(entry.Values[0].Value.ToString(), entryValue);
            }

            var delCount = db.StreamDelete(streamKey, [retId]);
            ClassicAssert.AreEqual(delCount, 1);

            // just for messing around, let's add multiple key value pairs for this id?
            retId = db.StreamAdd(streamKey, [new NameValueEntry("field1", "value1"), new NameValueEntry("field2", "value2")], messageId: $"{2}-0");

            // check if all the values are there
            res = db.StreamRange(streamKey, "-", "+");
            ClassicAssert.AreEqual(res.Length, 1);
            foreach (var entry in res)
            {
                ClassicAssert.AreEqual(entry.Id.ToString(), retId.ToString());
                ClassicAssert.AreEqual(entry.Values.Length, 2);
                ClassicAssert.AreEqual(entry.Values[0].Name.ToString(), "field1");
                ClassicAssert.AreEqual(entry.Values[0].Value.ToString(), "value1");
                ClassicAssert.AreEqual(entry.Values[1].Name.ToString(), "field2");
                ClassicAssert.AreEqual(entry.Values[1].Value.ToString(), "value2");
            }
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
        
        [Test]
        [Category("Trim")]
        public void StreamTrimFullTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var streamKey = "trimmer";
            long count = 1000;
            string[] ids = new string[count];
            for (long i = 0; i < count; i++)
            {
                var entryKey = GenerateRandomString(4); // generate random ascii string of length 4
                var entryValue = GenerateRandomString(4); // generate random ascii string of length 4
                RedisValue retId = db.StreamAdd(streamKey, entryKey, entryValue, $"{i + 1}-0");
                ids[i] = retId.ToString();
            }

            // Trim in random steps from 1-150 until we have 0 entries
            long currentLength = count;
            var random = new Random(42); // Fixed seed for reproducibility
            
            while (currentLength > 0)
            {
                // Determine how many to keep (trim to this length)
                long trimAmount = random.Next(1, Math.Min(151, (int)currentLength + 1));
                long newLength = Math.Max(0, currentLength - trimAmount);
                
                // Verify stream length before trim
                long lengthBefore = db.StreamLength(streamKey);
                ClassicAssert.AreEqual(currentLength, lengthBefore, "Stream length mismatch before trim");
                
                // Get first and last entries before trim
                var rangeBefore = db.StreamRange(streamKey, "-", "+");
                var firstIdBefore = rangeBefore.Length > 0 ? rangeBefore[0].Id.ToString() : null;
                var lastIdBefore = rangeBefore.Length > 0 ? rangeBefore[rangeBefore.Length - 1].Id.ToString() : null;
                
                // Perform the trim
                long trimmed = db.StreamTrim(streamKey, newLength);
                
                // Verify the correct number of entries were trimmed
                ClassicAssert.AreEqual(trimAmount, trimmed, $"Expected to trim {trimAmount} entries");
                
                // Verify new stream length
                long lengthAfter = db.StreamLength(streamKey);
                ClassicAssert.AreEqual(newLength, lengthAfter, "Stream length mismatch after trim");
                
                if (newLength > 0)
                {
                    // Get first and last entries after trim
                    var rangeAfter = db.StreamRange(streamKey, "-", "+");
                    ClassicAssert.AreEqual(newLength, rangeAfter.Length, "Range length should match stream length");
                    
                    var firstIdAfter = rangeAfter[0].Id.ToString();
                    var lastIdAfter = rangeAfter[rangeAfter.Length - 1].Id.ToString();
                    
                    // First entry should have changed (oldest entries were removed)
                    ClassicAssert.AreNotEqual(firstIdBefore, firstIdAfter, "First entry should change after trim");
                    
                    // Last entry should remain the same (we keep newest entries)
                    ClassicAssert.AreEqual(lastIdBefore, lastIdAfter, "Last entry should not change after trim");
                    
                    // Verify the new first entry is from the expected position in ids array
                    long expectedFirstIndex = count - newLength;
                    string expectedFirstId = ids[expectedFirstIndex];
                    ClassicAssert.AreEqual(expectedFirstId, firstIdAfter, "First entry ID should match expected from ids array");
                    
                    // Verify the last entry is still the original last entry from ids array
                    string expectedLastId = ids[count - 1];
                    ClassicAssert.AreEqual(expectedLastId, lastIdAfter, "Last entry should still be the original last from ids array");
                }
                
                currentLength = newLength;
            }
            
            // Final verification: stream should be empty or minimal
            long finalLength = db.StreamLength(streamKey);
            ClassicAssert.AreEqual(0, finalLength, "Stream should be empty at the end");
        }

        [Ignore("Havent fixed this yet")]
        [Test]
        [Category("XRANGE_XREVRANGE")]
        public void StreamRangeAndRevRangeTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var streamKey = "rangeScan1";

            // Store field-value pairs for verification
            var expectedEntries = new List<(string field, string value)>();

            // first add to stream with known field-value pairs
            for (int i = 0; i < 10; i++)
            {
                var entryKey = $"field{i}";
                var entryValue = $"value{i}";
                expectedEntries.Add((entryKey, entryValue));
                var retId = db.StreamAdd(streamKey, entryKey, entryValue);
            }

            // Full range tests
            var range = db.StreamRange(streamKey, "-", "+");
            ClassicAssert.AreEqual(range.Length, 10);
            // Verify forward range is in ascending order
            for (int i = 0; i < range.Length - 1; i++)
            {
                ClassicAssert.IsTrue(string.Compare(range[i].Id.ToString(), range[i + 1].Id.ToString()) < 0);
            }
            // Verify field-value pairs are in expected order
            for (int i = 0; i < range.Length; i++)
            {
                ClassicAssert.AreEqual(1, range[i].Values.Length);
                ClassicAssert.AreEqual(expectedEntries[i].field, (string)range[i].Values[0].Name);
                ClassicAssert.AreEqual(expectedEntries[i].value, (string)range[i].Values[0].Value);
            }

            var revRange = db.StreamRange(streamKey, "-", "+", messageOrder: Order.Descending);
            ClassicAssert.AreEqual(revRange.Length, 10);
            // Verify reverse range is in descending order
            for (int i = 0; i < revRange.Length - 1; i++)
            {
                ClassicAssert.IsTrue(string.Compare(revRange[i].Id.ToString(), revRange[i + 1].Id.ToString()) > 0);
            }
            // Verify field-value pairs are in reversed order
            for (int i = 0; i < revRange.Length; i++)
            {
                ClassicAssert.AreEqual(1, revRange[i].Values.Length);
                var expectedIndex = revRange.Length - 1 - i;
                ClassicAssert.AreEqual(expectedEntries[expectedIndex].field, (string)revRange[i].Values[0].Name);
                ClassicAssert.AreEqual(expectedEntries[expectedIndex].value, (string)revRange[i].Values[0].Value);
            }

            // Verify reverse range has same IDs as forward range (just reversed)
            for (int i = 0; i < range.Length; i++)
            {
                ClassicAssert.AreEqual(range[i].Id, revRange[range.Length - 1 - i].Id);
            }

            // Partial range tests
            var startId = range[2].Id;
            var endId = range[5].Id;
            var partialRange = db.StreamRange(streamKey, startId, endId);
            ClassicAssert.AreEqual(partialRange.Length, 4);
            // Verify partial range starts and ends with correct IDs
            ClassicAssert.AreEqual(partialRange[0].Id, startId);
            ClassicAssert.AreEqual(partialRange[3].Id, endId);
            // Verify entries match the corresponding entries from full range
            for (int i = 0; i < 4; i++)
            {
                ClassicAssert.AreEqual(partialRange[i].Id, range[2 + i].Id);
                // Verify field-value pairs match expected
                ClassicAssert.AreEqual(expectedEntries[2 + i].field, (string)partialRange[i].Values[0].Name);
                ClassicAssert.AreEqual(expectedEntries[2 + i].value, (string)partialRange[i].Values[0].Value);
            }

            // reverse partial range
            var partialRevRange = db.StreamRange(streamKey, startId, endId, messageOrder: Order.Descending);
            ClassicAssert.AreEqual(partialRevRange.Length, 4);
            // Verify reverse partial range is reversed
            for (int i = 0; i < 4; i++)
            {
                ClassicAssert.AreEqual(partialRevRange[i].Id, partialRange[3 - i].Id);
                // Verify field-value pairs are reversed
                ClassicAssert.AreEqual(expectedEntries[5 - i].field, (string)partialRevRange[i].Values[0].Name);
                ClassicAssert.AreEqual(expectedEntries[5 - i].value, (string)partialRevRange[i].Values[0].Value);
            }

            // limit tests
            int limit = 3;
            var limitedRange = db.StreamRange(streamKey, "-", "+", limit);
            ClassicAssert.AreEqual(limitedRange.Length, limit);
            // Verify limited range returns first N entries
            for (int i = 0; i < limit; i++)
            {
                ClassicAssert.AreEqual(limitedRange[i].Id, range[i].Id);
                // Verify field-value pairs match expected
                ClassicAssert.AreEqual(expectedEntries[i].field, (string)limitedRange[i].Values[0].Name);
                ClassicAssert.AreEqual(expectedEntries[i].value, (string)limitedRange[i].Values[0].Value);
            }

            // reverse limit tests
            var limitedRevRange = db.StreamRange(streamKey, "-", "+", limit, messageOrder: Order.Descending);
            ClassicAssert.AreEqual(limitedRevRange.Length, limit);
            // Verify limited reverse range returns last N entries in reverse order
            for (int i = 0; i < limit; i++)
            {
                ClassicAssert.AreEqual(limitedRevRange[i].Id, range[range.Length - 1 - i].Id);
                // Verify field-value pairs match expected (from end, reversed)
                var expectedIndex = range.Length - 1 - i;
                ClassicAssert.AreEqual(expectedEntries[expectedIndex].field, (string)limitedRevRange[i].Values[0].Name);
                ClassicAssert.AreEqual(expectedEntries[expectedIndex].value, (string)limitedRevRange[i].Values[0].Value);
            }
        }

        [Ignore("Havent fixed this yet")]
        [Test]
        public void StreamLastBasicTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var server = redis.GetServers()[0];

            var streamKey = "lastBasic";

            // Add entries with known field-value pairs
            var id1 = db.StreamAdd(streamKey, "field1", "value1", messageId: "1-0");
            var id2 = db.StreamAdd(streamKey, "field2", "value2", messageId: "2-0");
            var id3 = db.StreamAdd(streamKey, "field3", "value3", messageId: "3-0");

            // Execute XLAST
            var result = server.Execute("XLAST", streamKey);
            var lastEntry = (RedisResult[])result;

            // Verify the last entry
            ClassicAssert.AreEqual(2, lastEntry.Length);
            ClassicAssert.AreEqual("3-0", (string)lastEntry[0]);

            var values = (RedisResult[])lastEntry[1];
            ClassicAssert.AreEqual(2, values.Length);
            ClassicAssert.AreEqual("field3", (string)values[0]);
            ClassicAssert.AreEqual("value3", (string)values[1]);
        }

        [Ignore("Havent fixed this yet")]
        [Test]
        public void StreamLastMultipleFieldsTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var server = redis.GetServers()[0];

            var streamKey = "lastMultiFields";

            // Add an entry with multiple field-value pairs
            var id = db.StreamAdd(streamKey, [
                new NameValueEntry("field1", "value1"),
                new NameValueEntry("field2", "value2"),
                new NameValueEntry("field3", "value3")
            ], messageId: "100-0");

            // Execute XLAST
            var result = server.Execute("XLAST", streamKey);
            var lastEntry = (RedisResult[])result;

            // Verify the last entry
            ClassicAssert.AreEqual(2, lastEntry.Length);
            ClassicAssert.AreEqual("100-0", (string)lastEntry[0]);

            var values = (RedisResult[])lastEntry[1];
            ClassicAssert.AreEqual(6, values.Length); // 3 fields * 2 (name + value)
            ClassicAssert.AreEqual("field1", (string)values[0]);
            ClassicAssert.AreEqual("value1", (string)values[1]);
            ClassicAssert.AreEqual("field2", (string)values[2]);
            ClassicAssert.AreEqual("value2", (string)values[3]);
            ClassicAssert.AreEqual("field3", (string)values[4]);
            ClassicAssert.AreEqual("value3", (string)values[5]);
        }

        [Ignore("Havent fixed this yet")]
        [Test]
        public void StreamLastEmptyStreamTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var server = redis.GetServers()[0];

            var streamKey = "lastEmpty";

            // Execute XLAST on non-existent stream
            var result = server.Execute("XLAST", streamKey);

            // Should return empty array
            ClassicAssert.IsTrue(result.IsNull || ((RedisResult[])result).Length == 0);
        }

        [Ignore("Havent fixed this yet")]
        [Test]
        public void StreamLastSingleEntryTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var server = redis.GetServers()[0];

            var streamKey = "lastSingle";

            // Add only one entry
            var id = db.StreamAdd(streamKey, "onlyField", "onlyValue");

            // Execute XLAST
            var result = server.Execute("XLAST", streamKey);
            var lastEntry = (RedisResult[])result;

            // Verify it returns the only entry
            ClassicAssert.AreEqual(2, lastEntry.Length);
            ClassicAssert.AreEqual((string)id, (string)lastEntry[0]);

            var values = (RedisResult[])lastEntry[1];
            ClassicAssert.AreEqual(2, values.Length);
            ClassicAssert.AreEqual("onlyField", (string)values[0]);
            ClassicAssert.AreEqual("onlyValue", (string)values[1]);
        }

        [Ignore("Havent fixed this yet")]
        [Test]
        public void StreamLastAfterDeleteTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var server = redis.GetServers()[0];

            var streamKey = "lastAfterDelete";

            // Add multiple entries
            var id1 = db.StreamAdd(streamKey, "field1", "value1", messageId: "1-0");
            var id2 = db.StreamAdd(streamKey, "field2", "value2", messageId: "2-0");
            var id3 = db.StreamAdd(streamKey, "field3", "value3", messageId: "3-0");

            // Delete the last entry
            db.StreamDelete(streamKey, [id3]);

            // Execute XLAST
            var result = server.Execute("XLAST", streamKey);
            var lastEntry = (RedisResult[])result;

            // Should return the second entry as it's now the last
            ClassicAssert.AreEqual(2, lastEntry.Length);
            ClassicAssert.AreEqual("2-0", (string)lastEntry[0]);

            var values = (RedisResult[])lastEntry[1];
            ClassicAssert.AreEqual(2, values.Length);
            ClassicAssert.AreEqual("field2", (string)values[0]);
            ClassicAssert.AreEqual("value2", (string)values[1]);
        }

        [Ignore("Havent fixed this yet")]
        [Test]
        public void StreamLastAfterMultipleDeletesTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var server = redis.GetServers()[0];

            var streamKey = "lastAfterMultipleDeletes";

            // Add multiple entries
            for (int i = 0; i < 1000; i++)
            {
                db.StreamAdd(streamKey, $"field{i}", $"value{i}", messageId: $"{i}-0");
            }

            for (int i = 999; i > -1; i--)
            {
                var result = server.Execute("XLAST", streamKey);
                var lastEntry = (RedisResult[])result;
                // Should return the second entry as it's now the last
                ClassicAssert.AreEqual(2, lastEntry.Length);
                ClassicAssert.AreEqual($"{i}-0", (string)lastEntry[0]);
                var values = (RedisResult[])lastEntry[1];
                ClassicAssert.AreEqual(2, values.Length);
                ClassicAssert.AreEqual($"field{i}", (string)values[0]);
                ClassicAssert.AreEqual($"value{i}", (string)values[1]);

                // delete it so next time we get the new last
                db.StreamDelete(streamKey, [$"{i}-0"]);
            }

            // Finally, the stream should be empty
            var finalResult = server.Execute("XLAST", streamKey);
            ClassicAssert.IsTrue(finalResult.IsNull || ((RedisResult[])finalResult).Length == 0);
        }

        [Ignore("Havent fixed this yet")]
        [Test]
        public void StreamLastAfterTrimTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());

            var db = redis.GetDatabase(0);
            var server = redis.GetServers()[0];

            var streamKey = "lastAfterTrim";

            // Add multiple entries
            for (int i = 0; i < 10; i++)
            {
                db.StreamAdd(streamKey, $"field{i}", $"value{i}");
            }

            // Trim to keep only 3 entries
            db.StreamTrim(streamKey, 3);

            // Execute XLAST
            var result = server.Execute("XLAST", streamKey);
            var lastEntry = (RedisResult[])result;

            // Should return the last entry after trim
            ClassicAssert.AreEqual(2, lastEntry.Length);
            var values = (RedisResult[])lastEntry[1];
            ClassicAssert.AreEqual(2, values.Length);
            ClassicAssert.AreEqual("field9", (string)values[0]);
            ClassicAssert.AreEqual("value9", (string)values[1]);
        }

        [Ignore("Havent fixed this yet")]
        [Test]
        public async Task StreamLastAutoGeneratedIdTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var server = redis.GetServers()[0];

            var streamKey = "lastAutoId";

            // Add entries with auto-generated IDs
            db.StreamAdd(streamKey, "field1", "value1");
            await Task.Delay(1);
            db.StreamAdd(streamKey, "field2", "value2");
            await Task.Delay(1);
            var lastId = db.StreamAdd(streamKey, "field3", "value3");

            // Execute XLAST
            var result = server.Execute("XLAST", streamKey);
            var lastEntry = (RedisResult[])result;

            // Verify it returns the last entry with the correct auto-generated ID
            ClassicAssert.AreEqual(2, lastEntry.Length);
            ClassicAssert.AreEqual(lastId.ToString(), (string)lastEntry[0]);

            var values = (RedisResult[])lastEntry[1];
            ClassicAssert.AreEqual(2, values.Length);
            ClassicAssert.AreEqual("field3", (string)values[0]);
            ClassicAssert.AreEqual("value3", (string)values[1]);
        }


        #endregion

        #region StreamCompatabilityTests
        // check if common things like KEYS, and SCAN work with streams

        [Ignore("Havent fixed this yet")]
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

        [Ignore("Havent fixed this yet")]
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
                    ClassicAssert.IsTrue(scannedKeys.Add((string)key), "Did not expect a duplicate");
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

        [Ignore("Havent fixed this yet")]
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

        [Ignore("Havent fixed this yet")]
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