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
    public class RespStreamTests : TestBase
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

        [Test]
        public void StreamTypeCommandTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var server = redis.GetServer(TestUtils.GetConfig().EndPoints[0]);

            var streamKey = "typeCmd:stream";
            db.StreamAdd(streamKey, "field", "value");

            // TYPE on a stream key must report "stream" (consistent with SCAN ... TYPE stream).
            var type = (string)server.Execute("TYPE", streamKey);
            ClassicAssert.AreEqual("stream", type);

            // StackExchange.Redis maps the same response to RedisType.Stream.
            ClassicAssert.AreEqual(RedisType.Stream, db.KeyType(streamKey));
        }

        #endregion

        #region Persistence
        [Test]
        [Category("Persistence")]
        public void StreamSaveAndRecoverTest()
        {
            // Stand up an isolated server with a per-stream log directory, write entries, SAVE,
            // tear it down, restart against the same directory, and confirm the stream was rebuilt
            // by replaying its log.
            var saveDir = System.IO.Path.Combine(TestUtils.MethodTestDir, "streams");

            // Tear down the auto-setup server (uses default in-memory streams) and stand up a
            // disk-backed one for this test only.
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);

            const string streamKey = "persist";
            const int entryCount = 25;
            var addedIds = new string[entryCount];

            using (var firstServer = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true,
                streamLogDir: saveDir))
            {
                firstServer.Start();
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                var db = redis.GetDatabase(0);
                var s = redis.GetServers()[0];

                for (int i = 0; i < entryCount; i++)
                {
                    var id = db.StreamAdd(streamKey, $"f{i}", $"v{i}", $"{i + 1}-0");
                    addedIds[i] = id.ToString();
                }

                // Trigger a stream commit; SAVE fans out to streams via StoreWrapper.TakeCheckpoint.
                s.Execute("SAVE");
                // Give the fire-and-forget commit task a moment to drain to disk before we tear down.
                System.Threading.Thread.Sleep(500);
            }

            // Re-stand the server pointing at the same streamLogDir. The BTree is rebuilt by
            // scanning the recovered Tsavorite log.
            using (var secondServer = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true,
                streamLogDir: saveDir, tryRecover: true))
            {
                secondServer.Start();
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                var db = redis.GetDatabase(0);

                ClassicAssert.AreEqual(entryCount, db.StreamLength(streamKey),
                    "Recovered stream length must match what was added");

                var range = db.StreamRange(streamKey, "-", "+");
                ClassicAssert.AreEqual(entryCount, range.Length);
                for (int i = 0; i < entryCount; i++)
                {
                    ClassicAssert.AreEqual(addedIds[i], range[i].Id.ToString(),
                        $"Entry {i} ID mismatch after recovery");
                }
            }

            // Re-create the auto-setup server so [TearDown] has something to dispose.
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();
        }

        [Test]
        [Category("Persistence")]
        public void StreamRecoveryPreservesDeletesAndTrim()
        {
            // Add 20 entries, XDEL a few, XTRIM MAXLEN to drop the oldest, restart, and
            // confirm the recovered stream matches what was visible before the restart —
            // i.e. control records (tombstones and trim markers) survive recovery.
            var saveDir = System.IO.Path.Combine(TestUtils.MethodTestDir, "streams");

            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);

            const string streamKey = "delAndTrim";
            const int initialCount = 20;
            // Entries XDEL'd before the trim; the trim will remove some of these implicitly
            // anyway, but we want to prove the explicit delete is recorded.
            var deletedIds = new[] { "5-0", "12-0" };
            // Trim to keep the newest 8.
            const int trimTo = 8;

            using (var firstServer = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true,
                streamLogDir: saveDir))
            {
                firstServer.Start();
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                var db = redis.GetDatabase(0);
                var s = redis.GetServers()[0];

                for (int i = 0; i < initialCount; i++)
                {
                    db.StreamAdd(streamKey, $"f{i}", $"v{i}", $"{i + 1}-0");
                }

                foreach (var id in deletedIds)
                {
                    var deleted = db.StreamDelete(streamKey, [(RedisValue)id]);
                    ClassicAssert.AreEqual(1, deleted, $"XDEL {id} should remove one entry");
                }

                // After 2 deletes from 20 entries, validCount = 18. After XTRIM MAXLEN 8,
                // validCount = 8 (only the newest 8 ids: 13-0 through 20-0).
                long trimmed = db.StreamTrim(streamKey, trimTo);
                ClassicAssert.AreEqual(10, trimmed, "XTRIM should remove 18 - 8 = 10 entries");
                ClassicAssert.AreEqual(trimTo, db.StreamLength(streamKey));

                s.Execute("SAVE");
                System.Threading.Thread.Sleep(500);
            }

            using (var secondServer = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true,
                streamLogDir: saveDir, tryRecover: true))
            {
                secondServer.Start();
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                var db = redis.GetDatabase(0);

                ClassicAssert.AreEqual(trimTo, db.StreamLength(streamKey),
                    "Recovered length must match — without tombstone/trim markers, validCount would balloon back to 20");

                var range = db.StreamRange(streamKey, "-", "+");
                ClassicAssert.AreEqual(trimTo, range.Length);
                // The surviving entries are 13-0 .. 20-0 (the newest 8).
                for (int i = 0; i < trimTo; i++)
                {
                    var expectedId = $"{initialCount - trimTo + i + 1}-0";
                    ClassicAssert.AreEqual(expectedId, range[i].Id.ToString(),
                        $"Entry {i} id mismatch after recovery");
                }
            }

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();
        }

        [Test]
        [Category("Persistence")]
        public void StreamFlushDbRemovesInMemoryAndOnDisk(
            [Values("FLUSHDB", "FLUSHALL")] string flushCmd)
        {
            // FLUSHDB / FLUSHALL must wipe streams along with the main store: dispose each
            // StreamObject (frees its BTree and closes its TsavoriteLog) and remove the
            // per-stream subdirectory so a subsequent recovery sees nothing.
            var saveDir = System.IO.Path.Combine(TestUtils.MethodTestDir, "streams");

            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);

            const string streamA = "flushA";
            const string streamB = "flushB";

            // Pre-FLUSH: add entries, SAVE so data is on disk, capture the on-disk paths.
            string dirA, dirB;
            using (var firstServer = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true,
                streamLogDir: saveDir))
            {
                firstServer.Start();
                using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
                {
                    var db = redis.GetDatabase(0);
                    var s = redis.GetServers()[0];

                    for (int i = 0; i < 10; i++)
                    {
                        db.StreamAdd(streamA, $"f{i}", $"v{i}");
                        db.StreamAdd(streamB, $"f{i}", $"v{i}");
                    }

                    s.Execute("SAVE");
                    System.Threading.Thread.Sleep(500);

                    // Hex of the UTF-8 key bytes — matches the subdir naming in StreamObject.CreateForKey.
                    dirA = System.IO.Path.Combine(saveDir,
                        Convert.ToHexString(System.Text.Encoding.UTF8.GetBytes(streamA)));
                    dirB = System.IO.Path.Combine(saveDir,
                        Convert.ToHexString(System.Text.Encoding.UTF8.GetBytes(streamB)));

                    ClassicAssert.IsTrue(System.IO.Directory.Exists(dirA),
                        "Expected on-disk dir for stream A after SAVE");
                    ClassicAssert.IsTrue(System.IO.Directory.Exists(dirB),
                        "Expected on-disk dir for stream B after SAVE");

                    // Trigger the flush.
                    s.Execute(flushCmd);

                    // In-memory state: streams are gone, XLEN reports 0.
                    ClassicAssert.AreEqual(0, db.StreamLength(streamA));
                    ClassicAssert.AreEqual(0, db.StreamLength(streamB));
                }

                // On-disk state: per-stream subdirs were deleted.
                ClassicAssert.IsFalse(System.IO.Directory.Exists(dirA),
                    "Stream A on-disk directory should be removed by " + flushCmd);
                ClassicAssert.IsFalse(System.IO.Directory.Exists(dirB),
                    "Stream B on-disk directory should be removed by " + flushCmd);
            }

            // Tear down and recover from the same dir: nothing should come back.
            using (var secondServer = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true,
                streamLogDir: saveDir, tryRecover: true))
            {
                secondServer.Start();
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual(0, db.StreamLength(streamA),
                    "Recovery must not bring back a flushed stream");
                ClassicAssert.AreEqual(0, db.StreamLength(streamB));
            }

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();
        }

        [Test]
        [Category("Persistence")]
        public void StreamDeleteRemovesInMemoryAndOnDisk(
            [Values("DEL", "UNLINK")] string delCmd,
            [Values(true, false)] bool diskBacked)
        {
            // DEL / UNLINK must wipe a single stream completely: drop the dictionary entry,
            // dispose the StreamObject (free BTree, close TsavoriteLog + device, drop consumer
            // groups), and delete the per-stream on-disk subdirectory when one exists. Other
            // streams in the same server must be untouched.
            var saveDir = diskBacked ? System.IO.Path.Combine(TestUtils.MethodTestDir, "streams") : null;

            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);

            const string streamA = "delA";
            const string streamB = "delB";
            const string missing = "doesNotExist";
            const string stringKey = "plainString";

            using (var test = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true,
                streamLogDir: saveDir))
            {
                test.Start();
                using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
                {
                    var db = redis.GetDatabase(0);
                    var s = redis.GetServers()[0];

                    // Populate two streams (with a consumer group on A) and a plain string.
                    for (int i = 0; i < 5; i++)
                    {
                        db.StreamAdd(streamA, $"f{i}", $"v{i}");
                        db.StreamAdd(streamB, $"f{i}", $"v{i}");
                    }
                    db.Execute("XGROUP", "CREATE", streamA, "g1", "0");
                    db.StringSet(stringKey, "value");

                    string dirA = null, dirB = null;
                    if (diskBacked)
                    {
                        s.Execute("SAVE");
                        System.Threading.Thread.Sleep(500);

                        dirA = System.IO.Path.Combine(saveDir,
                            Convert.ToHexString(System.Text.Encoding.UTF8.GetBytes(streamA)));
                        dirB = System.IO.Path.Combine(saveDir,
                            Convert.ToHexString(System.Text.Encoding.UTF8.GetBytes(streamB)));
                        ClassicAssert.IsTrue(System.IO.Directory.Exists(dirA), "stream A dir should exist before DEL");
                        ClassicAssert.IsTrue(System.IO.Directory.Exists(dirB), "stream B dir should exist before DEL");
                    }

                    // 1) DEL a single existing stream — returns 1.
                    var deleted = (int)db.Execute(delCmd, streamA);
                    ClassicAssert.AreEqual(1, deleted, $"{delCmd} on existing stream should return 1");

                    // 2) The stream is gone in memory: XLEN reports 0 and consumer-group state is
                    //    dropped. XINFO GROUPS against a missing key errors out per Redis, which
                    //    is the strongest signal that the dictionary entry — and therefore the
                    //    consumerGroups dictionary on the StreamObject — is gone.
                    ClassicAssert.AreEqual(0, db.StreamLength(streamA), "Deleted stream must be empty");
                    var infoGroupsEx = Assert.Throws<RedisServerException>(
                        () => db.Execute("XINFO", "GROUPS", streamA),
                        "XINFO GROUPS on a DELed stream should error: the key no longer exists");
                    StringAssert.Contains("no such key", infoGroupsEx.Message);

                    // 3) On disk: dir for A is gone, dir for B is preserved (DEL is per-key).
                    if (diskBacked)
                    {
                        ClassicAssert.IsFalse(System.IO.Directory.Exists(dirA),
                            "Stream A on-disk directory should be removed by " + delCmd);
                        ClassicAssert.IsTrue(System.IO.Directory.Exists(dirB),
                            "Stream B on-disk directory must survive a DEL on A");
                    }

                    // 4) Other streams are unaffected.
                    ClassicAssert.AreEqual(5, db.StreamLength(streamB),
                        "Stream B must be untouched by DEL on A");

                    // 5) DEL a missing key — returns 0.
                    var notFound = (int)db.Execute(delCmd, missing);
                    ClassicAssert.AreEqual(0, notFound, $"{delCmd} on missing key returns 0");

                    // 6) Multi-arg DEL spanning a stream, a string, and a missing key — count = 2.
                    var multi = (int)db.Execute(delCmd, streamB, stringKey, missing);
                    ClassicAssert.AreEqual(2, multi, $"{delCmd} of [stream, string, missing] should return 2");
                    ClassicAssert.AreEqual(0, db.StreamLength(streamB));
                    ClassicAssert.IsFalse(db.KeyExists(stringKey));

                    // 7) Session-cache invalidation: the same connection had populated its cache via
                    //    XADD on A; XLEN after DEL must observe the eviction and return 0 (already
                    //    asserted above, but confirm a fresh XADD recreates and works).
                    db.StreamAdd(streamA, "fresh", "value");
                    ClassicAssert.AreEqual(1, db.StreamLength(streamA),
                        "Re-creating a stream by the same name after DEL must work");
                }
            }

            // 8) Restart from the same on-disk root — the deleted streams must not come back.
            if (diskBacked)
            {
                using (var restart = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true,
                    streamLogDir: saveDir, tryRecover: true))
                {
                    restart.Start();
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                    var db = redis.GetDatabase(0);
                    // streamB was deleted in step 6; streamA was re-created in step 7 (but we
                    // didn't SAVE again, so the post-recreate entry won't survive recovery).
                    ClassicAssert.AreEqual(0, db.StreamLength(streamB),
                        "Recovery must not resurrect a DELed stream");
                }
            }

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();
        }

        [Test]
        [Category("Persistence")]
        public void StreamObjectDisposeIsIdempotent()
        {
            // Dispose must be safe to call twice. BTree.Deallocate frees native memory and would
            // crash on a double free — but a SessionStreamCache may hold a stale reference to a
            // disposed StreamObject until eviction, and a future change that decides to clean up
            // such references could trigger a second Dispose. The idempotency guard on the
            // disposed flag is what makes that safe.
            var stream = new Garnet.server.StreamObject(
                streamsRootDir: null, streamDirName: null, pageSize: 4096, memorySize: 8192);
            stream.Dispose();
            ClassicAssert.IsTrue(stream.IsDisposed);
            // The interesting bit: this must not crash, double-free, or throw.
            Assert.DoesNotThrow(() => stream.Dispose());
            ClassicAssert.IsTrue(stream.IsDisposed);
        }
        #endregion

        #region BenchmarkRepro

        [Test]
        public void StreamXRangeExplicitIdsScanCrashRepro()
        {
            // Reproduces the crash from StreamBenchmark:
            // "Invalid length of record found: 83886080 at address 252"
            // Prepopulate with explicit IDs like the benchmark, then XRANGE.
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var streamKey = "benchRepro";

            const int count = 1000;
            // Add entries with explicit IDs 1-0 through count-0
            for (int i = 1; i <= count; i++)
            {
                db.Execute("XADD", streamKey, $"{i}-0", "f", "v");
            }

            // Do XRANGE over various windows (including the one that crashed at ID 5-0)
            for (int start = 1; start <= count - 50; start += 47)
            {
                int end = start + 49;
                var result = db.Execute("XRANGE", streamKey, $"{start}-0", $"{end}-0", "COUNT", "50");
                ClassicAssert.IsNotNull(result);
            }

            // Specifically test the range that includes the problematic ID 5-0
            var result2 = db.Execute("XRANGE", streamKey, "1-0", "10-0");
            ClassicAssert.IsNotNull(result2);
        }

        [Test]
        public void StreamXRangeSequentialPageBoundaryScanRepro()
        {
            // Tests XRANGE across page boundaries (100K entries span 2 pages at 4MB/page).
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var streamKey = "pageBoundaryScan";

            const int count = 100000;
            for (int i = 1; i <= count; i++)
            {
                db.Execute("XADD", streamKey, $"{i}-0", "f", "v");
            }

            var len = (long)db.Execute("XLEN", streamKey);
            ClassicAssert.AreEqual(count, len, "Prepopulation failed");

            // Scan within page 0 (should be safe)
            var result0 = db.Execute("XRANGE", streamKey, "1-0", "100-0");
            ClassicAssert.IsNotNull(result0, "Page 0 scan failed");

            // Cross-page scan — use new connection in case first one dies
            try
            {
                using var redis2 = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                var db2 = redis2.GetDatabase(0);
                var result2 = db2.Execute("XRANGE", streamKey, "95300-0", "95350-0");
                ClassicAssert.IsNotNull(result2, "Cross-page scan failed");
            }
            catch (Exception ex)
            {
                ClassicAssert.Fail($"Cross-page scan threw: {ex.Message}");
            }
        }

        [Test]
        public void StreamXRangeConcurrentScanCrashRepro()
        {
            // Tests concurrent XRANGE like the benchmark (16 threads)
            // The crash was "Invalid length of record found: 83886080 at address 252"
            // which suggests scanner misalignment during page transitions with ~100K entries.
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var streamKey = "benchReproConcurrent";

            const int count = 100000;
            // Add entries
            for (int i = 1; i <= count; i++)
            {
                db.Execute("XADD", streamKey, $"{i}-0", "f", "v");
            }

            // Verify prepopulation
            var len = (long)db.Execute("XLEN", streamKey);
            ClassicAssert.AreEqual(count, len, "Prepopulation failed");

            // Concurrent XRANGE from multiple connections (like the benchmark)
            var exceptions = new System.Collections.Concurrent.ConcurrentBag<Exception>();
            var tasks = new Task[16];
            for (int t = 0; t < 16; t++)
            {
                int seed = 42 + t;
                tasks[t] = Task.Run(() =>
                {
                    try
                    {
                        using var conn = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                        var tdb = conn.GetDatabase(0);
                        var localRng = new Random(seed);
                        for (int i = 0; i < 500; i++)
                        {
                            int s = 1 + localRng.Next(count - 50);
                            int e = s + 49;
                            tdb.Execute("XRANGE", streamKey, $"{s}-0", $"{e}-0", "COUNT", "50");
                        }
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                });
            }
            Task.WaitAll(tasks);
            ClassicAssert.IsEmpty(exceptions, $"Got {exceptions.Count} exceptions: {string.Join("; ", exceptions.Select(e => e.Message).Take(5))}");
        }

        #endregion

        #region Consumer Group Tests

        [Test]
        public async Task XGroupCreateAndDestroyTest()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Add entry first
            await c.ExecuteAsync("XADD", "cg-stream", "*", "field1", "value1");

            // Create a consumer group
            var result = await c.ExecuteAsync("XGROUP", "CREATE", "cg-stream", "mygroup", "0-0");
            ClassicAssert.AreEqual("OK", result);

            // Destroy the group
            result = await c.ExecuteAsync("XGROUP", "DESTROY", "cg-stream", "mygroup");
            ClassicAssert.AreEqual("1", result);

            // Destroy again should return 0
            result = await c.ExecuteAsync("XGROUP", "DESTROY", "cg-stream", "mygroup");
            ClassicAssert.AreEqual("0", result);
        }

        [Test]
        public async Task XGroupCreateConsumerAndDelConsumerTest()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            await c.ExecuteAsync("XADD", "cg-consumers", "*", "f1", "v1");
            await c.ExecuteAsync("XGROUP", "CREATE", "cg-consumers", "grp1", "0-0");

            // Create consumer
            var result = await c.ExecuteAsync("XGROUP", "CREATECONSUMER", "cg-consumers", "grp1", "myConsumer");
            ClassicAssert.AreEqual("1", result);

            // Create same consumer again
            result = await c.ExecuteAsync("XGROUP", "CREATECONSUMER", "cg-consumers", "grp1", "myConsumer");
            ClassicAssert.AreEqual("0", result);

            // Delete consumer
            result = await c.ExecuteAsync("XGROUP", "DELCONSUMER", "cg-consumers", "grp1", "myConsumer");
            ClassicAssert.AreEqual("0", result); // 0 pending entries removed
        }

        [Test]
        public async Task XReadGroupBasicTest()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            await c.ExecuteAsync("XADD", "cg-readgroup", "*", "f1", "v1");
            await c.ExecuteAsync("XADD", "cg-readgroup", "*", "f2", "v2");
            await c.ExecuteAsync("XADD", "cg-readgroup", "*", "f3", "v3");

            await c.ExecuteAsync("XGROUP", "CREATE", "cg-readgroup", "grp1", "0-0");

            // Read new entries with ">"
            var result = await c.ExecuteForArrayAsync("XREADGROUP", "GROUP", "grp1", "consumer1", "COUNT", "2", "STREAMS", "cg-readgroup", ">");
            ClassicAssert.IsNotNull(result);
            ClassicAssert.IsTrue(result.Length > 0); // Multi-stream format wraps results
        }

        [Test]
        public async Task XAckTest()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            var id1 = await c.ExecuteAsync("XADD", "cg-ack", "*", "f1", "v1");
            var id2 = await c.ExecuteAsync("XADD", "cg-ack", "*", "f2", "v2");

            await c.ExecuteAsync("XGROUP", "CREATE", "cg-ack", "grp1", "0-0");

            // Read all entries
            await c.ExecuteForArrayAsync("XREADGROUP", "GROUP", "grp1", "consumer1", "STREAMS", "cg-ack", ">");

            // Acknowledge first entry
            var result = await c.ExecuteAsync("XACK", "cg-ack", "grp1", id1);
            ClassicAssert.AreEqual("1", result);

            // Acknowledge second
            result = await c.ExecuteAsync("XACK", "cg-ack", "grp1", id2);
            ClassicAssert.AreEqual("1", result);

            // Acknowledge non-existent
            result = await c.ExecuteAsync("XACK", "cg-ack", "grp1", "999-999");
            ClassicAssert.AreEqual("0", result);
        }

        [Test]
        public async Task XReadGroupPendingRereadTest()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            await c.ExecuteAsync("XADD", "cg-pending", "*", "f1", "v1");
            await c.ExecuteAsync("XADD", "cg-pending", "*", "f2", "v2");

            await c.ExecuteAsync("XGROUP", "CREATE", "cg-pending", "grp1", "0-0");

            // Deliver to consumer
            await c.ExecuteForArrayAsync("XREADGROUP", "GROUP", "grp1", "consumer1", "STREAMS", "cg-pending", ">");

            // Re-read pending entries with "0"
            var result = await c.ExecuteForArrayAsync("XREADGROUP", "GROUP", "grp1", "consumer1", "STREAMS", "cg-pending", "0");
            ClassicAssert.IsNotNull(result);
            ClassicAssert.IsTrue(result.Length > 0); // Should have pending entries
        }

        [Test]
        public async Task XReadGroupNoAckTest()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            await c.ExecuteAsync("XADD", "cg-noack", "*", "f1", "v1");
            await c.ExecuteAsync("XGROUP", "CREATE", "cg-noack", "grp1", "0-0");

            // Read with NOACK
            await c.ExecuteForArrayAsync("XREADGROUP", "GROUP", "grp1", "consumer1", "NOACK", "STREAMS", "cg-noack", ">");

            // No pending entries since NOACK was used — re-read should give empty entries
            var result = await c.ExecuteForArrayAsync("XREADGROUP", "GROUP", "grp1", "consumer1", "STREAMS", "cg-noack", "0");
            // Result should still be valid (not error)
            ClassicAssert.IsNotNull(result);
        }

        [Test]
        public async Task XInfoStreamTest()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            await c.ExecuteAsync("XADD", "cg-info", "*", "f1", "v1");
            await c.ExecuteAsync("XADD", "cg-info", "*", "f2", "v2");
            await c.ExecuteAsync("XGROUP", "CREATE", "cg-info", "grp1", "0-0");

            var result = await c.ExecuteForArrayAsync("XINFO", "STREAM", "cg-info");
            ClassicAssert.IsNotNull(result);
            ClassicAssert.IsTrue(result.Length >= 12); // at least 6 field-value pairs
        }

        [Test]
        public async Task XInfoGroupsTest()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            await c.ExecuteAsync("XADD", "cg-infogroups", "*", "f1", "v1");
            await c.ExecuteAsync("XGROUP", "CREATE", "cg-infogroups", "grp1", "0-0");
            await c.ExecuteAsync("XGROUP", "CREATE", "cg-infogroups", "grp2", "$");

            var result = await c.ExecuteForArrayAsync("XINFO", "GROUPS", "cg-infogroups");
            ClassicAssert.IsNotNull(result);
            ClassicAssert.AreEqual(2, result.Length);
        }

        [Test]
        public async Task XReadBasicTest()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            var id1 = await c.ExecuteAsync("XADD", "cg-xread", "*", "f1", "v1");
            await c.ExecuteAsync("XADD", "cg-xread", "*", "f2", "v2");
            await c.ExecuteAsync("XADD", "cg-xread", "*", "f3", "v3");

            // Read entries after id1 via XREAD
            var result = await c.ExecuteForArrayAsync("XREAD", "COUNT", "10", "STREAMS", "cg-xread", id1);
            ClassicAssert.IsNotNull(result);
        }

        [Test]
        public async Task XReadReturnsNullWhenNoNewEntries()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            var lastId = await c.ExecuteAsync("XADD", "xr-empty", "*", "f1", "v1");

            // Reading after the last ID yields no new entries -> Redis replies with a null array.
            var result = await c.ExecuteForArrayAsync("XREAD", "STREAMS", "xr-empty", lastId);
            ClassicAssert.IsNull(result);
        }

        [Test]
        public async Task XReadReturnsNullWhenAllStreamsEmpty()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            var a = await c.ExecuteAsync("XADD", "xr-all-a", "*", "f1", "v1");
            var b = await c.ExecuteAsync("XADD", "xr-all-b", "*", "f1", "v1");

            // Both read past their last IDs -> no stream has new entries -> null.
            var result = await c.ExecuteForArrayAsync("XREAD", "STREAMS", "xr-all-a", "xr-all-b", a, b);
            ClassicAssert.IsNull(result);
        }

        [Test]
        public async Task XReadOmitsStreamsWithNoNewEntries()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            await c.ExecuteAsync("XADD", "xr-a", "*", "f1", "v1");
            await c.ExecuteAsync("XADD", "xr-b", "*", "f1", "v1");
            var lastB = await c.ExecuteAsync("XADD", "xr-b", "*", "f2", "v2");

            // xr-a from 0 has entries; xr-b read past its last ID has none and must be omitted.
            var result = await c.ExecuteForArrayAsync("XREAD", "STREAMS", "xr-a", "xr-b", "0", lastB);
            ClassicAssert.IsNotNull(result);
            ClassicAssert.AreEqual(1, result.Length); // only xr-a returned; xr-b omitted
            ClassicAssert.IsTrue(result[0].Contains("xr-a"));
            ClassicAssert.IsTrue(result[0].Contains("v1")); // entry data is intact (no truncation/garbage)
        }

        #endregion
    }
}