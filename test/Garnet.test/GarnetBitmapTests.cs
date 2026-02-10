// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics.Tensors;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    public class GarnetBitmapTests
    {
        GarnetServer server;
        Random rng;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            var useReviv = false;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is RevivificationMode revivMode)
                {
                    useReviv = revivMode == RevivificationMode.UseReviv;
                    continue;
                }
            }

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useReviv: useReviv);
            server.Start();
            rng = new Random(674386);
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        private long LongRandom() => rng.NextInt64(long.MinValue, long.MaxValue);

        private ulong ULongRandom() => (ulong)LongRandom();

        private unsafe long ResponseToLong(byte[] response, int offset)
        {
            fixed (byte* ptr = response)
                return NumUtils.BytesToLong(ptr + offset);
        }

        [Test, Order(1)]
        [Category("SETBIT")]
        public void BitmapSetBitResponseTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "setResponseTest";
            ClassicAssert.IsFalse(db.StringSetBit(key, 7, true));
            ClassicAssert.IsFalse(db.StringSetBit(key, 14, true));
            ClassicAssert.IsFalse(db.StringSetBit(key, 37, true));
            ClassicAssert.IsFalse(db.StringSetBit(key, 144, true));
            ClassicAssert.IsFalse(db.StringSetBit(key, 777, true));
            ClassicAssert.IsFalse(db.StringSetBit(key, 1444, true));
            ClassicAssert.IsFalse(db.StringSetBit(key, 9999, true));


            ClassicAssert.IsTrue(db.StringSetBit(key, 7, true));
            ClassicAssert.IsTrue(db.StringSetBit(key, 14, true));
            ClassicAssert.IsTrue(db.StringSetBit(key, 37, true));
            ClassicAssert.IsTrue(db.StringSetBit(key, 144, true));
            ClassicAssert.IsTrue(db.StringSetBit(key, 777, true));
            ClassicAssert.IsTrue(db.StringSetBit(key, 1444, true));
            ClassicAssert.IsTrue(db.StringSetBit(key, 9999, true));

            ClassicAssert.IsTrue(db.StringGetBit(key, 7));
            ClassicAssert.IsFalse(db.StringGetBit(key, 8));
            ClassicAssert.IsTrue(db.StringGetBit(key, 14));
            ClassicAssert.IsFalse(db.StringGetBit(key, 15));

            ClassicAssert.IsTrue(db.StringGetBit(key, 37));
            ClassicAssert.IsFalse(db.StringGetBit(key, 42));
            ClassicAssert.IsFalse(db.StringGetBit(key, 52));

            ClassicAssert.IsTrue(db.StringGetBit(key, 144));
            ClassicAssert.IsFalse(db.StringGetBit(key, 164));
            ClassicAssert.IsFalse(db.StringGetBit(key, 174));

            ClassicAssert.IsTrue(db.StringGetBit(key, 777));
            ClassicAssert.IsFalse(db.StringGetBit(key, 888));
            ClassicAssert.IsFalse(db.StringGetBit(key, 999));

            ClassicAssert.IsTrue(db.StringGetBit(key, 1444));
            ClassicAssert.IsFalse(db.StringGetBit(key, 2444));
            ClassicAssert.IsFalse(db.StringGetBit(key, 3444));
            ClassicAssert.IsFalse(db.StringGetBit(key, 4444));

            ClassicAssert.IsFalse(db.StringGetBit(key, 6999));
            ClassicAssert.IsFalse(db.StringGetBit(key, 7999));
            ClassicAssert.IsFalse(db.StringGetBit(key, 8999));
            ClassicAssert.IsTrue(db.StringGetBit(key, 9999));

            var ex = Assert.Throws<RedisServerException>(() => db.Execute("SETBIT", key, "-1", "1"));
            ClassicAssert.AreEqual("ERR bit offset is not an integer or out of range",
                                   ex.Message);
        }

        [Test, Order(2)]
        [Category("GETBIT")]
        public void BitmapGetBitResponseTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "getResponseTest";
            for (long i = 0; i < (1 << 5); i++)
            {
                ClassicAssert.IsFalse(db.StringGetBit(key, i));
            }

            var ex = Assert.Throws<RedisServerException>(() => db.Execute("GETBIT", key, "-1"));
            ClassicAssert.AreEqual("ERR bit offset is not an integer or out of range",
                                   ex.Message);
        }

        [Test, Order(3)]
        [Category("SET+GET+BIT")]
        public void BitmapSetGetBitResponseTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "setGetResponseTest";
            long span = 1 << 10;
            for (long i = 0; i < span; i += 2)
            {
                ClassicAssert.IsFalse(db.StringSetBit(key, i, true));
            }

            for (long i = 0; i < span; i += 2)
            {
                ClassicAssert.IsTrue(db.StringGetBit(key, i));
                ClassicAssert.IsFalse(db.StringSetBit(key, i + 1, true));
            }

            for (long i = 0; i < span; i += 2)
            {
                ClassicAssert.IsTrue(db.StringSetBit(key, i, false));
                ClassicAssert.IsFalse(db.StringGetBit(key, i));
            }
        }

        [Test, Order(4)]
        [TestCase(10)]
        [TestCase(20)]
        [TestCase(30)]
        public void BitmapSimpleSetGet_PCT(int bytesPerSend)
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            var expectedResponse = ":0\r\n";
            var response = lightClientRequest.SendCommandChunks("SETBIT mykey 7 1", bytesPerSend);
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            expectedResponse = ":1\r\n";
            response = lightClientRequest.SendCommandChunks("GETBIT mykey 7", bytesPerSend);
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test, Order(5)]
        [TestCase(false)]
        [TestCase(true)]
        [Category("SET+GET+BIT")]
        public void BitmapSetGetBitTest_LTM(bool preSet)
        {
            int bitmapBytes = 512;
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int keyCount = 8;
            int keyIter = 256;
            byte[] bitmap = new byte[bitmapBytes];
            Dictionary<int, Dictionary<long, bool>> state = [];

            if (preSet)
            {
                for (int i = 0; i < bitmapBytes; i++)
                    bitmap[i] = 0;
                for (int i = 0; i < keyCount; i++)
                {
                    string sKey = i.ToString();
                    db.StringSet(sKey, bitmap);
                }
            }

            //1. test SETBIT
            for (int i = 0; i < keyCount; i++)
            {
                int key = i;
                string sKey = key.ToString();

                for (int j = 0; j < keyIter; j++)
                {
                    long offset = rng.Next(0, bitmapBytes << 3);
                    bool set = rng.Next(0, 1) == 0 ? false : true;

                    bool returnedVal = db.StringSetBit(sKey, offset, set);
                    bool expectedVal = false;

                    if (state.ContainsKey(key) && state[key].ContainsKey(offset))
                    {
                        expectedVal = state[key][offset];
                        state[key][offset] = set;
                    }
                    else if (state.ContainsKey(key))
                    {
                        state[key].Add(offset, set);
                    }
                    else
                    {
                        state.Add(key, []);
                        state[key].Add(offset, set);
                    }

                    ClassicAssert.AreEqual(returnedVal, expectedVal);
                }
            }

            //2. Test GETBIT
            for (int i = 0; i < keyCount; i++)
            {
                int key = i;
                string sKey = key.ToString();

                for (int j = 0; j < keyIter; j++)
                {
                    long offset = rng.Next(0, bitmapBytes << 3);
                    bool returnedVal = db.StringGetBit(sKey, offset);
                    bool expectedVal = false;
                    if (state.ContainsKey(key) && state[key].ContainsKey(offset))
                        expectedVal = state[key][offset];
                    ClassicAssert.AreEqual(expectedVal, returnedVal, $"offset {offset}");
                }
            }
        }

        [Test, Order(6)]
        [Category("BITCOUNT")]
        [TestCase("DOTNET_EnableAVX2", "0")]
        [TestCase("DOTNET_EnableHWIntrinsic", "1")]
        [TestCase("DOTNET_EnableHWIntrinsic", "0")]
        public void BitmapSimpleBitCountTest(string arg, string val)
        {
            using var server = new GarnetServerTestProcess(new() { [arg] = val });
            try
            {
                using var redis = ConnectionMultiplexer.Connect(server.Options);

                var db = redis.GetDatabase(0);
                var maxBitmapLen = 1 << 12;
                var iter = 1024;
                var expectedCount = 0;
                var key = "SimpleBitCountTest";

                for (var i = 0; i < iter; i++)
                {
                    var offset = rng.Next(1, maxBitmapLen);
                    var set = !db.StringSetBit(key, offset, true);
                    expectedCount += set ? 1 : 0;
                }

                var count = db.StringBitCount(key);
                ClassicAssert.AreEqual(expectedCount, count);
            }
            catch
            {
                server.RecordTestOutput();
                throw;
            }
        }

        private static int Index(long offset) => (int)(offset >> 3);

        private static unsafe long Count(byte[] bitmap, int startOffset = 0, int endOffset = -1)
        {
            fixed (byte* b = bitmap)
                return Count(b, bitmap.Length, startOffset, endOffset);
        }

        private static unsafe long Count(byte* bitmap, int bitmapLen, int startOffset = 0, int endOffset = -1)
        {
            long count = 0;
            int start = startOffset < 0 ? (startOffset % bitmapLen) + bitmapLen : startOffset;
            int end = endOffset < 0 ? (endOffset % bitmapLen) + bitmapLen : endOffset;

            if (start >= bitmapLen) // If startOffset greater that valLen always bitcount zero
                return 0;

            if (start > end) // If start offset beyond endOffset return 0
                return 0;

            for (int i = start; i < end + 1; i++)
            {
                byte byteVal = bitmap[i];
                count += (byteVal & 1);
                count += ((byteVal & 2) >> 1);
                count += ((byteVal & 4) >> 2);
                count += ((byteVal & 8) >> 3);
                count += ((byteVal & 16) >> 4);
                count += ((byteVal & 32) >> 5);
                count += ((byteVal & 64) >> 6);
                count += ((byteVal & 128) >> 7);
            }
            return count;
        }

        [Test, Order(7)]
        [Category("BITCOUNT")]
        public void BitmapBitCountBetweenOffsetsTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "BitCountBetweenOffsetsTest";

            long count = db.StringBitCount(key);
            ClassicAssert.AreEqual(count, 0);

            int maxBitmapLen = 1 << 12;
            int iter = 1024;

            List<long> offsets = [];
            long maxOffset = 0;
            for (int i = 0; i < iter; i++)
            {
                long offset = rng.Next(1, maxBitmapLen);
                db.StringSetBit(key, offset, true);
                maxOffset = Math.Max(offset, maxOffset);
                offsets.Add(offset);
            }

            long maxSizeInBytes = (maxOffset >> 3) + 1;
            byte[] bitmap = new byte[maxSizeInBytes];
            for (int i = 0; i < iter; i++)
            {
                long offset = offsets[i];
                int byteIndex = Index(offset);
                int bitIndex = (int)(offset & 7);

                byte byteVal = bitmap[byteIndex];
                byteVal = (byte)((byteVal & ~(1 << bitIndex)) | (1 << bitIndex));
                bitmap[byteIndex] = byteVal;
            }

            long expectedCount = Count(bitmap, 0, -1);
            count = db.StringBitCount(key, 0, -1);
            ClassicAssert.AreEqual(expectedCount, count, $"startOffset {0}, endOffset {-1}, bitmapLength {bitmap.Length}");

            //Test with startOffset
            for (int i = 0; i < iter; i++)
            {
                int startOffset = rng.Next(1, (int)maxSizeInBytes);
                expectedCount = Count(bitmap, startOffset, -1);
                count = db.StringBitCount(key, startOffset);

                ClassicAssert.AreEqual(expectedCount, count, $"startOffset {startOffset}, endOffset {-1}, maxSizeInBytes {maxSizeInBytes}");
            }

            //Test with startOffset and endOffset
            for (int i = 0; i < iter; i++)
            {
                int startOffset = rng.Next(1, (int)maxSizeInBytes);
                int endOffset = rng.Next(startOffset, (int)maxSizeInBytes);
                expectedCount = Count(bitmap, startOffset, endOffset);
                count = db.StringBitCount(key, startOffset, endOffset);

                ClassicAssert.AreEqual(expectedCount, count, $"startOffset {startOffset}, endOffset {endOffset}, maxSizeInBytes {maxSizeInBytes}");
            }
        }

        [Test, Order(8)]
        [Category("BITCOUNT")]
        public void BitmapBitCountBetweenOffsetsTestV2()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "BitCountBetweenOffsetsTestV2";

            long count = db.StringBitCount(key);
            ClassicAssert.AreEqual(count, 0);

            int maxBitmapLen = 1 << 12;
            int iter = 1 << 5;
            byte[] buf = new byte[maxBitmapLen >> 3];

            for (int j = 0; j < iter; j++)
            {
                for (int i = 0; i < buf.Length; i++)
                    buf[i] = (byte)rng.Next(0, 128);

                db.StringSet(key, buf);

                int startOffset = rng.Next(1, buf.Length);
                int endOffset = rng.Next(startOffset, buf.Length);

                long expectedCount = Count(buf, startOffset, endOffset);
                count = db.StringBitCount(key, startOffset, endOffset);

                ClassicAssert.AreEqual(expectedCount, count, $"{startOffset} {endOffset}");
            }
        }

        [Test, Order(9)]
        [Category("BITCOUNT")]
        public void BitmapBitCountNegativeOffsets()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "BitmapBitCountNegativeOffsets";
            int maxBitmapLen = 1 << 12;
            int maxByteLen = maxBitmapLen >> 3;
            int iter = 1 << 5;
            byte[] buf = new byte[maxByteLen];
            long expectedCount;
            long count;

            //check offsets in range
            for (int j = 0; j < iter; j++)
            {
                rng.NextBytes(buf);
                db.StringSet(key, buf);

                int startOffset = j == 0 ? -10 : rng.Next(-maxByteLen, 0);
                int endOffset = j == 0 ? -1 : rng.Next(startOffset, 0);

                expectedCount = Count(buf, startOffset, endOffset);
                count = db.StringBitCount(key, startOffset, endOffset);

                ClassicAssert.AreEqual(expectedCount, count, $"{startOffset} {endOffset}");
            }

            //check negative offsets beyond range
            for (int j = 0; j < iter; j++)
            {
                rng.NextBytes(buf);
                db.StringSet(key, buf);

                int startOffset = j == 0 ? -10 : rng.Next(-maxByteLen << 1, -maxByteLen);
                int endOffset = j == 0 ? -1 : rng.Next(startOffset, -maxByteLen);

                expectedCount = Count(buf, startOffset, endOffset);
                count = db.StringBitCount(key, startOffset, endOffset);

                ClassicAssert.AreEqual(expectedCount, count, $"{startOffset} {endOffset}");
            }
        }

        [Test, Order(10)]
        [Category("BITCOUNT")]
        public void BitmapBitCountTest_LTM()
        {
            int bitmapBytes = 512;
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                lowMemory: true,
                pageCount: 2,  // Specify pageCount instead of memorySize to avoid LogSizeTracker.MinTargetPageCount requirement
                pageSize: (bitmapBytes << 1).ToString());
            server.Start();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int keyCount = 64;
            byte[] bitmap = new byte[bitmapBytes];
            List<long> bitmapList = [];

            for (int i = 0; i < keyCount; i++)
            {
                string sKey = i.ToString();
                rng.NextBytes(bitmap);

                bitmapList.Add(Count(bitmap));
                db.StringSet(sKey, bitmap);
            }

            int iter = 128;
            for (int i = 0; i < iter; i++)
            {
                int key = rng.Next(0, keyCount);
                string sKey = key.ToString();
                long count = db.StringBitCount(sKey);
                long expectedCount = bitmapList[key];
                ClassicAssert.AreEqual(expectedCount, count);
            }
        }

        [Test, Order(11)]
        [TestCase(10)]
        [TestCase(20)]
        [TestCase(30)]
        public unsafe void BitmapSimpleBITCOUNT_PCT(int bytesPerSend)
        {
            //*2\r\n$8\r\nBITCOUNT\r\n
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var lightClientRequest = TestUtils.CreateRequest();
            var db = redis.GetDatabase(0);

            string key = "mykey";
            int maxBitmapLen = 1 << 12;
            byte[] buf = new byte[maxBitmapLen >> 3];
            rng.NextBytes(buf);
            db.StringSet(key, buf);

            long expectedCount = Count(buf);
            long count = 0;
            byte[] response = lightClientRequest.SendCommandChunks("BITCOUNT mykey", bytesPerSend);
            count = ResponseToLong(response, 1);
            ClassicAssert.AreEqual(expectedCount, count);
        }

        private static unsafe long Bitpos(byte[] bitmap, int startOffset = 0, int endOffset = -1, bool set = true)
        {
            long pos = 0;
            var start = startOffset < 0 ? (startOffset % bitmap.Length) + bitmap.Length : startOffset;
            var end = endOffset < 0 ? (endOffset % bitmap.Length) + bitmap.Length : endOffset;

            if (start >= bitmap.Length) // If startOffset greater that valLen alway bitcount zero
                return -1;

            if (start > end) // If start offset beyond endOffset return 0
                return -1;

            var mask = (byte)(!set ? 0xFF : 0x00);
            var setbit = set ? 1 : 0;
            fixed (byte* b = bitmap)
            {
                var curr = b + start;
                var vend = b + end + 1;
                while (curr < vend)
                {
                    if (*curr != mask) break;
                    curr++;
                }

                if (curr > vend) return -1;

                pos = (curr - b) << 3;

                var value = *curr;
                for (var i = 7; i >= 0; i--)
                {
                    if (((value & (1 << i)) >> i) == setbit)
                        return pos;
                    pos++;
                }
            }

            return pos;
        }

        [Test, Order(12)]
        [Category("BITPOS")]
        public void BitmapSimpleBitPosTests()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "SimpleBitPosTests";

            byte[] buf;
            var maxBitmapLen = 1 << 10;
            var iter = 256;
            long maxOffset = 0;
            for (var i = 0; i < iter; i++)
            {
                long offset = rng.Next(1, maxBitmapLen);
                _ = db.StringSetBit(key, offset, true);
                buf = db.StringGet(key);

                var offsetPos = db.StringBitPosition(key, true);
                ClassicAssert.AreEqual(offset, offsetPos, $"iter:{i}");

                buf = db.StringGet(key);
                var expectedPos = Bitpos(buf, set: true);
                ClassicAssert.AreEqual(expectedPos, offsetPos, $"iter:{i}");

                _ = db.StringSetBit(key, offset, false);
                maxOffset = Math.Max(maxOffset, offset);
            }

            for (var i = 0; i < maxOffset; i++)
                _ = db.StringSetBit(key, i, true);

            var count = db.StringBitCount(key);
            ClassicAssert.AreEqual(count, maxOffset);

            for (var i = 0; i < iter; i++)
            {
                long offset = rng.Next(1, (int)maxOffset);
                _ = db.StringSetBit(key, offset, false);

                buf = db.StringGet(key);
                var offsetPos = db.StringBitPosition(key, false);
                ClassicAssert.AreEqual(offset, offsetPos, $"iter:{i}");

                buf = db.StringGet(key);
                var expectedPos = Bitpos(buf, set: false);
                ClassicAssert.AreEqual(expectedPos, offsetPos, $"iter:{i}");

                _ = db.StringSetBit(key, offset, true);
            }
        }

        [Test, Order(13)]
        [Category("BITPOS")]
        public void BitmapBitPosOffsetsTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "BitmapBitPosNegativeOffsets";

            var maxBitmapLen = 1 << 12;
            var maxByteLen = maxBitmapLen >> 3;
            var iter = 1 << 5;
            var buf = new byte[maxByteLen];
            long expectedPos;
            long pos;

            for (var j = 0; j < iter; j++)
            {
                rng.NextBytes(buf);
                _ = db.StringSet(key, buf);

                var startOffset = rng.Next(0, maxByteLen);
                var endOffset = rng.Next(startOffset, maxByteLen);

                var set = rng.Next(0, 1) == 0 ? false : true;
                expectedPos = Bitpos(buf, startOffset, endOffset, set);
                pos = db.StringBitPosition(key, set, startOffset, endOffset);

                ClassicAssert.AreEqual(expectedPos, pos, $"{j} {set} {startOffset} {endOffset}");

                var startBitOffset = startOffset << 3;
                var endBitOffset = endOffset << 3;
                pos = db.StringBitPosition(key, set, startBitOffset, endBitOffset, StringIndexType.Bit);
                ClassicAssert.AreEqual(expectedPos, pos, $"{j} {set} {startBitOffset} {endBitOffset} bit");
            }

            // check negative offsets in range
            for (var j = 0; j < iter; j++)
            {
                rng.NextBytes(buf);
                _ = db.StringSet(key, buf);

                int startOffset = j == 0 ? -10 : rng.Next(-maxByteLen, 0);
                var endOffset = j == 0 ? -1 : rng.Next(startOffset, 0);

                var set = rng.Next(0, 1) != 0;
                expectedPos = Bitpos(buf, startOffset, endOffset, set);
                pos = db.StringBitPosition(key, set, startOffset, endOffset);
                ClassicAssert.AreEqual(expectedPos, pos, $"{j} {set} {startOffset} {endOffset}");

                var startBitOffset = startOffset << 3;
                var endBitOffset = endOffset << 3;
                pos = db.StringBitPosition(key, set, startBitOffset, endBitOffset, StringIndexType.Bit);
                ClassicAssert.AreEqual(expectedPos, pos, $"{j} {set} {startBitOffset} {endBitOffset} bit");
            }
        }

        [Test, Order(14)]
        [Category("BITPOS")]
        public void BitmapBitPosTest_LTM()
        {
            var bitmapBytes = 512;
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                lowMemory: true,
                memorySize: (bitmapBytes << 3).ToString(),  // Must be LogSizeTracker.MinTargetPageCount pages due to memory size tracking
                pageSize: (bitmapBytes << 1).ToString());
            server.Start();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var keyCount = 64;
            var bitmap = new byte[bitmapBytes];
            List<long> bitmapList = [];

            for (var i = 0; i < keyCount; i++)
            {
                string sKey = i.ToString();
                rng.NextBytes(bitmap);

                bitmapList.Add(Bitpos(bitmap, set: true));
                _ = db.StringSet(sKey, bitmap);
            }

            var iter = 128;
            for (var i = 0; i < iter; i++)
            {
                int key = rng.Next(0, keyCount);
                var sKey = key.ToString();
                var pos = db.StringBitPosition(sKey, true);
                var expectedPos = bitmapList[key];
                ClassicAssert.AreEqual(expectedPos, pos);
            }
        }

        [Test]
        [Category("BITPOS")]
        public void BitmapBitPosTest_BoundaryConditions()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const int bitmapSize = 24;
            byte[] bitmap = new byte[bitmapSize];

            string key = "mybitmap";
            ClassicAssert.IsTrue(db.StringSet(key, bitmap));

            // first unset bit, should increment
            for (int i = 0; i < bitmapSize; i++)
            {
                // first unset bit
                ClassicAssert.AreEqual(i, db.StringBitPosition(key, false));
                ClassicAssert.IsFalse(db.StringSetBit(key, i, true));
            }
        }

        [Test, Order(15)]
        [TestCase(10)]
        [TestCase(20)]
        [TestCase(30)]
        public unsafe void BitmapSimpleBITPOS_PCT(int bytesPerSend)
        {
            //*2\r\n$8\r\nBITCOUNT\r\n
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var lightClientRequest = TestUtils.CreateRequest();
            var db = redis.GetDatabase(0);

            string key = "mykey";
            var maxBitmapLen = 1 << 12;
            var buf = new byte[maxBitmapLen >> 3];
            rng.NextBytes(buf);
            db.StringSet(key, buf);

            var expectedPos = Bitpos(buf);
            long pos = 0;
            var response = lightClientRequest.SendCommandChunks("BITPOS mykey 1", bytesPerSend);
            pos = ResponseToLong(response, 1);
            ClassicAssert.AreEqual(expectedPos, pos);
        }

        private static byte[] CopyBitmap(byte[] sourceBitmap, bool invert = false)
        {
            var dst = new byte[sourceBitmap.Length];
            if (invert)
                TensorPrimitives.OnesComplement<byte>(sourceBitmap, dst);
            else
                sourceBitmap.AsSpan().CopyTo(dst);

            return dst;
        }

        private static void ApplyBitop(ref byte[] dst, byte[] src, Func<byte, byte, byte> op)
        {
            if (dst.Length < src.Length)
            {
                var newDst = new byte[src.Length];
                dst.AsSpan().CopyTo(newDst);
                dst = newDst;
            }

            for (var i = 0; i < src.Length; i++)
            {
                dst[i] = op(dst[i], src[i]);
            }

            for (var i = src.Length; i < dst.Length; i++)
            {
                dst[i] = op(dst[i], 0);
            }
        }

        [Test, Order(19)]
        [Category("BITOP")]
        public void BitOp_Unary_BitwiseNot(
            [Values(Bitwise.Not)] Bitwise op,
            [Values(1, 2, 16, 32 + 3, 128 + 32 + 3, 256 + 32 + 3, 512 + 32 + 3, 4096, 4096 + 32, 4096 + 32 + 3)] int bitmapLength)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var srcKey = "src";
            var dstKey = "dst";

            var srcKeyBitmap = new byte[bitmapLength];
            rng.NextBytes(srcKeyBitmap);
            var expectedBitmap = CopyBitmap(srcKeyBitmap, invert: true);
            db.StringSet(srcKey, srcKeyBitmap);

            var size = db.StringBitOperation(op, dstKey, srcKey);
            ClassicAssert.AreEqual(expectedBitmap.Length, size);

            byte[] actualBitmap = db.StringGet(dstKey);
            ClassicAssert.AreEqual(expectedBitmap.Length, actualBitmap.Length);
            ClassicAssert.AreEqual(expectedBitmap, actualBitmap);
        }

        [Test]
        [Category("BITOP")]
        public void BitOp_Binary_SameSize(
            [Values(new[] { "DOTNET_EnableHWIntrinsic", "1" }, new[] { "DOTNET_PreferredVectorBitWidth", "128" }, new[] { "DOTNET_EnableHWIntrinsic", "0" })] string[] environment,
            [Values(Bitwise.And, Bitwise.Or, Bitwise.Xor, Bitwise.Diff)] Bitwise op,
            [Values(512 + 32 + 3)] int bitmapSize,
            [Values(2, 3, 4)] int keys)
        {
            Dictionary<string, string> args = [];
            for (var i = 0; i < environment.Length; i += 2)
            {
                args[environment[i]] = environment[i + 1];
            }

            using var server = new GarnetServerTestProcess(args);
            try
            {
                BitOp_Binary_SameSize(server.Options, op, bitmapSize, keys);
            }
            catch
            {
                server.RecordTestOutput();

                throw;
            }
        }

        [Test]
        [Category("BITOP")]
        public void BitOp_Binary_SameSize(
            [Values(Bitwise.And, Bitwise.Or, Bitwise.Xor, Bitwise.Diff)] Bitwise op,
            [Values(1, 2, 16, 32 + 3, 128 + 32 + 3, 256 + 32 + 3, 512 + 32 + 3, 4096, 4096 + 32, 4096 + 32 + 3)] int bitmapSize,
            [Values(2, 3, 4)] int keys)
        {
            BitOp_Binary_SameSize(TestUtils.GetConfig(), op, bitmapSize, keys);
        }

        private void BitOp_Binary_SameSize(
            ConfigurationOptions configOptions,
            Bitwise op,
            int bitmapSize,
            int keys)
        {
            Func<byte, byte, byte> opFunc = op switch
            {
                Bitwise.And => static (a, b) => (byte)(a & b),
                Bitwise.Or => static (a, b) => (byte)(a | b),
                Bitwise.Xor => static (a, b) => (byte)(a ^ b),
                Bitwise.Diff => static (a, b) => (byte)(a & ~b),

                _ => throw new NotSupportedException()
            };

            using var redis = ConnectionMultiplexer.Connect(configOptions);
            var db = redis.GetDatabase(0);

            var srcKeys = new RedisKey[keys];
            var srcKeyBitmaps = new byte[keys][];

            var dstKey = "dst";
            var expectedBitmap = new byte[bitmapSize];

            for (var i = 0; i < srcKeys.Length; i++)
            {
                srcKeyBitmaps[i] = new byte[bitmapSize];
                rng.NextBytes(srcKeyBitmaps[i]);

                srcKeys[i] = "src" + i;
                db.StringSet(srcKeys[i], srcKeyBitmaps[i]);

                if (i == 0)
                    srcKeyBitmaps[i].AsSpan().CopyTo(expectedBitmap);
                else
                    ApplyBitop(ref expectedBitmap, srcKeyBitmaps[i], opFunc);
            }

            var size = db.StringBitOperation(op, dstKey, srcKeys);
            ClassicAssert.AreEqual(expectedBitmap.Length, size);

            byte[] actualBitmap = db.StringGet(dstKey);
            ClassicAssert.AreEqual(expectedBitmap.Length, actualBitmap.Length);
            ClassicAssert.AreEqual(expectedBitmap, actualBitmap);
        }


        [Test, Order(20)]
        [Category("BITOP")]
        public void BitOp_Binary_DifferentTails(
            [Values(Bitwise.And, Bitwise.Or, Bitwise.Xor, Bitwise.Diff)] Bitwise op,
            [Values(1, 2, 16, 32 + 3, 128 + 32 + 3, 256 + 32 + 3, 512 + 32 + 3, 4096, 4096 + 32, 4096 + 32 + 3)] int sharedLength,
            [Values(new int[] { 0, 7 }, new int[] { 16, 0, 7 }, new int[] { 1, 16, 1, 32 })] int[] additionalLengths)
        {
            Func<byte, byte, byte> opFunc = op switch
            {
                Bitwise.And => static (a, b) => (byte)(a & b),
                Bitwise.Or => static (a, b) => (byte)(a | b),
                Bitwise.Xor => static (a, b) => (byte)(a ^ b),
                Bitwise.Diff => static (a, b) => (byte)(a & ~b),

                _ => throw new NotSupportedException()
            };

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var srcKeyCount = additionalLengths.Length;
            var srcKeys = new RedisKey[srcKeyCount];
            var srcKeyBitmaps = new byte[srcKeyCount][];
            var srcMaxLength = sharedLength + Enumerable.Max(additionalLengths);

            var dstKey = "dst";
            var expectedBitmap = new byte[srcMaxLength];

            for (var i = 0; i < srcKeys.Length; i++)
            {
                srcKeyBitmaps[i] = new byte[sharedLength + additionalLengths[i]];
                rng.NextBytes(srcKeyBitmaps[i]);

                srcKeys[i] = "src" + i;
                db.StringSet(srcKeys[i], srcKeyBitmaps[i]);

                if (i == 0)
                    srcKeyBitmaps[i].AsSpan().CopyTo(expectedBitmap);
                else
                    ApplyBitop(ref expectedBitmap, srcKeyBitmaps[i], opFunc);
            }

            var size = db.StringBitOperation(op, dstKey, srcKeys);
            ClassicAssert.AreEqual(expectedBitmap.Length, size);

            byte[] actualBitmap = db.StringGet(dstKey);
            ClassicAssert.AreEqual(expectedBitmap.Length, actualBitmap.Length);
            ClassicAssert.AreEqual(expectedBitmap, actualBitmap);
        }

        private static long GetValueFromBitmap(ref byte[] bitmap, long offset, int bitCount, bool signed)
        {
            long startBit = offset;
            long endBit = offset + bitCount;

            long indexBit = 0;
            long value = 0;
            int bI = 63;
            byte[] si = [1, 2, 4, 8, 16, 32, 64, 128];
            while (indexBit < (bitmap.Length << 3))
            {
                for (int i = 7; i >= 0; i--)
                {
                    if (indexBit >= startBit && indexBit < endBit)
                    {
                        long indexByte = indexBit >> 3;
                        byte bVal = bitmap[indexByte];
                        byte bit = (byte)((bVal & si[i]) > 0 ? 1 : 0);
                        long or = (long)((long)bit << bI);
                        value = value | or;
                        bI--;
                    }
                    indexBit++;
                }
            }

            int shf = 64 - bitCount;
            if (signed)
            {
                return value >> shf;
            }
            else
            {
                return (long)(((ulong)value) >> shf);
            }
        }

        private static ulong getUnsigned(ref byte[] p, ulong offset, ulong bits)
        {
            ulong byteIndex = 0;
            ulong bit = 0;
            ulong byteval = 0;
            ulong bitval = 0;
            ulong j = 0;
            ulong value = 0;

            for (j = 0; j < bits; j++)
            {
                byteIndex = offset >> 3;
                bit = 7 - (offset & 0x7);
                byteval = byteIndex < (ulong)p.Length ? (p[byteIndex]) : (ulong)0;
                bitval = (byteval >> ((byte)bit)) & 1;
                value = (value << 1) | bitval;
                offset++;
            }
            return value;
        }

        private static long getSigned(ref byte[] bitmap, ulong offset, ulong bits)
        {
            ulong value = getUnsigned(ref bitmap, offset, bits);

            if (bits < 64 && (value & (1UL << (int)(bits - 1))) != 0)
            {
                value |= ulong.MaxValue << (byte)bits;
            }

            return (long)value;
        }

        private static long GetFromBitmapRedis(ref byte[] bitmap, ulong offset, ulong bits, bool signed)
        {
            return signed ? getSigned(ref bitmap, offset, bits) : (long)getUnsigned(ref bitmap, offset, bits);
        }

        [Test, Order(21)]
        [Category("BITFIELD")]
        public void BitmapBitfieldGetTest([Values(RespCommand.BITFIELD, RespCommand.BITFIELD_RO)] RespCommand testCmd)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            string key = "BitmapBitFieldGetTest";

            byte[] bitmapData = null;
            long expectedValue;
            long returnedValue;
            long redisValue;
            rng = new Random(Guid.NewGuid().GetHashCode());

            bitmapData = new byte[16];
            rng.NextBytes(bitmapData);
            db.StringSet(key, bitmapData);
            for (int i = 0; i < (bitmapData.Length << 3) + 64; i++)//offset in bits
            {
                for (int j = 1; j <= 64; j++)//bitcount
                {
                    //signed
                    expectedValue = GetValueFromBitmap(ref bitmapData, i, j, true);
                    redisValue = GetFromBitmapRedis(ref bitmapData, (ulong)i, (ulong)j, true);
                    returnedValue = (long)(db.Execute($"{testCmd}", (RedisKey)key, "get", $"i{j}", $"{i}"));
                    ClassicAssert.AreEqual(expectedValue, redisValue);
                    ClassicAssert.AreEqual(expectedValue, returnedValue);

                    //unsigned
                    if (j < 64)
                    {
                        expectedValue = GetValueFromBitmap(ref bitmapData, i, j, false);
                        redisValue = GetFromBitmapRedis(ref bitmapData, (ulong)i, (ulong)j, false);
                        returnedValue = ((long)db.Execute($"{testCmd}", (RedisKey)key, "GET", $"u{j}", $"{i}"));
                        ClassicAssert.AreEqual(expectedValue, redisValue);
                        ClassicAssert.AreEqual(expectedValue, returnedValue);
                    }
                }
            }
        }

        private unsafe (int, int) SingleBitfieldReceive(byte* buf, int bytesRead, int opType)
        {
            int count = 0;
            for (int i = 0; i < bytesRead; i++)
            {
                if (buf[i] == '*')
                    count++;
            }
            return (bytesRead, count);
        }

        [Test, Order(22)]
        [Category("BITFIELD")]
        public unsafe void BitmapBitfieldGetTest_PCT([Values(RespCommand.BITFIELD, RespCommand.BITFIELD_RO)] RespCommand testCmd, [Values(100)] int bytesPerSend)
        {
            var lighClientOnResponseDelegate = new LightClient.OnResponseDelegateUnsafe(SingleBitfieldReceive);
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var lightClientRequest = TestUtils.CreateRequest(lighClientOnResponseDelegate);
            var db = redis.GetDatabase(0);
            string key = "BitmapBitFieldGetTest";

            byte[] bitmapData = null;
            long expectedValue;
            long returnedValue;
            long redisValue;
            //r = new Random(Guid.NewGuid().GetHashCode());

            bitmapData = new byte[16];
            rng.NextBytes(bitmapData);
            db.StringSet(key, bitmapData);
            for (int i = 0; i < (bitmapData.Length << 3) + 64; i++)//offset in bits
            {
                for (int j = 1; j <= 64; j++)//bitcount
                {
                    //signed
                    expectedValue = GetValueFromBitmap(ref bitmapData, i, j, true);
                    redisValue = GetFromBitmapRedis(ref bitmapData, (ulong)i, (ulong)j, true);
                    byte[] response = lightClientRequest.SendCommandChunks($"{testCmd} {key} GET i{j} {i}", bytesPerSend);
                    returnedValue = ResponseToLong(response, 5);

                    ClassicAssert.AreEqual(expectedValue, redisValue);
                    ClassicAssert.AreEqual(expectedValue, returnedValue);

                    //unsigned
                    if (j < 64)
                    {
                        expectedValue = GetValueFromBitmap(ref bitmapData, i, j, false);
                        redisValue = GetFromBitmapRedis(ref bitmapData, (ulong)i, (ulong)j, false);
                        response = lightClientRequest.SendCommandChunks($"{testCmd} {key} GET u{j} {i}", bytesPerSend);
                        returnedValue = ResponseToLong(response, 5);

                        ClassicAssert.AreEqual(expectedValue, redisValue);
                        ClassicAssert.AreEqual(expectedValue, returnedValue);
                    }
                }
            }
        }

        [Test, Order(23)]
        [Category("BITFIELD")]
        public void BitmapBitfieldGetTest_LTM([Values(RespCommand.BITFIELD, RespCommand.BITFIELD_RO)] RespCommand testCmd)
        {
            int bitmapBytes = 512;
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                lowMemory: true,
                memorySize: (bitmapBytes << 3).ToString(),  // Must be LogSizeTracker.MinTargetPageCount pages due to memory size tracking
                pageSize: (bitmapBytes << 1).ToString());
            //LogMemorySize: "16g",
            //PageSize: "32m");
            server.Start();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int keyCount = 16;
            byte[][] bitmapData = new byte[keyCount][];
            long expectedValue;
            long returnedValue;

            //0. Initialize bitmapData            
            for (int i = 0; i < keyCount; i++)
            {
                bitmapData[i] = new byte[bitmapBytes];
                rng.NextBytes(bitmapData[i]);

                int key = i;
                string sKey = i.ToString();
                db.StringSet(sKey, bitmapData[i]);
            }

            int iter = 1 << 12;
            for (int i = 0; i < iter; i++)
            {
                int key = rng.Next(0, keyCount);
                byte[] currBitmap = bitmapData[key];
                string sKey = key.ToString();
                int offset = rng.Next(0, (bitmapData.Length << 3));
                int bitCount = rng.Next(1, 65);

                //signed
                expectedValue = GetValueFromBitmap(ref currBitmap, offset, bitCount, true);
                returnedValue = (long)(db.Execute(testCmd.ToString(), (RedisKey)sKey, "get", $"i{bitCount}", $"{offset}"));
                ClassicAssert.AreEqual(expectedValue, returnedValue);

                //unsigned
                if (bitCount < 64)
                {
                    expectedValue = GetValueFromBitmap(ref currBitmap, offset, bitCount, false);
                    returnedValue = ((long)db.Execute(testCmd.ToString(), (RedisKey)sKey, "GET", $"u{bitCount}", $"{offset}"));
                    ClassicAssert.AreEqual(expectedValue, returnedValue);
                }
            }
        }

        private long RandomIntBitRange(int bitCount, bool signed)
        {
            if (signed)
            {
                long maxVal = bitCount == 64 ? long.MaxValue : (1L << (bitCount - 1)) - 1;
                long minVal = -maxVal - 1;

                long value = LongRandom();

                value = (rng.Next() & 0x1) == 0x1 ? -value : value;
                value = value >> (64 - bitCount);

                ClassicAssert.IsTrue(value >= minVal);
                ClassicAssert.IsTrue(value <= maxVal);
                return value;
            }
            else
            {
                ulong minVal = 0;
                ulong maxVal = (1UL << bitCount);

                ulong value = ULongRandom();
                value = value >> (64 - bitCount);

                ClassicAssert.IsTrue(value >= minVal);
                ClassicAssert.IsTrue(value <= maxVal);
                return (long)value;
            }
        }

        private static void setUnsignedBitfield(ref byte[] bitmap, ulong offset, ulong bitCount, ulong value)
        {
            ulong byteIndex, bit, byteVal, bitVal, j;

            for (j = 0; j < bitCount; j++)
            {
                bitVal = (value & (1UL << (int)(bitCount - 1 - j))) == 0 ? 0UL : 1UL;
                byteIndex = offset >> 3;
                bit = 7 - (offset & 0x7);
                byteVal = bitmap[byteIndex];
                byteVal &= ~(1UL << (int)bit);
                byteVal |= bitVal << (int)bit;
                bitmap[byteIndex] = (byte)(byteVal & 0xff);
                offset++;
            }
        }

        private static void setSignedBitfield(ref byte[] bitmap, ulong offset, ulong bitCount, long value)
        {
            ulong uv = (ulong)value; /* Casting will add UINT64_MAX + 1 if v is negative. */
            setUnsignedBitfield(ref bitmap, offset, bitCount, uv);
        }

        [Test, Order(24)]
        [Category("BITFIELD")]
        [TestCase(100)]
        public unsafe void BitmapBitfieldSetTest_PCT(int bytesPerSend)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var lightClientRequest = TestUtils.CreateRequest(SingleBitfieldReceive);
            var db = redis.GetDatabase(0);
            string key = "BitmapBitFieldSetTest";
            int tests = 1024;

            byte[] bitmapData = null;
            byte[] expectedBitmap = null;
            byte[] response;
            //r = new Random(Guid.NewGuid().GetHashCode());        

            bitmapData = new byte[16];
            rng.NextBytes(bitmapData);
            db.StringSet(key, bitmapData);

            long oldVal, expectedOldVal;
            long returnVal, expectedReturnVal;

            //1. Test signed set bitfield
            for (int i = 0; i < tests; i++)
            {
                int bitCount = rng.Next(1, 64);
                long offset = rng.Next(0, (bitmapData.Length << 3) - bitCount - 1);
                //expectedReturnVal = RandomIntBitRange(bitCount);
                expectedReturnVal = RandomIntBitRange(bitCount, true);

                //expectedOldVal = (long)(db.Execute("BITFIELD", (RedisKey)key, "GET", "i" + bitCount.ToString(), $"{offset}"));
                response = lightClientRequest.SendCommandChunks("BITFIELD " + key + " GET i" + bitCount.ToString() + " " + $"{offset}", bytesPerSend);
                expectedOldVal = ResponseToLong(response, 5);

                setSignedBitfield(ref bitmapData, (ulong)offset, (ulong)bitCount, expectedReturnVal);
                //oldVal = (long)(db.Execute("BITFIELD", (RedisKey)key, "set", "i" + bitCount.ToString(), $"{offset}", expectedReturnVal));
                response = lightClientRequest.SendCommandChunks("BITFIELD " + key + " SET i" + bitCount.ToString() + " " + $"{offset}" + " " + $"{expectedReturnVal}", bytesPerSend);
                oldVal = ResponseToLong(response, 5);
                ClassicAssert.AreEqual(expectedOldVal, oldVal);

                //returnVal = (long)(db.Execute("BITFIELD", (RedisKey)key, "GET", "i" + bitCount.ToString(), $"{offset}"));
                response = lightClientRequest.SendCommandChunks("BITFIELD " + key + " GET i" + bitCount.ToString() + " " + $"{offset}", bytesPerSend);
                returnVal = ResponseToLong(response, 5);
                ClassicAssert.AreEqual(expectedReturnVal, returnVal);

                expectedBitmap = db.StringGet(key);
                ClassicAssert.AreEqual(expectedBitmap, bitmapData);
            }
        }

        [Test, Order(25)]
        [Category("BITFIELD")]
        public void BitmapBitfieldSetTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            string key = "BitmapBitFieldSetTest";
            int tests = 1024;

            byte[] bitmapData = null;
            byte[] expectedBitmap = null;
            //r = new Random(Guid.NewGuid().GetHashCode());        

            bitmapData = new byte[16];
            rng.NextBytes(bitmapData);
            db.StringSet(key, bitmapData);

            long oldVal, expectedOldVal;
            long returnVal, expectedReturnVal;

            //1. Test signed set bitfield
            for (int i = 0; i < tests; i++)
            {
                int bitCount = rng.Next(1, 64);
                long offset = rng.Next(0, (bitmapData.Length << 3) - bitCount - 1);
                //expectedReturnVal = RandomIntBitRange(bitCount);
                expectedReturnVal = RandomIntBitRange(bitCount, true);

                expectedOldVal = (long)(db.Execute("BITFIELD", (RedisKey)key, "GET", "i" + bitCount.ToString(), $"{offset}"));
                setSignedBitfield(ref bitmapData, (ulong)offset, (ulong)bitCount, expectedReturnVal);
                oldVal = (long)(db.Execute("BITFIELD", (RedisKey)key, "set", "i" + bitCount.ToString(), $"{offset}", expectedReturnVal));
                ClassicAssert.AreEqual(expectedOldVal, oldVal);

                returnVal = (long)(db.Execute("BITFIELD", (RedisKey)key, "GET", "i" + bitCount.ToString(), $"{offset}"));
                ClassicAssert.AreEqual(expectedReturnVal, returnVal);

                expectedBitmap = db.StringGet(key);
                ClassicAssert.AreEqual(expectedBitmap, bitmapData);
            }
        }

        [Test, Order(26)]
        [Category("BITFIELD")]
        public void BitmapBitfieldSetTest_LTM()
        {
            int bitmapBytes = 512;
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                lowMemory: true,
                pageCount: 2,   // Specify pageCount instead of memorySize to avoid LogSizeTracker.MinTargetPageCount requirement
                pageSize: (bitmapBytes << 1).ToString());
            //LogMemorySize: "16g",
            //PageSize: "32m");
            server.Start();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int keyCount = 16;
            byte[][] bitmapData = new byte[keyCount][];

            //0. Initialize bitmapData            
            for (int i = 0; i < keyCount; i++)
            {
                bitmapData[i] = new byte[bitmapBytes];
                rng.NextBytes(bitmapData[i]);

                int key = i;
                string sKey = i.ToString();
                db.StringSet(sKey, bitmapData[i]);
            }

            long expectedOldValue;
            long returnedOldValue;
            long setNewValue;
            long returnedValue;

            int iter = 1 << 12;
            for (int i = 0; i < iter; i++)
            {
                int key = rng.Next(0, keyCount);
                byte[] currBitmap = bitmapData[key];
                string sKey = key.ToString();
                int offset = rng.Next(0, (bitmapData.Length << 3));
                int bitCount = rng.Next(1, 65);

                setNewValue = RandomIntBitRange(bitCount, true);

                //signed
                expectedOldValue = GetValueFromBitmap(ref currBitmap, offset, bitCount, true);
                returnedOldValue = (long)(db.Execute("BITFIELD", (RedisKey)sKey, "get", "i" + bitCount.ToString(), $"{offset}"));
                ClassicAssert.AreEqual(expectedOldValue, returnedOldValue);

                setSignedBitfield(ref currBitmap, (ulong)offset, (ulong)bitCount, setNewValue);
                returnedOldValue = (long)(db.Execute("BITFIELD", (RedisKey)sKey, "set", "i" + bitCount.ToString(), $"{offset}", setNewValue));
                ClassicAssert.AreEqual(expectedOldValue, returnedOldValue);

                returnedValue = (long)(db.Execute("BITFIELD", (RedisKey)sKey, "GET", "i" + bitCount.ToString(), $"{offset}"));
                ClassicAssert.AreEqual(setNewValue, returnedValue);
            }
        }

        private static (long, bool) CheckSignedBitfieldOverflowRedis(long value, long incrBy, byte bitCount, byte overflowType)
        {
            long maxVal = bitCount == 64 ? long.MaxValue : (1L << (bitCount - 1)) - 1;
            long minVal = -maxVal - 1;

            long maxAdd = maxVal - value;
            long maxSub = minVal - value;

            switch (overflowType)
            {
                case 0://wrap
                    if ((bitCount < 64 && incrBy > maxAdd) || (value >= 0 && incrBy > 0 && incrBy > maxVal) ||
                        ((bitCount < 64 && incrBy < maxSub) || (value < 0 && incrBy < 0 && incrBy < maxSub)))
                    {
                        ulong signb = 1UL << (bitCount - 1);
                        ulong opA = (ulong)value;
                        ulong opB = (ulong)incrBy;
                        ulong res = opA + opB;

                        if (bitCount < 64)
                        {
                            ulong mask = (1UL << bitCount) - 1;
                            res = (res & signb) > 0 ? (res | ~mask) : (res & mask);
                        }
                        return ((long)res, true);
                    }
                    return ((value + incrBy), false);
                case 1://sat                                   
                    if ((bitCount < 64 && incrBy > maxAdd) || (value >= 0 && incrBy > 0 && incrBy > maxVal))
                        return (maxVal, true);
                    if ((bitCount < 64 && incrBy < maxSub) || (value < 0 && incrBy < 0 && incrBy < maxSub))
                        return (minVal, true);
                    return ((value + incrBy), false);
                case 2://fail // detect overflow/underflow do not do anything else
                    if ((bitCount < 64 && incrBy > maxAdd) || (value >= 0 && incrBy > 0 && incrBy > maxVal) ||
                        ((bitCount < 64 && incrBy < maxSub) || (value < 0 && incrBy < 0 && incrBy < maxSub)))
                        return (0, true);
                    return ((value + incrBy), false);
            }
            return (0, true);
        }

        private static (long, bool) CheckSignedBitfieldOverflow(long value, long incrBy, byte bitCount, byte overflowType)
        {
            long signbit = 1L << (bitCount - 1);
            long mask = bitCount == 64 ? -1 : (signbit - 1);

            long result = (value + incrBy);
            //if operands are both negative possibility for underflow
            //underflow if sign bit is zero
            bool underflow = (result & signbit) == 0 && value < 0 && incrBy < 0;
            //if operands are both positive possibility of overflow
            //overflow if any of the 64-bitcount most significant bits are set.
            bool overflow = (ulong)(result & ~mask) > 0 && value >= 0 && incrBy > 0;

            switch (overflowType)
            {
                case 0://wrap
                    if (underflow || overflow)
                    {
                        ulong res = (ulong)result;
                        if (bitCount < 64)
                        {
                            ulong msb = (ulong)signbit;
                            ulong smask = (ulong)mask;
                            res = (res & msb) > 0 ? (res | ~smask) : (res & smask);
                        }
                        return ((long)res, true);
                    }
                    return (result, false);
                case 1://sat                                       
                    long maxVal = bitCount == 64 ? long.MaxValue : (signbit - 1);
                    if (overflow) //overflow
                    {
                        return (maxVal, true);
                    }
                    else if (underflow) //underflow
                    {
                        long minVal = -maxVal - 1;
                        return (minVal, true);
                    }
                    return (result, false);
                case 2://fail
                    if (underflow || overflow)
                        return (0, true);
                    return (result, false);
            }
            return (0, true);
        }

        [Test, Order(27)]
        [Category("BITFIELD")]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(90)]
        [TestCase(100)]
        public unsafe void BitmapBitfieldSignedIncrTest_PCT(int bytesPerSend)
        {
            using var lightClientRequest = TestUtils.CreateRequest(SingleBitfieldReceive);
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            string key = "x";
            int tests = 1 << 10;

            byte[] response;
            byte[] bitmapData = new byte[16];
            for (int i = 0; i < 16; i++) bitmapData[i] = 0;

            int bitCount = 0;
            long offset = 0;
            long incrementValue = 0;
            long result;
            long expectedResult;
            db.KeyDelete(key);
            db.StringSet(key, bitmapData);

            int testCheckOverflow = 1 << 15;
            for (int i = 0; i < testCheckOverflow; i++)
            {
                bitCount = rng.Next(1, 64);

                long value = RandomIntBitRange(bitCount, true);
                long incrBy = RandomIntBitRange(bitCount, true);

                //wrap
                long resV1, resV2;
                bool overflowV1, overflowV2;
                (resV1, overflowV1) = CheckSignedBitfieldOverflowRedis(value, incrBy, (byte)bitCount, 0);
                (resV2, overflowV2) = CheckSignedBitfieldOverflow(value, incrBy, (byte)bitCount, 0);
                ClassicAssert.AreEqual(resV1, resV2);
                ClassicAssert.AreEqual(overflowV1, overflowV2);

                //sat
                value = RandomIntBitRange(bitCount, true);
                incrBy = RandomIntBitRange(bitCount, true);
                (resV1, overflowV1) = CheckSignedBitfieldOverflowRedis(value, incrBy, (byte)bitCount, 1);
                (resV2, overflowV2) = CheckSignedBitfieldOverflow(value, incrBy, (byte)bitCount, 1);
                ClassicAssert.AreEqual(resV1, resV2);
                ClassicAssert.AreEqual(overflowV1, overflowV2);
            }

            //signed overflow-underflow tests
            for (int i = 1; i <= 64; i++)
            {
                bitCount = i;
                long maxValue = (1L << bitCount - 1) - 1;
                long minValue = -maxValue - 1;
                long resV1, resV2;
                bool overflowV1, overflowV2;

                //overflow wrap test
                incrementValue = 1;
                //db.Execute("BITFIELD", (RedisKey)key, "SET", "i" + bitCount.ToString(), $"{offset}", maxValue);
                response = lightClientRequest.SendCommandChunks("BITFIELD " + key + " SET i" + bitCount.ToString() + " " + $"{offset}" + " " + $"{maxValue}", bytesPerSend);
                //result = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "wrap", "incrby", "i" + bitCount.ToString(), $"{offset}", incrementValue);
                response = lightClientRequest.SendCommandChunks("BITFIELD " + key + " OVERFLOW wrap INCRBY i" + bitCount.ToString() + " " + $"{offset}" + " " + $"{incrementValue}", bytesPerSend);
                result = ResponseToLong(response, 5);

                (resV1, overflowV1) = CheckSignedBitfieldOverflowRedis(maxValue, incrementValue, (byte)bitCount, 0);
                (resV2, overflowV2) = CheckSignedBitfieldOverflow(maxValue, incrementValue, (byte)bitCount, 0);
                ClassicAssert.AreEqual(resV1, resV2);
                ClassicAssert.AreEqual(overflowV1, overflowV2);
                ClassicAssert.AreEqual(result, resV2);

                //underflow wrap test
                incrementValue = -1;
                //db.Execute("BITFIELD", (RedisKey)key, "SET", "i" + bitCount.ToString(), $"{offset}", minValue);
                response = lightClientRequest.SendCommandChunks("BITFIELD " + key + " SET i" + bitCount.ToString() + " " + $"{offset}" + " " + $"{minValue}", bytesPerSend);
                //long get = (long)db.Execute("BITFIELD", (RedisKey)key, "GET", "i" + bitCount.ToString(), $"{offset}");
                response = lightClientRequest.SendCommandChunks("BITFIELD " + key + " GET i" + bitCount.ToString() + " " + $"{offset}", bytesPerSend);
                long get = ResponseToLong(response, 5);
                //result = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "WRAP", "INCRBY", "i" + bitCount.ToString(), $"{offset}", incrementValue);
                response = lightClientRequest.SendCommandChunks("BITFIELD " + key + " OVERFLOW wrap INCRBY i" + bitCount.ToString() + " " + $"{offset}" + " " + $"{incrementValue}", bytesPerSend);
                result = ResponseToLong(response, 5);

                (resV1, overflowV1) = CheckSignedBitfieldOverflowRedis(minValue, incrementValue, (byte)bitCount, 0);
                (resV2, overflowV2) = CheckSignedBitfieldOverflow(minValue, incrementValue, (byte)bitCount, 0);
                ClassicAssert.AreEqual(resV1, resV2);
                ClassicAssert.AreEqual(overflowV1, overflowV2);
                ClassicAssert.AreEqual(result, resV2);

                //overflow wrap test
                incrementValue = maxValue + 2;
                //db.Execute("BITFIELD", (RedisKey)key, "SET", "i" + bitCount.ToString(), $"{offset}", maxValue);
                response = lightClientRequest.SendCommandChunks("BITFIELD " + key + " SET i" + bitCount.ToString() + " " + $"{offset}" + " " + $"{maxValue}", bytesPerSend);
                //result = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "WRAP", "INCRBY", "i" + bitCount.ToString(), $"{offset}", incrementValue);
                response = lightClientRequest.SendCommandChunks("BITFIELD " + key + " OVERFLOW wrap INCRBY i" + bitCount.ToString() + " " + $"{offset}" + " " + $"{incrementValue}", bytesPerSend);
                result = ResponseToLong(response, 5);

                (resV1, overflowV1) = CheckSignedBitfieldOverflowRedis(maxValue, incrementValue, (byte)bitCount, 0);
                (resV2, overflowV2) = CheckSignedBitfieldOverflow(maxValue, incrementValue, (byte)bitCount, 0);
                ClassicAssert.AreEqual(resV1, resV2);
                ClassicAssert.AreEqual(overflowV1, overflowV2);
                ClassicAssert.AreEqual(result, resV2);
            }

            //signed overflow with wrap and sat
            for (int i = 0; i < tests; i++)
            {
                bitCount = rng.Next(1, 64);

                long value = RandomIntBitRange(bitCount, true);
                long incrBy = RandomIntBitRange(bitCount, true);
                bool overflow;
                long resV1;
                bool overflowV1;

                //wrap overflowtype
                //db.Execute("BITFIELD", (RedisKey)key, "SET", "i" + bitCount.ToString(), $"{offset}", value);
                response = lightClientRequest.SendCommandChunks("BITFIELD " + key + " SET i" + bitCount.ToString() + " " + $"{offset}" + " " + $"{value}", bytesPerSend);
                //expectedResult = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "WRAP", "INCRBY", "i" + bitCount.ToString(), $"{offset}", incrBy);
                response = lightClientRequest.SendCommandChunks("BITFIELD " + key + " OVERFLOW wrap INCRBY i" + bitCount.ToString() + " " + $"{offset}" + " " + $"{incrBy}", bytesPerSend);
                expectedResult = ResponseToLong(response, 5);
                (result, overflow) = CheckSignedBitfieldOverflow(value, incrBy, (byte)bitCount, 0);
                (resV1, overflowV1) = CheckSignedBitfieldOverflowRedis(value, incrBy, (byte)bitCount, 0);
                ClassicAssert.AreEqual(resV1, result);
                ClassicAssert.AreEqual(result, expectedResult);

                //sat overflowtype
                value = RandomIntBitRange(bitCount, true);
                incrBy = RandomIntBitRange(bitCount, true);
                //db.Execute("BITFIELD", (RedisKey)key, "SET", "i" + bitCount.ToString(), $"{offset}", value);
                response = lightClientRequest.SendCommandChunks("BITFIELD " + key + " SET i" + bitCount.ToString() + " " + $"{offset}" + " " + $"{value}", bytesPerSend);
                //if ((i & 0x1) == 0x1)
                //    expectedResult = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "sat", "INCRBY", "i" + bitCount.ToString(), $"{offset}", incrBy);
                //else
                //    expectedResult = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "SAT", "INCRBY", "i" + bitCount.ToString(), $"{offset}", incrBy);
                if ((i & 0x1) == 0x1)
                    response = lightClientRequest.SendCommandChunks("BITFIELD " + key + " OVERFLOW sat INCRBY i" + bitCount.ToString() + " " + $"{offset}" + " " + $"{incrBy}", bytesPerSend);
                else
                    response = lightClientRequest.SendCommandChunks("BITFIELD " + key + " OVERFLOW SAT INCRBY i" + bitCount.ToString() + " " + $"{offset}" + " " + $"{incrBy}", bytesPerSend);
                expectedResult = ResponseToLong(response, 5);
                (result, overflow) = CheckSignedBitfieldOverflow(value, incrBy, (byte)bitCount, 1);
                (resV1, overflowV1) = CheckSignedBitfieldOverflowRedis(value, incrBy, (byte)bitCount, 1);
                ClassicAssert.AreEqual(resV1, result);
                ClassicAssert.AreEqual(result, expectedResult);

                //fail overflowtype
                value = RandomIntBitRange(bitCount, true);
                incrBy = RandomIntBitRange(bitCount, true);
                //db.Execute("BITFIELD", (RedisKey)key, "SET", "i" + bitCount.ToString(), $"{offset}", value);
                response = lightClientRequest.SendCommandChunks("BITFIELD " + key + " SET i" + bitCount.ToString() + " " + $"{offset}" + " " + $"{value}", bytesPerSend);
                //RedisResult[] redisResult;
                //if ((i & 0x1) == 0x1)
                //    redisResult = (RedisResult[])db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "fail", "INCRBY", "i" + bitCount.ToString(), $"{offset}", incrBy);
                //else
                //    redisResult = (RedisResult[])db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "FAIL", "INCRBY", "i" + bitCount.ToString(), $"{offset}", incrBy);
                //if (!overflow)
                //    ClassicAssert.AreEqual(result, (long)redisResult[0]);
                //else
                //    ClassicAssert.AreEqual(redisResult[0].IsNull, true);
                if ((i & 0x1) == 0x1)
                    response = lightClientRequest.SendCommandChunks("BITFIELD " + key + " OVERFLOW fail INCRBY i" + bitCount.ToString() + " " + $"{offset}" + " " + $"{incrBy}", bytesPerSend);
                else
                    response = lightClientRequest.SendCommandChunks("BITFIELD " + key + " OVERFLOW FAIL INCRBY i" + bitCount.ToString() + " " + $"{offset}" + " " + $"{incrBy}", bytesPerSend);
                //expectedResult = ResponseToLong(response, 5);
                (result, overflow) = CheckSignedBitfieldOverflow(value, incrBy, (byte)bitCount, 2);

                //Debug.WriteLine(System.Text.Encoding.ASCII.GetString(response, 4, 3));
                if (!overflow)
                    ClassicAssert.AreEqual(result, ResponseToLong(response, 5));
                else
                    ClassicAssert.AreEqual(System.Text.Encoding.ASCII.GetString(response, 4, 3), "$-1");
            }
        }

        [Test, Order(28)]
        [Category("BITFIELD")]
        public void BitmapBitfieldSignedIncrTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            string key = "x";
            int tests = 1 << 10;

            byte[] bitmapData = new byte[16];
            for (int i = 0; i < 16; i++) bitmapData[i] = 0;

            int bitCount = 0;
            long offset = 0;
            long incrementValue = 0;
            long result;
            long expectedResult;
            db.KeyDelete(key);
            db.StringSet(key, bitmapData);

            int testCheckOverflow = 1 << 15;
            for (int i = 0; i < testCheckOverflow; i++)
            {
                bitCount = rng.Next(1, 64);

                long value = RandomIntBitRange(bitCount, true);
                long incrBy = RandomIntBitRange(bitCount, true);

                //wrap
                long resV1, resV2;
                bool overflowV1, overflowV2;
                (resV1, overflowV1) = CheckSignedBitfieldOverflowRedis(value, incrBy, (byte)bitCount, 0);
                (resV2, overflowV2) = CheckSignedBitfieldOverflow(value, incrBy, (byte)bitCount, 0);
                ClassicAssert.AreEqual(resV1, resV2);
                ClassicAssert.AreEqual(overflowV1, overflowV2);

                //sat
                value = RandomIntBitRange(bitCount, true);
                incrBy = RandomIntBitRange(bitCount, true);
                (resV1, overflowV1) = CheckSignedBitfieldOverflowRedis(value, incrBy, (byte)bitCount, 1);
                (resV2, overflowV2) = CheckSignedBitfieldOverflow(value, incrBy, (byte)bitCount, 1);
                ClassicAssert.AreEqual(resV1, resV2);
                ClassicAssert.AreEqual(overflowV1, overflowV2);
            }

            //signed overflow-underflow tests
            for (int i = 1; i <= 64; i++)
            {
                bitCount = i;
                long maxValue = (1L << bitCount - 1) - 1;
                long minValue = -maxValue - 1;
                long resV1, resV2;
                bool overflowV1, overflowV2;

                //overflow wrap test
                incrementValue = 1;
                db.Execute("BITFIELD", (RedisKey)key, "SET", "i" + bitCount.ToString(), $"{offset}", maxValue);
                result = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "wrap", "incrby", "i" + bitCount.ToString(), $"{offset}", incrementValue);

                (resV1, overflowV1) = CheckSignedBitfieldOverflowRedis(maxValue, incrementValue, (byte)bitCount, 0);
                (resV2, overflowV2) = CheckSignedBitfieldOverflow(maxValue, incrementValue, (byte)bitCount, 0);
                ClassicAssert.AreEqual(resV1, resV2);
                ClassicAssert.AreEqual(overflowV1, overflowV2);
                ClassicAssert.AreEqual(result, resV2);

                //underflow wrap test
                incrementValue = -1;
                db.Execute("BITFIELD", (RedisKey)key, "SET", "i" + bitCount.ToString(), $"{offset}", minValue);
                long get = (long)db.Execute("BITFIELD", (RedisKey)key, "GET", "i" + bitCount.ToString(), $"{offset}");
                result = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "WRAP", "INCRBY", "i" + bitCount.ToString(), $"{offset}", incrementValue);

                (resV1, overflowV1) = CheckSignedBitfieldOverflowRedis(minValue, incrementValue, (byte)bitCount, 0);
                (resV2, overflowV2) = CheckSignedBitfieldOverflow(minValue, incrementValue, (byte)bitCount, 0);
                ClassicAssert.AreEqual(resV1, resV2);
                ClassicAssert.AreEqual(overflowV1, overflowV2);
                ClassicAssert.AreEqual(result, resV2);

                //overflow wrap test
                incrementValue = maxValue + 2;
                db.Execute("BITFIELD", (RedisKey)key, "SET", "i" + bitCount.ToString(), $"{offset}", maxValue);
                result = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "WRAP", "INCRBY", "i" + bitCount.ToString(), $"{offset}", incrementValue);

                (resV1, overflowV1) = CheckSignedBitfieldOverflowRedis(maxValue, incrementValue, (byte)bitCount, 0);
                (resV2, overflowV2) = CheckSignedBitfieldOverflow(maxValue, incrementValue, (byte)bitCount, 0);
                ClassicAssert.AreEqual(resV1, resV2);
                ClassicAssert.AreEqual(overflowV1, overflowV2);
                ClassicAssert.AreEqual(result, resV2);
            }

            //signed overflow with wrap and sat
            for (int i = 0; i < tests; i++)
            {
                bitCount = rng.Next(1, 64);

                long value = RandomIntBitRange(bitCount, true);
                long incrBy = RandomIntBitRange(bitCount, true);
                bool overflow;
                long resV1;
                bool overflowV1;

                //wrap overflowtype
                db.Execute("BITFIELD", (RedisKey)key, "SET", "i" + bitCount.ToString(), $"{offset}", value);
                expectedResult = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "WRAP", "INCRBY", "i" + bitCount.ToString(), $"{offset}", incrBy);
                (result, overflow) = CheckSignedBitfieldOverflow(value, incrBy, (byte)bitCount, 0);
                (resV1, overflowV1) = CheckSignedBitfieldOverflowRedis(value, incrBy, (byte)bitCount, 0);
                ClassicAssert.AreEqual(resV1, result);
                ClassicAssert.AreEqual(result, expectedResult);

                //sat overflowtype
                value = RandomIntBitRange(bitCount, true);
                incrBy = RandomIntBitRange(bitCount, true);
                db.Execute("BITFIELD", (RedisKey)key, "SET", "i" + bitCount.ToString(), $"{offset}", value);
                if ((i & 0x1) == 0x1)
                    expectedResult = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "sat", "INCRBY", "i" + bitCount.ToString(), $"{offset}", incrBy);
                else
                    expectedResult = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "SAT", "INCRBY", "i" + bitCount.ToString(), $"{offset}", incrBy);
                (result, overflow) = CheckSignedBitfieldOverflow(value, incrBy, (byte)bitCount, 1);
                (resV1, overflowV1) = CheckSignedBitfieldOverflowRedis(value, incrBy, (byte)bitCount, 1);
                ClassicAssert.AreEqual(resV1, result);
                ClassicAssert.AreEqual(result, expectedResult);

                //fail overflowtype
                value = RandomIntBitRange(bitCount, true);
                incrBy = RandomIntBitRange(bitCount, true);
                db.Execute("BITFIELD", (RedisKey)key, "SET", "i" + bitCount.ToString(), $"{offset}", value);
                RedisResult[] redisResult;
                if ((i & 0x1) == 0x1)
                    redisResult = (RedisResult[])db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "fail", "INCRBY", "i" + bitCount.ToString(), $"{offset}", incrBy);
                else
                    redisResult = (RedisResult[])db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "FAIL", "INCRBY", "i" + bitCount.ToString(), $"{offset}", incrBy);

                (result, overflow) = CheckSignedBitfieldOverflow(value, incrBy, (byte)bitCount, 2);

                if (!overflow)
                    ClassicAssert.AreEqual(result, (long)redisResult[0]);
                else
                    ClassicAssert.AreEqual(redisResult[0].IsNull, true);
            }
        }

        [Test, Order(29)]
        [Category("BITFIELD")]
        public void BitmapBitfieldIncrTest_LTM()
        {
            int bitmapBytes = 512;
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                lowMemory: true,
                pageCount: 2,   // Specify pageCount instead of memorySize to avoid LogSizeTracker.MinTargetPageCount requirement
                pageSize: (bitmapBytes << 1).ToString());
            //LogMemorySize: "16g",
            //PageSize: "32m");
            server.Start();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int keyCount = 16;
            byte[][] bitmapData = new byte[keyCount][];

            //0. Initialize bitmapData            
            for (int i = 0; i < keyCount; i++)
            {
                bitmapData[i] = new byte[bitmapBytes];
                rng.NextBytes(bitmapData[i]);

                int key = i;
                string sKey = i.ToString();
                db.StringSet(sKey, bitmapData[i]);
            }

            long setNewValue;
            long incrByValue;
            long expectedValue;
            long returnedValue;
            bool overflow;

            int iter = 1 << 12;
            for (int i = 0; i < iter; i++)
            {
                int key = rng.Next(0, keyCount);
                byte[] currBitmap = bitmapData[key];
                string sKey = key.ToString();
                int offset = rng.Next(0, (bitmapData.Length << 3));
                int bitCount = rng.Next(1, 65);

                setNewValue = RandomIntBitRange(bitCount, true);
                incrByValue = RandomIntBitRange(bitCount, true);

                db.Execute("BITFIELD", (RedisKey)sKey, "SET", "i" + bitCount.ToString(), $"{offset}", setNewValue);
                returnedValue = (long)db.Execute("BITFIELD", (RedisKey)sKey, "OVERFLOW", "WRAP", "INCRBY", "i" + bitCount.ToString(), $"{offset}", incrByValue);
                (expectedValue, overflow) = CheckSignedBitfieldOverflow(setNewValue, incrByValue, (byte)bitCount, 0);
                ClassicAssert.AreEqual(expectedValue, returnedValue);
            }
        }

        private static (ulong, bool) CheckUnsignedBitfieldOverflow(ulong value, long incrBy, byte bitCount, byte overflowType)
        {
            ulong maxVal = bitCount == 64 ? ulong.MaxValue : (1UL << bitCount) - 1;
            ulong maxAdd = maxVal - value;

            bool neg = incrBy < 0 ? true : false;
            //get absolute value of given increment
            ulong absIncrBy = incrBy < 0 ? (ulong)(~incrBy) + 1UL : (ulong)incrBy;
            //overflow if absolute increment is larger than diff of maxVal and current value
            bool overflow = (absIncrBy > maxAdd);
            //underflow if absolute increment bigger than increment and increment is negative
            bool underflow = (absIncrBy > value) && neg;

            ulong result;
            ulong mask = maxVal;
            result = neg ? value - absIncrBy : value + absIncrBy;
            result &= mask;
            switch (overflowType)
            {
                case 0://wrap                    
                    if (overflow || underflow)
                        return (result, true);
                    return (result, false);
                case 1://sat
                    if (overflow) return (maxVal, true);
                    else if (underflow) return (0, true);
                    return (result, false);
                case 2://fail 
                    if (overflow || underflow)
                        return (0, true);
                    return (result, false);
            }
            return (0, true);
        }

        [Test, Order(30)]
        [Category("BITFIELD")]
        public void BitmapBitfieldUnsignedIncrTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            string key = "x";
            int tests = 1 << 10;

            byte[] bitmapData = new byte[16];
            for (int i = 0; i < 16; i++) bitmapData[i] = 0;

            int bitCount = 0;
            long offset = 0;
            long result;
            ulong expectedResult;
            db.KeyDelete(key);
            db.StringSet(key, bitmapData);

            for (int i = 0; i < tests; i++)
            {
                bitCount = rng.Next(1, 63);

                long value = RandomIntBitRange(bitCount, false);
                long incrBy = RandomIntBitRange(bitCount, true);
                bool overflow;

                //wrap overflowtype
                db.Execute("BITFIELD", (RedisKey)key, "SET", "u" + bitCount.ToString(), $"{offset}", value);
                result = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "WRAP", "INCRBY", "u" + bitCount.ToString(), $"{offset}", incrBy);

                (expectedResult, overflow) = CheckUnsignedBitfieldOverflow((ulong)value, incrBy, (byte)bitCount, 0);
                ClassicAssert.AreEqual(result, expectedResult);

                //sat overflowtype
                db.Execute("BITFIELD", (RedisKey)key, "SET", "u" + bitCount.ToString(), $"{offset}", value);
                result = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "SAT", "INCRBY", "u" + bitCount.ToString(), $"{offset}", incrBy);

                (expectedResult, overflow) = CheckUnsignedBitfieldOverflow((ulong)value, incrBy, (byte)bitCount, 1);
                ClassicAssert.AreEqual(result, expectedResult);

                //fail overflowtype
                db.Execute("BITFIELD", (RedisKey)key, "SET", "u" + bitCount.ToString(), $"{offset}", value);
                RedisResult[] redisResult = (RedisResult[])db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "FAIL", "INCRBY", "u" + bitCount.ToString(), $"{offset}", incrBy);

                (expectedResult, overflow) = CheckUnsignedBitfieldOverflow((ulong)value, incrBy, (byte)bitCount, 2);
                if (!overflow)
                    ClassicAssert.AreEqual((long)redisResult[0], expectedResult);
                else
                    ClassicAssert.AreEqual(redisResult[0].IsNull, true);
            }
        }

        [Test, Order(31)]
        [Category("BITFIELD")]
        public void BitmapBitfieldGrowingTest([Values] RevivificationMode revivificationModeUsedBySetupOnly)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            string key = "x";
            int tests = 1 << 5;

            int bitCount = 0;
            long offset = 0;
            long result;
            long expectedResult;
            bool overflow;

            //set signed growing
            for (int j = 1; j <= 64; j++)
            {
                db.KeyDelete(key);
                List<long> values = [];
                bitCount = j;
                for (int i = 0; i < tests; i++)
                {
                    offset = i;
                    long value = RandomIntBitRange(bitCount, true);
                    values.Add(value);
                    db.Execute("BITFIELD", (RedisKey)key, "SET", "i" + bitCount.ToString(), "#" + offset.ToString(), value);
                }

                for (int i = 0; i < tests; i++)
                {
                    offset = i;
                    result = (long)db.Execute("BITFIELD", (RedisKey)key, "GET", "i" + bitCount.ToString(), "#" + offset.ToString());
                    expectedResult = values[i];
                    ClassicAssert.AreEqual(result, expectedResult);
                }
            }

            //incrby signed growing
            for (int j = 1; j <= 64; j++)
            {
                bitCount = j;

                //wrap incrby
                db.KeyDelete(key);
                for (int i = 0; i < tests; i++)
                {
                    offset = i;
                    long value = RandomIntBitRange(bitCount, true);
                    long incrBy = RandomIntBitRange(bitCount, true);

                    result = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "WRAP", "INCRBY", "i" + bitCount.ToString(), "#" + offset.ToString(), value);
                    ClassicAssert.AreEqual(value, result);

                    result = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "WRAP", "INCRBY", "i" + bitCount.ToString(), "#" + offset.ToString(), incrBy);
                    (expectedResult, overflow) = CheckSignedBitfieldOverflow(value, incrBy, (byte)bitCount, 0);
                    ClassicAssert.AreEqual(expectedResult, result);
                }

                //sat incrby
                db.KeyDelete(key);
                for (int i = 0; i < tests; i++)
                {
                    offset = i;
                    long value = RandomIntBitRange(bitCount, true);
                    long incrBy = RandomIntBitRange(bitCount, true);

                    result = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "SAT", "INCRBY", "i" + bitCount.ToString(), "#" + offset.ToString(), value);
                    ClassicAssert.AreEqual(result, value);

                    result = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "SAT", "INCRBY", "i" + bitCount.ToString(), "#" + offset.ToString(), incrBy);
                    (expectedResult, overflow) = CheckSignedBitfieldOverflow(value, incrBy, (byte)bitCount, 1);
                    ClassicAssert.AreEqual(result, expectedResult);
                }

                db.KeyDelete(key);
                for (int i = 0; i < tests; i++)
                {
                    offset = i;
                    long value = RandomIntBitRange(bitCount, true);
                    long incrBy = RandomIntBitRange(bitCount, true);

                    result = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "SAT", "INCRBY", "i" + bitCount.ToString(), "#" + offset.ToString(), value);
                    ClassicAssert.AreEqual(result, value);

                    result = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "SAT", "INCRBY", "i" + bitCount.ToString(), "#" + offset.ToString(), incrBy);
                    (expectedResult, overflow) = CheckSignedBitfieldOverflow(value, incrBy, (byte)bitCount, 1);
                    ClassicAssert.AreEqual(result, expectedResult);
                }
            }

            // incrby growing fail test
            for (int j = 1; j <= 64; j++)
            {
                bitCount = j;
                List<long> values = [];

                db.KeyDelete(key);
                for (int i = 0; i < tests; i++)
                {
                    offset = i;
                    long value = RandomIntBitRange(bitCount, true);
                    long incrBy = RandomIntBitRange(bitCount, true);

                    result = (long)db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "FAIL", "INCRBY", "i" + bitCount.ToString(), "#" + offset.ToString(), value);
                    ClassicAssert.AreEqual(result, value);

                    db.Execute("BITFIELD", (RedisKey)key, "OVERFLOW", "FAIL", "INCRBY", "i" + bitCount.ToString(), "#" + offset.ToString(), incrBy);
                    (expectedResult, overflow) = CheckSignedBitfieldOverflow(value, incrBy, (byte)bitCount, 2);

                    if (overflow) values.Add(0);
                    else values.Add(expectedResult);
                }

                for (int i = 0; i < tests; i++)
                {
                    offset = i;
                    result = (long)db.Execute("BITFIELD", (RedisKey)key, "GET", "i" + bitCount.ToString(), "#" + offset.ToString());
                    expectedResult = values[i];
                    ClassicAssert.AreEqual(result, expectedResult);
                }
            }
        }

        [Test, Order(32)]
        [Category("BITMAPPROC")]
        public void BitmapCmdsProcedureTest()
        {
            server.Register.NewTransactionProc("BITMAPPROC", () => new TestProcedureBitmap(), new RespCommandsInfo { Arity = 6 });
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string bitmapA = "user:Activity";
            string bitmapB = "user:KeyOperation";
            string bitmapC = "bitmapB";

            var result = db.Execute("BITMAPPROC", bitmapA, DateTime.Now.Day, 1, bitmapB, bitmapC);
            ClassicAssert.AreEqual("SUCCESS", (string)result);
        }

        [Test, Order(33)]
        [Category("BITCOUNT")]
        public void BitmapBitCountSimpleTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "mykey";
            db.StringSet(key, "foobar");

            long count = db.StringBitCount(key);
            ClassicAssert.AreEqual(26, count);

            count = db.StringBitCount(key, 0, int.MaxValue);
            ClassicAssert.AreEqual(26, count);

            count = db.StringBitCount(key, 0, 0);
            ClassicAssert.AreEqual(4, count);

            count = db.StringBitCount(key, 1, 1);
            ClassicAssert.AreEqual(6, count);

            count = db.StringBitCount(key, 1, 1, StringIndexType.Byte);
            ClassicAssert.AreEqual(6, count);

            count = db.StringBitCount(key, 5, 30, StringIndexType.Bit);
            ClassicAssert.AreEqual(17, count);

            count = db.StringBitCount(key, 16, 22, StringIndexType.Bit);
            ClassicAssert.AreEqual(5, count);

            count = db.StringBitCount(key, -30, -5, StringIndexType.Bit);
            ClassicAssert.AreEqual(14, count);
        }

        [Test, Order(34)]
        [Category("BITPOS")]
        public void BitmapBitPosFixedTests()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mykey";
            byte[] value = [0x00, 0xff, 0xf0];
            db.StringSet(key, value);

            var pos = db.StringBitPosition(key, true, 0);
            ClassicAssert.AreEqual(8, pos);

            pos = db.StringBitPosition(key, true, 2, -1, StringIndexType.Byte);
            ClassicAssert.AreEqual(16, pos);

            pos = db.StringBitPosition(key, true, 0, 0, StringIndexType.Byte);
            ClassicAssert.AreEqual(-1, pos);

            pos = db.StringBitPosition(key, false, 0, 0, StringIndexType.Byte);
            ClassicAssert.AreEqual(0, pos);

            pos = db.StringBitPosition(key, true, 7, 15, StringIndexType.Bit);
            ClassicAssert.AreEqual(8, pos);

            value = [0xf8, 0x6f, 0xf0];
            _ = db.StringSet(key, value);
            pos = db.StringBitPosition(key, true, 5, 17, StringIndexType.Bit);
            ClassicAssert.AreEqual(9, pos);

            pos = db.StringBitPosition(key, true, 10, 12, StringIndexType.Bit);
            ClassicAssert.AreEqual(10, pos);

            pos = db.StringBitPosition(key, true, 20, 25, StringIndexType.Bit);
            ClassicAssert.AreEqual(-1, pos);

            key = "mykey2";
            _ = db.StringSetBit(key, 63, false);
            pos = db.StringBitPosition(key, false, 1);
            ClassicAssert.AreEqual(8, pos);

            pos = db.StringBitPosition(key, false, 0);
            ClassicAssert.AreEqual(0, pos);

            value = [0xff, 0x7f, 0xf0];
            _ = db.StringSet(key, value);
            pos = db.StringBitPosition(key, false, 7, 15, StringIndexType.Bit);
            ClassicAssert.AreEqual(8, pos);
        }

        [Test, Order(35)]
        [Category("BITOP")]
        public void BitmapOperationNonExistentSourceKeys()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            RedisKey dstKey = "dstKey";
            RedisKey[] sourceKeys = ["a", "b", "c"];
            var size = db.StringBitOperation(Bitwise.And, dstKey, sourceKeys);
            ClassicAssert.AreEqual(0, size);
        }

        [Test, Order(36)]
        [Category("BITOP")]
        public void BitmapOperationInvalidOption()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var args = new List<object> { "INVALID_OPTION", "a", "b", "c" };
            var ex = Assert.Throws<RedisServerException>(() => db.Execute("BITOP", args));
            ClassicAssert.AreEqual("ERR syntax error", ex.Message);
        }

        [Test, Order(37)]
        [Category("BITOP")]
        public void BitmapOperationTooManyKeys()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var args = new List<object> { "AND", "a" };

            for (var i = 0; i < 128; i++)
                args.Add(i.ToString());

            var ex = Assert.Throws<RedisServerException>(() => db.Execute("BITOP", args));
            ClassicAssert.AreEqual("ERR Bitop source key limit (64) exceeded", ex.Message);
        }

        [Test, Order(38)]
        [Category("BITPOS")]
        public void BitmapBitPosBitOffsetTests([Values] bool searchFor)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mykey";
            byte[] value = searchFor ?
                [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00] :
                [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
            _ = db.StringSet(key, value);

            var bitLength = value.Length * 8;
            var expectedPosOffset = 5;

            for (var i = 0; i < 10; i++)
            {
                // Set or clear bit
                _ = db.StringSetBit(key, offset: expectedPosOffset, bit: searchFor);

                // Find pos of bit set/clear
                var pos = db.StringBitPosition(key, bit: searchFor, 0, 19, StringIndexType.Bit);
                ClassicAssert.AreEqual(expectedPosOffset, pos);

                // Toggle bit back to initial value
                _ = db.StringSetBit(key, offset: expectedPosOffset, bit: !searchFor);

                expectedPosOffset++;
            }
        }

        [Test, Order(38)]
        [Category("BITPOS")]
        public void BitmapBitPosBitInvalidMaskTests()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mykey";
            // 0x3e = 00111110
            byte[] value = [0x3e];
            _ = db.StringSet(key, value);

            // 0x3e = 00111110
            var pos = db.StringBitPosition(key, bit: false, start: 0, end: 5, StringIndexType.Bit);
            ClassicAssert.AreEqual(0, pos);

            pos = db.StringBitPosition(key, bit: false, start: 1, end: 5, StringIndexType.Bit);
            ClassicAssert.AreEqual(1, pos);

            pos = db.StringBitPosition(key, bit: false, start: 2, end: 5, StringIndexType.Bit);
            ClassicAssert.AreEqual(-1, pos);

            pos = db.StringBitPosition(key, bit: false, start: 2, end: 6, StringIndexType.Bit);
            ClassicAssert.AreEqual(-1, pos);

            pos = db.StringBitPosition(key, bit: false, start: 2, end: 7, StringIndexType.Bit);
            ClassicAssert.AreEqual(7, pos);

            // 0x7e02 = 0111111000000010
            value = [0x7e, 0x02];
            _ = db.StringSet(key, value);
            pos = db.StringBitPosition(key, bit: true, start: 7, end: 13, StringIndexType.Bit);
            ClassicAssert.AreEqual(-1, pos);

            pos = db.StringBitPosition(key, bit: true, start: 7, end: 14, StringIndexType.Bit);
            ClassicAssert.AreEqual(14, pos);
        }

        [Test, Order(39)]
        [Category("BITPOS")]
        public void BitmapBitPosBitSearchSingleBitRangeTests()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mykey";
            var valueLen = 1 << 12;
            var value = new byte[valueLen];
            for (var i = 0; i < valueLen; i++)
                value[i] = 0xAA;

            _ = db.StringSet(key, value);

            var iter = 1 << 12;
            var valueLenBits = valueLen << 3;
            for (var i = 0; i < iter; i++)
            {
                var offset = rng.NextInt64(0, valueLenBits);
                BitSearch(offset, searchFor: true);
                BitSearch(offset, searchFor: false);
            }

            void BitSearch(long offset, bool searchFor)
            {
                var pos = db.StringBitPosition(key, bit: searchFor, start: offset, end: offset, StringIndexType.Bit);
                var equalsSearchFor = (offset & 0x1) == (searchFor ? 0 : 1);

                if (equalsSearchFor)
                    ClassicAssert.AreEqual(offset, pos);
                else
                    ClassicAssert.AreEqual(-1, pos);
            }
        }

        [Test, Order(40)]
        [Category("BITFIELD")]
        public void BitmapBitfieldBoundaryTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            var bit = db.StringSetBit(key, offset: 0, bit: true);
            ClassicAssert.AreEqual(expected: false, actual: bit);
            bit = db.StringSetBit(key, offset: 8, bit: true);
            ClassicAssert.AreEqual(expected: false, actual: bit);

            var ret = db.Execute("BITFIELD", (RedisKey)key, "SET", "u8", 0, 1);
            ClassicAssert.AreEqual(1, ((string[])ret).Length);
            ClassicAssert.AreEqual("128", ret[0].ToString());

            ret = db.Execute("BITFIELD", (RedisKey)key, "SET", "u8", 0, 128);
            ClassicAssert.AreEqual(1, ((string[])ret).Length);
            ClassicAssert.AreEqual("1", ret[0].ToString());

            ret = db.Execute("BITFIELD", (RedisKey)key, "SET", "u8", 8, 1);
            ClassicAssert.AreEqual(1, ((string[])ret).Length);
            ClassicAssert.AreEqual("128", ret[0].ToString());

            var result = (byte[])db.StringGet(key);
            ClassicAssert.AreEqual(expected: new byte[] { 0x80, 0x01 }, actual: result);

            ret = db.Execute("BITFIELD", (RedisKey)key, "SET", "u8", 8, 128, "GET", "u8", 8);
            ClassicAssert.AreEqual(2, ((string[])ret).Length);
            ClassicAssert.AreEqual("1", ret[0].ToString());
            ClassicAssert.AreEqual("128", ret[1].ToString());

            result = (byte[])db.StringGet(key);
            ClassicAssert.AreEqual(expected: new byte[] { 0x80, 0x80 }, actual: result);
        }

        [Order(41)]
        [Test]
        [Category("BITFIELD")]
        public void BitmapBitFieldInvalidOptionsTest([Values(RespCommand.BITFIELD, RespCommand.BITFIELD_RO)] RespCommand testCmd)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "BitmapBitFieldInvalidOptionsTest";

            var ex = Assert.Throws<RedisServerException>(() => db.Execute(testCmd.ToString(), key, "GET"));
            ClassicAssert.AreEqual("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is",
                                   ex.Message);

            ex = Assert.Throws<RedisServerException>(() => db.Execute(testCmd.ToString(), key, "GET", "u64", "0"));
            ClassicAssert.AreEqual("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is",
                                   ex.Message);

            ex = Assert.Throws<RedisServerException>(() => db.Execute(testCmd.ToString(), key, "GET", "i-1", "0"));
            ClassicAssert.AreEqual("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is",
                                   ex.Message);

            ex = Assert.Throws<RedisServerException>(() => db.Execute(testCmd.ToString(), key, "GET", "u8", @""""));
            ClassicAssert.AreEqual("ERR bit offset is not an integer or out of range",
                                   ex.Message);

            ex = Assert.Throws<RedisServerException>(() => db.Execute(testCmd.ToString(), key, "GET", "i16", "#"));
            ClassicAssert.AreEqual("ERR bit offset is not an integer or out of range",
                                   ex.Message);

            ex = Assert.Throws<RedisServerException>(() => db.Execute(testCmd.ToString(), key, "GET", "32", "1"));
            ClassicAssert.AreEqual("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is",
                                   ex.Message);

            ex = Assert.Throws<RedisServerException>(() => db.Execute(testCmd.ToString(), key, "GET", "u32", @"-1"));
            ClassicAssert.AreEqual("ERR bit offset is not an integer or out of range",
                                   ex.Message);

            if (testCmd == RespCommand.BITFIELD)
            {
                ex = Assert.Throws<RedisServerException>(() => db.Execute(testCmd.ToString(), key, "SET", "i32", "0"));
                ClassicAssert.AreEqual("ERR value is not an integer or out of range.",
                                       ex.Message);

                ex = Assert.Throws<RedisServerException>(() => db.Execute(testCmd.ToString(), key, "OVERFLOW", "NONE"));
                ClassicAssert.AreEqual("ERR Invalid OVERFLOW type specified",
                                       ex.Message);
            }
            else
            {
                ex = Assert.Throws<RedisServerException>(() => db.Execute(testCmd.ToString(), key, "SET", "i64", "0"));
                ClassicAssert.AreEqual("ERR syntax error",
                                       ex.Message);
            }
        }
    }
}