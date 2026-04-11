// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using Allure.NUnit;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public unsafe class HyperLogLogTests : AllureTestBase
    {
        GarnetServer server;
        Random r;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();
            r = new Random(674386);
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        [Repeat(1)]
        public void SimpleHyperLogLogAddCount()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string[] data = ["a", "b", "c", "d", "e", "f"];
            string key = "hllKey";
            bool fUpdated = false;

            //HLL updated
            for (int i = 0; i < data.Length; i++)
            {
                fUpdated = db.HyperLogLogAdd(key, data[i]);
                ClassicAssert.IsTrue(fUpdated);
            }

            //HLL not updated
            for (int i = 0; i < data.Length; i++)
            {
                fUpdated = db.HyperLogLogAdd(key, data[i]);
                ClassicAssert.IsFalse(fUpdated);
            }

            //estimate cardinality
            long pfcount = db.HyperLogLogLength(key);
            ClassicAssert.AreEqual(6, pfcount);
        }

        public static void SimpleHyperLogLogArrayAddCount()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            RedisValue[] x = ["h", "e", "l", "l", "o"];
            RedisValue[] y = ["w", "o", "r", "l", "d"];

            string keyX = "x";
            var ret = db.HyperLogLogAdd(keyX, x);
            ClassicAssert.IsTrue(ret);

            ret = db.HyperLogLogAdd(keyX, y);
            ClassicAssert.IsTrue(ret);

            long pfcount = db.HyperLogLogLength(keyX);
            ClassicAssert.AreEqual(7, pfcount);
        }

        [Test]
        [Repeat(1)]
        public void SimpleHyperLogLogMerge()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            RedisValue[] x = ["h", "e", "l", "l", "o"];
            RedisValue[] y = ["w", "o", "r", "l", "d"];
            string keyX = "x";
            string keyY = "y";
            string keyW = "w";

            //HLL updated           
            db.HyperLogLogAdd(keyX, x);
            long keyXCount = db.HyperLogLogLength(keyX);
            ClassicAssert.AreEqual(keyXCount, 4);

            db.HyperLogLogAdd(keyY, y);
            long keyYCount = db.HyperLogLogLength(keyY);
            ClassicAssert.AreEqual(keyYCount, 5);

            var res = db.Execute("PFMERGE", keyW, keyX);
            long keyWCount = db.HyperLogLogLength(keyW);
            ClassicAssert.AreEqual(keyWCount, 4);

            res = db.Execute("PFMERGE", keyW, keyY);
            keyWCount = db.HyperLogLogLength(keyW);
            ClassicAssert.AreEqual(keyWCount, 7);
        }

        [Test]
        public void HyperLogLogSimpleInvalidHLLTypeTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            RedisValue[] x = ["h", "e", "l", "l", "o"];
            RedisValue[] y = ["w", "o", "r", "l", "d"];
            string keyX = "x";
            string keyY = "y";
            string keyW = "w";

            db.HyperLogLogAdd(keyX, x);
            db.HyperLogLogAdd(keyY, y);
            db.StringSet(keyW, "100");

            try
            {
                db.HyperLogLogAdd(keyW, x);
            }
            catch (Exception ex)
            {
                Assert.That(ex.Message, Does.EndWith("WRONGTYPE Key is not a valid HyperLogLog string value."));
            }

            try
            {
                db.HyperLogLogLength(keyW);
            }
            catch (Exception ex)
            {
                Assert.That(ex.Message, Does.EndWith("WRONGTYPE Key is not a valid HyperLogLog string value."));
            }

            try
            {
                db.HyperLogLogMerge(keyW, keyY, keyX);
            }
            catch (Exception ex)
            {
                Assert.That(ex.Message, Does.EndWith("WRONGTYPE Key is not a valid HyperLogLog string value."));
            }

            try
            {
                db.HyperLogLogMerge(keyY, keyW, keyX);
            }
            catch (Exception ex)
            {
                Assert.That(ex.Message, Does.EndWith("WRONGTYPE Key is not a valid HyperLogLog string value."));
            }

            try
            {
                db.HyperLogLogMerge(keyY, keyX, keyW);
            }
            catch (Exception ex)
            {
                Assert.That(ex.Message, Does.EndWith("WRONGTYPE Key is not a valid HyperLogLog string value."));
            }
        }

        private unsafe (int, int) SingleHyperLogLogPCT(byte* buf, int bytesRead, int opType)
        {
            int count = 0;
            for (int i = 0; i < bytesRead; i++)
            {
                if (buf[i] == ':')
                    count++;
            }
            return (bytesRead, count);
        }

        [Test]
        [TestCase(5)]
        [TestCase(10)]
        [TestCase(20)]
        public void HyperLogLogSimple_PCT(int bytesPerSend)
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            string expectedResponse = "";
            byte[] response;
            string data = "hello";

            //1. PFADD mykey
            for (int i = 0; i < data.Length; i++)
            {
                response = lightClientRequest.SendCommandChunks("PFADD mykey " + data[i], bytesPerSend);
                expectedResponse = i == 0 || data[i - 1] != data[i] ? ":1\r\n" : ":0\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            }
            lightClientRequest.Dispose();
        }

        [Test]
        [TestCase(5)]
        [TestCase(10)]
        [TestCase(20)]
        public void HyperLogLogArraySimple_PCT(int bytesPerSend)
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            string expectedResponse = "";
            byte[] response;

            //1. PFADD mykey
            response = lightClientRequest.SendCommandChunks("PFADD mykey h e l l o", bytesPerSend);
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //2. PFCOUNT mykey
            response = lightClientRequest.SendCommandChunks("PFCOUNT mykey", bytesPerSend);
            expectedResponse = ":4\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //3. PFADD mykey2
            response = lightClientRequest.SendCommandChunks("PFADD mykey2 w o r l d", bytesPerSend);
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //4. PFCOUNT mykey mykey2
            response = lightClientRequest.SendCommandChunks("PFCOUNT mykey mykey2", bytesPerSend);
            expectedResponse = ":7\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //5. PFMERGE mykey3
            response = lightClientRequest.SendCommandChunks("PFMERGE mykey3 mykey", bytesPerSend);
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //6. PFCOUNT mykey3
            response = lightClientRequest.SendCommandChunks("PFCOUNT mykey3", bytesPerSend);
            expectedResponse = ":4\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //7. PFMERGE mykey4 mykey mykey2
            response = lightClientRequest.SendCommandChunks("PFMERGE mykey4 mykey mykey2", bytesPerSend);
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //8. PFCOUNT mykey4
            response = lightClientRequest.SendCommandChunks("PFCOUNT mykey4", bytesPerSend);
            expectedResponse = ":7\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        private static unsafe ulong MurmurHash2x64A(byte* bString, int len, uint seed = 0)
        {
            ulong m = (ulong)0xc6a4a7935bd1e995;
            int r = 47;
            ulong h = seed ^ ((ulong)len * m);
            byte* data = bString;
            byte* end = data + (len - (len & 7));

            while (data != end)
            {
                ulong k;
                k = (ulong)data[0];
                k |= (ulong)data[1] << 8;
                k |= (ulong)data[2] << 16;
                k |= (ulong)data[3] << 24;
                k |= (ulong)data[4] << 32;
                k |= (ulong)data[5] << 40;
                k |= (ulong)data[6] << 48;
                k |= (ulong)data[7] << 56;

                k *= m;
                k ^= k >> r;
                k *= m;
                h ^= k;
                h *= m;

                data += 8;
            }

            int cs = len & 7;

            if (cs >= 7)
                h ^= ((ulong)data[6] << 48);

            if (cs >= 6)
                h ^= ((ulong)data[5] << 40);

            if (cs >= 5)
                h ^= ((ulong)data[4] << 32);

            if (cs >= 4)
                h ^= ((ulong)data[3] << 24);

            if (cs >= 3)
                h ^= ((ulong)data[2] << 16);

            if (cs >= 2) h ^= ((ulong)data[1] << 8);
            if (cs >= 1)
            {
                h ^= (ulong)data[0];
                h *= m;
            }

            h ^= h >> r;
            h *= m;
            h ^= h >> r;
            return h;
        }

        [Test]
        [Repeat(1)]
        public void HyperLogLogUpdateReturnTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int insertCount = 128;
            string key = "HyperLogLogUpdateSparseTest";
            byte[] value = new byte[32];
            byte[] hll = new byte[HyperLogLog.DefaultHLL.DenseBytes];

            fixed (byte* hllPtr = hll)
                HyperLogLog.DefaultHLL.InitDense(hllPtr);

            for (int i = 0; i < insertCount; i++)
            {
                r.NextBytes(value);

                bool updated = db.HyperLogLogAdd(key, value);
                bool expectedUpdated = false;

                fixed (byte* hllPtr = hll)
                fixed (byte* v = value)
                {
                    long hv = (long)MurmurHash2x64A(v, value.Length);
                    expectedUpdated = HyperLogLog.DefaultHLL.UpdateDense(hllPtr, hv);
                }

                ClassicAssert.AreEqual(expectedUpdated, updated);
            }
        }

        public class ByteArrayComparer : IEqualityComparer<byte[]>
        {
            public bool Equals(byte[] a, byte[] b)
            {
                if (a.Length != b.Length) return false;
                for (int i = 0; i < a.Length; i++)
                    if (a[i] != b[i]) return false;
                return true;
            }
            public int GetHashCode(byte[] a)
            {
                uint b = 0;
                for (int i = 0; i < a.Length; i++)
                    b = ((b << 23) | (b >> 9)) ^ a[i];
                return unchecked((int)b);
            }
        }

        readonly byte[] ascii_chars = System.Text.Encoding.ASCII.GetBytes("abcdefghijklmnopqrstvuwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
        private void RandomString(ref byte[] valuebuffer)
        {
            for (int i = 0; i < valuebuffer.Length; i++)
                valuebuffer[i] = ascii_chars[r.Next(ascii_chars.Length)];
        }

        [Test]
        [Repeat(1)]
        public void HyperLogLogMultiValueUpdateReturnTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int insertCount = 128;
            string key = "HyperLogLogMultiValueUpdateReturnTest";
            byte[] value = new byte[32];
            byte[] hll = new byte[HyperLogLog.DefaultHLL.DenseBytes];
            bool expectedUpdated = false;

            fixed (byte* hllPtr = hll)
                HyperLogLog.DefaultHLL.InitDense(hllPtr);

            RedisValue[] insertValues = new RedisValue[insertCount];
            HashSet<byte[]> set = new HashSet<byte[]>(new ByteArrayComparer());

            for (int i = 0; i < insertCount; i++)
            {
                RandomString(ref value);
                set.Add(value);

                insertValues[i] = new RedisValue(System.Text.Encoding.ASCII.GetString(value));
                fixed (byte* hllPtr = hll)
                fixed (byte* v = value)
                {
                    long hv = (long)MurmurHash2x64A(v, value.Length);
                    expectedUpdated |= HyperLogLog.DefaultHLL.UpdateDense(hllPtr, hv);
                }
            }

            bool updated = db.HyperLogLogAdd(key, insertValues);
            ClassicAssert.AreEqual(updated, expectedUpdated);

            long estimate = db.HyperLogLogLength(key);
            double error = EstimationError(estimate, set.Count);
            ClassicAssert.IsTrue(error < 4.0);

            for (int i = 0; i < 10; i++)
            {
                insertCount = r.Next(1, 128);
                insertValues = new RedisValue[insertCount];
                expectedUpdated = false;

                for (int j = 0; j < insertCount; j++)
                {
                    RandomString(ref value);
                    set.Add(value);

                    insertValues[j] = new RedisValue(System.Text.Encoding.ASCII.GetString(value));
                    fixed (byte* hllPtr = hll)
                    fixed (byte* v = value)
                    {
                        long hv = (long)MurmurHash2x64A(v, value.Length);
                        expectedUpdated |= HyperLogLog.DefaultHLL.UpdateDense(hllPtr, hv);
                    }
                }

                updated = db.HyperLogLogAdd(key, insertValues);
                ClassicAssert.AreEqual(updated, expectedUpdated);

                estimate = db.HyperLogLogLength(key);
                error = EstimationError(estimate, set.Count);
                ClassicAssert.IsTrue(error < 4.0);
            }
        }

        [Test]
        [Repeat(1)]
        public void HyperLogLogMultiCountTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            RedisValue[] dataA = ["h", "e", "l", "l", "o"];
            RedisValue[] dataB = ["w", "o", "r", "l", "d"];
            RedisValue[] dataC = ["a", "b", "c", "d", "e", "f"];
            string keyA = "HyperLogLogMultiCountTestA";
            string keyB = "HyperLogLogMultiCountTestB";
            string keyC = "HyperLogLogMultiCountTestC";
            RedisKey[] keys = [keyA, keyB, keyC];

            db.KeyDelete(keyA);
            db.KeyDelete(keyB);
            db.KeyDelete(keyC);

            db.HyperLogLogAdd(keyA, dataA);
            db.HyperLogLogAdd(keyB, dataB);
            db.HyperLogLogAdd(keyC, dataC);

            long countA = db.HyperLogLogLength(keyA);
            long countB = db.HyperLogLogLength(keyB);
            long countC = db.HyperLogLogLength(keyC);

            ClassicAssert.AreEqual(4, countA);
            ClassicAssert.AreEqual(5, countB);
            ClassicAssert.AreEqual(6, countC);

            db.KeyDelete(keyA);
            db.KeyDelete(keyB);
            db.KeyDelete(keyC);

            db.HyperLogLogAdd(keyA, dataA);
            db.HyperLogLogAdd(keyB, dataB);
            db.HyperLogLogAdd(keyC, dataC);

            long totalCount = db.HyperLogLogLength(keys);
            ClassicAssert.AreEqual(11, totalCount);
        }

        public long LongRandom() => ((long)this.r.Next() << 32) | (long)this.r.Next();

        public List<long> RandomSubSeq(List<long> list, int count)
        {
            List<long> rss = [];
            for (int i = 0; i < count; i++) rss.Add(list[r.Next(list.Count)]);
            return rss;
        }

        public RedisValue[] RandomRedisValueSubseq(List<long> list, int count)
        {
            List<RedisValue> rss = [];
            for (int i = 0; i < count; i++)
                rss.Add(list[r.Next(list.Count)]);
            return [.. rss];
        }

        public static List<long> ToList(RedisValue[] rss)
        {
            return [.. rss.Select(x => (long)x)];
        }

        [Test]
        [Repeat(1)]
        public void HyperLogLogTestPFADDV2()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            int smallSeq = 1 << 5;
            int largeSeq = 1 << 10;

            var keyA = System.Text.Encoding.ASCII.GetBytes("HyperLogLogTestPFADDA");//sparse
            var keyB = System.Text.Encoding.ASCII.GetBytes("HyperLogLogTestPFADDB");//sparse            

            HashSet<long> setA = [];
            HashSet<long> setB = [];

            for (int i = 0; i < smallSeq; i++)
            {
                long valA = LongRandom();
                long valB = LongRandom();
                db.HyperLogLogAdd(keyA, valA);
                db.HyperLogLogAdd(keyB, valB);
                setA.Add(valA);
                setB.Add(valB);
            }

            long estimateA = db.HyperLogLogLength(keyA);
            long estimateB = db.HyperLogLogLength(keyB);
            ClassicAssert.IsTrue(EstimationError(estimateA, setA.Count) < 4.0);
            ClassicAssert.IsTrue(EstimationError(estimateB, setB.Count) < 4.0);

            setA.Clear();
            setB.Clear();
            db.KeyDelete(keyA);
            db.KeyDelete(keyB);
            for (int i = 0; i < largeSeq; i++)
            {
                long valC = LongRandom();
                long valD = LongRandom();
                db.HyperLogLogAdd(keyA, valC);
                db.HyperLogLogAdd(keyB, valD);
                setA.Add(valC);
                setB.Add(valD);
            }

            long estimateC = db.HyperLogLogLength(keyA);
            long estimateD = db.HyperLogLogLength(keyB);
            ClassicAssert.IsTrue(EstimationError(estimateC, setA.Count) < 4.0, $"{estimateC} ~ {setA.Count}");
            ClassicAssert.IsTrue(EstimationError(estimateD, setB.Count) < 4.0, $"{estimateD} ~ {setB.Count}");
        }

        [Test]
        [TestCase(32)]
        [TestCase(4096)]
        [Repeat(1)]
        public void HyperLogLogPFADD_LTM(int seqSize)
        {
            bool sparse = seqSize < 128 ? true : false;
            server.Dispose();
            if (seqSize < 128)
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                    lowMemory: true,
                    memorySize: "2k",   // Must be LogSizeTracker.MinTargetPageCount pages due to memory size tracking
                    pageSize: "512");
            else
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                    lowMemory: true,
                    memorySize: "64k",  // Must be LogSizeTracker.MinTargetPageCount pages due to memory size tracking
                    pageSize: "16k");
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            int keyCount = sparse ? 32 : 4;
            int smallSeq = seqSize;

            Dictionary<int, HashSet<long>> hllKeyCollection = [];

            //1. Populate HLL
            for (int i = 0; i < keyCount; i++)
            {
                int key = i;
                string sKey = key.ToString();

                hllKeyCollection.Add(key, []);
                for (int j = 0; j < smallSeq; j++)
                {
                    long valA = LongRandom();
                    hllKeyCollection[key].Add(valA);
                    db.HyperLogLogAdd(sKey, valA);
                }
            }

            //2. Estimate cardinality
            for (int i = 0; i < keyCount; i++)
            {
                int key = i;
                string sKey = key.ToString();

                long estimate = db.HyperLogLogLength(sKey);
                long expectedEstimate = hllKeyCollection[key].Count;
                ClassicAssert.IsTrue(EstimationError(estimate, expectedEstimate) < 4.0);
            }
        }

        [Test]
        [Repeat(1)]
        public void HyperLogLogTestPFADD_DuplicatesV2()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            int smallSeq = 1 << 10;
            int largeSeq = 1 << 15;
            var keyA = System.Text.Encoding.ASCII.GetBytes("keyA");//sparse
            HashSet<long> setA = [];
            List<long> largeInput = [];

            for (int i = 0; i < largeSeq; i++)
                largeInput.Add(LongRandom());

            for (int i = 0; i < 16; i++)
            {
                RedisValue[] vals = RandomRedisValueSubseq(largeInput, smallSeq);
                db.HyperLogLogAdd(keyA, vals);
                setA.UnionWith(ToList(vals));

                long estimate = db.HyperLogLogLength(keyA);
                ClassicAssert.IsTrue(EstimationError(estimate, setA.Count) < 4.0);
            }
        }

        [Test]
        [Repeat(1)]
        public void HyperLogLogTestPFMERGE_SparseToSparseV2()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int smallSeq = 1 << 5;
            int largeSeq = 1 << 8;

            var keyA = System.Text.Encoding.ASCII.GetBytes("SSkeyA");//sparse
            var keyB = System.Text.Encoding.ASCII.GetBytes("SSkeyB");//sparse
            HashSet<long> setA = [];
            HashSet<long> setB = [];
            long estimate = 0;

            List<long> largeInput = [];
            RedisValue[] rss;
            for (int i = 0; i < largeSeq; i++)
                largeInput.Add(LongRandom());

            //1. HLL A
            rss = RandomRedisValueSubseq(largeInput, smallSeq);
            db.HyperLogLogAdd(keyA, rss);
            setA.UnionWith(ToList(rss));
            estimate = db.HyperLogLogLength(keyA);
            ClassicAssert.IsTrue(EstimationError(estimate, setA.Count) < 4.0);

            //2. HLL B
            rss = RandomRedisValueSubseq(largeInput, smallSeq);
            db.HyperLogLogAdd(keyB, rss);
            setB.UnionWith(ToList(rss));
            estimate = db.HyperLogLogLength(keyB);
            ClassicAssert.IsTrue(EstimationError(estimate, setB.Count) < 4.0);

            //3. Merge HLL B to HLL A
            db.HyperLogLogMerge(keyA, keyA, keyB);
            setA.UnionWith(setB);
            estimate = db.HyperLogLogLength(keyA);
            ClassicAssert.IsTrue(EstimationError(estimate, setA.Count) < 4.0);
        }

        [Test]
        [Repeat(10)]
        public void HyperLogLogTestPFMERGE_LTM_SparseToSparse()
        {
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                lowMemory: true,
                memorySize: "2k",   // Must be LogSizeTracker.MinTargetPageCount pages due to memory size tracking
                pageSize: "512");
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            int keyCount = 64;
            int smallSeq = 32;

            Dictionary<int, HashSet<long>> hllKeyCollection = [];

            //1. Populate HLL
            for (int i = 0; i < keyCount; i++)
            {
                int key = i;
                string sKey = key.ToString();

                hllKeyCollection.Add(key, []);
                for (int j = 0; j < smallSeq; j++)
                {
                    long valA = LongRandom();
                    hllKeyCollection[key].Add(valA);
                    db.HyperLogLogAdd(sKey, valA);
                }
            }

            //2. Merge HLL
            for (int i = 0; i < keyCount; i += 2)
            {
                int dstKey = i;
                int srcKey = i + 1;

                string sDstKey = dstKey.ToString();
                string sSrcKey = srcKey.ToString();

                hllKeyCollection[dstKey].UnionWith(hllKeyCollection[srcKey]);
                db.HyperLogLogMerge(sDstKey, sDstKey, sSrcKey);
            }

            //3. Estimate cardinality
            for (int i = 0; i < keyCount; i += 2)
            {
                int key = i;
                string sKey = key.ToString();

                long estimate = db.HyperLogLogLength(sKey);
                long expectedEstimate = hllKeyCollection[key].Count;
                ClassicAssert.IsTrue(EstimationError(estimate, expectedEstimate) < 4.0);
            }
        }

        [Test]
        [Repeat(1)]
        public void HyperLogLogTestPFMERGE_SparseToDenseV2()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int smallSeq = 1 << 5;
            int largeSeq = 1 << 12;

            var keyA = System.Text.Encoding.ASCII.GetBytes("SDkeyA");//sparse
            var keyB = System.Text.Encoding.ASCII.GetBytes("SDkeyB");//dense
            var keyC = System.Text.Encoding.ASCII.GetBytes("SDkeyC");//dense
            HashSet<long> setA = [];
            HashSet<long> setB = [];
            HashSet<long> setC = [];
            long estimate = 0;

            List<long> largeInput = [];
            RedisValue[] rss;
            for (int i = 0; i < largeSeq; i++)
                largeInput.Add(LongRandom());

            //1. HLL A
            rss = RandomRedisValueSubseq(largeInput, smallSeq);
            db.HyperLogLogAdd(keyA, rss);
            setA.UnionWith(ToList(rss));
            estimate = db.HyperLogLogLength(keyA);
            ClassicAssert.IsTrue(EstimationError(estimate, setA.Count) < 4.0);

            //2. HLL B
            rss = RandomRedisValueSubseq(largeInput, smallSeq);
            db.HyperLogLogAdd(keyB, rss);
            setB.UnionWith(ToList(rss));
            estimate = db.HyperLogLogLength(keyB);
            ClassicAssert.IsTrue(EstimationError(estimate, setB.Count) < 4.0);

            //3. Merge HLL B to HLL C => dense to empty
            db.HyperLogLogMerge(keyC, keyC, keyB);
            setC.UnionWith(setB);
            estimate = db.HyperLogLogLength(keyC);
            ClassicAssert.IsTrue(EstimationError(estimate, setC.Count) < 4.0);

            //4. Merge HLL A to HLL C => sparse to dense
            db.HyperLogLogMerge(keyA, keyA, keyC);
            setA.UnionWith(setC);
            estimate = db.HyperLogLogLength(keyA);
            ClassicAssert.IsTrue(EstimationError(estimate, setA.Count) < 4.0);
        }

        [Test]
        [TestCase(false)]
        [TestCase(true)]
        [Repeat(1)]
        public void HyperLogLogTestPFMERGE_LTM_SparseToDense(bool reverse)
        {
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                memorySize: "64k",  // Must be LogSizeTracker.MinTargetPageCount pages due to memory size tracking
                pageSize: "16k");
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            int keyCount = 4;
            int smallSeq = 32;
            int largeSeq = 1 << 13;

            Dictionary<int, HashSet<long>> hllKeyCollection = [];

            //1. Populate HLL
            for (int i = 0; i < keyCount; i++)
            {
                int key = i;
                string sKey = key.ToString();

                hllKeyCollection.Add(key, []);
                int seq = i % 2 == 0 ? smallSeq : largeSeq;
                for (int j = 0; j < seq; j++)
                {
                    long valA = LongRandom();
                    hllKeyCollection[key].Add(valA);
                    db.HyperLogLogAdd(sKey, valA);
                }
            }

            //2. Merge HLL
            for (int i = 0; i < keyCount; i += 2)
            {
                int dstKey = reverse ? i + 1 : i;
                int srcKey = reverse ? i : i + 1;

                string sDstKey = dstKey.ToString();
                string sSrcKey = srcKey.ToString();

                hllKeyCollection[dstKey].UnionWith(hllKeyCollection[srcKey]);
                db.HyperLogLogMerge(sDstKey, sDstKey, sSrcKey);
            }

            //3. Estimate cardinality
            for (int i = 0; i < keyCount; i += 2)
            {
                int key = reverse ? i + 1 : i;
                string sKey = key.ToString();

                long estimate = db.HyperLogLogLength(sKey);
                long expectedEstimate = hllKeyCollection[key].Count;
                ClassicAssert.IsTrue(EstimationError(estimate, expectedEstimate) < 4.0);
            }
        }

        [Test]
        [Repeat(1)]
        public void HyperLogLogTestPFMERGE_DenseToDenseV2()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int smallSeq = 1 << 13;
            int largeSeq = 1 << 13;

            var keyA = System.Text.Encoding.ASCII.GetBytes("DDkeyA");//dense
            var keyB = System.Text.Encoding.ASCII.GetBytes("DDkeyB");//dense
            var keyC = System.Text.Encoding.ASCII.GetBytes("DDkeyC");//dense
            HashSet<long> setA = [];
            HashSet<long> setB = [];
            HashSet<long> setC = [];
            long estimate = 0;

            List<long> largeInput = [];
            RedisValue[] rss;
            for (int i = 0; i < largeSeq; i++)
                largeInput.Add(LongRandom());

            //1. HLL A
            rss = RandomRedisValueSubseq(largeInput, smallSeq);
            db.HyperLogLogAdd(keyA, rss);
            setA.UnionWith(ToList(rss));
            estimate = db.HyperLogLogLength(keyA);
            ClassicAssert.IsTrue(EstimationError(estimate, setA.Count) < 4.0);

            //2. HLL B
            rss = RandomRedisValueSubseq(largeInput, smallSeq);
            db.HyperLogLogAdd(keyB, rss);
            setB.UnionWith(ToList(rss));
            estimate = db.HyperLogLogLength(keyB);
            ClassicAssert.IsTrue(EstimationError(estimate, setB.Count) < 4.0);

            //3. Merge HLL B to HLL C => dense to empty
            db.HyperLogLogMerge(keyC, keyC, keyB);
            setC.UnionWith(setB);
            estimate = db.HyperLogLogLength(keyC);
            ClassicAssert.IsTrue(EstimationError(estimate, setC.Count) < 4.0);

            //4. Merge HLL A to HLL C => sparse to dense
            db.HyperLogLogMerge(keyA, keyA, keyC);
            setA.UnionWith(setC);
            estimate = db.HyperLogLogLength(keyA);
            ClassicAssert.IsTrue(EstimationError(estimate, setA.Count) < 4.0);
        }

        [Test]
        [Repeat(1)]
        public void HyperLogLogTestPFMERGE_LTM_DenseToDense()
        {
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                lowMemory: true,
                memorySize: "64k",  // Must be LogSizeTracker.MinTargetPageCount pages due to memory size tracking
                pageSize: "16k");
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            int keyCount = 4;
            int largeSeq = 1 << 13;

            Dictionary<int, HashSet<long>> hllKeyCollection = [];

            //1. Populate HLL
            for (int i = 0; i < keyCount; i++)
            {
                int key = i;
                string sKey = key.ToString();

                hllKeyCollection.Add(key, []);
                for (int j = 0; j < largeSeq; j++)
                {
                    long valA = LongRandom();
                    hllKeyCollection[key].Add(valA);
                    db.HyperLogLogAdd(sKey, valA);
                }
            }

            //2. Merge HLL
            for (int i = 0; i < keyCount; i += 2)
            {
                int dstKey = i;
                int srcKey = i + 1;

                string sDstKey = dstKey.ToString();
                string sSrcKey = srcKey.ToString();

                hllKeyCollection[dstKey].UnionWith(hllKeyCollection[srcKey]);
                db.HyperLogLogMerge(sDstKey, sDstKey, sSrcKey);
            }

            //3. Estimate cardinality
            for (int i = 0; i < keyCount; i += 2)
            {
                int key = i;
                string sKey = key.ToString();

                long estimate = db.HyperLogLogLength(sKey);
                long expectedEstimate = hllKeyCollection[key].Count;
                ClassicAssert.IsTrue(EstimationError(estimate, expectedEstimate) < 4.0);
            }
        }

        [Test]
        [Repeat(1)]
        public void HyperLogLogPFMerge_MultiHLLMergeV2()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            int smallSeq = 1 << 10;
            int largeSeq = 1 << 14;
            int hllCount = 10;
            HashSet<long> dstSet = [];
            int srcKey = r.Next(1024, 2048);
            RedisKey[] srcKeys = new RedisKey[hllCount];

            List<long> values = [];
            for (int i = 0; i < largeSeq; i++)
                values.Add(LongRandom());

            for (int i = 0; i < hllCount; i++)
            {
                RedisValue[] rss = RandomRedisValueSubseq(values, smallSeq);
                int currKey = i + srcKey;
                srcKeys[i] = new RedisKey(currKey.ToString());

                db.HyperLogLogAdd(srcKeys[i], rss);
                dstSet.UnionWith(ToList(rss));
            }

            RedisKey key = new RedisKey("dstKey");
            db.HyperLogLogMerge(key, srcKeys);
            long estimate = db.HyperLogLogLength(key);
            double error = EstimationError(estimate, dstSet.Count);
            ClassicAssert.IsTrue(error < 4.0);
        }

        [Test]
        public void CanRunHLLProcedureTest()
        {
            server.Register.NewTransactionProc("HLLPROC", () => new TestProcedureHLL(), new RespCommandsInfo { Arity = 9 });
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var result = db.Execute("HLLPROC", "hll", "a", "b", "c", "d", "e", "f", "g");
            ClassicAssert.AreEqual("SUCCESS", (string)result);
        }

        private static double EstimationError(long estimate, long cardinality)
        {
            double error = ((double)Math.Abs(cardinality - estimate) / (double)cardinality) * 100;
            error = (double)(int)(error * 10000) / (double)10000;
            return error;
        }

        private static (int valueLength, int valueStart) ParseDumpValueLengthAndStart(byte[] dump)
        {
            if (dump[1] < 0x40)
            {
                return (dump[1], 2);
            }

            if (dump[1] < 0x80)
            {
                return (((dump[1] & 0x3F) << 8) | dump[2], 3);
            }

            return (BinaryPrimitives.ReadInt32BigEndian(dump.AsSpan(2, 4)), 6);
        }

        [Test]
        [Category("HLL_RESTORE")]
        public void HyperLogLogRestoreCorruptedDumpPayloadIsRejected()
        {
            // Test verifies that RESTORE command properly rejects a dump payload when the CRC checksum
            // doesn't match due to corruption. This ensures data integrity by preventing restoration
            // of corrupted HLL data.
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const string sourceKey = "hll_dump_source";
            const string restoredKey = "hll_dump_restored";

            // Step 1: Clean up and create source HLL
            _ = db.KeyDelete(sourceKey);
            _ = db.KeyDelete(restoredKey);

            for (var i = 0; i < 2; i++)
                _ = db.HyperLogLogAdd(sourceKey, $"elem_{i}");

            // Step 2: Dump the HLL
            var dump = db.KeyDump(sourceKey)!;
            var corruptedDump = (byte[])dump.Clone();

            // Step 3: Corrupt the dump by flipping a bit (avoiding the last 8 bytes which are CRC)
            var corruptIndex = Math.Min(20, corruptedDump.Length - 11);
            corruptedDump[corruptIndex] ^= 0x01;

            // Step 4 & 5: Attempt restore and verify it fails with checksum error
            var ex = Assert.Throws<RedisServerException>(() => db.KeyRestore(restoredKey, corruptedDump));
            StringAssert.Contains("ERR DUMP payload version or checksum are wrong", ex!.Message);

            // Step 6: Verify key was not created
            ClassicAssert.IsFalse(db.KeyExists(restoredKey));
        }

        [Test]
        [Category("HLL_RESTORE")]
        public void HyperLogLogRestoreZeroCrcDumpPayloadIsRejected()
        {
            // Test verifies that RESTORE command rejects a payload when the CRC checksum is zeroed out.
            // This prevents accepting invalid dumps that might bypass checksum validation.
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const string sourceKey = "hll_zero_crc_source";
            const string restoredKey = "hll_zero_crc_restored";

            // Step 1: Clean up and create source HLL
            _ = db.KeyDelete(sourceKey);
            _ = db.KeyDelete(restoredKey);

            for (var i = 0; i < 20; i++)
                _ = db.HyperLogLogAdd(sourceKey, $"elem_{i}");

            // Step 2: Dump the HLL
            var dump = db.KeyDump(sourceKey)!;
            var zeroCrcDump = (byte[])dump.Clone();

            // Step 3: Zero out the last 8 bytes (CRC64 checksum)
            Array.Fill(zeroCrcDump, (byte)0, zeroCrcDump.Length - 8, 8);

            // Step 4 & 5: Attempt restore and verify it fails with checksum error
            var ex = Assert.Throws<RedisServerException>(() => db.KeyRestore(restoredKey, zeroCrcDump));
            StringAssert.Contains("ERR DUMP payload version or checksum are wrong", ex!.Message);

            // Step 6: Verify key was not created
            ClassicAssert.IsFalse(db.KeyExists(restoredKey));
        }

        [Test]
        [Category("HLL_RESTORE")]
        public void HyperLogLogRestoreCorruptedSparseRlePayloadIsRejected()
        {
            // Test verifies that RESTORE command properly validates sparse HLL representation
            // by rejecting payloads with corrupted RLE (Run-Length Encoding) metadata.
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const string sourceKey = "hll_sparse_source";
            const string restoredKey = "hll_sparse_restored";

            // Step 1: Clean up and create sparse HLL
            _ = db.KeyDelete(sourceKey);
            _ = db.KeyDelete(restoredKey);

            for (var i = 0; i < 20; i++)
                _ = db.HyperLogLogAdd(sourceKey, $"elem_{i}");

            // Step 2: Dump and parse the structure
            var dump = db.KeyDump(sourceKey)!;
            var (valueLength, valueStart) = ParseDumpValueLengthAndStart(dump);

            // Step 3: Extract and corrupt the HLL value
            var hllValue = dump.AsSpan(valueStart, valueLength).ToArray();
            // Corrupt sparse stream length at offset 16-17
            BinaryPrimitives.WriteUInt16LittleEndian(hllValue.AsSpan(16, 2), 65000);
            // Corrupt cardinality cache at offset 8-15
            BinaryPrimitives.WriteInt64LittleEndian(hllValue.AsSpan(8, 8), long.MinValue);

            // Step 4: Reconstruct the dump with RDB encoding
            // Encode the length using Redis RDB string encoding format
            byte[] encodedLength = hllValue.Length < 64
                ? [(byte)(hllValue.Length & 0x3F)]
                : hllValue.Length < 16384
                    ? [(byte)(((hllValue.Length >> 8) & 0x3F) | 0x40), (byte)(hllValue.Length & 0xFF)]
                    : [0x80, .. BitConverter.GetBytes(System.Net.IPAddress.HostToNetworkOrder(hllValue.Length))];

            var rdbVersion = dump.AsSpan(valueStart + valueLength, 2).ToArray();
            var crafted = new byte[1 + encodedLength.Length + hllValue.Length + rdbVersion.Length + 8];
            crafted[0] = 0x00; // RDB type encoding
            encodedLength.CopyTo(crafted.AsSpan(1, encodedLength.Length));
            hllValue.CopyTo(crafted.AsSpan(1 + encodedLength.Length, hllValue.Length));
            rdbVersion.CopyTo(crafted.AsSpan(1 + encodedLength.Length + hllValue.Length, rdbVersion.Length));

            // Step 5 & 6: Attempt restore and verify it fails
            var ex = Assert.Throws<RedisServerException>(() => db.KeyRestore(restoredKey, crafted));
            StringAssert.Contains("ERR DUMP payload version or checksum are wrong", ex!.Message);

            // Step 7: Verify key was not created
            ClassicAssert.IsFalse(db.KeyExists(restoredKey));
        }

        [Test]
        [Category("HLL_RESTORE")]
        public void HyperLogLogValidatorRejectsMalformedSparsePayload()
        {
            // Test verifies that the HLL validator correctly identifies and rejects sparse HLL
            // payloads where the declared stream length doesn't match the actual payload size.
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const string sourceKey = "hll_malformed_source";

            // Step 1: Clean up and create sparse HLL
            _ = db.KeyDelete(sourceKey);

            for (var i = 0; i < 20; i++)
                _ = db.HyperLogLogAdd(sourceKey, $"elem_{i}");

            // Step 2: Dump and parse the structure
            var dump = db.KeyDump(sourceKey)!;
            var (valueLength, valueStart) = ParseDumpValueLengthAndStart(dump);

            // Step 3: Extract the HLL value and verify it's sparse
            var malformedValue = dump.AsSpan(valueStart, valueLength).ToArray();
            ClassicAssert.AreEqual(0, malformedValue[3], "Expected sparse HLL representation for malformed payload test.");

            // Step 4: Corrupt the sparse stream length (bytes 16-17) to exceed actual size
            BinaryPrimitives.WriteUInt16LittleEndian(malformedValue.AsSpan(16, 2), (ushort)(valueLength + 100));

            // Steps 5 & 6: Validate original passes and malformed fails
            fixed (byte* validPtr = dump.AsSpan(valueStart, valueLength))
            fixed (byte* malformedPtr = malformedValue)
            {
                ClassicAssert.IsTrue(HyperLogLog.DefaultHLL.IsValidHYLL(validPtr, valueLength));
                ClassicAssert.IsFalse(HyperLogLog.DefaultHLL.IsValidHYLL(malformedPtr, malformedValue.Length));
            }
        }

        [Test]
        [Category("HLL_RESTORE")]
        public void HyperLogLogValidatorRejectsSparseStreamCoverageMismatch()
        {
            // Test verifies that the validator rejects sparse HLL payloads where the RLE stream
            // doesn't cover all expected register positions (16384 registers total).
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const string sourceKey = "hll_sparse_stream_src";

            // Step 1: Clean up and create minimal sparse HLL
            _ = db.KeyDelete(sourceKey);

            for (int i = 0; i < 2; i++)
                _ = db.HyperLogLogAdd(sourceKey, $"elem_{i}");

            // Step 2: Dump and parse the structure
            var dump = db.KeyDump(sourceKey)!;
            var (valueLength, valueStart) = ParseDumpValueLengthAndStart(dump);

            // Step 3: Extract the HLL value and verify it's sparse
            var malformedValue = dump.AsSpan(valueStart, valueLength).ToArray();
            ClassicAssert.AreEqual(0, malformedValue[3], "Expected sparse HLL representation for stream coverage test.");

            // Step 4: Corrupt the first RLE opcode (offset 18) to create coverage gap
            // Setting to 0x80 (ZERO opcode with invalid parameters)
            var rleStart = 18;
            malformedValue[rleStart] = 0x80;

            // Steps 5 & 6: Validate original passes and malformed fails
            fixed (byte* validPtr = dump.AsSpan(valueStart, valueLength))
            fixed (byte* malformedPtr = malformedValue)
            {
                ClassicAssert.IsTrue(HyperLogLog.DefaultHLL.IsValidHYLL(validPtr, valueLength));
                ClassicAssert.IsFalse(HyperLogLog.DefaultHLL.IsValidHYLL(malformedPtr, malformedValue.Length));
            }
        }

        [Test]
        [Category("HLL_RESTORE")]
        public void HyperLogLogChecksumMalformedOnRestore()
        {
            // Test verifies that RESTORE properly validates checksums even when the HLL payload
            // contains malformed sparse metadata. Ensures checksum validation happens before
            // structural validation.
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const string sourceKey = "hll_skip_src";
            const string malformedKey = "hll_skip_bad";
            const string mergeDst = "hll_skip_dst";

            // Step 2: Clean up and create sparse HLL
            _ = db.KeyDelete(sourceKey);
            _ = db.KeyDelete(malformedKey);
            _ = db.KeyDelete(mergeDst);

            for (var i = 0; i < 2; i++)
                _ = db.HyperLogLogAdd(sourceKey, $"elem_{i}");

            // Step 3: Dump and parse the structure
            var dump = db.KeyDump(sourceKey)!;
            var (valueLength, valueStart) = ParseDumpValueLengthAndStart(dump);

            // Step 4: Extract and corrupt the sparse stream length
            var hllValue = dump.AsSpan(valueStart, valueLength).ToArray();
            ClassicAssert.AreEqual(0, hllValue[3], "Expected sparse HLL representation for checksum-skip malformed payload test.");
            BinaryPrimitives.WriteUInt16LittleEndian(hllValue.AsSpan(16, 2), (ushort)(valueLength + 100));

            // Step 5: Reconstruct the dump with RDB encoding (CRC will be zeros)
            // Encode the length using Redis RDB string encoding format
            byte[] encodedLength = hllValue.Length < 64
                ? [(byte)(hllValue.Length & 0x3F)]
                : hllValue.Length < 16384
                    ? [(byte)(((hllValue.Length >> 8) & 0x3F) | 0x40), (byte)(hllValue.Length & 0xFF)]
                    : [0x80, .. BitConverter.GetBytes(System.Net.IPAddress.HostToNetworkOrder(hllValue.Length))];

            var rdbVersion = dump.AsSpan(valueStart + valueLength, 2).ToArray();
            var crafted = new byte[1 + encodedLength.Length + hllValue.Length + rdbVersion.Length + 8];
            crafted[0] = 0x00; // RDB type encoding
            encodedLength.CopyTo(crafted.AsSpan(1, encodedLength.Length));
            hllValue.CopyTo(crafted.AsSpan(1 + encodedLength.Length, hllValue.Length));
            rdbVersion.CopyTo(crafted.AsSpan(1 + encodedLength.Length + hllValue.Length, rdbVersion.Length));
            // Note: Last 8 bytes (CRC) remain as zeros

            // Steps 6 & 7: Attempt restore and verify checksum error (not structural error)
            var restoreEx = Assert.Throws<RedisServerException>(() => db.KeyRestore(malformedKey, crafted));
            StringAssert.Contains("ERR DUMP payload version or checksum are wrong", restoreEx!.Message);
        }

        [Test]
        [Category("HLL_RESTORE")]
        public void HyperLogLogDumpVariantCoverage_SparseAndDenseRepresentations()
        {
            // Test verifies that HLL dumps correctly handle both sparse and dense representations,
            // and that the encoding type byte correctly identifies each format.
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const string sparseKey = "hll_variant_sparse";
            const string denseKey = "hll_variant_dense";

            // Step 1: Create sparse HLL with minimal elements
            _ = db.KeyDelete(sparseKey);
            _ = db.KeyDelete(denseKey);

            for (var i = 0; i < 2; i++)
                _ = db.HyperLogLogAdd(sparseKey, $"s_{i}");

            // Step 2: Dump and verify sparse encoding (type byte = 0 at offset 3)
            var sparseDump = db.KeyDump(sparseKey)!;
            var (sparseLen, sparseStart) = ParseDumpValueLengthAndStart(sparseDump);
            ClassicAssert.AreEqual(0, sparseDump[sparseStart + 3], "Expected sparse HLL representation type.");

            // Step 3: Create dense HLL by adding elements until sparse->dense conversion
            // HLL converts to dense when sparse representation becomes inefficient
            byte[] denseDump;
            var inserts = 0;
            do
            {
                var batch = Math.Min(1000, 50_000 - inserts);
                for (var j = 0; j < batch; j++)
                {
                    _ = db.HyperLogLogAdd(denseKey, $"d_{inserts}");
                    inserts++;
                }
                denseDump = db.KeyDump(denseKey)!;
            }
            while (denseDump[ParseDumpValueLengthAndStart(denseDump).valueStart + 3] == 0 && inserts < 50_000);

            // Step 4: Verify dense encoding (type byte = 1 at offset 3)
            var (denseLen, denseStart) = ParseDumpValueLengthAndStart(denseDump);
            ClassicAssert.AreEqual(1, denseDump[denseStart + 3], "Expected dense HLL representation type.");

            // Step 5: Verify dense representation is larger
            ClassicAssert.Greater(denseLen, sparseLen);
        }

        [Test]
        [Category("HLL_RESTORE")]
        public void ParseDumpValueLengthAndStart_Covers6Bit14BitAnd32BitBranches()
        {
            // Test verifies that the RDB length encoding parser correctly handles all three
            // Redis RDB string length encoding formats: 6-bit, 14-bit, and 32-bit.
            //
            // RDB Length Encoding Format:
            // - 6-bit:  First byte[1] < 0x40, length in lower 6 bits, value starts at byte 2
            // - 14-bit: First byte[1] < 0x80, length in 14 bits (6 bits + 8 bits), value starts at byte 3
            // - 32-bit: First byte[1] = 0x80, length in next 4 bytes (big-endian), value starts at byte 6
            //
            // Steps:
            // 1. Test 6-bit encoding: length = 5 (< 64)
            // 2. Test 14-bit encoding: length = 400 (< 16384)
            // 3. Test 32-bit encoding: length = 70000 (>= 16384)

            // Step 1: Test 6-bit encoding
            // Format: [type_byte, 0x05, ...] where 0x05 encodes length 5
            var sixBitDump = new byte[] { 0x24, 0x05, 0x00, 0x00, 0x00 };
            var (len6, start6) = ParseDumpValueLengthAndStart(sixBitDump);
            ClassicAssert.AreEqual(5, len6);
            ClassicAssert.AreEqual(2, start6);

            // Step 2: Test 14-bit encoding
            // Format: [type_byte, (upper_6_bits | 0x40), lower_8_bits, ...]
            var len14 = 400;
            var dump14 = new byte[] { 0x24, (byte)(((len14 >> 8) & 0x3F) | 0x40), (byte)(len14 & 0xFF), 0x00, 0x00 };
            var (lenParsed14, start14) = ParseDumpValueLengthAndStart(dump14);
            ClassicAssert.AreEqual(len14, lenParsed14);
            ClassicAssert.AreEqual(3, start14);

            // Step 3: Test 32-bit encoding
            // Format: [type_byte, 0x80, 4_bytes_big_endian, ...]
            // 70000 = 0x00011170 in big-endian
            var len32 = 70000;
            var dump32 = new byte[] { 0x24, 0x80, 0x00, 0x01, 0x11, 0x70, 0x00 };
            var (lenParsed32, start32) = ParseDumpValueLengthAndStart(dump32);
            ClassicAssert.AreEqual(len32, lenParsed32);
            ClassicAssert.AreEqual(6, start32);
        }

        [Test]
        [Category("HLL_RESTORE")]
        public void HyperLogLogOperationsValidateStructuralIntegrity()
        {
            // Test verifies that even when a corrupted HLL payload passes CRC validation during RESTORE,
            // subsequent HLL operations (PFADD, PFCOUNT, PFMERGE) properly detect structural corruption
            // and reject the operations.
            //
            // This tests defense-in-depth: CRC only validates data integrity during transfer,
            // but operations must still validate structural correctness.
            //
            // Steps:
            // 1. Create a valid sparse HLL
            // 2. Dump it and corrupt the SparseRLESize to be larger than payload
            // 3. Recalculate CRC using Garnet's Crc64 to make corruption pass RESTORE
            // 4. Verify RESTORE succeeds (CRC is valid)
            // 5. Verify PFADD, PFCOUNT, PFMERGE all fail with WRONGTYPE (structural validation)
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const string sourceKey = "hll_valid_source";
            const string corruptedKey = "hll_corrupted_key";
            const string validKey = "hll_valid_for_merge";

            // Step 1: Create valid sparse HLL and test data
            _ = db.KeyDelete(sourceKey);
            _ = db.KeyDelete(corruptedKey);
            _ = db.KeyDelete(validKey);

            for (var i = 0; i < 5; i++)
                _ = db.HyperLogLogAdd(sourceKey, $"elem_{i}");

            _ = db.HyperLogLogAdd(validKey, "test");

            // Step 2: Dump and corrupt the SparseRLESize field
            var dump = db.KeyDump(sourceKey)!;
            var (valueLength, valueStart) = ParseDumpValueLengthAndStart(dump);

            // Extract HLL value
            var hllValue = dump.AsSpan(valueStart, valueLength).ToArray();
            ClassicAssert.AreEqual(0, hllValue[3], "Expected sparse HLL representation.");

            // Corrupt RLE size: make it larger than actual payload
            // This makes the declared RLE stream size exceed what's actually available
            var originalRleSize = BinaryPrimitives.ReadUInt16LittleEndian(hllValue.AsSpan(16, 2));
            var availablePayload = valueLength - 18; // Total length minus header (16) and RLE size field (2)
            var corruptedRleSize = (ushort)(availablePayload + 50); // Exceeds actual available space
            BinaryPrimitives.WriteUInt16LittleEndian(hllValue.AsSpan(16, 2), corruptedRleSize);

            // Step 3: Reconstruct dump with valid CRC using Garnet.common.Crc64
            byte[] encodedLength = hllValue.Length < 64
                ? [(byte)(hllValue.Length & 0x3F)]
                : hllValue.Length < 16384
                    ? [(byte)(((hllValue.Length >> 8) & 0x3F) | 0x40), (byte)(hllValue.Length & 0xFF)]
                    : [0x80, .. BitConverter.GetBytes(System.Net.IPAddress.HostToNetworkOrder(hllValue.Length))];

            var rdbVersion = dump.AsSpan(valueStart + valueLength, 2).ToArray();

            // Build payload without CRC
            var payloadWithoutCrc = new byte[1 + encodedLength.Length + hllValue.Length + rdbVersion.Length];
            payloadWithoutCrc[0] = 0x00; // RDB type encoding
            encodedLength.CopyTo(payloadWithoutCrc.AsSpan(1, encodedLength.Length));
            hllValue.CopyTo(payloadWithoutCrc.AsSpan(1 + encodedLength.Length, hllValue.Length));
            rdbVersion.CopyTo(payloadWithoutCrc.AsSpan(1 + encodedLength.Length + hllValue.Length, rdbVersion.Length));

            // Calculate CRC64 using Garnet's implementation (CRC64-Jones polynomial)
            var crc64 = Garnet.common.Crc64.Hash(payloadWithoutCrc);

            // Build final dump with valid CRC
            var craftedDump = new byte[payloadWithoutCrc.Length + 8];
            payloadWithoutCrc.CopyTo(craftedDump, 0);
            crc64.CopyTo(craftedDump.AsSpan(payloadWithoutCrc.Length, 8));

            // Step 4: RESTORE should succeed (CRC is valid, basic validation passes)
            db.KeyRestore(corruptedKey, craftedDump);
            ClassicAssert.IsTrue(db.KeyExists(corruptedKey), "Corrupted HLL should be restored with valid CRC");

            // Step 5: Verify operations fail due to structural validation

            // PFADD should fail - structural validation detects corrupted RLE size
            var pfaddEx = Assert.Throws<RedisServerException>(() => db.HyperLogLogAdd(corruptedKey, "newelem"));
            StringAssert.Contains("WRONGTYPE", pfaddEx!.Message, "PFADD should reject corrupted HLL");

            // PFCOUNT should fail
            var pfcountEx = Assert.Throws<RedisServerException>(() => db.HyperLogLogLength(corruptedKey));
            StringAssert.Contains("WRONGTYPE", pfcountEx!.Message, "PFCOUNT should reject corrupted HLL");

            // PFMERGE should fail when corrupted HLL is source
            var pfmergeEx1 = Assert.Throws<RedisServerException>(() =>
                db.HyperLogLogMerge(validKey, validKey, corruptedKey));
            StringAssert.Contains("WRONGTYPE", pfmergeEx1!.Message, "PFMERGE should reject corrupted HLL as source");

            // PFMERGE should fail when corrupted HLL is destination
            var pfmergeEx2 = Assert.Throws<RedisServerException>(() =>
                db.HyperLogLogMerge(corruptedKey, corruptedKey, validKey));
            StringAssert.Contains("WRONGTYPE", pfmergeEx2!.Message, "PFMERGE should reject corrupted HLL as destination");
        }
    }
}