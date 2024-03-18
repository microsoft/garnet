// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using Garnet.server;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test
{
    public unsafe class HyperLogLogTests
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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Repeat(1)]
        public void SimpleHyperLogLogAddCount()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string[] data = new string[] { "a", "b", "c", "d", "e", "f" };
            string key = "hllKey";
            bool fUpdated = false;

            //HLL updated
            for (int i = 0; i < data.Length; i++)
            {
                fUpdated = db.HyperLogLogAdd(key, data[i]);
                Assert.IsTrue(fUpdated);
            }

            //HLL not updated
            for (int i = 0; i < data.Length; i++)
            {
                fUpdated = db.HyperLogLogAdd(key, data[i]);
                Assert.IsFalse(fUpdated);
            }

            //estimate cardinality
            long pfcount = db.HyperLogLogLength(key);
            Assert.AreEqual(6, pfcount);
        }

        public static void SimpleHyperLogLogArrayAddCount()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            RedisValue[] x = new RedisValue[] { "h", "e", "l", "l", "o" };
            RedisValue[] y = new RedisValue[] { "w", "o", "r", "l", "d" };

            string keyX = "x";
            var ret = db.HyperLogLogAdd(keyX, x);
            Assert.IsTrue(ret);

            ret = db.HyperLogLogAdd(keyX, y);
            Assert.IsTrue(ret);

            long pfcount = db.HyperLogLogLength(keyX);
            Assert.AreEqual(7, pfcount);
        }

        [Test]
        [Repeat(1)]
        public void SimpleHyperLogLogMerge()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            RedisValue[] x = new RedisValue[] { "h", "e", "l", "l", "o" };
            RedisValue[] y = new RedisValue[] { "w", "o", "r", "l", "d" };
            string keyX = "x";
            string keyY = "y";
            string keyW = "w";

            //HLL updated           
            db.HyperLogLogAdd(keyX, x);
            long keyXCount = db.HyperLogLogLength(keyX);
            Assert.AreEqual(keyXCount, 4);

            db.HyperLogLogAdd(keyY, y);
            long keyYCount = db.HyperLogLogLength(keyY);
            Assert.AreEqual(keyYCount, 5);

            var res = db.Execute("PFMERGE", keyW, keyX);
            long keyWCount = db.HyperLogLogLength(keyW);
            Assert.AreEqual(keyWCount, 4);

            res = db.Execute("PFMERGE", keyW, keyY);
            keyWCount = db.HyperLogLogLength(keyW);
            Assert.AreEqual(keyWCount, 7);
        }

        [Test]
        public void HyperLogLogSimpleInvalidHLLTypeTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            RedisValue[] x = new RedisValue[] { "h", "e", "l", "l", "o" };
            RedisValue[] y = new RedisValue[] { "w", "o", "r", "l", "d" };
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
                Assert.AreEqual(ex.Message, "WRONGTYPE Key is not a valid HyperLogLog string value.");
            }

            try
            {
                db.HyperLogLogLength(keyW);
            }
            catch (Exception ex)
            {
                Assert.AreEqual(ex.Message, "WRONGTYPE Key is not a valid HyperLogLog string value.");
            }

            try
            {
                db.HyperLogLogMerge(keyW, keyY, keyX);
            }
            catch (Exception ex)
            {
                Assert.AreEqual(ex.Message, "WRONGTYPE Key is not a valid HyperLogLog string value.");
            }

            try
            {
                db.HyperLogLogMerge(keyY, keyW, keyX);
            }
            catch (Exception ex)
            {
                Assert.AreEqual(ex.Message, "WRONGTYPE Key is not a valid HyperLogLog string value.");
            }

            try
            {
                db.HyperLogLogMerge(keyY, keyX, keyW);
            }
            catch (Exception ex)
            {
                Assert.AreEqual(ex.Message, "WRONGTYPE Key is not a valid HyperLogLog string value.");
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
                Assert.AreEqual(response.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
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
            Assert.AreEqual(response.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            //2. PFCOUNT mykey
            response = lightClientRequest.SendCommandChunks("PFCOUNT mykey", bytesPerSend);
            expectedResponse = ":4\r\n";
            Assert.AreEqual(response.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            //3. PFADD mykey2
            response = lightClientRequest.SendCommandChunks("PFADD mykey2 w o r l d", bytesPerSend);
            expectedResponse = ":1\r\n";
            Assert.AreEqual(response.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            //4. PFCOUNT mykey mykey2
            response = lightClientRequest.SendCommandChunks("PFCOUNT mykey mykey2", bytesPerSend);
            expectedResponse = ":9\r\n";
            Assert.AreEqual(response.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            //5. PFMERGE mykey3
            response = lightClientRequest.SendCommandChunks("PFMERGE mykey3 mykey", bytesPerSend);
            expectedResponse = "+OK\r\n";
            Assert.AreEqual(response.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            //6. PFCOUNT mykey3
            response = lightClientRequest.SendCommandChunks("PFCOUNT mykey3", bytesPerSend);
            expectedResponse = ":4\r\n";
            Assert.AreEqual(response.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            //7. PFMERGE mykey4 mykey mykey2
            response = lightClientRequest.SendCommandChunks("PFMERGE mykey4 mykey mykey2", bytesPerSend);
            expectedResponse = "+OK\r\n";
            Assert.AreEqual(response.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            //8. PFCOUNT mykey4
            response = lightClientRequest.SendCommandChunks("PFCOUNT mykey4", bytesPerSend);
            expectedResponse = ":7\r\n";
            Assert.AreEqual(response.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
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

                Assert.AreEqual(expectedUpdated, updated);
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
            Assert.AreEqual(updated, expectedUpdated);

            long estimate = db.HyperLogLogLength(key);
            double error = EstimationError(estimate, set.Count);
            Assert.IsTrue(error < 4.0);

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
                Assert.AreEqual(updated, expectedUpdated);

                estimate = db.HyperLogLogLength(key);
                error = EstimationError(estimate, set.Count);
                Assert.IsTrue(error < 4.0);
            }
        }

        [Test]
        [Repeat(1)]
        public void HyperLogLogMultiCountTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            RedisValue[] dataA = new RedisValue[] { "h", "e", "l", "l", "o" };
            RedisValue[] dataB = new RedisValue[] { "w", "o", "r", "l", "d" };
            RedisValue[] dataC = new RedisValue[] { "a", "b", "c", "d", "e", "f" };
            string keyA = "HyperLogLogMultiCountTestA";
            string keyB = "HyperLogLogMultiCountTestB";
            string keyC = "HyperLogLogMultiCountTestC";
            RedisKey[] keys = new RedisKey[] { keyA, keyB, keyC };

            db.KeyDelete(keyA);
            db.KeyDelete(keyB);
            db.KeyDelete(keyC);

            db.HyperLogLogAdd(keyA, dataA);
            db.HyperLogLogAdd(keyB, dataB);
            db.HyperLogLogAdd(keyC, dataC);

            long countA = db.HyperLogLogLength(keyA);
            long countB = db.HyperLogLogLength(keyB);
            long countC = db.HyperLogLogLength(keyC);

            Assert.AreEqual(4, countA);
            Assert.AreEqual(5, countB);
            Assert.AreEqual(6, countC);

            db.KeyDelete(keyA);
            db.KeyDelete(keyB);
            db.KeyDelete(keyC);

            db.HyperLogLogAdd(keyA, dataA);
            db.HyperLogLogAdd(keyB, dataB);
            db.HyperLogLogAdd(keyC, dataC);

            long totalCount = db.HyperLogLogLength(keys);
            Assert.AreEqual(countA + countB + countC, totalCount);
        }

        public long LongRandom() => ((long)this.r.Next() << 32) | (long)this.r.Next();

        public List<long> RandomSubSeq(List<long> list, int count)
        {
            List<long> rss = new List<long>();
            for (int i = 0; i < count; i++) rss.Add(list[r.Next(list.Count)]);
            return rss;
        }

        public RedisValue[] RandomRedisValueSubseq(List<long> list, int count)
        {
            List<RedisValue> rss = new List<RedisValue>();
            for (int i = 0; i < count; i++)
                rss.Add(list[r.Next(list.Count)]);
            return rss.ToArray();
        }

        public static List<long> ToList(RedisValue[] rss)
        {
            return rss.Select(x => (long)x).ToList();
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

            HashSet<long> setA = new HashSet<long>();
            HashSet<long> setB = new HashSet<long>();

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
            Assert.IsTrue(EstimationError(estimateA, setA.Count) < 4.0);
            Assert.IsTrue(EstimationError(estimateB, setB.Count) < 4.0);

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
            Assert.IsTrue(EstimationError(estimateC, setA.Count) < 4.0, $"{estimateC} ~ {setA.Count}");
            Assert.IsTrue(EstimationError(estimateD, setB.Count) < 4.0, $"{estimateD} ~ {setB.Count}");
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
                    MemorySize: "512",
                    PageSize: "512");
            else
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                    lowMemory: true,
                    MemorySize: "16384",
                    PageSize: "16384");
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            int keyCount = sparse ? 32 : 4;
            int smallSeq = seqSize;

            Dictionary<int, HashSet<long>> hllKeyCollection = new Dictionary<int, HashSet<long>>();

            //1. Populate HLL
            for (int i = 0; i < keyCount; i++)
            {
                int key = i;
                string sKey = key.ToString();

                hllKeyCollection.Add(key, new HashSet<long>());
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
                Assert.IsTrue(EstimationError(estimate, expectedEstimate) < 4.0);
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
            HashSet<long> setA = new HashSet<long>();
            List<long> largeInput = new List<long>();

            for (int i = 0; i < largeSeq; i++)
                largeInput.Add(LongRandom());

            for (int i = 0; i < 16; i++)
            {
                RedisValue[] vals = RandomRedisValueSubseq(largeInput, smallSeq);
                db.HyperLogLogAdd(keyA, vals);
                setA.UnionWith(ToList(vals));

                long estimate = db.HyperLogLogLength(keyA);
                Assert.IsTrue(EstimationError(estimate, setA.Count) < 4.0);
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
            HashSet<long> setA = new HashSet<long>();
            HashSet<long> setB = new HashSet<long>();
            long estimate = 0;

            List<long> largeInput = new List<long>();
            RedisValue[] rss;
            for (int i = 0; i < largeSeq; i++)
                largeInput.Add(LongRandom());

            //1. HLL A
            rss = RandomRedisValueSubseq(largeInput, smallSeq);
            db.HyperLogLogAdd(keyA, rss);
            setA.UnionWith(ToList(rss));
            estimate = db.HyperLogLogLength(keyA);
            Assert.IsTrue(EstimationError(estimate, setA.Count) < 4.0);

            //2. HLL B
            rss = RandomRedisValueSubseq(largeInput, smallSeq);
            db.HyperLogLogAdd(keyB, rss);
            setB.UnionWith(ToList(rss));
            estimate = db.HyperLogLogLength(keyB);
            Assert.IsTrue(EstimationError(estimate, setB.Count) < 4.0);

            //3. Merge HLL B to HLL A
            db.HyperLogLogMerge(keyA, keyA, keyB);
            setA.UnionWith(setB);
            estimate = db.HyperLogLogLength(keyA);
            Assert.IsTrue(EstimationError(estimate, setA.Count) < 4.0);
        }

        [Test]
        [Repeat(10)]
        public void HyperLogLogTestPFMERGE_LTM_SparseToSparse()
        {
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                lowMemory: true,
                MemorySize: "512",
                PageSize: "512");
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            int keyCount = 64;
            int smallSeq = 32;

            Dictionary<int, HashSet<long>> hllKeyCollection = new Dictionary<int, HashSet<long>>();

            //1. Populate HLL
            for (int i = 0; i < keyCount; i++)
            {
                int key = i;
                string sKey = key.ToString();

                hllKeyCollection.Add(key, new HashSet<long>());
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
                Assert.IsTrue(EstimationError(estimate, expectedEstimate) < 4.0);
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
            HashSet<long> setA = new HashSet<long>();
            HashSet<long> setB = new HashSet<long>();
            HashSet<long> setC = new HashSet<long>();
            long estimate = 0;

            List<long> largeInput = new List<long>();
            RedisValue[] rss;
            for (int i = 0; i < largeSeq; i++)
                largeInput.Add(LongRandom());

            //1. HLL A
            rss = RandomRedisValueSubseq(largeInput, smallSeq);
            db.HyperLogLogAdd(keyA, rss);
            setA.UnionWith(ToList(rss));
            estimate = db.HyperLogLogLength(keyA);
            Assert.IsTrue(EstimationError(estimate, setA.Count) < 4.0);

            //2. HLL B
            rss = RandomRedisValueSubseq(largeInput, smallSeq);
            db.HyperLogLogAdd(keyB, rss);
            setB.UnionWith(ToList(rss));
            estimate = db.HyperLogLogLength(keyB);
            Assert.IsTrue(EstimationError(estimate, setB.Count) < 4.0);

            //3. Merge HLL B to HLL C => dense to empty
            db.HyperLogLogMerge(keyC, keyC, keyB);
            setC.UnionWith(setB);
            estimate = db.HyperLogLogLength(keyC);
            Assert.IsTrue(EstimationError(estimate, setC.Count) < 4.0);

            //4. Merge HLL A to HLL C => sparse to dense
            db.HyperLogLogMerge(keyA, keyA, keyC);
            setA.UnionWith(setC);
            estimate = db.HyperLogLogLength(keyA);
            Assert.IsTrue(EstimationError(estimate, setA.Count) < 4.0);
        }

        [Test]
        [TestCase(false)]
        [TestCase(true)]
        [Repeat(1)]
        public void HyperLogLogTestPFMERGE_LTM_SparseToDense(bool reverse)
        {
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                MemorySize: "16384",
                PageSize: "16384");
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            int keyCount = 4;
            int smallSeq = 32;
            int largeSeq = 1 << 13;

            Dictionary<int, HashSet<long>> hllKeyCollection = new Dictionary<int, HashSet<long>>();

            //1. Populate HLL
            for (int i = 0; i < keyCount; i++)
            {
                int key = i;
                string sKey = key.ToString();

                hllKeyCollection.Add(key, new HashSet<long>());
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
                Assert.IsTrue(EstimationError(estimate, expectedEstimate) < 4.0);
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
            HashSet<long> setA = new HashSet<long>();
            HashSet<long> setB = new HashSet<long>();
            HashSet<long> setC = new HashSet<long>();
            long estimate = 0;

            List<long> largeInput = new List<long>();
            RedisValue[] rss;
            for (int i = 0; i < largeSeq; i++)
                largeInput.Add(LongRandom());

            //1. HLL A
            rss = RandomRedisValueSubseq(largeInput, smallSeq);
            db.HyperLogLogAdd(keyA, rss);
            setA.UnionWith(ToList(rss));
            estimate = db.HyperLogLogLength(keyA);
            Assert.IsTrue(EstimationError(estimate, setA.Count) < 4.0);

            //2. HLL B
            rss = RandomRedisValueSubseq(largeInput, smallSeq);
            db.HyperLogLogAdd(keyB, rss);
            setB.UnionWith(ToList(rss));
            estimate = db.HyperLogLogLength(keyB);
            Assert.IsTrue(EstimationError(estimate, setB.Count) < 4.0);

            //3. Merge HLL B to HLL C => dense to empty
            db.HyperLogLogMerge(keyC, keyC, keyB);
            setC.UnionWith(setB);
            estimate = db.HyperLogLogLength(keyC);
            Assert.IsTrue(EstimationError(estimate, setC.Count) < 4.0);

            //4. Merge HLL A to HLL C => sparse to dense
            db.HyperLogLogMerge(keyA, keyA, keyC);
            setA.UnionWith(setC);
            estimate = db.HyperLogLogLength(keyA);
            Assert.IsTrue(EstimationError(estimate, setA.Count) < 4.0);
        }

        [Test]
        [Repeat(1)]
        public void HyperLogLogTestPFMERGE_LTM_DenseToDense()
        {
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                lowMemory: true,
                MemorySize: "16384",
                PageSize: "16384");
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            int keyCount = 4;
            int largeSeq = 1 << 13;

            Dictionary<int, HashSet<long>> hllKeyCollection = new Dictionary<int, HashSet<long>>();

            //1. Populate HLL
            for (int i = 0; i < keyCount; i++)
            {
                int key = i;
                string sKey = key.ToString();

                hllKeyCollection.Add(key, new HashSet<long>());
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
                Assert.IsTrue(EstimationError(estimate, expectedEstimate) < 4.0);
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
            HashSet<long> dstSet = new HashSet<long>();
            int srcKey = r.Next(1024, 2048);
            RedisKey[] srcKeys = new RedisKey[hllCount];

            List<long> values = new List<long>();
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
            Assert.IsTrue(error < 4.0);
        }

        [Test]
        public void CanRunHLLProcedureTest()
        {
            server.Register.NewTransactionProc("HLLPROC", 8, () => new TestProcedureHLL());
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var result = db.Execute("HLLPROC", "hll", "a", "b", "c", "d", "e", "f", "g");
            Assert.AreEqual("SUCCESS", (string)result);
        }

        private static double EstimationError(long estimate, long cardinality)
        {
            double error = ((double)Math.Abs(cardinality - estimate) / (double)cardinality) * 100;
            error = (double)(int)(error * 10000) / (double)10000;
            return error;
        }
    }
}