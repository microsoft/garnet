// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Text;
using BenchmarkDotNet.Attributes;
using Embedded.perftest;
using Garnet.common;
using Garnet.server;

namespace BDN.benchmark.Cluster
{
    public unsafe struct Request
    {
        public byte[] buffer;
        public byte* ptr;

        public Request(int size)
        {
            buffer = GC.AllocateArray<byte>(size, pinned: true);
            ptr = (byte*)Unsafe.AsPointer(ref buffer[0]);
        }
    }

    public unsafe class ClusterContext
    {
        EmbeddedRespServer server;
        RespServerSession session;
        BenchUtils benchUtils = new();

        private readonly int Port = 7000;

        public ReadOnlySpan<byte> keyTag => "{0}"u8;
        public Request[] singleGetSet;
        public Request[] singleMGetMSet;

        public void Dispose()
        {
            session.Dispose();
            server.Dispose();
        }

        public void SetupSingleInstance()
        {
            var opt = new GarnetServerOptions
            {
                QuietMode = true,
                EnableCluster = true,
                Port = Port,
                CleanClusterConfig = true,
            };
            server = new EmbeddedRespServer(opt);
            session = server.GetRespSession();
        }

        public void AddSlotRange(List<(int, int)> slotRanges)
        {
            foreach (var slotRange in slotRanges)
            {
                var clusterAddSlotsRange = Encoding.ASCII.GetBytes($"*4\r\n$7\r\nCLUSTER\r\n$13\r\nADDSLOTSRANGE\r\n" +
                    $"${NumUtils.NumDigits(slotRange.Item1)}\r\n{slotRange.Item1}\r\n" +
                    $"${NumUtils.NumDigits(slotRange.Item2)}\r\n{slotRange.Item2}\r\n");
                fixed (byte* req = clusterAddSlotsRange)
                    _ = session.TryConsumeMessages(req, clusterAddSlotsRange.Length);
            }
        }

        public void CreateGetSet(int keySize = 8, int valueSize = 32)
        {
            var key = new byte[keySize];
            var value = new byte[valueSize];

            keyTag.CopyTo(key.AsSpan());
            benchUtils.RandomBytes(ref key, startOffset: keyTag.Length);
            benchUtils.RandomBytes(ref value);

            var getByteCount = "*2\r\n$3\r\nGET\r\n"u8.Length + 1 + NumUtils.NumDigits(keySize) + 2 + keySize + 2;
            var getReq = new Request(getByteCount);
            var curr = getReq.ptr;
            var end = curr + getReq.buffer.Length;
            _ = RespWriteUtils.WriteArrayLength(2, ref curr, end);
            _ = RespWriteUtils.WriteBulkString("GET"u8, ref curr, end);
            _ = RespWriteUtils.WriteBulkString(key, ref curr, end);

            var setByteCount = "*2\r\n$3\r\nSET\r\n"u8.Length;
            setByteCount += 1 + NumUtils.NumDigits(keySize) + 2 + keySize + 2;
            setByteCount += 1 + NumUtils.NumDigits(valueSize) + 2 + valueSize + 2;
            var setReq = new Request(setByteCount);
            curr = setReq.ptr;
            end = curr + setReq.buffer.Length;
            _ = RespWriteUtils.WriteArrayLength(3, ref curr, end);
            _ = RespWriteUtils.WriteBulkString("SET"u8, ref curr, end);
            _ = RespWriteUtils.WriteBulkString(key, ref curr, end);
            _ = RespWriteUtils.WriteBulkString(value, ref curr, end);
            singleGetSet = [getReq, setReq];
        }

        public void CreateMGetMSet(int keySize = 8, int valueSize = 32, int batchSize = 32)
        {
            var pairs = new (byte[], byte[])[batchSize];
            for (var i = 0; i < batchSize; i++)
            {
                pairs[i] = (new byte[keySize], new byte[valueSize]);

                keyTag.CopyTo(pairs[i].Item1.AsSpan());
                benchUtils.RandomBytes(ref pairs[i].Item1, startOffset: keyTag.Length);
                benchUtils.RandomBytes(ref pairs[i].Item2, startOffset: keyTag.Length);
            }

            var mGetHeaderSize = 1 + NumUtils.NumDigits(1 + batchSize) + 2 + "$4\r\nMGET\r\n"u8.Length;
            var getRespSize = 1 + NumUtils.NumDigits(keySize) + 2 + keySize + 2;
            var mGetByteCount = mGetHeaderSize + (batchSize * getRespSize);
            var mGetReq = new Request(mGetByteCount);

            var curr = mGetReq.ptr;
            var end = curr + mGetReq.buffer.Length;
            _ = RespWriteUtils.WriteArrayLength(1 + batchSize, ref curr, end);
            _ = RespWriteUtils.WriteBulkString("MGET"u8, ref curr, end);
            for (var i = 0; i < batchSize; i++)
                _ = RespWriteUtils.WriteBulkString(pairs[i].Item1, ref curr, end);

            var mSetHeaderSize = 1 + NumUtils.NumDigits(1 + batchSize) + 2 + "$4\r\nMSET\r\n"u8.Length;
            var setRespSize = 1 + NumUtils.NumDigits(keySize) + 2 + keySize + 2 + 1 + NumUtils.NumDigits(valueSize) + 2 + valueSize + 2;
            var mSetByteCount = mSetHeaderSize + (batchSize * setRespSize);
            var mSetReq = new Request(mSetByteCount);

            curr = mSetReq.ptr;
            end = curr + mSetReq.buffer.Length;
            _ = RespWriteUtils.WriteArrayLength(1 + batchSize, ref curr, end);
            _ = RespWriteUtils.WriteBulkString("MSET"u8, ref curr, end);
            for (var i = 0; i < batchSize; i++)
            {
                _ = RespWriteUtils.WriteBulkString(pairs[i].Item1, ref curr, end);
                _ = RespWriteUtils.WriteBulkString(pairs[i].Item2, ref curr, end);
            }
            singleMGetMSet = [mGetReq, mSetReq];
        }

        public void Consume(byte* ptr, int length)
            => session.TryConsumeMessages(ptr, length);
    }

    [MemoryDiagnoser]
    public unsafe class RespClusterBench
    {
        ClusterContext cc;

        [GlobalSetup]
        public void GlobalSetup()
        {
            cc = new ClusterContext();
            cc.SetupSingleInstance();
            cc.AddSlotRange([(0, 16383)]);
            cc.CreateGetSet();
            cc.CreateMGetMSet();

            cc.Consume(cc.singleGetSet[1].ptr, cc.singleGetSet[1].buffer.Length);
            cc.Consume(cc.singleMGetMSet[1].ptr, cc.singleMGetMSet[1].buffer.Length);
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            cc.Dispose();
        }

        [Benchmark]
        public void Get()
        {
            cc.Consume(cc.singleGetSet[0].ptr, cc.singleGetSet[0].buffer.Length);
        }

        [Benchmark]
        public void Set()
        {
            cc.Consume(cc.singleGetSet[1].ptr, cc.singleGetSet[1].buffer.Length);
        }

        [Benchmark]
        public void MGet()
        {
            cc.Consume(cc.singleMGetMSet[0].ptr, cc.singleMGetMSet[0].buffer.Length);
        }

        [Benchmark]
        public void MSet()
        {
            cc.Consume(cc.singleMGetMSet[1].ptr, cc.singleMGetMSet[1].buffer.Length);
        }
    }
}
