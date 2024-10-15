// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Text;
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
        public Request singleCPBSET;

        public void Dispose()
        {
            session.Dispose();
            server.Dispose();
        }

        public void SetupSingleInstance(bool enableCluster = true)
        {
            var opt = new GarnetServerOptions
            {
                QuietMode = true,
                EnableCluster = enableCluster,
                Port = Port,
                CleanClusterConfig = true,
            };
            server = new EmbeddedRespServer(opt);
            session = server.GetRespSession();
            server.Register.NewTransactionProc(CustomProcSetBench.CommandName, () => new CustomProcSetBench(), new RespCommandsInfo { Arity = CustomProcSetBench.Arity });
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

        public void CreateGetSet(int keySize = 8, int valueSize = 32, int batchSize = 128)
        {
            var pairs = new (byte[], byte[])[batchSize];
            for (var i = 0; i < batchSize; i++)
            {
                pairs[i] = (new byte[keySize], new byte[valueSize]);

                keyTag.CopyTo(pairs[i].Item1.AsSpan());
                benchUtils.RandomBytes(ref pairs[i].Item1, startOffset: keyTag.Length);
                benchUtils.RandomBytes(ref pairs[i].Item2);
            }

            var setByteCount = batchSize * ("*2\r\n$3\r\nSET\r\n"u8.Length + (1 + NumUtils.NumDigits(keySize) + 2 + keySize + 2) + (1 + NumUtils.NumDigits(valueSize) + 2 + valueSize + 2));
            var setReq = new Request(setByteCount);
            var curr = setReq.ptr;
            var end = curr + setReq.buffer.Length;
            for (var i = 0; i < batchSize; i++)
            {
                _ = RespWriteUtils.WriteArrayLength(3, ref curr, end);
                _ = RespWriteUtils.WriteBulkString("SET"u8, ref curr, end);
                _ = RespWriteUtils.WriteBulkString(pairs[i].Item1, ref curr, end);
                _ = RespWriteUtils.WriteBulkString(pairs[i].Item2, ref curr, end);
            }

            var getByteCount = batchSize * ("*2\r\n$3\r\nGET\r\n"u8.Length + 1 + NumUtils.NumDigits(keySize) + 2 + keySize + 2);
            var getReq = new Request(getByteCount);
            curr = getReq.ptr;
            end = curr + getReq.buffer.Length;
            for (var i = 0; i < batchSize; i++)
            {
                _ = RespWriteUtils.WriteArrayLength(2, ref curr, end);
                _ = RespWriteUtils.WriteBulkString("GET"u8, ref curr, end);
                _ = RespWriteUtils.WriteBulkString(pairs[i].Item1, ref curr, end);
            }
            singleGetSet = [getReq, setReq];
        }

        public void CreateMGetMSet(int keySize = 8, int valueSize = 32, int batchSize = 128)
        {
            var pairs = new (byte[], byte[])[batchSize];
            for (var i = 0; i < batchSize; i++)
            {
                pairs[i] = (new byte[keySize], new byte[valueSize]);

                keyTag.CopyTo(pairs[i].Item1.AsSpan());
                benchUtils.RandomBytes(ref pairs[i].Item1, startOffset: keyTag.Length);
                benchUtils.RandomBytes(ref pairs[i].Item2);
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

            var mSetHeaderSize = 1 + NumUtils.NumDigits(1 + batchSize * 2) + 2 + "$4\r\nMSET\r\n"u8.Length;
            var setRespSize = 1 + NumUtils.NumDigits(keySize) + 2 + keySize + 2 + 1 + NumUtils.NumDigits(valueSize) + 2 + valueSize + 2;
            var mSetByteCount = mSetHeaderSize + (batchSize * setRespSize);
            var mSetReq = new Request(mSetByteCount);

            curr = mSetReq.ptr;
            end = curr + mSetReq.buffer.Length;
            _ = RespWriteUtils.WriteArrayLength(1 + batchSize * 2, ref curr, end);
            _ = RespWriteUtils.WriteBulkString("MSET"u8, ref curr, end);
            for (var i = 0; i < batchSize; i++)
            {
                _ = RespWriteUtils.WriteBulkString(pairs[i].Item1, ref curr, end);
                _ = RespWriteUtils.WriteBulkString(pairs[i].Item2, ref curr, end);
            }
            singleMGetMSet = [mGetReq, mSetReq];
        }

        public void CreateCPBSET(int keySize = 8, int valueSize = 32, int batchSize = 128)
        {
            var keys = new byte[8][];
            for (var i = 0; i < 8; i++)
            {
                keys[i] = new byte[keySize];

                keyTag.CopyTo(keys[i].AsSpan());
                benchUtils.RandomBytes(ref keys[i], startOffset: keyTag.Length);
            }

            var cpbsetByteCount = "*9\r\n$6\r\nCPBSET\r\n"u8.Length + (8 * (1 + NumUtils.NumDigits(keySize) + 2 + keySize + 2));
            var cpbsetReq = new Request(batchSize * cpbsetByteCount);
            var curr = cpbsetReq.ptr;
            var end = curr + cpbsetReq.buffer.Length;

            for (var i = 0; i < batchSize; i++)
            {
                _ = RespWriteUtils.WriteArrayLength(9, ref curr, end);
                _ = RespWriteUtils.WriteBulkString("CPBSET"u8, ref curr, end);
                for (var j = 0; j < 8; j++)
                {
                    _ = RespWriteUtils.WriteBulkString(keys[j], ref curr, end);
                }
            }

            singleCPBSET = cpbsetReq;
        }


        public void Consume(byte* ptr, int length)
            => session.TryConsumeMessages(ptr, length);
    }

}