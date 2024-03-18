// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace Garnet.test
{
    public enum OpType
    {
        NONE, MGET, INCR, MSET, GET, PING, PFADD, PFCOUNT, PFMERGE, MPFMERGE
    }

    /// <summary>
    /// Buffer of generated RESP requests
    /// </summary>
    public unsafe class ReqGen
    {
        const int BufferSize = 1 << 18;

        readonly Random r;
        readonly OpType opType;
        readonly byte[] opHeader;

        readonly int srcKeys;

        public ReqGen(OpType opType, int srcKeys = 1)
        {
            this.r = new Random(Guid.NewGuid().GetHashCode());
            this.opType = opType;
            this.srcKeys = srcKeys;

            this.opType = opType;
            opHeader = opType switch
            {
                OpType.INCR => System.Text.Encoding.ASCII.GetBytes($"*2\r\n$4\r\nINCR\r\n"),
                OpType.GET => System.Text.Encoding.ASCII.GetBytes($"*2\r\n$3\r\nGET\r\n"),
                OpType.PFADD => System.Text.Encoding.ASCII.GetBytes($"*3\r\n$5\r\nPFADD\r\n"),
                OpType.PFCOUNT => System.Text.Encoding.ASCII.GetBytes($"*2\r\n$7\r\nPFCOUNT\r\n"),
                OpType.PFMERGE => System.Text.Encoding.ASCII.GetBytes($"*3\r\n$7\r\nPFMERGE\r\n"),
                OpType.MPFMERGE => System.Text.Encoding.ASCII.GetBytes($"*{2 + srcKeys}\r\n$7\r\nPFMERGE\r\n"),
                OpType.PING => System.Text.Encoding.ASCII.GetBytes("PING\r\n"),
                _ => null
            };
        }

        public long LongRandom() => ((long)this.r.Next() << 32) | (long)this.r.Next();

        public (List<long>, int) GenSingleKeyValueReqs(ref byte[] buffer, byte[] key, int reqNum, List<long> inputValues = null, int offset = 0)
        {
            int len = 0;
            int index = offset;
            List<long> values = new();

            fixed (byte* buf = buffer, opBuf = opHeader)
            {
                byte* curr = buf;
                for (int i = 0; i < reqNum; i++)
                {
                    Buffer.MemoryCopy(opBuf, curr, opHeader.Length, opHeader.Length);
                    curr += opHeader.Length;
                    WriteStringBytes(ref curr, key);

                    long val = inputValues == null ? LongRandom() : inputValues[index + i];
                    values.Add(val);
                    WriteOpLong(ref curr, opType, values[values.Count - 1]);
                }
                len = (int)(curr - buf);
            }

            return (values, len);
        }

        public (int, int) GenSingleKeyValueRequest(ref byte[] buffer, byte[] key = null, byte[] value = null, int index = 0)
        {
            int val = 0;
            int len = 0;

            Buffer.BlockCopy(opHeader, 0, buffer, 0, opHeader.Length);
            fixed (byte* buf = buffer)
            {
                byte* curr = buf + opHeader.Length;

                WriteStringBytes(ref curr, key);
                if (value != null) WriteStringBytes(ref curr, value);
                else
                {
                    val = index == 0 ? r.Next() : index + 1;
                    WriteOpLong(ref curr, opType, LongRandom());
                }
                len = (int)(curr - buf);
            }

            return (val, len);
        }

        public (int, int) GenArrayKeyValueRequest(ref byte[] buffer, byte[] key = null, byte[][] value = null, int index = 0)
        {
            int val = 0;
            int len = 0;

            Buffer.BlockCopy(opHeader, 0, buffer, 0, opHeader.Length);
            fixed (byte* buf = buffer)
            {
                byte* curr = buf + opHeader.Length;

                WriteStringBytes(ref curr, key);
                for (int i = 0; i < value.Length; i++)
                {
                    byte[] vv = value[i];
                    WriteStringBytes(ref curr, vv);
                }
                len = (int)(curr - buf);
            }

            return (val, len);
        }

        public int GenSingleKeyRequest(ref byte[] buffer, byte[] key = null, int index = 0)
        {
            int len = 0;

            Buffer.BlockCopy(opHeader, 0, buffer, 0, opHeader.Length);
            fixed (byte* buf = buffer)
            {
                byte* curr = buf + opHeader.Length;
                WriteStringBytes(ref curr, key);
                len = (int)(curr - buf);
            }

            return len;
        }

        private void WriteStringBytes(ref byte* curr, byte[] data)
        {
            *curr++ = (byte)'$';
            NumUtils.IntToBytes(data.Length, ref curr);
            *curr++ = (byte)'\r';
            *curr++ = (byte)'\n';

            for (int i = 0; i < data.Length; i++) *curr++ = (byte)data[i];
            *curr++ = (byte)'\r';
            *curr++ = (byte)'\n';
        }

        private void WriteOpLong(ref byte* curr, OpType opType, long n)
        {
            int nd = NumUtils.NumDigitsInLong(n);
            switch (opType)
            {
                case OpType.GET:
                case OpType.INCR:
                case OpType.MGET:
                case OpType.MSET:
                case OpType.PFADD:
                    *curr++ = (byte)'$';
                    NumUtils.IntToBytes(nd, ref curr);
                    *curr++ = (byte)'\r';
                    *curr++ = (byte)'\n';
                    NumUtils.LongToBytes(n, nd, ref curr);
                    *curr++ = (byte)'\r';
                    *curr++ = (byte)'\n';
                    break;
                default:
                    break;
            }

            switch (opType)
            {
                case OpType.MSET:
                    *curr++ = (byte)'$';
                    NumUtils.IntToBytes(nd, ref curr);
                    *curr++ = (byte)'\r';
                    *curr++ = (byte)'\n';
                    NumUtils.LongToBytes(n, nd, ref curr);
                    *curr++ = (byte)'\r';
                    *curr++ = (byte)'\n';
                    break;
                default:
                    break;
            }
        }
    }
}