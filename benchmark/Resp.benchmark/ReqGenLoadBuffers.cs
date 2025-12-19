// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;

namespace Resp.benchmark
{
    public unsafe partial class ReqGen
    {
        static readonly int bitOpSrckeyCount = 2;

        /// <summary>
        /// Generate requests for performance benchmarks
        /// </summary>
        public void Generate()
        {
            if (verbose)
            {
                Console.WriteLine();
                Console.WriteLine($"Generating {NumBuffs} {opType} request batches of size {BatchCount} each; total {NumBuffs * BatchCount} ops");

                if (opType == OpType.PFMERGE)
                {
                    Console.WriteLine("PFMERGE config > mergeDstKeyCount:{0}, hllDstMergeKeyFraction:{1}", hllDstMergeKeyCount, hllDstMergeKeyFraction);
                }
            }

            var sw = Stopwatch.StartNew();
            int maxBytesWritten = 0;
            while (true)
            {
                InitializeRNG();
                for (int i = 0; i < NumBuffs; i++)
                {
                    buffers[i] = new byte[BufferSize];
                    lens[i] = 0;

                    switch (opType)
                    {
                        case OpType.ZADDREM:
                            InitializeRNG(keySeed: i);
                            if (!GenerateBatch(i, 0, BatchCount / 2, OpType.ZADD)) goto resizeBuffer;
                            InitializeRNG(keySeed: i);
                            if (!GenerateBatch(i, BatchCount / 2, BatchCount, OpType.ZREM)) goto resizeBuffer;
                            break;
                        case OpType.GEOADDREM:
                            InitializeRNG(keySeed: i);
                            if (!GenerateBatch(i, 0, BatchCount / 2, OpType.GEOADD)) goto resizeBuffer;
                            InitializeRNG(keySeed: i);
                            if (!GenerateBatch(i, BatchCount / 2, BatchCount, OpType.ZREM)) goto resizeBuffer;
                            break;
                        case OpType.ZADDCARD:
                            InitializeRNG(keySeed: i);
                            if (!GenerateBatch(i, 0, BatchCount / 2, OpType.ZADD)) goto resizeBuffer;
                            InitializeRNG(keySeed: i);
                            if (!GenerateBatch(i, BatchCount / 2, BatchCount, OpType.ZCARD)) goto resizeBuffer;
                            break;
                        default:
                            if (!GenerateBatch(i, 0, BatchCount, opType)) goto resizeBuffer;
                            break;
                    }
                    maxBytesWritten = Math.Max(lens[i], maxBytesWritten);
                }
                break;

            resizeBuffer:
                if (verbose)
                {
                    Console.Write("Resizing request buffer from {0}", BufferSize);
                    BufferSize <<= 1;
                    Console.WriteLine(" to {0}", BufferSize);
                }
            }
            sw.Stop();
            if (verbose)
            {
                Console.WriteLine("Request generation complete");
                Console.WriteLine("maxBytesWritten out of maxBufferSize: {0}/{1}", maxBytesWritten, BufferSize);
                Console.WriteLine("Loading time: {0} secs", sw.ElapsedMilliseconds / 1000.0);
            }

            if (flatBufferClient)
            {
                ConvertToSERedisInput(opType);
                if (flatRequestBuffer.Count > 0)
                {
                    for (int i = 0; i < NumBuffs; i++)
                        buffers[i] = null;
                }
            }
        }

        private bool GenerateBatch(int i, int start, int end, OpType opType)
        {
            var batchHeader = opType switch
            {
                OpType.MGET => System.Text.Encoding.ASCII.GetBytes($"*{BatchCount + 1}\r\n$4\r\nMGET\r\n"),
                OpType.MSET => System.Text.Encoding.ASCII.GetBytes($"*{BatchCount * 2 + 1}\r\n$4\r\nMSET\r\n"),
                OpType.MPFADD => System.Text.Encoding.ASCII.GetBytes($"*{2 + BatchCount}\r\n$5\r\nPFADD\r\n"),
                _ => null
            };

            var opHeader = opType switch
            {
                OpType.INCR => System.Text.Encoding.ASCII.GetBytes($"*2\r\n$4\r\nINCR\r\n"),
                OpType.GET => System.Text.Encoding.ASCII.GetBytes($"*2\r\n$3\r\nGET\r\n"),
                OpType.SET => System.Text.Encoding.ASCII.GetBytes($"*3\r\n$3\r\nSET\r\n"),
                OpType.SETEX => System.Text.Encoding.ASCII.GetBytes($"*4\r\n$5\r\nSETEX\r\n"),
                OpType.PFADD => System.Text.Encoding.ASCII.GetBytes($"*3\r\n$5\r\nPFADD\r\n"),
                OpType.PFCOUNT => System.Text.Encoding.ASCII.GetBytes($"*2\r\n$7\r\nPFCOUNT\r\n"),
                OpType.PFMERGE => System.Text.Encoding.ASCII.GetBytes($"*3\r\n$7\r\nPFMERGE\r\n"),
                OpType.ZADD => System.Text.Encoding.ASCII.GetBytes($"*4\r\n$4\r\nZADD\r\n"),
                OpType.GEOADD => System.Text.Encoding.ASCII.GetBytes($"*5\r\n$6\r\nGEOADD\r\n"),
                OpType.ZREM => System.Text.Encoding.ASCII.GetBytes($"*3\r\n$4\r\nZREM\r\n"),
                OpType.ZCARD => System.Text.Encoding.ASCII.GetBytes($"*2\r\n$5\r\nZCARD\r\n"),
                OpType.SETBIT => System.Text.Encoding.ASCII.GetBytes($"*4\r\n$6\r\nSETBIT\r\n"),
                OpType.GETBIT => System.Text.Encoding.ASCII.GetBytes($"*3\r\n$6\r\nGETBIT\r\n"),
                OpType.BITCOUNT => System.Text.Encoding.ASCII.GetBytes($"*2\r\n$8\r\nBITCOUNT\r\n"),
                OpType.BITPOS => System.Text.Encoding.ASCII.GetBytes($"*3\r\n$6\r\nBITPOS\r\n"),
                OpType.BITOP_AND => System.Text.Encoding.ASCII.GetBytes($"*{3 + bitOpSrckeyCount}\r\n$5\r\nBITOP\r\n"),
                OpType.BITOP_OR => System.Text.Encoding.ASCII.GetBytes($"*{3 + bitOpSrckeyCount}\r\n$5\r\nBITOP\r\n"),
                OpType.BITOP_XOR => System.Text.Encoding.ASCII.GetBytes($"*{3 + bitOpSrckeyCount}\r\n$5\r\nBITOP\r\n"),
                OpType.BITOP_NOT => System.Text.Encoding.ASCII.GetBytes($"*{3 + 1}\r\n$5\r\nBITOP\r\n"),
                OpType.BITFIELD => System.Text.Encoding.ASCII.GetBytes($"*13\r\n$8\r\nBITFIELD\r\n"),
                OpType.BITFIELD_GET => System.Text.Encoding.ASCII.GetBytes($"*5\r\n$8\r\nBITFIELD\r\n"),
                OpType.BITFIELD_SET => System.Text.Encoding.ASCII.GetBytes($"*6\r\n$8\r\nBITFIELD\r\n"),
                OpType.BITFIELD_INCR => System.Text.Encoding.ASCII.GetBytes($"*6\r\n$8\r\nBITFIELD\r\n"),
                OpType.PING => System.Text.Encoding.ASCII.GetBytes("PING\r\n"),
                OpType.SETIFPM => System.Text.Encoding.ASCII.GetBytes($"*4\r\n$7\r\nSETIFPM\r\n"),
                OpType.MYDICTSET => System.Text.Encoding.ASCII.GetBytes($"*4\r\n$9\r\nMYDICTSET\r\n"),
                OpType.MYDICTGET => System.Text.Encoding.ASCII.GetBytes($"*3\r\n$9\r\nMYDICTGET\r\n"),
                OpType.SCRIPTSET => System.Text.Encoding.ASCII.GetBytes($"*5\r\n$7\r\nEVALSHA\r\n{BenchUtils.sha1SetScript}\r\n$1\r\n1\r\n"),
                OpType.SCRIPTGET => System.Text.Encoding.ASCII.GetBytes($"*4\r\n$7\r\nEVALSHA\r\n{BenchUtils.sha1GetScript}\r\n$1\r\n1\r\n"),
                OpType.SCRIPTRETKEY => System.Text.Encoding.ASCII.GetBytes($"*4\r\n$7\r\nEVALSHA\r\n{BenchUtils.sha1RetKeyScript}\r\n$1\r\n1\r\n"),
                OpType.PUBLISH => System.Text.Encoding.ASCII.GetBytes($"*3\r\n$7\r\nPUBLISH\r\n"),
                OpType.SPUBLISH => System.Text.Encoding.ASCII.GetBytes($"*3\r\n$8\r\nSPUBLISH\r\n"),
                OpType.XADD => System.Text.Encoding.ASCII.GetBytes($"*5\r\n$4\r\nXADD\r\n"),
                _ => null
            };

            bool writeSuccess;
            switch (opType)
            {
                case OpType.MGET:
                case OpType.MSET:
                case OpType.MPFADD:
                    writeSuccess = GenerateArrayOp(i, batchHeader, start, end, opType);
                    return writeSuccess;
                case OpType.ZADD:
                case OpType.ZREM:
                case OpType.ZCARD:
                case OpType.GEOADD:
                case OpType.PING:
                case OpType.INCR:
                case OpType.GET:
                case OpType.SET:
                case OpType.SETEX:
                case OpType.PFADD:
                case OpType.PFCOUNT:
                case OpType.PFMERGE:
                case OpType.SETBIT:
                case OpType.GETBIT:
                case OpType.BITCOUNT:
                case OpType.BITPOS:
                case OpType.BITOP_AND:
                case OpType.BITOP_OR:
                case OpType.BITOP_XOR:
                case OpType.BITOP_NOT:
                case OpType.BITFIELD:
                case OpType.BITFIELD_GET:
                case OpType.BITFIELD_SET:
                case OpType.BITFIELD_INCR:
                case OpType.SETIFPM:
                case OpType.MYDICTSET:
                case OpType.MYDICTGET:
                case OpType.SCRIPTSET:
                case OpType.SCRIPTGET:
                case OpType.SCRIPTRETKEY:
                case OpType.PUBLISH:
                case OpType.SPUBLISH:
                case OpType.XADD:
                    writeSuccess = GenerateSingleKeyValueOp(i, opHeader, start, end, opType);
                    return writeSuccess;
                default:
                    throw new Exception("ReqGen command not supported!");
            }
        }

        private bool GenerateArrayOp(int i, byte[] batchHeader, int start, int end, OpType opType)
        {
            var buffer = buffers[i];
            fixed (byte* b = buffer)
            {
                byte* curr = b + lens[i];
                byte* vend = b + BufferSize;

                if (!WriteHeader(batchHeader, ref curr, vend))
                    return false;

                if (opType == OpType.MPFADD)
                {
                    if (!WriteKey(ref curr, vend))
                        return false;
                }

                for (int c = start; c < end; c++)
                {
                    if (!WriteOp(ref curr, vend, opType))
                        return false;
                }
                lens[i] = (int)(curr - b);
            }
            return true;
        }

        private bool GenerateSingleKeyValueOp(int i, byte[] opHeader, int start, int end, OpType opType)
        {
            var buffer = buffers[i];
            fixed (byte* b = buffer)
            {
                byte* curr = b + lens[i];
                byte* vend = b + BufferSize;
                for (int c = start; c < end; c++)
                {
                    byte* tmp = curr;
                    // Write Op Header
                    if (!WriteHeader(opHeader, ref curr, vend))
                        return false;

                    if (!WriteOp(ref curr, vend, opType))
                        return false;
                }
                lens[i] = (int)(curr - b);
            }
            return true;
        }
    }
}