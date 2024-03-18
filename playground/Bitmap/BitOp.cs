// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#define ALIGNED_PTR 
//#define ALLOC_BUFFER_ON_THE_FLY
//#define USE_PRECOMPUTED_BATCHES

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Tsavorite.core;

namespace Bitmap
{
    public static unsafe class BitOp
    {
        static LinkedList<SectorAlignedMemory> alignedMemoryPool = new LinkedList<SectorAlignedMemory>();
        static LinkedList<MemoryPoolBuffers> memoryPoolBuffers = new LinkedList<MemoryPoolBuffers>();

        static List<SectorAlignedMemory> sectorAlignedMemoryBuffers = new List<SectorAlignedMemory>();
        static List<byte[]> unalignedMemoryBuffers = new List<byte[]>();

        private static byte* alloc_aligned(int numRecords, int sectorSize)
        {
            alignedMemoryPool.AddFirst(new SectorAlignedMemory(numRecords, sectorSize));
            return alignedMemoryPool.First.Value.GetValidPointer();
        }

        private static void dispose_aligned()
        {
            if (alignedMemoryPool != null)
            {
                foreach (SectorAlignedMemory alignedMemory in alignedMemoryPool)
                {
                    alignedMemory.Dispose();
                }
                alignedMemoryPool = new LinkedList<SectorAlignedMemory>();
            }
        }

        private static byte* alloc_unaligned_memory_pool_buffers(int numRecords)
        {
            memoryPoolBuffers.AddFirst(new MemoryPoolBuffers(numRecords));
            return memoryPoolBuffers.First.Value.GetValidPointer();
        }

        private static void InitializeInputAlignedMemoryPool(int batchSize, int bitmapLen, int alignment)
        {
            Console.WriteLine("Initializing aligned input data...");
            Random r = new Random(Guid.NewGuid().GetHashCode());
            byte[] buffer = new byte[bitmapLen];

            for (int i = 0; i < batchSize; i++)
            {
                r.NextBytes(buffer);
                sectorAlignedMemoryBuffers.Add(new SectorAlignedMemory(bitmapLen, alignment));

                fixed (byte* b = buffer)
                {
                    byte* alignedPtr = sectorAlignedMemoryBuffers[i].GetValidPointer();
                    Buffer.MemoryCopy(b, alignedPtr, bitmapLen, bitmapLen);
                }
            }
        }

        private static void DisposeInputAlignedMemoryPool()
        {
            if (sectorAlignedMemoryBuffers != null)
            {
                foreach (SectorAlignedMemory alignedMemory in sectorAlignedMemoryBuffers)
                {
                    alignedMemory.Dispose();
                }
            }
        }

        private static void InitializeInputUnalignedMemoryPool(int batchSize, int bitmapLen)
        {
            Console.WriteLine("Initializing unaligned input data...");
            Random r = new Random(Guid.NewGuid().GetHashCode());
            byte[] buffer = new byte[bitmapLen];

            for (int i = 0; i < batchSize; i++)
            {
                r.NextBytes(buffer);
                unalignedMemoryBuffers.Add(new byte[bitmapLen]);
                Buffer.BlockCopy(buffer, 0, unalignedMemoryBuffers[i], 0, bitmapLen);
            }
        }

        private static void dispose_memory_pool_buffers()
        {
            if (memoryPoolBuffers != null)
            {
                foreach (MemoryPoolBuffers memoryPoolBuffer in memoryPoolBuffers)
                {
                    memoryPoolBuffer.Dispose();
                }
                memoryPoolBuffers = new LinkedList<MemoryPoolBuffers>();
            }
        }

        public static void __bitop_scalar_and(byte* dstBitmap, long dstLen, byte* srcBitmap, long srcLen)
        {
            int batchSize = 8 * 4;
            long slen = srcLen;
            long stail = slen & (batchSize - 1);

            //iterate using srcBitmap because always dstLen >= srcLen 
            byte* srcCurr = srcBitmap;
            byte* srcEnd = srcCurr + (slen - stail);
            byte* dstCurr = dstBitmap;
            while (srcCurr < srcEnd)
            {
                long s00 = *(long*)(srcCurr);
                long s01 = *(long*)(srcCurr + 8);
                long s02 = *(long*)(srcCurr + 16);
                long s03 = *(long*)(srcCurr + 24);

                long d00 = *(long*)(dstCurr);
                long d01 = *(long*)(dstCurr + 8);
                long d02 = *(long*)(dstCurr + 16);
                long d03 = *(long*)(dstCurr + 24);

                d00 = d00 & s00;
                d01 = d01 & s01;
                d02 = d02 & s02;
                d03 = d03 & s03;

                *(long*)dstCurr = d00;
                *(long*)(dstCurr + 8) = d01;
                *(long*)(dstCurr + 16) = d02;
                *(long*)(dstCurr + 24) = d03;

                srcCurr += batchSize;
                dstCurr += batchSize;
            }

            slen = stail;
            batchSize = 8;
            stail = slen & (batchSize - 1);
            srcEnd = srcCurr + (slen - stail);
            while (srcCurr < srcEnd)
            {
                long s00 = *(long*)(srcCurr);
                long d00 = *(long*)(dstCurr);

                *(long*)dstCurr = d00 & s00;

                srcCurr += batchSize;
                dstCurr += batchSize;
            }

            if (stail >= 7) dstCurr[6] = (byte)(dstCurr[6] & srcCurr[6]);
            if (stail >= 6) dstCurr[5] = (byte)(dstCurr[5] & srcCurr[5]);
            if (stail >= 5) dstCurr[4] = (byte)(dstCurr[4] & srcCurr[4]);
            if (stail >= 4) dstCurr[3] = (byte)(dstCurr[3] & srcCurr[3]);
            if (stail >= 3) dstCurr[2] = (byte)(dstCurr[2] & srcCurr[2]);
            if (stail >= 2) dstCurr[1] = (byte)(dstCurr[1] & srcCurr[1]);
            if (stail >= 1) dstCurr[0] = (byte)(dstCurr[0] & srcCurr[0]);

            #region dstTailCombineWithZero
            if (dstLen > srcLen)
            {
                batchSize = 8 * 4;
                dstCurr += stail;
                long dlen = dstLen - srcLen;
                long dtail = dlen & (batchSize - 1);
                byte* dstEnd = dstCurr + (dlen - dtail);
                while (dstCurr < dstEnd)
                {
                    long d00 = *(long*)(dstCurr);
                    long d01 = *(long*)(dstCurr + 8);
                    long d02 = *(long*)(dstCurr + 16);
                    long d03 = *(long*)(dstCurr + 32);

                    d00 = d00 & 0;
                    d01 = d01 & 0;
                    d02 = d02 & 0;
                    d03 = d03 & 0;

                    *(long*)dstCurr = d00;
                    *(long*)(dstCurr + 8) = d01;
                    *(long*)(dstCurr + 16) = d02;
                    *(long*)(dstCurr + 24) = d03;
                    dstCurr += batchSize;
                }

                dlen = dtail;
                batchSize = 8;
                dtail = dlen & (batchSize - 1);
                dstEnd = dstCurr + (dlen - dtail);
                while (dstCurr < dstEnd)
                {
                    *(long*)dstCurr = *(long*)(dstCurr) & 0;
                    dstCurr += batchSize;
                }

                if (stail >= 7) dstCurr[6] = (byte)(dstCurr[6] & 0);
                if (stail >= 6) dstCurr[5] = (byte)(dstCurr[5] & 0);
                if (stail >= 5) dstCurr[4] = (byte)(dstCurr[4] & 0);
                if (stail >= 4) dstCurr[3] = (byte)(dstCurr[3] & 0);
                if (stail >= 3) dstCurr[2] = (byte)(dstCurr[2] & 0);
                if (stail >= 2) dstCurr[1] = (byte)(dstCurr[1] & 0);
                if (stail >= 1) dstCurr[0] = (byte)(dstCurr[0] & 0);
            }
            #endregion
        }

        private static void __bitop_simdX128_and(byte* dstBitmap, long dstLen, byte* srcBitmap, long srcLen)
        {
            #region bitop_8x16
            int batchSize = 8 * 16;
            long slen = srcLen;
            long stail = slen & (batchSize - 1);

            byte* srcCurr = srcBitmap;
            byte* srcEnd = srcCurr + (slen - stail);
            byte* dstCurr = dstBitmap;
            while (srcCurr < srcEnd)
            {
#if ALIGNED_PTR
                Vector128<byte> s00 = Sse2.LoadAlignedVector128(srcCurr);
                Vector128<byte> s01 = Sse2.LoadAlignedVector128(srcCurr + 16);
                Vector128<byte> s02 = Sse2.LoadAlignedVector128(srcCurr + 32);
                Vector128<byte> s03 = Sse2.LoadAlignedVector128(srcCurr + 48);
                Vector128<byte> s04 = Sse2.LoadAlignedVector128(srcCurr + 64);
                Vector128<byte> s05 = Sse2.LoadAlignedVector128(srcCurr + 80);
                Vector128<byte> s06 = Sse2.LoadAlignedVector128(srcCurr + 96);
                Vector128<byte> s07 = Sse2.LoadAlignedVector128(srcCurr + 112);

                Vector128<byte> d00 = Sse2.LoadAlignedVector128(dstCurr);
                Vector128<byte> d01 = Sse2.LoadAlignedVector128(dstCurr + 16);
                Vector128<byte> d02 = Sse2.LoadAlignedVector128(dstCurr + 32);
                Vector128<byte> d03 = Sse2.LoadAlignedVector128(dstCurr + 48);
                Vector128<byte> d04 = Sse2.LoadAlignedVector128(dstCurr + 64);
                Vector128<byte> d05 = Sse2.LoadAlignedVector128(dstCurr + 80);
                Vector128<byte> d06 = Sse2.LoadAlignedVector128(dstCurr + 96);
                Vector128<byte> d07 = Sse2.LoadAlignedVector128(dstCurr + 112);

                Sse2.StoreAligned(dstCurr, Sse2.And(d00, s00));
                Sse2.StoreAligned(dstCurr + 16, Sse2.And(d01, s01));
                Sse2.StoreAligned(dstCurr + 32, Sse2.And(d02, s02));
                Sse2.StoreAligned(dstCurr + 48, Sse2.And(d03, s03));
                Sse2.StoreAligned(dstCurr + 64, Sse2.And(d04, s04));
                Sse2.StoreAligned(dstCurr + 80, Sse2.And(d05, s05));
                Sse2.StoreAligned(dstCurr + 96, Sse2.And(d06, s06));
                Sse2.StoreAligned(dstCurr + 112, Sse2.And(d07, s07));
#else
                Vector128<byte> s00 = Sse42.LoadVector128(srcCurr);
                Vector128<byte> s01 = Sse42.LoadVector128(srcCurr + 16);
                Vector128<byte> s02 = Sse42.LoadVector128(srcCurr + 32);
                Vector128<byte> s03 = Sse42.LoadVector128(srcCurr + 48);
                Vector128<byte> s04 = Sse42.LoadVector128(srcCurr + 64);
                Vector128<byte> s05 = Sse42.LoadVector128(srcCurr + 80);
                Vector128<byte> s06 = Sse42.LoadVector128(srcCurr + 96);
                Vector128<byte> s07 = Sse42.LoadVector128(srcCurr + 112);

                Vector128<byte> d00 = Sse42.LoadVector128(dstCurr);
                Vector128<byte> d01 = Sse42.LoadVector128(dstCurr + 16);
                Vector128<byte> d02 = Sse42.LoadVector128(dstCurr + 32);
                Vector128<byte> d03 = Sse42.LoadVector128(dstCurr + 48);
                Vector128<byte> d04 = Sse42.LoadVector128(dstCurr + 64);
                Vector128<byte> d05 = Sse42.LoadVector128(dstCurr + 80);
                Vector128<byte> d06 = Sse42.LoadVector128(dstCurr + 96);
                Vector128<byte> d07 = Sse42.LoadVector128(dstCurr + 112);
                
                Sse42.Store(dstCurr, Sse42.And(d00, s00));
                Sse42.Store(dstCurr + 16, Sse42.And(d01, s01));
                Sse42.Store(dstCurr + 32, Sse42.And(d02, s02));
                Sse42.Store(dstCurr + 48, Sse42.And(d03, s03));
                Sse42.Store(dstCurr + 64, Sse42.And(d04, s04));
                Sse42.Store(dstCurr + 80, Sse42.And(d05, s05));
                Sse42.Store(dstCurr + 96, Sse42.And(d06, s06));
                Sse42.Store(dstCurr + 112, Sse42.And(d07, s07));
#endif

                srcCurr += batchSize;
                dstCurr += batchSize;
            }
            if (stail == 0) goto fillTail;
            #endregion            

            #region bitop_1x16
            slen = stail;
            batchSize = 16;
            stail = slen & (batchSize - 1);
            srcEnd = srcCurr + (slen - stail);
            while (srcCurr < srcEnd)
            {
                Vector128<byte> s00 = Sse2.LoadVector128(srcCurr);
                Vector128<byte> d00 = Sse2.LoadVector128(dstCurr);

                Sse2.Store(dstCurr, Sse2.And(d00, s00));

                srcCurr += batchSize;
                dstCurr += batchSize;
            }
            if (stail == 0) goto fillTail;
            #endregion

            #region bitop_4x8
            slen = stail;
            batchSize = 4 * 8;
            stail = slen & (batchSize - 1);
            srcEnd = srcCurr + (slen - stail);
            while (srcCurr < srcEnd)
            {
                long s00 = *(long*)(srcCurr);
                long s01 = *(long*)(srcCurr + 8);
                long s02 = *(long*)(srcCurr + 16);
                long s03 = *(long*)(srcCurr + 24);

                long d00 = *(long*)(dstCurr);
                long d01 = *(long*)(dstCurr + 8);
                long d02 = *(long*)(dstCurr + 16);
                long d03 = *(long*)(dstCurr + 24);

                d00 = d00 & s00;
                d01 = d01 & s01;
                d02 = d02 & s02;
                d03 = d03 & s03;

                *(long*)dstCurr = d00;
                *(long*)(dstCurr + 8) = d01;
                *(long*)(dstCurr + 16) = d02;
                *(long*)(dstCurr + 24) = d03;

                srcCurr += batchSize;
                dstCurr += batchSize;
            }
            if (stail == 0) goto fillTail;
            #endregion

            #region bitop_1x8
            slen = stail;
            batchSize = 8;
            stail = slen & (batchSize - 1);
            srcEnd = srcCurr + (slen - stail);
            while (srcCurr < srcEnd)
            {
                long s00 = *(long*)(srcCurr);
                long d00 = *(long*)(dstCurr);

                *(long*)dstCurr = d00 & s00;

                srcCurr += batchSize;
                dstCurr += batchSize;
            }

            if (stail >= 7) dstCurr[6] = (byte)(dstCurr[6] & srcCurr[6]);
            if (stail >= 6) dstCurr[5] = (byte)(dstCurr[5] & srcCurr[5]);
            if (stail >= 5) dstCurr[4] = (byte)(dstCurr[4] & srcCurr[4]);
            if (stail >= 4) dstCurr[3] = (byte)(dstCurr[3] & srcCurr[3]);
            if (stail >= 3) dstCurr[2] = (byte)(dstCurr[2] & srcCurr[2]);
            if (stail >= 2) dstCurr[1] = (byte)(dstCurr[1] & srcCurr[1]);
            if (stail >= 1) dstCurr[0] = (byte)(dstCurr[0] & srcCurr[0]);
            #endregion

            #region fillDstTail
            fillTail:
            if (dstLen > srcLen)
            {
                batchSize = 8 * 4;
                dstCurr += stail;
                long dlen = dstLen - srcLen;
                long dtail = dlen & (batchSize - 1);
                byte* dstEnd = dstCurr + (dlen - dtail);
                while (dstCurr < dstEnd)
                {
                    long d00 = *(long*)(dstCurr);
                    long d01 = *(long*)(dstCurr + 8);
                    long d02 = *(long*)(dstCurr + 16);
                    long d03 = *(long*)(dstCurr + 32);

                    d00 = d00 & 0;
                    d01 = d01 & 0;
                    d02 = d02 & 0;
                    d03 = d03 & 0;

                    *(long*)dstCurr = d00;
                    *(long*)(dstCurr + 8) = d01;
                    *(long*)(dstCurr + 16) = d02;
                    *(long*)(dstCurr + 24) = d03;
                    dstCurr += batchSize;
                }

                dlen = dtail;
                batchSize = 8;
                dtail = dlen & (batchSize - 1);
                dstEnd = dstCurr + (dlen - dtail);
                while (dstCurr < dstEnd)
                {
                    *(long*)dstCurr = *(long*)(dstCurr) & 0;
                    dstCurr += batchSize;
                }

                if (dtail >= 7) dstCurr[6] = (byte)(dstCurr[6] & 0);
                if (dtail >= 6) dstCurr[5] = (byte)(dstCurr[5] & 0);
                if (dtail >= 5) dstCurr[4] = (byte)(dstCurr[4] & 0);
                if (dtail >= 4) dstCurr[3] = (byte)(dstCurr[3] & 0);
                if (dtail >= 3) dstCurr[2] = (byte)(dstCurr[2] & 0);
                if (dtail >= 2) dstCurr[1] = (byte)(dstCurr[1] & 0);
                if (dtail >= 1) dstCurr[0] = (byte)(dstCurr[0] & 0);
            }
            #endregion
        }

        private static void __bitop_simdX128_and_long(byte* dstBitmap, long dstLen, byte* srcBitmap, long srcLen)
        {
            #region bitop_8x16
            int batchSize = 8 * 16;
            long slen = srcLen;
            long stail = slen & (batchSize - 1);

            byte* srcCurr = srcBitmap;
            byte* srcEnd = srcCurr + (slen - stail);
            byte* dstCurr = dstBitmap;
            while (srcCurr < srcEnd)
            {
                Vector128<long> s00 = Sse2.LoadVector128((long*)srcCurr);
                Vector128<long> s01 = Sse2.LoadVector128((long*)(srcCurr + 16));
                Vector128<long> s02 = Sse2.LoadVector128((long*)(srcCurr + 32));
                Vector128<long> s03 = Sse2.LoadVector128((long*)(srcCurr + 48));
                Vector128<long> s04 = Sse2.LoadVector128((long*)(srcCurr + 64));
                Vector128<long> s05 = Sse2.LoadVector128((long*)(srcCurr + 80));
                Vector128<long> s06 = Sse2.LoadVector128((long*)(srcCurr + 96));
                Vector128<long> s07 = Sse2.LoadVector128((long*)(srcCurr + 112));

                Vector128<long> d00 = Sse2.LoadVector128((long*)dstCurr);
                Vector128<long> d01 = Sse2.LoadVector128((long*)(dstCurr + 16));
                Vector128<long> d02 = Sse2.LoadVector128((long*)(dstCurr + 32));
                Vector128<long> d03 = Sse2.LoadVector128((long*)(dstCurr + 48));
                Vector128<long> d04 = Sse2.LoadVector128((long*)(dstCurr + 64));
                Vector128<long> d05 = Sse2.LoadVector128((long*)(dstCurr + 80));
                Vector128<long> d06 = Sse2.LoadVector128((long*)(dstCurr + 96));
                Vector128<long> d07 = Sse2.LoadVector128((long*)(dstCurr + 112));

                Sse2.Store((long*)dstCurr, Sse2.And(d00, s00));
                Sse2.Store((long*)(dstCurr + 16), Sse2.And(d01, s01));
                Sse2.Store((long*)(dstCurr + 32), Sse2.And(d02, s02));
                Sse2.Store((long*)(dstCurr + 48), Sse2.And(d03, s03));
                Sse2.Store((long*)(dstCurr + 64), Sse2.And(d04, s04));
                Sse2.Store((long*)(dstCurr + 80), Sse2.And(d05, s05));
                Sse2.Store((long*)(dstCurr + 96), Sse2.And(d06, s06));
                Sse2.Store((long*)(dstCurr + 112), Sse2.And(d07, s07));

                srcCurr += batchSize;
                dstCurr += batchSize;
            }
            if (stail == 0) goto fillTail;
            #endregion            

            #region bitop_1x16
            slen = stail;
            batchSize = 16;
            stail = slen & (batchSize - 1);
            srcEnd = srcCurr + (slen - stail);
            while (srcCurr < srcEnd)
            {
                Vector128<long> s00 = Sse2.LoadVector128((long*)srcCurr);
                Vector128<long> d00 = Sse2.LoadVector128((long*)dstCurr);

                Sse2.Store((long*)dstCurr, Sse2.And(d00, s00));

                srcCurr += batchSize;
                dstCurr += batchSize;
            }
            if (stail == 0) goto fillTail;
            #endregion

            #region bitop_4x8
            slen = stail;
            batchSize = 4 * 8;
            stail = slen & (batchSize - 1);
            srcEnd = srcCurr + (slen - stail);
            while (srcCurr < srcEnd)
            {
                long s00 = *(long*)(srcCurr);
                long s01 = *(long*)(srcCurr + 8);
                long s02 = *(long*)(srcCurr + 16);
                long s03 = *(long*)(srcCurr + 24);

                long d00 = *(long*)(dstCurr);
                long d01 = *(long*)(dstCurr + 8);
                long d02 = *(long*)(dstCurr + 16);
                long d03 = *(long*)(dstCurr + 24);

                d00 = d00 & s00;
                d01 = d01 & s01;
                d02 = d02 & s02;
                d03 = d03 & s03;

                *(long*)dstCurr = d00;
                *(long*)(dstCurr + 8) = d01;
                *(long*)(dstCurr + 16) = d02;
                *(long*)(dstCurr + 24) = d03;

                srcCurr += batchSize;
                dstCurr += batchSize;
            }
            if (stail == 0) goto fillTail;
            #endregion

            #region bitop_1x8
            slen = stail;
            batchSize = 8;
            stail = slen & (batchSize - 1);
            srcEnd = srcCurr + (slen - stail);
            while (srcCurr < srcEnd)
            {
                long s00 = *(long*)(srcCurr);
                long d00 = *(long*)(dstCurr);

                *(long*)dstCurr = d00 & s00;

                srcCurr += batchSize;
                dstCurr += batchSize;
            }

            if (stail >= 7) dstCurr[6] = (byte)(dstCurr[6] & srcCurr[6]);
            if (stail >= 6) dstCurr[5] = (byte)(dstCurr[5] & srcCurr[5]);
            if (stail >= 5) dstCurr[4] = (byte)(dstCurr[4] & srcCurr[4]);
            if (stail >= 4) dstCurr[3] = (byte)(dstCurr[3] & srcCurr[3]);
            if (stail >= 3) dstCurr[2] = (byte)(dstCurr[2] & srcCurr[2]);
            if (stail >= 2) dstCurr[1] = (byte)(dstCurr[1] & srcCurr[1]);
            if (stail >= 1) dstCurr[0] = (byte)(dstCurr[0] & srcCurr[0]);
            #endregion

            #region fillDstTail
            fillTail:
            if (dstLen > srcLen)
            {
                batchSize = 8 * 4;
                dstCurr += stail;
                long dlen = dstLen - srcLen;
                long dtail = dlen & (batchSize - 1);
                byte* dstEnd = dstCurr + (dlen - dtail);
                while (dstCurr < dstEnd)
                {
                    long d00 = *(long*)(dstCurr);
                    long d01 = *(long*)(dstCurr + 8);
                    long d02 = *(long*)(dstCurr + 16);
                    long d03 = *(long*)(dstCurr + 32);

                    d00 = d00 & 0;
                    d01 = d01 & 0;
                    d02 = d02 & 0;
                    d03 = d03 & 0;

                    *(long*)dstCurr = d00;
                    *(long*)(dstCurr + 8) = d01;
                    *(long*)(dstCurr + 16) = d02;
                    *(long*)(dstCurr + 24) = d03;
                    dstCurr += batchSize;
                }

                dlen = dtail;
                batchSize = 8;
                dtail = dlen & (batchSize - 1);
                dstEnd = dstCurr + (dlen - dtail);
                while (dstCurr < dstEnd)
                {
                    *(long*)dstCurr = *(long*)(dstCurr) & 0;
                    dstCurr += batchSize;
                }

                if (dtail >= 7) dstCurr[6] = (byte)(dstCurr[6] & 0);
                if (dtail >= 6) dstCurr[5] = (byte)(dstCurr[5] & 0);
                if (dtail >= 5) dstCurr[4] = (byte)(dstCurr[4] & 0);
                if (dtail >= 4) dstCurr[3] = (byte)(dstCurr[3] & 0);
                if (dtail >= 3) dstCurr[2] = (byte)(dstCurr[2] & 0);
                if (dtail >= 2) dstCurr[1] = (byte)(dstCurr[1] & 0);
                if (dtail >= 1) dstCurr[0] = (byte)(dstCurr[0] & 0);
            }
            #endregion
        }

        private static void __bitop_simdX256_and(byte* dstBitmap, long dstLen, byte* srcBitmap, long srcLen)
        {
            #region bitop_8x16
            int batchSize = 8 * 32;
            long slen = srcLen;
            long stail = slen & (batchSize - 1);

            byte* srcCurr = srcBitmap;
            byte* srcEnd = srcCurr + (slen - stail);
            byte* dstCurr = dstBitmap;
            while (srcCurr < srcEnd)
            {
#if ALIGNED_PTR
                Vector256<byte> s00 = Avx.LoadAlignedVector256(srcCurr);
                Vector256<byte> s01 = Avx.LoadAlignedVector256(srcCurr + 32);
                Vector256<byte> s02 = Avx.LoadAlignedVector256(srcCurr + 64);
                Vector256<byte> s03 = Avx.LoadAlignedVector256(srcCurr + 96);
                Vector256<byte> s04 = Avx.LoadAlignedVector256(srcCurr + 128);
                Vector256<byte> s05 = Avx.LoadAlignedVector256(srcCurr + 160);
                Vector256<byte> s06 = Avx.LoadAlignedVector256(srcCurr + 192);
                Vector256<byte> s07 = Avx.LoadAlignedVector256(srcCurr + 224);

                Vector256<byte> d00 = Avx.LoadAlignedVector256(dstCurr);
                Vector256<byte> d01 = Avx.LoadAlignedVector256(dstCurr + 32);
                Vector256<byte> d02 = Avx.LoadAlignedVector256(dstCurr + 64);
                Vector256<byte> d03 = Avx.LoadAlignedVector256(dstCurr + 96);
                Vector256<byte> d04 = Avx.LoadAlignedVector256(dstCurr + 128);
                Vector256<byte> d05 = Avx.LoadAlignedVector256(dstCurr + 160);
                Vector256<byte> d06 = Avx.LoadAlignedVector256(dstCurr + 192);
                Vector256<byte> d07 = Avx.LoadAlignedVector256(dstCurr + 224);

                Avx.StoreAligned(dstCurr, Avx2.And(d00, s00));
                Avx.StoreAligned(dstCurr + 32, Avx2.And(d01, s01));
                Avx.StoreAligned(dstCurr + 64, Avx2.And(d02, s02));
                Avx.StoreAligned(dstCurr + 96, Avx2.And(d03, s03));
                Avx.StoreAligned(dstCurr + 128, Avx2.And(d04, s04));
                Avx.StoreAligned(dstCurr + 160, Avx2.And(d05, s05));
                Avx.StoreAligned(dstCurr + 192, Avx2.And(d06, s06));
                Avx.StoreAligned(dstCurr + 224, Avx2.And(d07, s07));
#else
                Vector256<byte> s00 = Avx2.LoadVector256(srcCurr);
                Vector256<byte> s01 = Avx2.LoadVector256(srcCurr + 32);
                Vector256<byte> s02 = Avx2.LoadVector256(srcCurr + 64);
                Vector256<byte> s03 = Avx2.LoadVector256(srcCurr + 96);
                Vector256<byte> s04 = Avx2.LoadVector256(srcCurr + 128);
                Vector256<byte> s05 = Avx2.LoadVector256(srcCurr + 160);
                Vector256<byte> s06 = Avx2.LoadVector256(srcCurr + 192);
                Vector256<byte> s07 = Avx2.LoadVector256(srcCurr + 224);

                Vector256<byte> d00 = Avx2.LoadVector256(dstCurr);
                Vector256<byte> d01 = Avx2.LoadVector256(dstCurr + 32);
                Vector256<byte> d02 = Avx2.LoadVector256(dstCurr + 64);
                Vector256<byte> d03 = Avx2.LoadVector256(dstCurr + 96);
                Vector256<byte> d04 = Avx2.LoadVector256(dstCurr + 128);
                Vector256<byte> d05 = Avx2.LoadVector256(dstCurr + 160);
                Vector256<byte> d06 = Avx2.LoadVector256(dstCurr + 192);
                Vector256<byte> d07 = Avx2.LoadVector256(dstCurr + 224);

                Avx2.Store(dstCurr, Avx2.And(d00, s00));
                Avx2.Store(dstCurr + 32, Avx2.And(d01, s01));
                Avx2.Store(dstCurr + 64, Avx2.And(d02, s02));
                Avx2.Store(dstCurr + 96, Avx2.And(d03, s03));
                Avx2.Store(dstCurr + 128, Avx2.And(d04, s04));
                Avx2.Store(dstCurr + 160, Avx2.And(d05, s05));
                Avx2.Store(dstCurr + 192, Avx2.And(d06, s06));
                Avx2.Store(dstCurr + 224, Avx2.And(d07, s07));
#endif

                srcCurr += batchSize;
                dstCurr += batchSize;
            }
            if (stail == 0) goto fillTail;
            #endregion

            #region bitop_1x16
            slen = stail;
            batchSize = 32;
            stail = slen & (batchSize - 1);
            srcEnd = srcCurr + (slen - stail);
            while (srcCurr < srcEnd)
            {
                Vector256<byte> s00 = Avx.LoadVector256(srcCurr);
                Vector256<byte> d00 = Avx.LoadVector256(dstCurr);

                Avx.Store(dstCurr, Avx2.And(d00, s00));

                srcCurr += batchSize;
                dstCurr += batchSize;
            }
            if (stail == 0) goto fillTail;
            #endregion

            #region bitop_4x8
            slen = stail;
            batchSize = 4 * 8;
            stail = slen & (batchSize - 1);
            srcEnd = srcCurr + (slen - stail);
            while (srcCurr < srcEnd)
            {
                long s00 = *(long*)(srcCurr);
                long s01 = *(long*)(srcCurr + 8);
                long s02 = *(long*)(srcCurr + 16);
                long s03 = *(long*)(srcCurr + 24);

                long d00 = *(long*)(dstCurr);
                long d01 = *(long*)(dstCurr + 8);
                long d02 = *(long*)(dstCurr + 16);
                long d03 = *(long*)(dstCurr + 24);

                d00 = d00 & s00;
                d01 = d01 & s01;
                d02 = d02 & s02;
                d03 = d03 & s03;

                *(long*)dstCurr = d00;
                *(long*)(dstCurr + 8) = d01;
                *(long*)(dstCurr + 16) = d02;
                *(long*)(dstCurr + 24) = d03;

                srcCurr += batchSize;
                dstCurr += batchSize;
            }
            if (stail == 0) goto fillTail;
            #endregion

            #region bitop_1x8
            slen = stail;
            batchSize = 8;
            stail = slen & (batchSize - 1);
            srcEnd = srcCurr + (slen - stail);
            while (srcCurr < srcEnd)
            {
                long s00 = *(long*)(srcCurr);
                long d00 = *(long*)(dstCurr);

                *(long*)dstCurr = d00 & s00;

                srcCurr += batchSize;
                dstCurr += batchSize;
            }

            if (stail >= 7) dstCurr[6] = (byte)(dstCurr[6] & srcCurr[6]);
            if (stail >= 6) dstCurr[5] = (byte)(dstCurr[5] & srcCurr[5]);
            if (stail >= 5) dstCurr[4] = (byte)(dstCurr[4] & srcCurr[4]);
            if (stail >= 4) dstCurr[3] = (byte)(dstCurr[3] & srcCurr[3]);
            if (stail >= 3) dstCurr[2] = (byte)(dstCurr[2] & srcCurr[2]);
            if (stail >= 2) dstCurr[1] = (byte)(dstCurr[1] & srcCurr[1]);
            if (stail >= 1) dstCurr[0] = (byte)(dstCurr[0] & srcCurr[0]);
            #endregion

            #region fillDstTail
            fillTail:
            if (dstLen > srcLen)
            {
                batchSize = 8 * 4;
                dstCurr += stail;
                long dlen = dstLen - srcLen;
                long dtail = dlen & (batchSize - 1);
                byte* dstEnd = dstCurr + (dlen - dtail);
                while (dstCurr < dstEnd)
                {
                    long d00 = *(long*)(dstCurr);
                    long d01 = *(long*)(dstCurr + 8);
                    long d02 = *(long*)(dstCurr + 16);
                    long d03 = *(long*)(dstCurr + 32);

                    d00 = d00 & 0;
                    d01 = d01 & 0;
                    d02 = d02 & 0;
                    d03 = d03 & 0;

                    *(long*)dstCurr = d00;
                    *(long*)(dstCurr + 8) = d01;
                    *(long*)(dstCurr + 16) = d02;
                    *(long*)(dstCurr + 24) = d03;
                    dstCurr += batchSize;
                }

                dlen = dtail;
                batchSize = 8;
                dtail = dlen & (batchSize - 1);
                dstEnd = dstCurr + (dlen - dtail);
                while (dstCurr < dstEnd)
                {
                    *(long*)dstCurr = *(long*)(dstCurr) & 0;
                    dstCurr += batchSize;
                }

                if (dtail >= 7) dstCurr[6] = (byte)(dstCurr[6] & 0);
                if (dtail >= 6) dstCurr[5] = (byte)(dstCurr[5] & 0);
                if (dtail >= 5) dstCurr[4] = (byte)(dstCurr[4] & 0);
                if (dtail >= 4) dstCurr[3] = (byte)(dstCurr[3] & 0);
                if (dtail >= 3) dstCurr[2] = (byte)(dstCurr[2] & 0);
                if (dtail >= 2) dstCurr[1] = (byte)(dstCurr[1] & 0);
                if (dtail >= 1) dstCurr[0] = (byte)(dstCurr[0] & 0);
            }
            #endregion
        }

        private static void __bitop_multikey_scalar_and(byte* dstPtr, int dstLen, byte** srcStartPtrs, byte** srcEndPtrs, int srcKeyCount, int minSize)
        {
            int batchSize = 8 * 4;
            long slen = minSize;
            long stail = slen & (batchSize - 1);

            byte* dstCurr = dstPtr;
            byte* dstEnd = dstCurr + (slen - stail);
            #region scalar_4x8
            while (dstCurr < dstEnd)
            {
                long d00 = *(long*)(srcStartPtrs[0]);
                long d01 = *(long*)(srcStartPtrs[0] + 8);
                long d02 = *(long*)(srcStartPtrs[0] + 16);
                long d03 = *(long*)(srcStartPtrs[0] + 24);
                srcStartPtrs[0] += batchSize;
                for (int i = 1; i < srcKeyCount; i++)
                {
                    d00 &= *(long*)(srcStartPtrs[i]);
                    d01 &= *(long*)(srcStartPtrs[i] + 8);
                    d02 &= *(long*)(srcStartPtrs[i] + 16);
                    d03 &= *(long*)(srcStartPtrs[i] + 24);
                    srcStartPtrs[i] += batchSize;
                }

                *(long*)dstCurr = d00;
                *(long*)(dstCurr + 8) = d01;
                *(long*)(dstCurr + 16) = d02;
                *(long*)(dstCurr + 24) = d03;
                dstCurr += batchSize;
            }
            if (stail == 0) goto fillTail;
            #endregion

            #region scalar_1x8
            slen = stail;
            batchSize = 8;
            stail = slen & (batchSize - 1);
            dstEnd = dstCurr + (slen - stail);
            while (dstCurr < dstEnd)
            {
                long d00 = *(long*)(srcStartPtrs[0]);
                srcStartPtrs[0] += batchSize;
                for (int i = 1; i < srcKeyCount; i++)
                {
                    d00 &= *(long*)(srcStartPtrs[i]);
                    srcStartPtrs[i] += batchSize;
                }
                *(long*)dstCurr = d00;
                dstCurr += batchSize;
            }
        #endregion

        fillTail:
            #region scalar_1x1    
            byte* dstMaxEnd = dstPtr + dstLen;
            int offset = 0;
            while (dstCurr < dstMaxEnd)
            {
                byte d00;
                if (srcStartPtrs[0] + offset < srcEndPtrs[0])
                    d00 = srcStartPtrs[0][offset];
                else
                {
                    d00 = 0;
                    goto writeBack;
                }

                for (int i = 1; i < srcKeyCount; i++)
                {
                    if (srcStartPtrs[i] + offset < srcEndPtrs[i])
                        d00 &= srcStartPtrs[i][offset];
                    else
                    {
                        d00 = 0;
                        goto writeBack;
                    }
                }
            writeBack:
                *dstCurr++ = d00;
                offset++;
            }
            #endregion
        }

        private static void __bitop_multikey_simdX128_and(byte* dstPtr, int dstLen, byte** srcStartPtrs, byte** srcEndPtrs, int srcKeyCount, int minSize)
        {
            int batchSize = 8 * 16;
            long slen = minSize;
            long stail = slen & (batchSize - 1);

            byte* dstCurr = dstPtr;
            byte* dstEnd = dstCurr + (slen - stail);

            Vector128<byte> dv00;
            Vector128<byte> dv01;
            Vector128<byte> dv02;
            Vector128<byte> dv03;
            Vector128<byte> dv04;
            Vector128<byte> dv05;
            Vector128<byte> dv06;
            Vector128<byte> dv07;

            Vector128<byte> sv00;
            Vector128<byte> sv01;
            Vector128<byte> sv02;
            Vector128<byte> sv03;
            Vector128<byte> sv04;
            Vector128<byte> sv05;
            Vector128<byte> sv06;
            Vector128<byte> sv07;

            #region 8x16            
            while (dstCurr < dstEnd)
            {
#if ALIGNED_PTR
                dv00 = Sse2.LoadAlignedVector128(srcStartPtrs[0]);
                dv01 = Sse2.LoadAlignedVector128(srcStartPtrs[0] + 16);
                dv02 = Sse2.LoadAlignedVector128(srcStartPtrs[0] + 32);
                dv03 = Sse2.LoadAlignedVector128(srcStartPtrs[0] + 48);
                dv04 = Sse2.LoadAlignedVector128(srcStartPtrs[0] + 64);
                dv05 = Sse2.LoadAlignedVector128(srcStartPtrs[0] + 80);
                dv06 = Sse2.LoadAlignedVector128(srcStartPtrs[0] + 96);
                dv07 = Sse2.LoadAlignedVector128(srcStartPtrs[0] + 112);
                srcStartPtrs[0] += batchSize;
                for (int i = 1; i < srcKeyCount; i++)
                {
                    sv00 = Sse2.LoadAlignedVector128(srcStartPtrs[i]);
                    sv01 = Sse2.LoadAlignedVector128(srcStartPtrs[i] + 16);
                    sv02 = Sse2.LoadAlignedVector128(srcStartPtrs[i] + 32);
                    sv03 = Sse2.LoadAlignedVector128(srcStartPtrs[i] + 48);
                    sv04 = Sse2.LoadAlignedVector128(srcStartPtrs[i] + 64);
                    sv05 = Sse2.LoadAlignedVector128(srcStartPtrs[i] + 80);
                    sv06 = Sse2.LoadAlignedVector128(srcStartPtrs[i] + 96);
                    sv07 = Sse2.LoadAlignedVector128(srcStartPtrs[i] + 112);

                    dv00 = Sse2.And(dv00, sv00);
                    dv01 = Sse2.And(dv01, sv01);
                    dv02 = Sse2.And(dv02, sv02);
                    dv03 = Sse2.And(dv03, sv03);
                    dv04 = Sse2.And(dv04, sv04);
                    dv05 = Sse2.And(dv05, sv05);
                    dv06 = Sse2.And(dv06, sv06);
                    dv07 = Sse2.And(dv07, sv07);
                    srcStartPtrs[i] += batchSize;
                }

                Sse2.StoreAligned(dstCurr, dv00);
                Sse2.StoreAligned(dstCurr + 16, dv01);
                Sse2.StoreAligned(dstCurr + 32, dv02);
                Sse2.StoreAligned(dstCurr + 48, dv03);
                Sse2.StoreAligned(dstCurr + 64, dv04);
                Sse2.StoreAligned(dstCurr + 80, dv05);
                Sse2.StoreAligned(dstCurr + 96, dv06);
                Sse2.StoreAligned(dstCurr + 112, dv07);
#else
                Vector128<byte> d00 = Sse42.LoadVector128(srcStartPtrs[0]);
                Vector128<byte> d01 = Sse42.LoadVector128(srcStartPtrs[0] + 16);
                Vector128<byte> d02 = Sse42.LoadVector128(srcStartPtrs[0] + 32);
                Vector128<byte> d03 = Sse42.LoadVector128(srcStartPtrs[0] + 48);
                Vector128<byte> d04 = Sse42.LoadVector128(srcStartPtrs[0] + 64);
                Vector128<byte> d05 = Sse42.LoadVector128(srcStartPtrs[0] + 80);
                Vector128<byte> d06 = Sse42.LoadVector128(srcStartPtrs[0] + 96);
                Vector128<byte> d07 = Sse42.LoadVector128(srcStartPtrs[0] + 112);
                srcStartPtrs[0] += batchSize;
                for (int i = 1; i < srcKeyCount; i++)
                {
                    Vector128<byte> s00 = Sse42.LoadVector128(srcStartPtrs[i]);
                    Vector128<byte> s01 = Sse42.LoadVector128(srcStartPtrs[i] + 16);
                    Vector128<byte> s02 = Sse42.LoadVector128(srcStartPtrs[i] + 32);
                    Vector128<byte> s03 = Sse42.LoadVector128(srcStartPtrs[i] + 48);
                    Vector128<byte> s04 = Sse42.LoadVector128(srcStartPtrs[i] + 64);
                    Vector128<byte> s05 = Sse42.LoadVector128(srcStartPtrs[i] + 80);
                    Vector128<byte> s06 = Sse42.LoadVector128(srcStartPtrs[i] + 96);
                    Vector128<byte> s07 = Sse42.LoadVector128(srcStartPtrs[i] + 112);

                    d00 = Sse42.And(d00, s00);
                    d01 = Sse42.And(d01, s01);
                    d02 = Sse42.And(d02, s02);
                    d03 = Sse42.And(d03, s03);
                    d04 = Sse42.And(d04, s04);
                    d05 = Sse42.And(d05, s05);
                    d06 = Sse42.And(d06, s06);
                    d07 = Sse42.And(d07, s07);
                    srcStartPtrs[i] += batchSize;
                }

                Sse42.Store(dstCurr, d00);
                Sse42.Store(dstCurr + 16, d01);
                Sse42.Store(dstCurr + 32, d02);
                Sse42.Store(dstCurr + 48, d03);
                Sse42.Store(dstCurr + 64, d04);
                Sse42.Store(dstCurr + 80, d05);
                Sse42.Store(dstCurr + 96, d06);
                Sse42.Store(dstCurr + 112, d07);
#endif

                dstCurr += batchSize;
            }
            #endregion            

            #region 1x16
            slen = stail;
            batchSize = 1 * 16;
            stail = slen & (batchSize - 1);
            dstEnd = dstCurr + (slen - stail);

            while (dstCurr < dstEnd)
            {
                dv00 = Sse2.LoadVector128(srcStartPtrs[0]);
                srcStartPtrs[0] += batchSize;
                for (int i = 1; i < srcKeyCount; i++)
                {
                    sv00 = Sse2.LoadVector128(srcStartPtrs[i]);
                    dv00 = Sse2.And(dv00, sv00);
                    srcStartPtrs[i] += batchSize;
                }
                Sse2.Store(dstCurr, dv00);
                dstCurr += batchSize;
            }
            #endregion

            #region scalar_4x8
            slen = stail;
            batchSize = 4 * 8;
            stail = slen & (batchSize - 1);
            dstEnd = dstCurr + (slen - stail);
            while (dstCurr < dstEnd)
            {
                long d00 = *(long*)(srcStartPtrs[0]);
                long d01 = *(long*)(srcStartPtrs[0] + 8);
                long d02 = *(long*)(srcStartPtrs[0] + 16);
                long d03 = *(long*)(srcStartPtrs[0] + 24);
                srcStartPtrs[0] += batchSize;
                for (int i = 1; i < srcKeyCount; i++)
                {
                    d00 &= *(long*)(srcStartPtrs[i]);
                    d01 &= *(long*)(srcStartPtrs[i] + 8);
                    d02 &= *(long*)(srcStartPtrs[i] + 16);
                    d03 &= *(long*)(srcStartPtrs[i] + 24);
                    srcStartPtrs[i] += batchSize;
                }

                *(long*)dstCurr = d00;
                *(long*)(dstCurr + 8) = d01;
                *(long*)(dstCurr + 16) = d02;
                *(long*)(dstCurr + 24) = d03;
                dstCurr += batchSize;
            }
            if (stail == 0) goto fillTail;
            #endregion  

            #region scalar_1x8
            slen = stail;
            batchSize = 8;
            stail = slen & (batchSize - 1);
            dstEnd = dstCurr + (slen - stail);
            while (dstCurr < dstEnd)
            {
                long d00 = *(long*)(srcStartPtrs[0]);
                srcStartPtrs[0] += batchSize;
                for (int i = 1; i < srcKeyCount; i++)
                {
                    d00 &= *(long*)(srcStartPtrs[i]);
                    srcStartPtrs[i] += batchSize;
                }
                *(long*)dstCurr = d00;
                dstCurr += batchSize;
            }
        #endregion

        fillTail:
            #region scalar_1x1    
            byte* dstMaxEnd = dstPtr + dstLen;
            int offset = 0;
            while (dstCurr < dstMaxEnd)
            {
                byte d00;
                if (srcStartPtrs[0] + offset < srcEndPtrs[0])
                    d00 = srcStartPtrs[0][offset];
                else
                {
                    d00 = 0;
                    goto writeBack;
                }

                for (int i = 1; i < srcKeyCount; i++)
                {
                    if (srcStartPtrs[i] + offset < srcEndPtrs[i])
                        d00 &= srcStartPtrs[i][offset];
                    else
                    {
                        d00 = 0;
                        goto writeBack;
                    }
                }
            writeBack:
                *dstCurr++ = d00;
                offset++;
            }
            #endregion
        }

        private static void __bitop_multikey_simdX256_and(byte* dstPtr, int dstLen, byte** srcStartPtrs, byte** srcEndPtrs, int srcKeyCount, int minSize)
        {
            int batchSize = 8 * 32;
            long slen = minSize;
            long stail = slen & (batchSize - 1);

            byte* dstCurr = dstPtr;
            byte* dstEnd = dstCurr + (slen - stail);

            #region 8x32      
            Vector256<byte> dv00;
            Vector256<byte> dv01;
            Vector256<byte> dv02;
            Vector256<byte> dv03;
            Vector256<byte> dv04;
            Vector256<byte> dv05;
            Vector256<byte> dv06;
            Vector256<byte> dv07;

            Vector256<byte> sv00;
            Vector256<byte> sv01;
            Vector256<byte> sv02;
            Vector256<byte> sv03;
            Vector256<byte> sv04;
            Vector256<byte> sv05;
            Vector256<byte> sv06;
            Vector256<byte> sv07;

            while (dstCurr < dstEnd)
            {
#if ALIGNED_PTR
                dv00 = Avx.LoadAlignedVector256(srcStartPtrs[0]);
                dv01 = Avx.LoadAlignedVector256(srcStartPtrs[0] + 32);
                dv02 = Avx.LoadAlignedVector256(srcStartPtrs[0] + 64);
                dv03 = Avx.LoadAlignedVector256(srcStartPtrs[0] + 96);
                dv04 = Avx.LoadAlignedVector256(srcStartPtrs[0] + 128);
                dv05 = Avx.LoadAlignedVector256(srcStartPtrs[0] + 160);
                dv06 = Avx.LoadAlignedVector256(srcStartPtrs[0] + 192);
                dv07 = Avx.LoadAlignedVector256(srcStartPtrs[0] + 224);
                srcStartPtrs[0] += batchSize;
                for (int i = 1; i < srcKeyCount; i++)
                {
                    sv00 = Avx.LoadAlignedVector256(srcStartPtrs[i]);
                    sv01 = Avx.LoadAlignedVector256(srcStartPtrs[i] + 32);
                    sv02 = Avx.LoadAlignedVector256(srcStartPtrs[i] + 64);
                    sv03 = Avx.LoadAlignedVector256(srcStartPtrs[i] + 96);
                    sv04 = Avx.LoadAlignedVector256(srcStartPtrs[i] + 128);
                    sv05 = Avx.LoadAlignedVector256(srcStartPtrs[i] + 160);
                    sv06 = Avx.LoadAlignedVector256(srcStartPtrs[i] + 192);
                    sv07 = Avx.LoadAlignedVector256(srcStartPtrs[i] + 224);

                    dv00 = Avx2.And(dv00, sv00);
                    dv01 = Avx2.And(dv01, sv01);
                    dv02 = Avx2.And(dv02, sv02);
                    dv03 = Avx2.And(dv03, sv03);
                    dv04 = Avx2.And(dv04, sv04);
                    dv05 = Avx2.And(dv05, sv05);
                    dv06 = Avx2.And(dv06, sv06);
                    dv07 = Avx2.And(dv07, sv07);
                    srcStartPtrs[i] += batchSize;
                }

                Avx.StoreAligned(dstCurr, dv00);
                Avx.StoreAligned(dstCurr + 32, dv01);
                Avx.StoreAligned(dstCurr + 64, dv02);
                Avx.StoreAligned(dstCurr + 96, dv03);
                Avx.StoreAligned(dstCurr + 128, dv04);
                Avx.StoreAligned(dstCurr + 160, dv05);
                Avx.StoreAligned(dstCurr + 192, dv06);
                Avx.StoreAligned(dstCurr + 224, dv07);
#else
                Vector256<byte> d00 = Avx2.LoadVector256(srcStartPtrs[0]);
                Vector256<byte> d01 = Avx2.LoadVector256(srcStartPtrs[0] + 32);
                Vector256<byte> d02 = Avx2.LoadVector256(srcStartPtrs[0] + 64);
                Vector256<byte> d03 = Avx2.LoadVector256(srcStartPtrs[0] + 96);
                Vector256<byte> d04 = Avx2.LoadVector256(srcStartPtrs[0] + 128);
                Vector256<byte> d05 = Avx2.LoadVector256(srcStartPtrs[0] + 160);
                Vector256<byte> d06 = Avx2.LoadVector256(srcStartPtrs[0] + 192);
                Vector256<byte> d07 = Avx2.LoadVector256(srcStartPtrs[0] + 224);
                srcStartPtrs[0] += batchSize;
                for (int i = 1; i < srcKeyCount; i++)
                {
                    Vector256<byte> s00 = Avx2.LoadVector256(srcStartPtrs[i]);
                    Vector256<byte> s01 = Avx2.LoadVector256(srcStartPtrs[i] + 32);
                    Vector256<byte> s02 = Avx2.LoadVector256(srcStartPtrs[i] + 64);
                    Vector256<byte> s03 = Avx2.LoadVector256(srcStartPtrs[i] + 96);
                    Vector256<byte> s04 = Avx2.LoadVector256(srcStartPtrs[i] + 128);
                    Vector256<byte> s05 = Avx2.LoadVector256(srcStartPtrs[i] + 160);
                    Vector256<byte> s06 = Avx2.LoadVector256(srcStartPtrs[i] + 192);
                    Vector256<byte> s07 = Avx2.LoadVector256(srcStartPtrs[i] + 224);

                    d00 = Avx2.And(d00, s00);
                    d01 = Avx2.And(d01, s01);
                    d02 = Avx2.And(d02, s02);
                    d03 = Avx2.And(d03, s03);
                    d04 = Avx2.And(d04, s04);
                    d05 = Avx2.And(d05, s05);
                    d06 = Avx2.And(d06, s06);
                    d07 = Avx2.And(d07, s07);
                    srcStartPtrs[i] += batchSize;
                }

                Avx2.Store(dstCurr, d00);
                Avx2.Store(dstCurr + 32, d01);
                Avx2.Store(dstCurr + 64, d02);
                Avx2.Store(dstCurr + 96, d03);
                Avx2.Store(dstCurr + 128, d04);
                Avx2.Store(dstCurr + 160, d05);
                Avx2.Store(dstCurr + 192, d06);
                Avx2.Store(dstCurr + 224, d07);
#endif

                dstCurr += batchSize;
            }
            if (stail == 0) goto fillTail;
            #endregion

            #region 1x32
            slen = stail;
            batchSize = 1 * 32;
            stail = slen & (batchSize - 1);
            dstEnd = dstCurr + (slen - stail);

            while (dstCurr < dstEnd)
            {
                dv00 = Avx.LoadVector256(srcStartPtrs[0]);
                srcStartPtrs[0] += batchSize;
                for (int i = 1; i < srcKeyCount; i++)
                {
                    sv00 = Avx.LoadVector256(srcStartPtrs[i]);
                    dv00 = Avx2.And(dv00, sv00);
                    srcStartPtrs[i] += batchSize;
                }
                Avx.Store(dstCurr, dv00);
                dstCurr += batchSize;
            }
            if (stail == 0) goto fillTail;
            #endregion

            #region scalar_4x8
            slen = stail;
            batchSize = 4 * 8;
            stail = slen & (batchSize - 1);
            dstEnd = dstCurr + (slen - stail);
            while (dstCurr < dstEnd)
            {
                long d00 = *(long*)(srcStartPtrs[0]);
                long d01 = *(long*)(srcStartPtrs[0] + 8);
                long d02 = *(long*)(srcStartPtrs[0] + 16);
                long d03 = *(long*)(srcStartPtrs[0] + 24);
                srcStartPtrs[0] += batchSize;
                for (int i = 1; i < srcKeyCount; i++)
                {
                    d00 &= *(long*)(srcStartPtrs[i]);
                    d01 &= *(long*)(srcStartPtrs[i] + 8);
                    d02 &= *(long*)(srcStartPtrs[i] + 16);
                    d03 &= *(long*)(srcStartPtrs[i] + 24);
                    srcStartPtrs[i] += batchSize;
                }

                *(long*)dstCurr = d00;
                *(long*)(dstCurr + 8) = d01;
                *(long*)(dstCurr + 16) = d02;
                *(long*)(dstCurr + 24) = d03;
                dstCurr += batchSize;
            }
            if (stail == 0) goto fillTail;
            #endregion

            #region scalar_1x8
            slen = stail;
            batchSize = 8;
            stail = slen & (batchSize - 1);
            dstEnd = dstCurr + (slen - stail);
            while (dstCurr < dstEnd)
            {
                long d00 = *(long*)(srcStartPtrs[0]);
                srcStartPtrs[0] += batchSize;
                for (int i = 1; i < srcKeyCount; i++)
                {
                    d00 &= *(long*)(srcStartPtrs[i]);
                    srcStartPtrs[i] += batchSize;
                }
                *(long*)dstCurr = d00;
                dstCurr += batchSize;
            }
        #endregion

        fillTail:
            #region scalar_1x1    
            byte* dstMaxEnd = dstPtr + dstLen;
            int offset = 0;
            while (dstCurr < dstMaxEnd)
            {
                byte d00;
                if (srcStartPtrs[0] + offset < srcEndPtrs[0])
                    d00 = srcStartPtrs[0][offset];
                else
                {
                    d00 = 0;
                    goto writeBack;
                }

                for (int i = 1; i < srcKeyCount; i++)
                {
                    if (srcStartPtrs[i] + offset < srcEndPtrs[i])
                        d00 &= srcStartPtrs[i][offset];
                    else
                    {
                        d00 = 0;
                        goto writeBack;
                    }
                }
            writeBack:
                *dstCurr++ = d00;
                offset++;
            }
            #endregion
        }

        private static void RunBitOpScalarBenchV3(int bitmapLen, int iter, int batchSize)
        {
            Stopwatch swatch;
            TimeSpan tspan = new TimeSpan(0);
            Random r = new Random(1817245613);
            byte* D = alloc_aligned(bitmapLen, 32);

            Console.WriteLine("\nRunning RunBitOpScalarBenchV2...");
            for (int j = 0; j < iter; j++)
            {

                byte* dstAlignedPtr = sectorAlignedMemoryBuffers[r.Next(0, batchSize)].GetValidPointer();
                byte* srcAlignedPtr = sectorAlignedMemoryBuffers[r.Next(0, batchSize)].GetValidPointer();

                swatch = Stopwatch.StartNew();
                Buffer.MemoryCopy(dstAlignedPtr, D, bitmapLen, bitmapLen);
                __bitop_scalar_and(D, bitmapLen, srcAlignedPtr, bitmapLen);

                swatch.Stop();
                tspan = tspan.Add(swatch.Elapsed);
            }
            dispose_aligned();
            Console.WriteLine("RunBitOpScalarBenchV2 Completed!!!");
            Common.PrintElapsedTime(tspan, iter, bitmapLen * 3L, "RunBitOpScalarBenchV2");
        }

        private static void Run_SIMDx128V3(int bitmapLen, int iter, int batchSize)
        {
            Stopwatch swatch;
            TimeSpan tspan = new TimeSpan(0);
            Random r = new Random(1817245613);
            byte* D = alloc_aligned(bitmapLen, 32);

            Console.WriteLine("\nRunning SIMDx128...");
            for (int j = 0; j < iter; j++)
            {

                byte* dstAlignedPtr = sectorAlignedMemoryBuffers[r.Next(0, batchSize)].GetValidPointer();
                byte* srcAlignedPtr = sectorAlignedMemoryBuffers[r.Next(0, batchSize)].GetValidPointer();

                swatch = Stopwatch.StartNew();
                Buffer.MemoryCopy(dstAlignedPtr, D, bitmapLen, bitmapLen);
                __bitop_simdX128_and(D, bitmapLen, srcAlignedPtr, bitmapLen);

                swatch.Stop();
                tspan = tspan.Add(swatch.Elapsed);
            }
            dispose_aligned();
            Console.WriteLine("SIMDx128 Completed!!!");
            Common.PrintElapsedTime(tspan, iter, bitmapLen * 3L, "SIMDx128");
        }

        private static void Run_SIMDx256V3(int bitmapLen, int iter, int batchSize)
        {
            Stopwatch swatch;
            TimeSpan tspan = new TimeSpan(0);
            Random r = new Random(1817245613);
            byte* D = alloc_aligned(bitmapLen, 32);

            Console.WriteLine("\nRunning SIMDx256...");
            for (int j = 0; j < iter; j++)
            {

                byte* dstAlignedPtr = sectorAlignedMemoryBuffers[r.Next(0, batchSize)].GetValidPointer();
                byte* srcAlignedPtr = sectorAlignedMemoryBuffers[r.Next(0, batchSize)].GetValidPointer();

                swatch = Stopwatch.StartNew();
                Buffer.MemoryCopy(dstAlignedPtr, D, bitmapLen, bitmapLen);
                __bitop_simdX256_and(D, bitmapLen, srcAlignedPtr, bitmapLen);

                swatch.Stop();
                tspan = tspan.Add(swatch.Elapsed);
            }
            dispose_aligned();
            Console.WriteLine("SIMDx256 Completed!!!");
            Common.PrintElapsedTime(tspan, iter, bitmapLen * 3L, "SIMDx256");
        }

        private static void Run_mWayMergeScalarV3(int bitmapLen, int iter, int batchSize)
        {
            Stopwatch swatch;
            TimeSpan tspan = new TimeSpan(0);
            Random r = new Random(Guid.NewGuid().GetHashCode());

            byte** srcBitmapStartPtrs = stackalloc byte*[2];
            byte** srcBitmapEndPtrs = stackalloc byte*[2];
            byte* D = alloc_aligned(bitmapLen, 32);

            Console.WriteLine("\nRunning mWayMergeScalar...");
            for (int j = 0; j < iter; j++)
            {
                byte* src1AlignedPtr = sectorAlignedMemoryBuffers[r.Next(0, batchSize)].GetValidPointer();
                byte* src2AlignedPtr = sectorAlignedMemoryBuffers[r.Next(0, batchSize)].GetValidPointer();

                srcBitmapStartPtrs[0] = src1AlignedPtr;
                srcBitmapEndPtrs[0] = src1AlignedPtr + bitmapLen;
                srcBitmapStartPtrs[1] = src2AlignedPtr;
                srcBitmapEndPtrs[1] = src2AlignedPtr + bitmapLen;

                swatch = Stopwatch.StartNew();
                __bitop_multikey_scalar_and(D, bitmapLen, srcBitmapStartPtrs, srcBitmapEndPtrs, 2, bitmapLen);
                swatch.Stop();
                tspan = tspan.Add(swatch.Elapsed);
            }
            dispose_aligned();
            Console.WriteLine("mWayMergeScalar Completed!!!");
            Common.PrintElapsedTime(tspan, iter, bitmapLen * 3L, "mWayMergeScalar");
        }

        private static void Run_mWayMergeSIMDx128V3(int bitmapLen, int iter, int batchSize)
        {
            Stopwatch swatch;
            TimeSpan tspan = new TimeSpan(0);
            Random r = new Random(Guid.NewGuid().GetHashCode());

            byte** srcBitmapStartPtrs = stackalloc byte*[2];
            byte** srcBitmapEndPtrs = stackalloc byte*[2];
            byte[] dstBuffer = new byte[bitmapLen];
#if !ALLOC_BUFFER_ON_THE_FLY
            byte* D = alloc_aligned(bitmapLen, 32);
#endif

            Console.WriteLine("\nRunning mWayMergeSIMDx128...");
            for (int j = 0; j < iter; j++)
            {
                byte* src1AlignedPtr = sectorAlignedMemoryBuffers[r.Next(0, batchSize)].GetValidPointer();
                byte* src2AlignedPtr = sectorAlignedMemoryBuffers[r.Next(0, batchSize)].GetValidPointer();

                srcBitmapStartPtrs[0] = src1AlignedPtr;
                srcBitmapEndPtrs[0] = src1AlignedPtr + bitmapLen;
                srcBitmapStartPtrs[1] = src2AlignedPtr;
                srcBitmapEndPtrs[1] = src2AlignedPtr + bitmapLen;

                swatch = Stopwatch.StartNew();
                __bitop_multikey_simdX128_and(D, bitmapLen, srcBitmapStartPtrs, srcBitmapEndPtrs, 2, bitmapLen);
                swatch.Stop();
                tspan = tspan.Add(swatch.Elapsed);
            }
            dispose_aligned();
            Console.WriteLine("mWayMergeSIMDx128 Completed!!!");
            Common.PrintElapsedTime(tspan, iter, bitmapLen * 3L, "mWayMergeSIMDx128");
        }

        private static void Run_mWayMergeSIMDx256V3(int bitmapLen, int iter, int batchSize)
        {
            Stopwatch swatch;
            TimeSpan tspan = new TimeSpan(0);
            Random r = new Random(Guid.NewGuid().GetHashCode());

            byte** srcBitmapStartPtrs = stackalloc byte*[2];
            byte** srcBitmapEndPtrs = stackalloc byte*[2];
            byte[] dstBuffer = new byte[bitmapLen];
#if !ALLOC_BUFFER_ON_THE_FLY
            byte* D = alloc_aligned(bitmapLen, 32);
#endif

            Console.WriteLine("\nRunning mWayMergeSIMDx256...");
            for (int j = 0; j < iter; j++)
            {
                byte* src1AlignedPtr = sectorAlignedMemoryBuffers[r.Next(0, batchSize)].GetValidPointer();
                byte* src2AlignedPtr = sectorAlignedMemoryBuffers[r.Next(0, batchSize)].GetValidPointer();

                srcBitmapStartPtrs[0] = src1AlignedPtr;
                srcBitmapEndPtrs[0] = src1AlignedPtr + bitmapLen;
                srcBitmapStartPtrs[1] = src2AlignedPtr;
                srcBitmapEndPtrs[1] = src2AlignedPtr + bitmapLen;

                swatch = Stopwatch.StartNew();
                __bitop_multikey_simdX256_and(D, bitmapLen, srcBitmapStartPtrs, srcBitmapEndPtrs, 2, bitmapLen);
                swatch.Stop();
                tspan = tspan.Add(swatch.Elapsed);
            }
            dispose_aligned();
            Console.WriteLine("mWayMergeSIMDx256 Completed!!!");
            Common.PrintElapsedTime(tspan, iter, bitmapLen * 3L, "mWayMergeSIMDx256");
        }

        public static void RunBitOpBenchmark()
        {
            Random r = new Random(1817245613);
            int keys = 1 << 8;
            int workIter = 1 << 15;
            int bitmapLen = 1 << 20;

            InitializeInputAlignedMemoryPool(keys, bitmapLen, 32);
            InitializeInputUnalignedMemoryPool(keys, bitmapLen);

            RunBitOpScalarBenchV3(bitmapLen, workIter, keys);
            Run_SIMDx128V3(bitmapLen, workIter, keys);
            Run_SIMDx256V3(bitmapLen, workIter, keys);
            Run_mWayMergeScalarV3(bitmapLen, workIter, keys);
            Run_mWayMergeSIMDx128V3(bitmapLen, workIter, keys);
            Run_mWayMergeSIMDx256V3(bitmapLen, workIter, keys);

            DisposeInputAlignedMemoryPool();

        }
    }
}