// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Tsavorite.core;

namespace Bitmap
{
    public static unsafe class BitCount
    {
        static SectorAlignedMemory alignedMemory = null;

        private static byte* alloc_aligned(int numRecords, int sectorSize)
        {
            alignedMemory = new SectorAlignedMemory(numRecords, sectorSize);
            return alignedMemory.GetValidPointer();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long BitCountScalar(byte* value, int startOffset, int endOffset)
        {
            ulong count = 0;
            int len = (endOffset - startOffset);
            byte* curr = value + startOffset;
            byte* end = curr + (len - (len & 7));

            //TODO: unroll further
            while (curr < end)
            {
                ulong v = *(ulong*)(curr);
                count += Popcnt.X64.PopCount(v);
                curr += 8;
            }

            //Process tail of bitmap
            ulong tail = 0;
            int tsize = len & 7;
            if (tsize >= 7) tail |= (ulong)(((ulong)curr[6]) << 48);
            if (tsize >= 6) tail |= (ulong)(((ulong)curr[5]) << 40);
            if (tsize >= 5) tail |= (ulong)(((ulong)curr[4]) << 32);
            if (tsize >= 4) tail |= (ulong)(((ulong)curr[3]) << 24);
            if (tsize >= 3) tail |= (ulong)(((ulong)curr[2]) << 16);
            if (tsize >= 2) tail |= (ulong)(((ulong)curr[1]) << 8);
            if (tsize >= 1) tail |= (ulong)(((ulong)curr[0]));

            count += Popcnt.X64.PopCount(tail);
            return (long)count;
        }

        private static long __scalar_popc(byte* bitmap, int start, int end)
        {
            ulong count = 0;
            int batchSize = 8 * 4;
            int len = (end - start);
            int tail = len & (batchSize - 1);
            byte* curr = bitmap + start;
            byte* vend = curr + (len - (len & tail));

            #region popc_4x8
            while (curr < vend)
            {
                ulong v00 = Popcnt.X64.PopCount(*(ulong*)(curr));
                ulong v01 = Popcnt.X64.PopCount(*(ulong*)(curr + 8));
                ulong v02 = Popcnt.X64.PopCount(*(ulong*)(curr + 16));
                ulong v03 = Popcnt.X64.PopCount(*(ulong*)(curr + 24));

                v00 = v00 + v01;
                v02 = v02 + v03;
                count += v00 + v02;
                curr += batchSize;
            }
            #endregion

            #region popc_1x8
            len = tail;
            batchSize = 8;
            tail = len & (batchSize - 1);
            vend = curr + (len - (len & tail));
            while (curr < vend)
            {
                count += Popcnt.X64.PopCount(*(ulong*)(curr));
                curr += batchSize;
            }

            ulong tt = 0;
            if (tail >= 7) tt |= (ulong)(((ulong)curr[6]) << 48);
            if (tail >= 6) tt |= (ulong)(((ulong)curr[5]) << 40);
            if (tail >= 5) tt |= (ulong)(((ulong)curr[4]) << 32);
            if (tail >= 4) tt |= (ulong)(((ulong)curr[3]) << 24);
            if (tail >= 3) tt |= (ulong)(((ulong)curr[2]) << 16);
            if (tail >= 2) tt |= (ulong)(((ulong)curr[1]) << 8);
            if (tail >= 1) tt |= (ulong)(((ulong)curr[0]));

            count += Popcnt.X64.PopCount(tt);
            #endregion

            return (long)count;
        }

        private static long __simd_popcX128(byte* bitmap, int start, int end)
        {
            ulong count = 0;
            int batchSize = 8 * 16;
            int len = (end - start);
            int tail = len & (batchSize - 1);
            byte* curr = bitmap + start;
            byte* vend = curr + (len - (len & tail));

            Vector128<byte> lookupCountSIMDx128 = Vector128.Create(
                    (byte)0, /*0*/ (byte)1, /*1*/ (byte)1, /*2*/(byte)2, /*3*/(byte)1, /*4*/ (byte)2, /*5*/ (byte)2, /*6*/ (byte)3, /*7*/
                    (byte)1, /*8*/ (byte)2, /*9*/ (byte)2, /*10*/(byte)3, /*11*/ (byte)2, /*12*/ (byte)3, /*13*/ (byte)3, /*14*/ (byte)4  /*15*/);

            Vector128<byte> mskSIMDx128 = Vector128.Create((byte)0x0f);
            Vector128<long> acc = Vector128<long>.Zero;

            #region popc_8x16
            while (curr < vend)
            {
                //combine local population counts                
                Vector128<byte> xmm00 = Sse2.LoadVector128(curr);//0
                Vector128<byte> xmm01 = Sse2.LoadVector128(curr + 16);//1
                Vector128<byte> xmm02 = Sse2.LoadVector128(curr + 32);//2
                Vector128<byte> xmm03 = Sse2.LoadVector128(curr + 48);//3
                Vector128<byte> xmm04 = Sse2.LoadVector128(curr + 64);//4
                Vector128<byte> xmm05 = Sse2.LoadVector128(curr + 80);//5
                Vector128<byte> xmm06 = Sse2.LoadVector128(curr + 96);//6
                Vector128<byte> xmm07 = Sse2.LoadVector128(curr + 112);//7

                Vector128<byte> popc00 = Ssse3.Shuffle(lookupCountSIMDx128, Sse2.And(xmm00, mskSIMDx128));
                Vector128<byte> popc01 = Ssse3.Shuffle(lookupCountSIMDx128, Sse2.And(xmm01, mskSIMDx128));
                Vector128<byte> popc02 = Ssse3.Shuffle(lookupCountSIMDx128, Sse2.And(xmm02, mskSIMDx128));
                Vector128<byte> popc03 = Ssse3.Shuffle(lookupCountSIMDx128, Sse2.And(xmm03, mskSIMDx128));
                Vector128<byte> popc04 = Ssse3.Shuffle(lookupCountSIMDx128, Sse2.And(xmm04, mskSIMDx128));
                Vector128<byte> popc05 = Ssse3.Shuffle(lookupCountSIMDx128, Sse2.And(xmm05, mskSIMDx128));
                Vector128<byte> popc06 = Ssse3.Shuffle(lookupCountSIMDx128, Sse2.And(xmm06, mskSIMDx128));
                Vector128<byte> popc07 = Ssse3.Shuffle(lookupCountSIMDx128, Sse2.And(xmm07, mskSIMDx128));

                popc00 = Sse2.Add(popc00, Ssse3.Shuffle(lookupCountSIMDx128, Sse2.And(Vector128.AsByte(Sse2.ShiftRightLogical(Vector128.AsInt16(xmm00), 4)), mskSIMDx128)));
                popc01 = Sse2.Add(popc01, Ssse3.Shuffle(lookupCountSIMDx128, Sse2.And(Vector128.AsByte(Sse2.ShiftRightLogical(Vector128.AsInt16(xmm01), 4)), mskSIMDx128)));
                popc02 = Sse2.Add(popc02, Ssse3.Shuffle(lookupCountSIMDx128, Sse2.And(Vector128.AsByte(Sse2.ShiftRightLogical(Vector128.AsInt16(xmm02), 4)), mskSIMDx128)));
                popc03 = Sse2.Add(popc03, Ssse3.Shuffle(lookupCountSIMDx128, Sse2.And(Vector128.AsByte(Sse2.ShiftRightLogical(Vector128.AsInt16(xmm03), 4)), mskSIMDx128)));
                popc04 = Sse2.Add(popc04, Ssse3.Shuffle(lookupCountSIMDx128, Sse2.And(Vector128.AsByte(Sse2.ShiftRightLogical(Vector128.AsInt16(xmm04), 4)), mskSIMDx128)));
                popc05 = Sse2.Add(popc05, Ssse3.Shuffle(lookupCountSIMDx128, Sse2.And(Vector128.AsByte(Sse2.ShiftRightLogical(Vector128.AsInt16(xmm05), 4)), mskSIMDx128)));
                popc06 = Sse2.Add(popc06, Ssse3.Shuffle(lookupCountSIMDx128, Sse2.And(Vector128.AsByte(Sse2.ShiftRightLogical(Vector128.AsInt16(xmm06), 4)), mskSIMDx128)));
                popc07 = Sse2.Add(popc07, Ssse3.Shuffle(lookupCountSIMDx128, Sse2.And(Vector128.AsByte(Sse2.ShiftRightLogical(Vector128.AsInt16(xmm07), 4)), mskSIMDx128)));

                popc00 = Sse2.Add(popc00, popc01);
                popc02 = Sse2.Add(popc02, popc03);
                popc04 = Sse2.Add(popc04, popc05);
                popc06 = Sse2.Add(popc06, popc07);

                popc00 = Sse2.Add(popc00, popc02);
                popc04 = Sse2.Add(popc04, popc06);

                popc00 = Sse2.Add(popc00, popc04);

                acc = Sse2.Add(acc, Vector128.AsInt64(Sse2.SumAbsoluteDifferences(popc00, Vector128<byte>.Zero)));
                curr += batchSize;
            }
            if (tail == 0)
            {
                count += (ulong)acc.GetElement(0);
                count += (ulong)acc.GetElement(1);
                return (long)count;
            }
            #endregion

            #region popc_1x16
            len = tail;
            batchSize = 16;
            tail = len & (batchSize - 1);
            vend = curr + (len - (len & tail));
            Vector128<byte> popc = Vector128<byte>.Zero;
            while (curr < vend)
            {
                Vector128<byte> xmm00 = Sse2.LoadVector128(curr);//0
                Vector128<byte> popc00 = Ssse3.Shuffle(lookupCountSIMDx128, Sse2.And(xmm00, mskSIMDx128));
                popc00 = Sse2.Add(popc00, Ssse3.Shuffle(lookupCountSIMDx128, Sse2.And(Vector128.AsByte(Sse2.ShiftRightLogical(Vector128.AsInt16(xmm00), 4)), mskSIMDx128)));
                popc = Sse2.Add(popc00, popc);
                curr += batchSize;
            }
            acc = Sse2.Add(acc, Vector128.AsInt64(Sse2.SumAbsoluteDifferences(popc, Vector128<byte>.Zero)));
            count += (ulong)acc.GetElement(0);
            count += (ulong)acc.GetElement(1);
            if (tail == 0) return (long)count;
            #endregion

            #region popc_4x8
            len = tail;
            batchSize = 4 * 8;
            tail = len & (batchSize - 1);
            vend = curr + (len - (len & tail));
            while (curr < vend)
            {
                ulong v00 = Popcnt.X64.PopCount(*(ulong*)(curr));
                ulong v01 = Popcnt.X64.PopCount(*(ulong*)(curr + 8));
                ulong v02 = Popcnt.X64.PopCount(*(ulong*)(curr + 16));
                ulong v03 = Popcnt.X64.PopCount(*(ulong*)(curr + 24));

                v00 = v00 + v01;
                v02 = v02 + v03;
                count += v00 + v02;
                curr += batchSize;
            }
            if (tail == 0) return (long)count;
            #endregion

            #region popc_1x8
            len = tail;
            batchSize = 8;
            tail = len & (batchSize - 1);
            vend = curr + (len - (len & tail));
            while (curr < vend)
            {
                ulong v = *(ulong*)(curr);
                count += Popcnt.X64.PopCount(v);
                curr += 8;
            }
            if (tail == 0) return (long)count;

            ulong tt = 0;
            if (tail >= 7) tt |= (ulong)(((ulong)curr[6]) << 48);
            if (tail >= 6) tt |= (ulong)(((ulong)curr[5]) << 40);
            if (tail >= 5) tt |= (ulong)(((ulong)curr[4]) << 32);
            if (tail >= 4) tt |= (ulong)(((ulong)curr[3]) << 24);
            if (tail >= 3) tt |= (ulong)(((ulong)curr[2]) << 16);
            if (tail >= 2) tt |= (ulong)(((ulong)curr[1]) << 8);
            if (tail >= 1) tt |= (ulong)(((ulong)curr[0]));

            count += Popcnt.X64.PopCount(tt);
            #endregion
            return (long)count;
        }

        private static long __simd_popcX256(byte* bitmap, int start, int end)
        {
            ulong count = 0;
            int batchSize = 8 * 32;
            int len = (end - start);
            int tail = len & (batchSize - 1);
            byte* curr = bitmap + start;
            byte* vend = curr + (len - (len & tail));

            Vector256<byte> lookupCountSIMDx256 = Vector256.Create(
                    (byte)0, /*0*/ (byte)1, /*1*/ (byte)1, /*2*/(byte)2, /*3*/(byte)1, /*4*/ (byte)2, /*5*/ (byte)2, /*6*/ (byte)3, /*7*/
                    (byte)1, /*8*/ (byte)2, /*9*/ (byte)2, /*10*/(byte)3, /*11*/ (byte)2, /*12*/ (byte)3, /*13*/ (byte)3, /*14*/ (byte)4, /*15*/
                    (byte)0, /*0*/ (byte)1, /*1*/ (byte)1, /*2*/(byte)2, /*3*/(byte)1, /*4*/ (byte)2, /*5*/ (byte)2, /*6*/ (byte)3, /*7*/
                    (byte)1, /*8*/ (byte)2, /*9*/ (byte)2, /*10*/(byte)3, /*11*/ (byte)2, /*12*/ (byte)3, /*13*/ (byte)3, /*14*/ (byte)4  /*15*/);

            Vector256<byte> mskSIMDx256 = Vector256.Create((byte)0x0f);
            Vector256<long> acc = Vector256<long>.Zero;
            Vector256<ushort> sad;

            #region popc_8x32
            while (curr < vend)
            {
                Vector256<byte> xmm00 = Avx.LoadVector256(curr);//0
                Vector256<byte> xmm01 = Avx.LoadVector256(curr + 32);//1
                Vector256<byte> xmm02 = Avx.LoadVector256(curr + 64);//2
                Vector256<byte> xmm03 = Avx.LoadVector256(curr + 96);//3
                Vector256<byte> xmm04 = Avx.LoadVector256(curr + 128);//4
                Vector256<byte> xmm05 = Avx.LoadVector256(curr + 160);//5
                Vector256<byte> xmm06 = Avx.LoadVector256(curr + 192);//6
                Vector256<byte> xmm07 = Avx.LoadVector256(curr + 224);//7

                Vector256<byte> popc00 = Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(xmm00, mskSIMDx256));
                Vector256<byte> popc01 = Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(xmm01, mskSIMDx256));
                Vector256<byte> popc02 = Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(xmm02, mskSIMDx256));
                Vector256<byte> popc03 = Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(xmm03, mskSIMDx256));
                Vector256<byte> popc04 = Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(xmm04, mskSIMDx256));
                Vector256<byte> popc05 = Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(xmm05, mskSIMDx256));
                Vector256<byte> popc06 = Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(xmm06, mskSIMDx256));
                Vector256<byte> popc07 = Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(xmm07, mskSIMDx256));

                popc00 = Avx2.Add(popc00, Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(Vector256.AsByte(Avx2.ShiftRightLogical(Vector256.AsInt16(xmm00), 4)), mskSIMDx256)));
                popc01 = Avx2.Add(popc01, Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(Vector256.AsByte(Avx2.ShiftRightLogical(Vector256.AsInt16(xmm01), 4)), mskSIMDx256)));
                popc02 = Avx2.Add(popc02, Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(Vector256.AsByte(Avx2.ShiftRightLogical(Vector256.AsInt16(xmm02), 4)), mskSIMDx256)));
                popc03 = Avx2.Add(popc03, Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(Vector256.AsByte(Avx2.ShiftRightLogical(Vector256.AsInt16(xmm03), 4)), mskSIMDx256)));
                popc04 = Avx2.Add(popc04, Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(Vector256.AsByte(Avx2.ShiftRightLogical(Vector256.AsInt16(xmm04), 4)), mskSIMDx256)));
                popc05 = Avx2.Add(popc05, Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(Vector256.AsByte(Avx2.ShiftRightLogical(Vector256.AsInt16(xmm05), 4)), mskSIMDx256)));
                popc06 = Avx2.Add(popc06, Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(Vector256.AsByte(Avx2.ShiftRightLogical(Vector256.AsInt16(xmm06), 4)), mskSIMDx256)));
                popc07 = Avx2.Add(popc07, Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(Vector256.AsByte(Avx2.ShiftRightLogical(Vector256.AsInt16(xmm07), 4)), mskSIMDx256)));

                popc00 = Avx2.Add(popc00, popc01);
                popc02 = Avx2.Add(popc02, popc03);
                popc04 = Avx2.Add(popc04, popc05);
                popc06 = Avx2.Add(popc06, popc07);

                popc00 = Avx2.Add(popc00, popc02);
                popc04 = Avx2.Add(popc04, popc06);

                popc00 = Avx2.Add(popc00, popc04);

                sad = Avx2.SumAbsoluteDifferences(popc00, Vector256<byte>.Zero);
                acc = Avx2.Add(acc, Vector256.AsInt64(sad));

                curr += batchSize;
            }
            if (tail == 0)
            {
                count += (ulong)acc.GetElement(0);
                count += (ulong)acc.GetElement(1);
                count += (ulong)acc.GetElement(2);
                count += (ulong)acc.GetElement(3);
                return (long)count;
            }
            #endregion

            #region popc_1x32
            len = tail;
            batchSize = 32;
            tail = len & (batchSize - 1);
            vend = curr + (len - (len & tail));
            Vector256<byte> popc = Vector256<byte>.Zero;
            while (curr < vend)
            {
                Vector256<byte> xmm00 = Avx.LoadVector256(curr);//0
                Vector256<byte> popc00 = Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(xmm00, mskSIMDx256));
                popc00 = Avx2.Add(popc00, Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(Vector256.AsByte(Avx2.ShiftRightLogical(Vector256.AsInt16(xmm00), 4)), mskSIMDx256)));
                popc = Avx2.Add(popc00, popc);
                curr += batchSize;
            }
            sad = Avx2.SumAbsoluteDifferences(popc, Vector256<byte>.Zero);
            acc = Avx2.Add(acc, Vector256.AsInt64(sad));
            count += (ulong)acc.GetElement(0);
            count += (ulong)acc.GetElement(1);
            count += (ulong)acc.GetElement(2);
            count += (ulong)acc.GetElement(3);
            if (tail == 0) return (long)count;
            #endregion

            #region popc_4x8
            len = tail;
            batchSize = 4 * 8;
            tail = len & (batchSize - 1);
            vend = curr + (len - (len & tail));
            while (curr < vend)
            {
                ulong v00 = Popcnt.X64.PopCount(*(ulong*)(curr));
                ulong v01 = Popcnt.X64.PopCount(*(ulong*)(curr + 8));
                ulong v02 = Popcnt.X64.PopCount(*(ulong*)(curr + 16));
                ulong v03 = Popcnt.X64.PopCount(*(ulong*)(curr + 24));

                v00 = v00 + v01;
                v02 = v02 + v03;
                count += v00 + v02;
                curr += batchSize;
            }
            if (tail == 0) return (long)count;
            #endregion

            #region popc_1x8
            len = tail;
            batchSize = 8;
            tail = len & (batchSize - 1);
            vend = curr + (len - (len & tail));
            while (curr < vend)
            {
                ulong v = *(ulong*)(curr);
                count += Popcnt.X64.PopCount(v);
                curr += 8;
            }
            if (tail == 0) return (long)count;

            ulong tt = 0;
            if (tail >= 7) tt |= (ulong)(((ulong)curr[6]) << 48);
            if (tail >= 6) tt |= (ulong)(((ulong)curr[5]) << 40);
            if (tail >= 5) tt |= (ulong)(((ulong)curr[4]) << 32);
            if (tail >= 4) tt |= (ulong)(((ulong)curr[3]) << 24);
            if (tail >= 3) tt |= (ulong)(((ulong)curr[2]) << 16);
            if (tail >= 2) tt |= (ulong)(((ulong)curr[1]) << 8);
            if (tail >= 1) tt |= (ulong)(((ulong)curr[0]));

            count += Popcnt.X64.PopCount(tt);
            #endregion

            return (long)count;
        }

        private static long __simd_popcX256_aligned(byte* bitmap, int start, int end)
        {
            ulong count = 0;
            int batchSize = 8 * 32;
            int len = (end - start);
            int tail = len & (batchSize - 1);
            byte* curr = bitmap + start;
            byte* vend = curr + (len - (len & tail));

            Vector256<byte> lookupCountSIMDx256 = Vector256.Create(
                    (byte)0, /*0*/ (byte)1, /*1*/ (byte)1, /*2*/(byte)2, /*3*/(byte)1, /*4*/ (byte)2, /*5*/ (byte)2, /*6*/ (byte)3, /*7*/
                    (byte)1, /*8*/ (byte)2, /*9*/ (byte)2, /*10*/(byte)3, /*11*/ (byte)2, /*12*/ (byte)3, /*13*/ (byte)3, /*14*/ (byte)4, /*15*/
                    (byte)0, /*0*/ (byte)1, /*1*/ (byte)1, /*2*/(byte)2, /*3*/(byte)1, /*4*/ (byte)2, /*5*/ (byte)2, /*6*/ (byte)3, /*7*/
                    (byte)1, /*8*/ (byte)2, /*9*/ (byte)2, /*10*/(byte)3, /*11*/ (byte)2, /*12*/ (byte)3, /*13*/ (byte)3, /*14*/ (byte)4  /*15*/);

            Vector256<byte> mskSIMDx256 = Vector256.Create((byte)0x0f);
            Vector256<long> acc = Vector256<long>.Zero;
            Vector256<ushort> sad;

            #region popc_8x32
            while (curr < vend)
            {
                Vector256<byte> xmm00 = Avx.LoadAlignedVector256(curr);//0
                Vector256<byte> xmm01 = Avx.LoadAlignedVector256(curr + 32);//1
                Vector256<byte> xmm02 = Avx.LoadAlignedVector256(curr + 64);//2
                Vector256<byte> xmm03 = Avx.LoadAlignedVector256(curr + 96);//3
                Vector256<byte> xmm04 = Avx.LoadAlignedVector256(curr + 128);//4
                Vector256<byte> xmm05 = Avx.LoadAlignedVector256(curr + 160);//5
                Vector256<byte> xmm06 = Avx.LoadAlignedVector256(curr + 192);//6
                Vector256<byte> xmm07 = Avx.LoadAlignedVector256(curr + 224);//7

                Vector256<byte> popc00 = Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(xmm00, mskSIMDx256));
                Vector256<byte> popc01 = Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(xmm01, mskSIMDx256));
                Vector256<byte> popc02 = Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(xmm02, mskSIMDx256));
                Vector256<byte> popc03 = Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(xmm03, mskSIMDx256));
                Vector256<byte> popc04 = Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(xmm04, mskSIMDx256));
                Vector256<byte> popc05 = Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(xmm05, mskSIMDx256));
                Vector256<byte> popc06 = Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(xmm06, mskSIMDx256));
                Vector256<byte> popc07 = Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(xmm07, mskSIMDx256));

                popc00 = Avx2.Add(popc00, Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(Vector256.AsByte(Avx2.ShiftRightLogical(Vector256.AsInt16(xmm00), 4)), mskSIMDx256)));
                popc01 = Avx2.Add(popc01, Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(Vector256.AsByte(Avx2.ShiftRightLogical(Vector256.AsInt16(xmm01), 4)), mskSIMDx256)));
                popc02 = Avx2.Add(popc02, Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(Vector256.AsByte(Avx2.ShiftRightLogical(Vector256.AsInt16(xmm02), 4)), mskSIMDx256)));
                popc03 = Avx2.Add(popc03, Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(Vector256.AsByte(Avx2.ShiftRightLogical(Vector256.AsInt16(xmm03), 4)), mskSIMDx256)));
                popc04 = Avx2.Add(popc04, Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(Vector256.AsByte(Avx2.ShiftRightLogical(Vector256.AsInt16(xmm04), 4)), mskSIMDx256)));
                popc05 = Avx2.Add(popc05, Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(Vector256.AsByte(Avx2.ShiftRightLogical(Vector256.AsInt16(xmm05), 4)), mskSIMDx256)));
                popc06 = Avx2.Add(popc06, Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(Vector256.AsByte(Avx2.ShiftRightLogical(Vector256.AsInt16(xmm06), 4)), mskSIMDx256)));
                popc07 = Avx2.Add(popc07, Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(Vector256.AsByte(Avx2.ShiftRightLogical(Vector256.AsInt16(xmm07), 4)), mskSIMDx256)));

                popc00 = Avx2.Add(popc00, popc01);
                popc02 = Avx2.Add(popc02, popc03);
                popc04 = Avx2.Add(popc04, popc05);
                popc06 = Avx2.Add(popc06, popc07);

                popc00 = Avx2.Add(popc00, popc02);
                popc04 = Avx2.Add(popc04, popc06);

                popc00 = Avx2.Add(popc00, popc04);

                sad = Avx2.SumAbsoluteDifferences(popc00, Vector256<byte>.Zero);
                acc = Avx2.Add(acc, Vector256.AsInt64(sad));

                curr += batchSize;
            }
            if (tail == 0)
            {
                count += (ulong)acc.GetElement(0);
                count += (ulong)acc.GetElement(1);
                count += (ulong)acc.GetElement(2);
                count += (ulong)acc.GetElement(3);
                return (long)count;
            }
            #endregion

            #region popc_1x32
            len = tail;
            batchSize = 32;
            tail = len & (batchSize - 1);
            vend = curr + (len - (len & tail));
            Vector256<byte> popc = Vector256<byte>.Zero;
            while (curr < vend)
            {
                Vector256<byte> xmm00 = Avx.LoadAlignedVector256(curr);//0
                Vector256<byte> popc00 = Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(xmm00, mskSIMDx256));
                popc00 = Avx2.Add(popc00, Avx2.Shuffle(lookupCountSIMDx256, Avx2.And(Vector256.AsByte(Avx2.ShiftRightLogical(Vector256.AsInt16(xmm00), 4)), mskSIMDx256)));
                popc = Avx2.Add(popc00, popc);
                curr += batchSize;
            }
            sad = Avx2.SumAbsoluteDifferences(popc, Vector256<byte>.Zero);
            acc = Avx2.Add(acc, Vector256.AsInt64(sad));
            count += (ulong)acc.GetElement(0);
            count += (ulong)acc.GetElement(1);
            count += (ulong)acc.GetElement(2);
            count += (ulong)acc.GetElement(3);
            if (tail == 0) return (long)count;
            #endregion

            #region popc_4x8
            len = tail;
            batchSize = 4 * 8;
            tail = len & (batchSize - 1);
            vend = curr + (len - (len & tail));
            while (curr < vend)
            {
                ulong v00 = Popcnt.X64.PopCount(*(ulong*)(curr));
                ulong v01 = Popcnt.X64.PopCount(*(ulong*)(curr + 8));
                ulong v02 = Popcnt.X64.PopCount(*(ulong*)(curr + 16));
                ulong v03 = Popcnt.X64.PopCount(*(ulong*)(curr + 24));

                v00 = v00 + v01;
                v02 = v02 + v03;
                count += v00 + v02;
                curr += batchSize;
            }
            if (tail == 0) return (long)count;
            #endregion

            #region popc_1x8
            len = tail;
            batchSize = 8;
            tail = len & (batchSize - 1);
            vend = curr + (len - (len & tail));
            while (curr < vend)
            {
                ulong v = *(ulong*)(curr);
                count += Popcnt.X64.PopCount(v);
                curr += 8;
            }
            if (tail == 0) return (long)count;

            ulong tt = 0;
            if (tail >= 7) tt |= (ulong)(((ulong)curr[6]) << 48);
            if (tail >= 6) tt |= (ulong)(((ulong)curr[5]) << 40);
            if (tail >= 5) tt |= (ulong)(((ulong)curr[4]) << 32);
            if (tail >= 4) tt |= (ulong)(((ulong)curr[3]) << 24);
            if (tail >= 3) tt |= (ulong)(((ulong)curr[2]) << 16);
            if (tail >= 2) tt |= (ulong)(((ulong)curr[1]) << 8);
            if (tail >= 1) tt |= (ulong)(((ulong)curr[0]));

            count += Popcnt.X64.PopCount(tt);
            #endregion

            return (long)count;
        }

        private static void RunBitCountScalar(byte* ptr, int blen, int iter)
        {
            Console.WriteLine("Running BitCountScalarBench...");
            long count = 0;
            TimeSpan tspan = new TimeSpan(0);
            Stopwatch swatch = Stopwatch.StartNew();
            for (int i = 0; i < iter; i++)
            {
                count = __scalar_popc(ptr, 0, blen);
            }
            swatch.Stop();
            tspan = tspan.Add(swatch.Elapsed);

            Console.WriteLine("BitCountScalarBench Completed!!!\n");
            Console.WriteLine("popc_simd:{0}", count);
            Common.PrintElapsedTime(tspan, iter, blen, "BitCountScalarBench");
        }

        private static void RunBitCountSIMD(byte* ptr, int blen, int iter)
        {
            Console.WriteLine("Running BitCountSIMDBench...");
            long count = 0;
            TimeSpan tspan = new TimeSpan(0);
            Stopwatch swatch = Stopwatch.StartNew();
            for (int i = 0; i < iter; i++)
            {
                count = __simd_popcX128(ptr, 0, blen);
            }
            swatch.Stop();
            tspan = tspan.Add(swatch.Elapsed);

            Console.WriteLine("BitCountSIMDBench Completed!!!\n");
            Console.WriteLine("popc_simd:{0}", count);
            Common.PrintElapsedTime(tspan, iter, blen, "BitCountSIMDBench");
        }

        private static void RunBitCountSIMD_AVX(byte* ptr, int blen, int iter)
        {
            Console.WriteLine("Running RunBitCountSIMD_AVX...");
            long count = 0;
            TimeSpan tspan = new TimeSpan(0);
            Stopwatch swatch = Stopwatch.StartNew();
            for (int i = 0; i < iter; i++)
            {
                count = __simd_popcX256(ptr, 0, blen);
            }
            swatch.Stop();
            tspan = tspan.Add(swatch.Elapsed);

            Console.WriteLine("RunBitCountSIMD_AVX Completed!!!\n");
            Console.WriteLine("popc_simd:{0}", count);
            Common.PrintElapsedTime(tspan, iter, blen, "RunBitCountSIMD_AVX");
        }

        private static void RunBitCountSIMD_AVX_aligned(byte* ptr, int blen, int iter)
        {
            Console.WriteLine("Running RunBitCountSIMD_AVX_aligned...");
            long count = 0;
            TimeSpan tspan = new TimeSpan(0);
            Stopwatch swatch = Stopwatch.StartNew();
            for (int i = 0; i < iter; i++)
            {
                count = __simd_popcX256_aligned(ptr, 0, blen);
            }
            swatch.Stop();
            tspan = tspan.Add(swatch.Elapsed);

            Console.WriteLine("RunBitCountSIMD_AVX_aligned Completed!!!\n");
            Console.WriteLine("popc_simd:{0}", count);
            Common.PrintElapsedTime(tspan, iter, blen, "RunBitCountSIMD_AVX_aligned");
        }

        public static void RunBitCountBenchmark()
        {
            Random r = new Random(1817245613);
            int iter = 256;
            int bitmapLen = 1 << 28;
            byte[] bitmap = new byte[bitmapLen];

            r.NextBytes(bitmap);
            fixed (byte* ptr = bitmap)
            {
                byte* curr = ptr;

                RunBitCountScalar(curr, bitmapLen, iter);
                RunBitCountSIMD(curr, bitmapLen, iter);
                RunBitCountSIMD_AVX(curr, bitmapLen, iter);

                byte* alignedPtr = alloc_aligned(bitmapLen, 32);
                Buffer.MemoryCopy(curr, alignedPtr, bitmapLen, bitmapLen);

                curr = alignedPtr;
                RunBitCountSIMD_AVX(curr, bitmapLen, iter);

            }
            if (alignedMemory != null)
            {
                alignedMemory.Dispose();
            }
        }
    }
}