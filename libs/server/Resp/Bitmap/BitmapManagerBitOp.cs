// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Garnet.common;


namespace Garnet.server
{
    public unsafe partial class BitmapManager
    {
        /// <summary>
        /// BitOp main driver.
        /// </summary>
        /// <param name="dstPtr">Output buffer to write BitOp result</param>
        /// <param name="dstLen">Output buffer length.</param>
        /// <param name="srcStartPtrs">Array of pointers to bitmaps used as input in the corresponding bitop.</param>
        /// <param name="srcEndPtrs">Array of pointers to bitmap sources.</param>
        /// <param name="srcKeyCount">Number of source keys.</param>
        /// <param name="minSize">Minimum size of source bitmap.</param>
        /// <param name="bitop">Type of bitop operation being executed.</param>
        /// <returns></returns>
        public static bool BitOpMainUnsafeMultiKey(byte* dstPtr, int dstLen, byte** srcStartPtrs, byte** srcEndPtrs, int srcKeyCount, int minSize, byte bitop)
        {
            switch (bitop)
            {
                case (byte)BitmapOperation.NOT:
                    __bitop_multikey_simdX256_not(dstPtr, dstLen, srcStartPtrs[0], (srcEndPtrs[0] - srcStartPtrs[0]));
                    break;
                case (byte)BitmapOperation.AND:
                    __bitop_multikey_simdX256_and(dstPtr, dstLen, srcStartPtrs, srcEndPtrs, srcKeyCount, minSize);
                    break;
                case (byte)BitmapOperation.OR:
                    __bitop_multikey_simdX256_or(dstPtr, dstLen, srcStartPtrs, srcEndPtrs, srcKeyCount, minSize);
                    break;
                case (byte)BitmapOperation.XOR:
                    __bitop_multikey_simdX256_xor(dstPtr, dstLen, srcStartPtrs, srcEndPtrs, srcKeyCount, minSize);
                    break;
                default:
                    throw new GarnetException("Unsupported BitOp command");
            }
            return true;
        }

        /// <summary>
        /// Negation bitop implementation using 256-wide SIMD registers.
        /// </summary>
        /// <param name="dstPtr">Output buffer to write BitOp result</param>
        /// <param name="dstLen">Output buffer length.</param>
        /// <param name="srcBitmap">Pointer to source bitmap.</param>
        /// <param name="srcLen">Source bitmap length.</param>
        private static void __bitop_multikey_simdX256_not(byte* dstPtr, long dstLen, byte* srcBitmap, long srcLen)
        {
            int batchSize = 8 * 32;
            long slen = srcLen;
            long stail = slen & (batchSize - 1);

            //iterate using srcBitmap because always dstLen >= srcLen 
            byte* srcCurr = srcBitmap;
            byte* srcEnd = srcCurr + (slen - stail);
            byte* dstCurr = dstPtr;

            #region 8x32
            while (srcCurr < srcEnd)
            {
                Vector256<byte> d00 = Avx.LoadVector256(srcCurr);
                Vector256<byte> d01 = Avx.LoadVector256(srcCurr + 32);
                Vector256<byte> d02 = Avx.LoadVector256(srcCurr + 64);
                Vector256<byte> d03 = Avx.LoadVector256(srcCurr + 96);
                Vector256<byte> d04 = Avx.LoadVector256(srcCurr + 128);
                Vector256<byte> d05 = Avx.LoadVector256(srcCurr + 160);
                Vector256<byte> d06 = Avx.LoadVector256(srcCurr + 192);
                Vector256<byte> d07 = Avx.LoadVector256(srcCurr + 224);

                Avx.Store(dstCurr, Avx2.Xor(d00, Vector256<byte>.AllBitsSet));
                Avx.Store(dstCurr + 32, Avx2.Xor(d01, Vector256<byte>.AllBitsSet));
                Avx.Store(dstCurr + 64, Avx2.Xor(d02, Vector256<byte>.AllBitsSet));
                Avx.Store(dstCurr + 96, Avx2.Xor(d03, Vector256<byte>.AllBitsSet));
                Avx.Store(dstCurr + 128, Avx2.Xor(d04, Vector256<byte>.AllBitsSet));
                Avx.Store(dstCurr + 160, Avx2.Xor(d05, Vector256<byte>.AllBitsSet));
                Avx.Store(dstCurr + 192, Avx2.Xor(d06, Vector256<byte>.AllBitsSet));
                Avx.Store(dstCurr + 224, Avx2.Xor(d07, Vector256<byte>.AllBitsSet));

                srcCurr += batchSize;
                dstCurr += batchSize;
            }
            if (stail == 0) return;
            #endregion

            #region 1x32
            slen = stail;
            batchSize = 1 * 32;
            stail = slen & (batchSize - 1);
            srcEnd = srcCurr + (slen - stail);
            while (srcCurr < srcEnd)
            {
                Vector256<byte> d00 = Avx.LoadVector256(srcCurr);
                Avx.Store(dstCurr, Avx2.Xor(d00, Vector256<byte>.AllBitsSet));
                srcCurr += batchSize;
                dstCurr += batchSize;
            }
            if (stail == 0) return;
            #endregion

            #region 4x8
            slen = stail;
            batchSize = 4 * 8;
            stail = slen & (batchSize - 1);
            srcEnd = srcCurr + (slen - stail);
            while (srcCurr < srcEnd)
            {
                long d00 = *(long*)(srcCurr);
                long d01 = *(long*)(srcCurr + 8);
                long d02 = *(long*)(srcCurr + 16);
                long d03 = *(long*)(srcCurr + 24);

                *(long*)dstCurr = ~d00;
                *(long*)(dstCurr + 8) = ~d01;
                *(long*)(dstCurr + 16) = ~d02;
                *(long*)(dstCurr + 24) = ~d03;

                srcCurr += batchSize;
                dstCurr += batchSize;
            }
            if (stail == 0) return;
            #endregion

            #region 1x8
            slen = stail;
            batchSize = 8;
            stail = slen & (batchSize - 1);
            srcEnd = srcCurr + (slen - stail);
            while (srcCurr < srcEnd)
            {
                long d00 = *(long*)(srcCurr);

                *(long*)dstCurr = ~d00;

                srcCurr += batchSize;
                dstCurr += batchSize;
            }
            if (stail == 0) return;
            #endregion

            if (stail >= 7) dstCurr[6] = (byte)(~srcCurr[6]);
            if (stail >= 6) dstCurr[5] = (byte)(~srcCurr[5]);
            if (stail >= 5) dstCurr[4] = (byte)(~srcCurr[4]);
            if (stail >= 4) dstCurr[3] = (byte)(~srcCurr[3]);
            if (stail >= 3) dstCurr[2] = (byte)(~srcCurr[2]);
            if (stail >= 2) dstCurr[1] = (byte)(~srcCurr[1]);
            if (stail >= 1) dstCurr[0] = (byte)(~srcCurr[0]);
        }

        /// <summary>
        /// AND bitop implementation using 256-wide SIMD registers.
        /// </summary>
        /// <param name="dstPtr">Output buffer to write BitOp result</param>
        /// <param name="dstLen">Output buffer length.</param>
        /// <param name="srcStartPtrs">Pointer to start of bitmap sources.</param>
        /// <param name="srcEndPtrs">Pointer to end of bitmap sources</param>
        /// <param name="srcKeyCount">Number of source keys.</param>
        /// <param name="minSize">Minimum size of source bitmaps.</param>
        private static void __bitop_multikey_simdX256_and(byte* dstPtr, int dstLen, byte** srcStartPtrs, byte** srcEndPtrs, int srcKeyCount, int minSize)
        {
            int batchSize = 8 * 32;
            long slen = minSize;
            long stail = slen & (batchSize - 1);

            byte* dstCurr = dstPtr;
            byte* dstEnd = dstCurr + (slen - stail);

            #region 8x32
            while (dstCurr < dstEnd)
            {
                Vector256<byte> d00 = Avx.LoadVector256(srcStartPtrs[0]);
                Vector256<byte> d01 = Avx.LoadVector256(srcStartPtrs[0] + 32);
                Vector256<byte> d02 = Avx.LoadVector256(srcStartPtrs[0] + 64);
                Vector256<byte> d03 = Avx.LoadVector256(srcStartPtrs[0] + 96);
                Vector256<byte> d04 = Avx.LoadVector256(srcStartPtrs[0] + 128);
                Vector256<byte> d05 = Avx.LoadVector256(srcStartPtrs[0] + 160);
                Vector256<byte> d06 = Avx.LoadVector256(srcStartPtrs[0] + 192);
                Vector256<byte> d07 = Avx.LoadVector256(srcStartPtrs[0] + 224);
                srcStartPtrs[0] += batchSize;
                for (int i = 1; i < srcKeyCount; i++)
                {
                    Vector256<byte> s00 = Avx.LoadVector256(srcStartPtrs[i]);
                    Vector256<byte> s01 = Avx.LoadVector256(srcStartPtrs[i] + 32);
                    Vector256<byte> s02 = Avx.LoadVector256(srcStartPtrs[i] + 64);
                    Vector256<byte> s03 = Avx.LoadVector256(srcStartPtrs[i] + 96);
                    Vector256<byte> s04 = Avx.LoadVector256(srcStartPtrs[i] + 128);
                    Vector256<byte> s05 = Avx.LoadVector256(srcStartPtrs[i] + 160);
                    Vector256<byte> s06 = Avx.LoadVector256(srcStartPtrs[i] + 192);
                    Vector256<byte> s07 = Avx.LoadVector256(srcStartPtrs[i] + 224);

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

                Avx.Store(dstCurr, d00);
                Avx.Store(dstCurr + 32, d01);
                Avx.Store(dstCurr + 64, d02);
                Avx.Store(dstCurr + 96, d03);
                Avx.Store(dstCurr + 128, d04);
                Avx.Store(dstCurr + 160, d05);
                Avx.Store(dstCurr + 192, d06);
                Avx.Store(dstCurr + 224, d07);

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
                Vector256<byte> d00 = Avx.LoadVector256(srcStartPtrs[0]);
                srcStartPtrs[0] += batchSize;
                for (int i = 1; i < srcKeyCount; i++)
                {
                    Vector256<byte> s00 = Avx.LoadVector256(srcStartPtrs[i]);
                    d00 = Avx2.And(d00, s00);
                    srcStartPtrs[i] += batchSize;
                }
                Avx.Store(dstCurr, d00);
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

        /// <summary>
        /// OR bitop implementation using 256-wide SIMD registers.
        /// </summary>
        /// <param name="dstPtr">Output buffer to write BitOp result</param>
        /// <param name="dstLen">Output buffer length.</param>
        /// <param name="srcStartPtrs">Pointer to start of bitmap sources.</param>
        /// <param name="srcEndPtrs">Pointer to end of bitmap sources</param>
        /// <param name="srcKeyCount">Number of source keys.</param>
        /// <param name="minSize">Minimum size of source bitmaps.</param>
        private static void __bitop_multikey_simdX256_or(byte* dstPtr, int dstLen, byte** srcStartPtrs, byte** srcEndPtrs, int srcKeyCount, int minSize)
        {
            int batchSize = 8 * 32;
            long slen = minSize;
            long stail = slen & (batchSize - 1);

            byte* dstCurr = dstPtr;
            byte* dstEnd = dstCurr + (slen - stail);

            #region 8x32
            while (dstCurr < dstEnd)
            {
                Vector256<byte> d00 = Avx.LoadVector256(srcStartPtrs[0]);
                Vector256<byte> d01 = Avx.LoadVector256(srcStartPtrs[0] + 32);
                Vector256<byte> d02 = Avx.LoadVector256(srcStartPtrs[0] + 64);
                Vector256<byte> d03 = Avx.LoadVector256(srcStartPtrs[0] + 96);
                Vector256<byte> d04 = Avx.LoadVector256(srcStartPtrs[0] + 128);
                Vector256<byte> d05 = Avx.LoadVector256(srcStartPtrs[0] + 160);
                Vector256<byte> d06 = Avx.LoadVector256(srcStartPtrs[0] + 192);
                Vector256<byte> d07 = Avx.LoadVector256(srcStartPtrs[0] + 224);
                srcStartPtrs[0] += batchSize;
                for (int i = 1; i < srcKeyCount; i++)
                {
                    Vector256<byte> s00 = Avx.LoadVector256(srcStartPtrs[i]);
                    Vector256<byte> s01 = Avx.LoadVector256(srcStartPtrs[i] + 32);
                    Vector256<byte> s02 = Avx.LoadVector256(srcStartPtrs[i] + 64);
                    Vector256<byte> s03 = Avx.LoadVector256(srcStartPtrs[i] + 96);
                    Vector256<byte> s04 = Avx.LoadVector256(srcStartPtrs[i] + 128);
                    Vector256<byte> s05 = Avx.LoadVector256(srcStartPtrs[i] + 160);
                    Vector256<byte> s06 = Avx.LoadVector256(srcStartPtrs[i] + 192);
                    Vector256<byte> s07 = Avx.LoadVector256(srcStartPtrs[i] + 224);

                    d00 = Avx2.Or(d00, s00);
                    d01 = Avx2.Or(d01, s01);
                    d02 = Avx2.Or(d02, s02);
                    d03 = Avx2.Or(d03, s03);
                    d04 = Avx2.Or(d04, s04);
                    d05 = Avx2.Or(d05, s05);
                    d06 = Avx2.Or(d06, s06);
                    d07 = Avx2.Or(d07, s07);
                    srcStartPtrs[i] += batchSize;
                }

                Avx.Store(dstCurr, d00);
                Avx.Store(dstCurr + 32, d01);
                Avx.Store(dstCurr + 64, d02);
                Avx.Store(dstCurr + 96, d03);
                Avx.Store(dstCurr + 128, d04);
                Avx.Store(dstCurr + 160, d05);
                Avx.Store(dstCurr + 192, d06);
                Avx.Store(dstCurr + 224, d07);

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
                Vector256<byte> d00 = Avx.LoadVector256(srcStartPtrs[0]);
                srcStartPtrs[0] += batchSize;
                for (int i = 1; i < srcKeyCount; i++)
                {
                    Vector256<byte> s00 = Avx.LoadVector256(srcStartPtrs[i]);
                    d00 = Avx2.Or(d00, s00);
                    srcStartPtrs[i] += batchSize;
                }
                Avx.Store(dstCurr, d00);
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
                    d00 |= *(long*)(srcStartPtrs[i]);
                    d01 |= *(long*)(srcStartPtrs[i] + 8);
                    d02 |= *(long*)(srcStartPtrs[i] + 16);
                    d03 |= *(long*)(srcStartPtrs[i] + 24);
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
                    d00 |= *(long*)(srcStartPtrs[i]);
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
                byte d00 = 0;
                if (srcStartPtrs[0] + offset < srcEndPtrs[0])
                {
                    d00 = srcStartPtrs[0][offset];
                    if (d00 == 0xff) goto writeBack;
                }

                for (int i = 1; i < srcKeyCount; i++)
                {
                    if (srcStartPtrs[i] + offset < srcEndPtrs[i])
                    {
                        d00 |= srcStartPtrs[i][offset];
                        if (d00 == 0xff) goto writeBack;
                    }
                }
            writeBack:
                *dstCurr++ = d00;
                offset++;
            }
            #endregion
        }

        /// <summary>
        /// XOR bitop implementation using 256-wide SIMD registers.
        /// </summary>
        /// <param name="dstPtr">Output buffer to write BitOp result</param>
        /// <param name="dstLen">Output buffer length.</param>
        /// <param name="srcStartPtrs">Pointer to start of bitmap sources.</param>
        /// <param name="srcEndPtrs">Pointer to end of bitmap sources</param>
        /// <param name="srcKeyCount">Number of source keys.</param>
        /// <param name="minSize">Minimum size of source bitmaps.</param>
        private static void __bitop_multikey_simdX256_xor(byte* dstPtr, int dstLen, byte** srcStartPtrs, byte** srcEndPtrs, int srcKeyCount, int minSize)
        {
            int batchSize = 8 * 32;
            long slen = minSize;
            long stail = slen & (batchSize - 1);

            byte* dstCurr = dstPtr;
            byte* dstEnd = dstCurr + (slen - stail);

            #region 8x32
            while (dstCurr < dstEnd)
            {
                Vector256<byte> d00 = Avx.LoadVector256(srcStartPtrs[0]);
                Vector256<byte> d01 = Avx.LoadVector256(srcStartPtrs[0] + 32);
                Vector256<byte> d02 = Avx.LoadVector256(srcStartPtrs[0] + 64);
                Vector256<byte> d03 = Avx.LoadVector256(srcStartPtrs[0] + 96);
                Vector256<byte> d04 = Avx.LoadVector256(srcStartPtrs[0] + 128);
                Vector256<byte> d05 = Avx.LoadVector256(srcStartPtrs[0] + 160);
                Vector256<byte> d06 = Avx.LoadVector256(srcStartPtrs[0] + 192);
                Vector256<byte> d07 = Avx.LoadVector256(srcStartPtrs[0] + 224);
                srcStartPtrs[0] += batchSize;
                for (int i = 1; i < srcKeyCount; i++)
                {
                    Vector256<byte> s00 = Avx.LoadVector256(srcStartPtrs[i]);
                    Vector256<byte> s01 = Avx.LoadVector256(srcStartPtrs[i] + 32);
                    Vector256<byte> s02 = Avx.LoadVector256(srcStartPtrs[i] + 64);
                    Vector256<byte> s03 = Avx.LoadVector256(srcStartPtrs[i] + 96);
                    Vector256<byte> s04 = Avx.LoadVector256(srcStartPtrs[i] + 128);
                    Vector256<byte> s05 = Avx.LoadVector256(srcStartPtrs[i] + 160);
                    Vector256<byte> s06 = Avx.LoadVector256(srcStartPtrs[i] + 192);
                    Vector256<byte> s07 = Avx.LoadVector256(srcStartPtrs[i] + 224);

                    d00 = Avx2.Xor(d00, s00);
                    d01 = Avx2.Xor(d01, s01);
                    d02 = Avx2.Xor(d02, s02);
                    d03 = Avx2.Xor(d03, s03);
                    d04 = Avx2.Xor(d04, s04);
                    d05 = Avx2.Xor(d05, s05);
                    d06 = Avx2.Xor(d06, s06);
                    d07 = Avx2.Xor(d07, s07);
                    srcStartPtrs[i] += batchSize;
                }

                Avx.Store(dstCurr, d00);
                Avx.Store(dstCurr + 32, d01);
                Avx.Store(dstCurr + 64, d02);
                Avx.Store(dstCurr + 96, d03);
                Avx.Store(dstCurr + 128, d04);
                Avx.Store(dstCurr + 160, d05);
                Avx.Store(dstCurr + 192, d06);
                Avx.Store(dstCurr + 224, d07);

                dstCurr += batchSize;
            }
            #endregion

            #region 1x32
            slen = stail;
            batchSize = 1 * 32;
            stail = slen & (batchSize - 1);
            dstEnd = dstCurr + (slen - stail);

            while (dstCurr < dstEnd)
            {
                Vector256<byte> d00 = Avx.LoadVector256(srcStartPtrs[0]);
                srcStartPtrs[0] += batchSize;
                for (int i = 1; i < srcKeyCount; i++)
                {
                    Vector256<byte> s00 = Avx.LoadVector256(srcStartPtrs[i]);
                    d00 = Avx2.Xor(d00, s00);
                    srcStartPtrs[i] += batchSize;
                }
                Avx.Store(dstCurr, d00);
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
                    d00 ^= *(long*)(srcStartPtrs[i]);
                    d01 ^= *(long*)(srcStartPtrs[i] + 8);
                    d02 ^= *(long*)(srcStartPtrs[i] + 16);
                    d03 ^= *(long*)(srcStartPtrs[i] + 24);
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
                    d00 ^= *(long*)(srcStartPtrs[i]);
                    srcStartPtrs[i] += batchSize;
                }
                *(long*)dstCurr = d00;
                dstCurr += batchSize;
            }
        #endregion

        fillTail:
            #region scalar_1x1    
            byte* dstMaxEnd = dstPtr + dstLen;
            while (dstCurr < dstMaxEnd)
            {
                byte d00 = 0;
                if (srcStartPtrs[0] < srcEndPtrs[0])
                {
                    d00 = *srcStartPtrs[0];
                    srcStartPtrs[0]++;
                }

                for (int i = 1; i < srcKeyCount; i++)
                {
                    if (srcStartPtrs[i] < srcEndPtrs[i])
                    {
                        d00 ^= *srcStartPtrs[i];
                        srcStartPtrs[i]++;
                    }
                }
                *dstCurr++ = d00;
            }
            #endregion
        }

    }
}