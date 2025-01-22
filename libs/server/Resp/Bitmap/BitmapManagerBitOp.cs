// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Garnet.common;
using static Garnet.common.Numerics.TensorPrimitives;

namespace Garnet.server
{
    // TODO: Guard Vector### logic behind IsSupported & IsHardwareAccelerated
    // TODO: Add Vector512 & Vector128 paths atleast
    // TODO: Get rid of "IBinaryOperator<ulong>" scalar logic?
    // FÒLLOW-UP: Non-temporal stores after sizes larger than 256KB (like in TensorPrimitives)
    // FÒLLOW-UP: Investigate alignment -> overlapping & jump-table (like in TensorPrimitives)
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
                    InvokeSingleKeyBitwiseNot(dstPtr, dstLen, srcStartPtrs[0], srcEndPtrs[0] - srcStartPtrs[0]);
                    break;
                case (byte)BitmapOperation.AND:
                    InvokeMultiKeyBitwise<BitwiseAndOperator<byte>, BitwiseAndOperator<ulong>>(dstPtr, dstLen, srcStartPtrs, srcEndPtrs, srcKeyCount, minSize);
                    break;
                case (byte)BitmapOperation.OR:
                    InvokeMultiKeyBitwise<BitwiseOrOperator<byte>, BitwiseOrOperator<ulong>>(dstPtr, dstLen, srcStartPtrs, srcEndPtrs, srcKeyCount, minSize);
                    break;
                case (byte)BitmapOperation.XOR:
                    InvokeMultiKeyBitwise<BitwiseXorOperator<byte>, BitwiseXorOperator<ulong>>(dstPtr, dstLen, srcStartPtrs, srcEndPtrs, srcKeyCount, minSize);
                    break;
                default:
                    throw new GarnetException("Unsupported BitOp command");
            }
            return true;
        }

        /// <summary>
        /// Invokes unary bitwise-NOT operation for single source key using hardware accelerated SIMD intrinsics when possible.
        /// </summary>
        /// <param name="dstPtr">Output buffer to write BitOp result</param>
        /// <param name="dstLen">Output buffer length.</param>
        /// <param name="srcBitmap">Pointer to source bitmap.</param>
        /// <param name="srcLen">Source bitmap length.</param>
        private static void InvokeSingleKeyBitwiseNot(byte* dstPtr, long dstLen, byte* srcBitmap, long srcLen)
        {
            long slen = srcLen;
            long remainder = slen & ((Vector256<byte>.Count * 8) - 1);

            //iterate using srcBitmap because always dstLen >= srcLen 
            byte* srcCurr = srcBitmap;
            byte* srcEnd = srcCurr + (slen - remainder);
            byte* dstCurr = dstPtr;

            while (srcCurr < srcEnd)
            {
                var d00 = Vector256.Load(srcCurr);
                var d01 = Vector256.Load(srcCurr + Vector256<byte>.Count);
                var d02 = Vector256.Load(srcCurr + (Vector256<byte>.Count * 2));
                var d03 = Vector256.Load(srcCurr + (Vector256<byte>.Count * 3));
                var d04 = Vector256.Load(srcCurr + (Vector256<byte>.Count * 4));
                var d05 = Vector256.Load(srcCurr + (Vector256<byte>.Count * 5));
                var d06 = Vector256.Load(srcCurr + (Vector256<byte>.Count * 6));
                var d07 = Vector256.Load(srcCurr + (Vector256<byte>.Count * 7));

                Vector256.Store(~d00, dstCurr);
                Vector256.Store(~d01, dstCurr + Vector256<byte>.Count);
                Vector256.Store(~d02, dstCurr + Vector256<byte>.Count * 2);
                Vector256.Store(~d03, dstCurr + Vector256<byte>.Count * 3);
                Vector256.Store(~d04, dstCurr + Vector256<byte>.Count * 4);
                Vector256.Store(~d05, dstCurr + Vector256<byte>.Count * 5);
                Vector256.Store(~d06, dstCurr + Vector256<byte>.Count * 6);
                Vector256.Store(~d07, dstCurr + Vector256<byte>.Count * 7);

                srcCurr += Vector256<byte>.Count * 8;
                dstCurr += Vector256<byte>.Count * 8;
            }
            if (remainder == 0) return;

            slen = remainder;
            remainder = slen & (Vector256<byte>.Count - 1);
            srcEnd = srcCurr + (slen - remainder);
            while (srcCurr < srcEnd)
            {
                Vector256.Store(~Vector256.Load(srcCurr), dstCurr);

                srcCurr += Vector256<byte>.Count;
                dstCurr += Vector256<byte>.Count;
            }
            if (remainder == 0) return;

            slen = remainder;
            remainder = slen & (sizeof(ulong) - 1);
            srcEnd = srcCurr + (slen - remainder);
            while (srcCurr < srcEnd)
            {
                *(ulong*)dstCurr = ~*(ulong*)srcCurr;

                srcCurr += sizeof(ulong);
                dstCurr += sizeof(ulong);
            }
            if (remainder == 0) return;

            if (remainder >= 7) dstCurr[6] = (byte)~srcCurr[6];
            if (remainder >= 6) dstCurr[5] = (byte)~srcCurr[5];
            if (remainder >= 5) dstCurr[4] = (byte)~srcCurr[4];
            if (remainder >= 4) dstCurr[3] = (byte)~srcCurr[3];
            if (remainder >= 3) dstCurr[2] = (byte)~srcCurr[2];
            if (remainder >= 2) dstCurr[1] = (byte)~srcCurr[1];
            if (remainder >= 1) dstCurr[0] = (byte)~srcCurr[0];
        }

        public static void GenericCodeGenDebugAid(byte* dstPtr, int dstLen, byte** srcStartPtrs, byte** srcEndPtrs, int srcKeyCount, int minSize, byte bitop)
        {
            InvokeMultiKeyBitwise<BitwiseAndOperator<byte>, BitwiseAndOperator<ulong>>(dstPtr, dstLen, srcStartPtrs, srcEndPtrs, srcKeyCount, minSize);
        }

        /// <summary>
        /// Invokes bitwise bit-operation for multiple keys using hardware accelerated SIMD intrinsics when possible.
        /// </summary>
        /// <param name="dstPtr">Output buffer to write BitOp result</param>
        /// <param name="dstLen">Output buffer length.</param>
        /// <param name="srcStartPtrs">Pointer to start of bitmap sources.</param>
        /// <param name="srcEndPtrs">Pointer to end of bitmap sources</param>
        /// <param name="srcKeyCount">Number of source keys.</param>
        /// <param name="minSize">Minimum size of source bitmaps.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void InvokeMultiKeyBitwise<TBinaryOperator, TBinaryOperator2>(byte* dstPtr, int dstLen, byte** srcStartPtrs, byte** srcEndPtrs, int srcKeyCount, int minSize)
            where TBinaryOperator : struct, IBinaryOperator<byte>
            where TBinaryOperator2 : struct, IBinaryOperator<ulong>
        {
            long slen = minSize;
            var remainder = slen & ((Vector256<byte>.Count * 8) - 1);

            var dstEndPtr = dstPtr + dstLen;
            var dstBatchEndPtr = dstPtr + (slen - remainder);

            ref var firstKeyPtr = ref srcStartPtrs[0];

            while (dstPtr < dstBatchEndPtr)
            {
                var d00 = Vector256.Load(firstKeyPtr);
                var d01 = Vector256.Load(firstKeyPtr + Vector256<byte>.Count);
                var d02 = Vector256.Load(firstKeyPtr + (Vector256<byte>.Count * 2));
                var d03 = Vector256.Load(firstKeyPtr + (Vector256<byte>.Count * 3));
                var d04 = Vector256.Load(firstKeyPtr + (Vector256<byte>.Count * 4));
                var d05 = Vector256.Load(firstKeyPtr + (Vector256<byte>.Count * 5));
                var d06 = Vector256.Load(firstKeyPtr + (Vector256<byte>.Count * 6));
                var d07 = Vector256.Load(firstKeyPtr + (Vector256<byte>.Count * 7));

                firstKeyPtr += Vector256<byte>.Count * 8;

                for (var i = 1; i < srcKeyCount; i++)
                {
                    ref var keyStartPtr = ref srcStartPtrs[i];

                    var s00 = Vector256.Load(keyStartPtr);
                    var s01 = Vector256.Load(keyStartPtr + Vector256<byte>.Count);
                    var s02 = Vector256.Load(keyStartPtr + (Vector256<byte>.Count * 2));
                    var s03 = Vector256.Load(keyStartPtr + (Vector256<byte>.Count * 3));
                    var s04 = Vector256.Load(keyStartPtr + (Vector256<byte>.Count * 4));
                    var s05 = Vector256.Load(keyStartPtr + (Vector256<byte>.Count * 5));
                    var s06 = Vector256.Load(keyStartPtr + (Vector256<byte>.Count * 6));
                    var s07 = Vector256.Load(keyStartPtr + (Vector256<byte>.Count * 7));

                    d00 = TBinaryOperator.Invoke(d00, s00);
                    d01 = TBinaryOperator.Invoke(d01, s01);
                    d02 = TBinaryOperator.Invoke(d02, s02);
                    d03 = TBinaryOperator.Invoke(d03, s03);
                    d04 = TBinaryOperator.Invoke(d04, s04);
                    d05 = TBinaryOperator.Invoke(d05, s05);
                    d06 = TBinaryOperator.Invoke(d06, s06);
                    d07 = TBinaryOperator.Invoke(d07, s07);

                    keyStartPtr += Vector256<byte>.Count * 8;
                }

                Vector256.Store(d00, dstPtr);
                Vector256.Store(d01, dstPtr + Vector256<byte>.Count);
                Vector256.Store(d02, dstPtr + Vector256<byte>.Count * 2);
                Vector256.Store(d03, dstPtr + Vector256<byte>.Count * 3);
                Vector256.Store(d04, dstPtr + Vector256<byte>.Count * 4);
                Vector256.Store(d05, dstPtr + Vector256<byte>.Count * 5);
                Vector256.Store(d06, dstPtr + Vector256<byte>.Count * 6);
                Vector256.Store(d07, dstPtr + Vector256<byte>.Count * 7);

                dstPtr += Vector256<byte>.Count * 8;
            }
            if (remainder == 0) goto fillTail;

            slen = remainder;
            remainder = slen & (Vector256<byte>.Count - 1);
            dstBatchEndPtr = dstPtr + (slen - remainder);

            while (dstPtr < dstBatchEndPtr)
            {
                var d00 = Vector256.Load(firstKeyPtr);
                firstKeyPtr += Vector256<byte>.Count;

                for (var i = 1; i < srcKeyCount; i++)
                {
                    var s00 = Vector256.Load(srcStartPtrs[i]);
                    d00 = TBinaryOperator.Invoke(d00, s00);

                    srcStartPtrs[i] += Vector256<byte>.Count;
                }

                Vector256.Store(d00, dstPtr);

                dstPtr += Vector256<byte>.Count;
            }
            if (remainder == 0) goto fillTail;

            slen = remainder;
            remainder = slen & (sizeof(ulong) - 1);
            dstBatchEndPtr = dstPtr + (slen - remainder);

            while (dstPtr < dstBatchEndPtr)
            {
                ulong d00 = *(ulong*)firstKeyPtr;
                firstKeyPtr += sizeof(ulong);

                for (var i = 1; i < srcKeyCount; i++)
                {
                    d00 = TBinaryOperator2.Invoke(d00, *(ulong*)srcStartPtrs[i]);
                    srcStartPtrs[i] += sizeof(ulong);
                }

                *(ulong*)dstPtr = d00;
                dstPtr += sizeof(ulong);
            }

        fillTail:
            while (dstPtr < dstEndPtr)
            {
                byte d00 = 0;

                if (firstKeyPtr < srcEndPtrs[0])
                {
                    d00 = *firstKeyPtr;
                    firstKeyPtr++;
                }

                for (var i = 1; i < srcKeyCount; i++)
                {
                    if (srcStartPtrs[i] < srcEndPtrs[i])
                    {
                        d00 = TBinaryOperator.Invoke(d00, *srcStartPtrs[i]);
                        srcStartPtrs[i]++;
                    }
                    else if (typeof(TBinaryOperator) == typeof(BitwiseAndOperator<byte>))
                    {
                        d00 = 0;
                    }
                }

                *dstPtr++ = d00;
            }
        }
    }
}