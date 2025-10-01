// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Garnet.common;
using Garnet.common.Numerics;

namespace Garnet.server
{
    public unsafe partial class BitmapManager
    {
        /// <summary>
        /// Performs a bitwise operation across one or more source buffers and writes the result to the destination buffer.
        /// </summary>
        /// <param name="op">The bitwise operation to perform.</param>
        /// <param name="srcCount">Number of source buffers</param>
        /// <param name="srcPtrs">Array of pointers to source buffers. The array length must be greater than or equal to <paramref name="srcCount"/></param>
        /// <param name="srcEndPtrs">Array of the buffer lengths specified in <paramref name="srcPtrs"/>. The array length must be greater than or equal to <paramref name="srcCount"/></param>
        /// <param name="dstPtr">Destination buffer to write the result.</param>
        /// <param name="dstLength">Destination buffer length.</param>
        /// <param name="shortestSrcLength">The length of shortest source buffer.</param>
        public static void InvokeBitOperationUnsafe(BitmapOperation op, int srcCount, byte** srcPtrs, byte** srcEndPtrs, byte* dstPtr, int dstLength, int shortestSrcLength)
        {
            Debug.Assert(op is BitmapOperation.NOT or BitmapOperation.AND or BitmapOperation.OR or BitmapOperation.XOR or BitmapOperation.DIFF);
            Debug.Assert(srcCount > 0);
            Debug.Assert(dstLength >= 0 && shortestSrcLength >= 0);
            Debug.Assert(dstLength >= shortestSrcLength);

            if (srcCount == 1)
            {
                if (op == BitmapOperation.DIFF) throw new GarnetException("BITOP DIFF operation requires at least two source bitmaps");

                var srcBitmap = new ReadOnlySpan<byte>(srcPtrs[0], checked((int)(srcEndPtrs[0] - srcPtrs[0])));
                var dstBitmap = new Span<byte>(dstPtr, dstLength);

                if (op == BitmapOperation.NOT)
                {
                    TensorPrimitives.OnesComplement(srcBitmap, dstBitmap);
                }
                else
                {
                    srcBitmap.CopyTo(dstBitmap);
                }
            }
            // srcCount ≥ 2
            else if (op == BitmapOperation.AND) InvokeNaryBitwiseOperation<BitwiseAndOperator>(srcCount, srcPtrs, srcEndPtrs, dstPtr, dstLength, shortestSrcLength);
            else if (op == BitmapOperation.OR) InvokeNaryBitwiseOperation<BitwiseOrOperator>(srcCount, srcPtrs, srcEndPtrs, dstPtr, dstLength, shortestSrcLength);
            else if (op == BitmapOperation.XOR) InvokeNaryBitwiseOperation<BitwiseXorOperator>(srcCount, srcPtrs, srcEndPtrs, dstPtr, dstLength, shortestSrcLength);
            else if (op == BitmapOperation.DIFF) InvokeNaryBitwiseOperation<BitwiseAndNotOperator>(srcCount, srcPtrs, srcEndPtrs, dstPtr, dstLength, shortestSrcLength);
        }

        /// <summary>
        /// Invokes bitwise binary operation across n-ary source bitmaps.
        /// </summary>
        /// <typeparam name="TBinaryOperator">The binary operator type to compute bitwise</typeparam>
        /// <param name="srcCount">Number of source bitmaps.</param>
        /// <param name="srcPtrs">Array of pointers to source bitmap buffers.</param>
        /// <param name="srcEndPtrs">Array of the of pointers pointing to the end of the respective the bitmaps specified in <paramref name="srcPtrs"/>.</param>
        /// <param name="dstPtr">Destination buffer to write the result.</param>
        /// <param name="dstLength">Destination buffer length.</param>
        /// <param name="shortestSrcLength">The length of shortest source buffer.</param>
        [SkipLocalsInit]
        private static void InvokeNaryBitwiseOperation<TBinaryOperator>(int srcCount, byte** srcPtrs, byte** srcEndPtrs, byte* dstPtr, int dstLength, int shortestSrcLength)
            where TBinaryOperator : struct, IBinaryOperator
        {
            var dstEndPtr = dstPtr + dstLength;

            var remainingLength = shortestSrcLength;
            var batchRemainder = shortestSrcLength;
            byte* dstBatchEndPtr;

            // Keep the cursor of the first source buffer in local to keep processing tidy.
            var firstSrcPtr = srcPtrs[0];

            // Copy remaining source buffer pointers so we don't increment caller's.
            var tmpSrcPtrs = stackalloc byte*[srcCount];
            for (var i = 0; i < srcCount; i++)
            {
                tmpSrcPtrs[i] = srcPtrs[i];
            }
            srcPtrs = tmpSrcPtrs;

            if (Vector512.IsHardwareAccelerated && Vector512<byte>.IsSupported)
            {
                // Vectorized: 64 bytes x 8
                batchRemainder = remainingLength & ((Vector512<byte>.Count * 8) - 1);
                dstBatchEndPtr = dstPtr + (remainingLength - batchRemainder);
                remainingLength = batchRemainder;

                Vectorized512(ref firstSrcPtr, srcCount, srcPtrs, ref dstPtr, dstBatchEndPtr);
            }
            else if (Vector256.IsHardwareAccelerated && Vector256<byte>.IsSupported)
            {
                // Vectorized: 32 bytes x 8
                batchRemainder = remainingLength & ((Vector256<byte>.Count * 8) - 1);
                dstBatchEndPtr = dstPtr + (remainingLength - batchRemainder);
                remainingLength = batchRemainder;

                Vectorized256(ref firstSrcPtr, srcCount, srcPtrs, ref dstPtr, dstBatchEndPtr);
            }
            else if (Vector128.IsHardwareAccelerated && Vector128<byte>.IsSupported)
            {
                // Vectorized: 16 bytes x 8
                batchRemainder = remainingLength & ((Vector128<byte>.Count * 8) - 1);
                dstBatchEndPtr = dstPtr + (remainingLength - batchRemainder);
                remainingLength = batchRemainder;

                Vectorized128(ref firstSrcPtr, srcCount, srcPtrs, ref dstPtr, dstBatchEndPtr);
            }

            // Scalar: 8 bytes x 4
            batchRemainder = remainingLength & ((sizeof(ulong) * 4) - 1);
            dstBatchEndPtr = dstPtr + (remainingLength - batchRemainder);
            remainingLength = batchRemainder;

            while (dstPtr < dstBatchEndPtr)
            {
                var d00 = *(ulong*)(firstSrcPtr + (sizeof(ulong) * 0));
                var d01 = *(ulong*)(firstSrcPtr + (sizeof(ulong) * 1));
                var d02 = *(ulong*)(firstSrcPtr + (sizeof(ulong) * 2));
                var d03 = *(ulong*)(firstSrcPtr + (sizeof(ulong) * 3));

                firstSrcPtr += sizeof(ulong) * 4;

                for (var i = 1; i < srcCount; i++)
                {
                    ref var startPtr = ref srcPtrs[i];

                    d00 = TBinaryOperator.Invoke(d00, *(ulong*)(startPtr + (sizeof(ulong) * 0)));
                    d01 = TBinaryOperator.Invoke(d01, *(ulong*)(startPtr + (sizeof(ulong) * 1)));
                    d02 = TBinaryOperator.Invoke(d02, *(ulong*)(startPtr + (sizeof(ulong) * 2)));
                    d03 = TBinaryOperator.Invoke(d03, *(ulong*)(startPtr + (sizeof(ulong) * 3)));

                    srcPtrs[i] += sizeof(ulong) * 4;
                }

                *(ulong*)(dstPtr + (sizeof(ulong) * 0)) = d00;
                *(ulong*)(dstPtr + (sizeof(ulong) * 1)) = d01;
                *(ulong*)(dstPtr + (sizeof(ulong) * 2)) = d02;
                *(ulong*)(dstPtr + (sizeof(ulong) * 3)) = d03;

                dstPtr += sizeof(ulong) * 4;
            }

            // Handle the remaining tails
            while (dstPtr < dstEndPtr)
            {
                byte d00 = 0;

                if (firstSrcPtr < srcEndPtrs[0])
                {
                    d00 = *firstSrcPtr;
                    firstSrcPtr++;
                }

                for (var i = 1; i < srcCount; i++)
                {
                    if (srcPtrs[i] < srcEndPtrs[i])
                    {
                        d00 = TBinaryOperator.Invoke(d00, *srcPtrs[i]);
                        srcPtrs[i]++;
                    }
                    else if (typeof(TBinaryOperator) == typeof(BitwiseAndOperator))
                    {
                        d00 = 0;
                    }
                }

                *dstPtr++ = d00;
            }

            static void Vectorized512(ref byte* firstPtr, int srcCount, byte** srcStartPtrs, ref byte* dstPtr, byte* dstBatchEndPtr)
            {
                while (dstPtr < dstBatchEndPtr)
                {
                    var d00 = Vector512.Load(firstPtr + (Vector512<byte>.Count * 0));
                    var d01 = Vector512.Load(firstPtr + (Vector512<byte>.Count * 1));
                    var d02 = Vector512.Load(firstPtr + (Vector512<byte>.Count * 2));
                    var d03 = Vector512.Load(firstPtr + (Vector512<byte>.Count * 3));
                    var d04 = Vector512.Load(firstPtr + (Vector512<byte>.Count * 4));
                    var d05 = Vector512.Load(firstPtr + (Vector512<byte>.Count * 5));
                    var d06 = Vector512.Load(firstPtr + (Vector512<byte>.Count * 6));
                    var d07 = Vector512.Load(firstPtr + (Vector512<byte>.Count * 7));

                    firstPtr += Vector512<byte>.Count * 8;

                    for (var i = 1; i < srcCount; i++)
                    {
                        ref var startPtr = ref srcStartPtrs[i];

                        var s00 = Vector512.Load(startPtr + (Vector512<byte>.Count * 0));
                        var s01 = Vector512.Load(startPtr + (Vector512<byte>.Count * 1));
                        var s02 = Vector512.Load(startPtr + (Vector512<byte>.Count * 2));
                        var s03 = Vector512.Load(startPtr + (Vector512<byte>.Count * 3));
                        var s04 = Vector512.Load(startPtr + (Vector512<byte>.Count * 4));
                        var s05 = Vector512.Load(startPtr + (Vector512<byte>.Count * 5));
                        var s06 = Vector512.Load(startPtr + (Vector512<byte>.Count * 6));
                        var s07 = Vector512.Load(startPtr + (Vector512<byte>.Count * 7));

                        d00 = TBinaryOperator.Invoke(d00, s00);
                        d01 = TBinaryOperator.Invoke(d01, s01);
                        d02 = TBinaryOperator.Invoke(d02, s02);
                        d03 = TBinaryOperator.Invoke(d03, s03);
                        d04 = TBinaryOperator.Invoke(d04, s04);
                        d05 = TBinaryOperator.Invoke(d05, s05);
                        d06 = TBinaryOperator.Invoke(d06, s06);
                        d07 = TBinaryOperator.Invoke(d07, s07);

                        startPtr += Vector512<byte>.Count * 8;
                    }

                    Vector512.Store(d00, dstPtr + (Vector512<byte>.Count * 0));
                    Vector512.Store(d01, dstPtr + (Vector512<byte>.Count * 1));
                    Vector512.Store(d02, dstPtr + (Vector512<byte>.Count * 2));
                    Vector512.Store(d03, dstPtr + (Vector512<byte>.Count * 3));
                    Vector512.Store(d04, dstPtr + (Vector512<byte>.Count * 4));
                    Vector512.Store(d05, dstPtr + (Vector512<byte>.Count * 5));
                    Vector512.Store(d06, dstPtr + (Vector512<byte>.Count * 6));
                    Vector512.Store(d07, dstPtr + (Vector512<byte>.Count * 7));

                    dstPtr += Vector512<byte>.Count * 8;
                }
            }

            static void Vectorized256(ref byte* firstPtr, int srcCount, byte** srcStartPtrs, ref byte* dstPtr, byte* dstBatchEndPtr)
            {
                while (dstPtr < dstBatchEndPtr)
                {
                    var d00 = Vector256.Load(firstPtr + (Vector256<byte>.Count * 0));
                    var d01 = Vector256.Load(firstPtr + (Vector256<byte>.Count * 1));
                    var d02 = Vector256.Load(firstPtr + (Vector256<byte>.Count * 2));
                    var d03 = Vector256.Load(firstPtr + (Vector256<byte>.Count * 3));
                    var d04 = Vector256.Load(firstPtr + (Vector256<byte>.Count * 4));
                    var d05 = Vector256.Load(firstPtr + (Vector256<byte>.Count * 5));
                    var d06 = Vector256.Load(firstPtr + (Vector256<byte>.Count * 6));
                    var d07 = Vector256.Load(firstPtr + (Vector256<byte>.Count * 7));

                    firstPtr += Vector256<byte>.Count * 8;

                    for (var i = 1; i < srcCount; i++)
                    {
                        ref var startPtr = ref srcStartPtrs[i];

                        var s00 = Vector256.Load(startPtr + (Vector256<byte>.Count * 0));
                        var s01 = Vector256.Load(startPtr + (Vector256<byte>.Count * 1));
                        var s02 = Vector256.Load(startPtr + (Vector256<byte>.Count * 2));
                        var s03 = Vector256.Load(startPtr + (Vector256<byte>.Count * 3));
                        var s04 = Vector256.Load(startPtr + (Vector256<byte>.Count * 4));
                        var s05 = Vector256.Load(startPtr + (Vector256<byte>.Count * 5));
                        var s06 = Vector256.Load(startPtr + (Vector256<byte>.Count * 6));
                        var s07 = Vector256.Load(startPtr + (Vector256<byte>.Count * 7));

                        d00 = TBinaryOperator.Invoke(d00, s00);
                        d01 = TBinaryOperator.Invoke(d01, s01);
                        d02 = TBinaryOperator.Invoke(d02, s02);
                        d03 = TBinaryOperator.Invoke(d03, s03);
                        d04 = TBinaryOperator.Invoke(d04, s04);
                        d05 = TBinaryOperator.Invoke(d05, s05);
                        d06 = TBinaryOperator.Invoke(d06, s06);
                        d07 = TBinaryOperator.Invoke(d07, s07);

                        startPtr += Vector256<byte>.Count * 8;
                    }

                    Vector256.Store(d00, dstPtr + (Vector256<byte>.Count * 0));
                    Vector256.Store(d01, dstPtr + (Vector256<byte>.Count * 1));
                    Vector256.Store(d02, dstPtr + (Vector256<byte>.Count * 2));
                    Vector256.Store(d03, dstPtr + (Vector256<byte>.Count * 3));
                    Vector256.Store(d04, dstPtr + (Vector256<byte>.Count * 4));
                    Vector256.Store(d05, dstPtr + (Vector256<byte>.Count * 5));
                    Vector256.Store(d06, dstPtr + (Vector256<byte>.Count * 6));
                    Vector256.Store(d07, dstPtr + (Vector256<byte>.Count * 7));

                    dstPtr += Vector256<byte>.Count * 8;
                }
            }

            static void Vectorized128(ref byte* firstPtr, int srcCount, byte** srcStartPtrs, ref byte* dstPtr, byte* dstBatchEndPtr)
            {
                while (dstPtr < dstBatchEndPtr)
                {
                    var d00 = Vector128.Load(firstPtr + (Vector128<byte>.Count * 0));
                    var d01 = Vector128.Load(firstPtr + (Vector128<byte>.Count * 1));
                    var d02 = Vector128.Load(firstPtr + (Vector128<byte>.Count * 2));
                    var d03 = Vector128.Load(firstPtr + (Vector128<byte>.Count * 3));
                    var d04 = Vector128.Load(firstPtr + (Vector128<byte>.Count * 4));
                    var d05 = Vector128.Load(firstPtr + (Vector128<byte>.Count * 5));
                    var d06 = Vector128.Load(firstPtr + (Vector128<byte>.Count * 6));
                    var d07 = Vector128.Load(firstPtr + (Vector128<byte>.Count * 7));

                    firstPtr += Vector128<byte>.Count * 8;

                    for (var i = 1; i < srcCount; i++)
                    {
                        ref var startPtr = ref srcStartPtrs[i];

                        var s00 = Vector128.Load(startPtr + (Vector128<byte>.Count * 0));
                        var s01 = Vector128.Load(startPtr + (Vector128<byte>.Count * 1));
                        var s02 = Vector128.Load(startPtr + (Vector128<byte>.Count * 2));
                        var s03 = Vector128.Load(startPtr + (Vector128<byte>.Count * 3));
                        var s04 = Vector128.Load(startPtr + (Vector128<byte>.Count * 4));
                        var s05 = Vector128.Load(startPtr + (Vector128<byte>.Count * 5));
                        var s06 = Vector128.Load(startPtr + (Vector128<byte>.Count * 6));
                        var s07 = Vector128.Load(startPtr + (Vector128<byte>.Count * 7));

                        d00 = TBinaryOperator.Invoke(d00, s00);
                        d01 = TBinaryOperator.Invoke(d01, s01);
                        d02 = TBinaryOperator.Invoke(d02, s02);
                        d03 = TBinaryOperator.Invoke(d03, s03);
                        d04 = TBinaryOperator.Invoke(d04, s04);
                        d05 = TBinaryOperator.Invoke(d05, s05);
                        d06 = TBinaryOperator.Invoke(d06, s06);
                        d07 = TBinaryOperator.Invoke(d07, s07);

                        startPtr += Vector128<byte>.Count * 8;
                    }

                    Vector128.Store(d00, dstPtr + (Vector128<byte>.Count * 0));
                    Vector128.Store(d01, dstPtr + (Vector128<byte>.Count * 1));
                    Vector128.Store(d02, dstPtr + (Vector128<byte>.Count * 2));
                    Vector128.Store(d03, dstPtr + (Vector128<byte>.Count * 3));
                    Vector128.Store(d04, dstPtr + (Vector128<byte>.Count * 4));
                    Vector128.Store(d05, dstPtr + (Vector128<byte>.Count * 5));
                    Vector128.Store(d06, dstPtr + (Vector128<byte>.Count * 6));
                    Vector128.Store(d07, dstPtr + (Vector128<byte>.Count * 7));

                    dstPtr += Vector128<byte>.Count * 8;
                }
            }
        }
    }
}