// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Numerics.Tensors;
using System.Runtime.Intrinsics;
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
        /// <param name="dstLength">Destination buffer length</param>
        /// <param name="shortestSrcLength">The length of shorted source buffer</param>
        public static void InvokeBitOperationUnsafe(BitmapOperation op, int srcCount, byte** srcPtrs, byte** srcEndPtrs, byte* dstPtr, int dstLength, int shortestSrcLength)
        {
            Debug.Assert(srcCount > 0);
            Debug.Assert(op is BitmapOperation.NOT or BitmapOperation.AND or BitmapOperation.OR or BitmapOperation.XOR);

            if (srcCount == 1)
            {
                var srcKey = new ReadOnlySpan<byte>(srcPtrs[0], checked((int)(srcEndPtrs[0] - srcPtrs[0])));
                var dst = new Span<byte>(dstPtr, dstLength);

                if (op == BitmapOperation.NOT)
                {
                    TensorPrimitives.OnesComplement(srcKey, dst);
                }
                else
                {
                    srcKey.CopyTo(dst);
                }

                return;
            }
            else if (srcCount == 2)
            {
                var firstSrcLength = checked((int)(srcEndPtrs[0] - srcPtrs[0]));
                var secondSrcLength = checked((int)(srcEndPtrs[1] - srcPtrs[1]));

                // Use fast-path for two-equal-length inputs
                if (firstSrcLength == secondSrcLength)
                {
                    var firstSrcKey = new ReadOnlySpan<byte>(srcPtrs[0], firstSrcLength);
                    var secondSrcKey = new ReadOnlySpan<byte>(srcPtrs[1], secondSrcLength);
                    var dst = new Span<byte>(dstPtr, dstLength);

                    if (op == BitmapOperation.AND) TensorPrimitives.BitwiseAnd(firstSrcKey, secondSrcKey, dst);
                    else if (op == BitmapOperation.OR) TensorPrimitives.BitwiseOr(firstSrcKey, secondSrcKey, dst);
                    else if (op == BitmapOperation.XOR) TensorPrimitives.Xor(firstSrcKey, secondSrcKey, dst);

                    return;
                }

                // If the keys are not the same length, fallback to the multi-key path for the tail handling
            }

            // Fallback for multi-key and tail handling
            if (op == BitmapOperation.AND) InvokeMultiKeyBitwise<BitwiseAndOperator>(srcCount, srcPtrs, srcEndPtrs, dstPtr, dstLength, shortestSrcLength);
            else if (op == BitmapOperation.OR) InvokeMultiKeyBitwise<BitwiseOrOperator>(srcCount, srcPtrs, srcEndPtrs, dstPtr, dstLength, shortestSrcLength);
            else if (op == BitmapOperation.XOR) InvokeMultiKeyBitwise<BitwiseXorOperator>(srcCount, srcPtrs, srcEndPtrs, dstPtr, dstLength, shortestSrcLength);
        }

        /// <summary>
        /// Invokes bitwise binary operation for multiple keys.
        /// </summary>
        /// <typeparam name="TBinaryOperator">The binary operator type to compute bitwise</typeparam>
        /// <param name="dstPtr">Output buffer to write BitOp result</param>
        /// <param name="dstLength">Output buffer length.</param>
        /// <param name="srcKeyPtrs">Array of pointers to source key buffers.</param>
        /// <param name="srcKeyEndPtrs">Array of the of pointers pointing to the end of the respective the keys specified in <paramref name="srcKeyPtrs"/>.</param>
        /// <param name="srcKeyCount">Number of source keys.</param>
        /// <param name="minLength">Minimum length of source bitmaps.</param>
        private static void InvokeMultiKeyBitwise<TBinaryOperator>(int srcKeyCount, byte** srcKeyPtrs, byte** srcKeyEndPtrs, byte* dstPtr, int dstLength, int minLength)
            where TBinaryOperator : struct, IBinaryOperator
        {
            var dstEndPtr = dstPtr + dstLength;

            long remainingLength = minLength;
            long batchRemainder = minLength;
            byte* dstBatchEndPtr;

            ref var firstKeyPtr = ref srcKeyPtrs[0];

            if (Vector256.IsHardwareAccelerated && Vector256<byte>.IsSupported)
            {
                // Vectorized: 32 bytes x 8
                batchRemainder = remainingLength & ((Vector256<byte>.Count * 8) - 1);
                dstBatchEndPtr = dstPtr + (remainingLength - batchRemainder);
                remainingLength = batchRemainder;

                Vectorized256(ref firstKeyPtr, srcKeyPtrs, srcKeyCount, ref dstPtr, dstBatchEndPtr);
            }
            else if (Vector128.IsHardwareAccelerated && Vector128<byte>.IsSupported)
            {
                // Vectorized: 16 bytes x 8
                batchRemainder = remainingLength & ((Vector128<byte>.Count * 8) - 1);
                dstBatchEndPtr = dstPtr + (remainingLength - batchRemainder);
                remainingLength = batchRemainder;

                Vectorized128(ref firstKeyPtr, srcKeyPtrs, srcKeyCount, ref dstPtr, dstBatchEndPtr);
            }

            // Scalar: 8 bytes x 4
            batchRemainder = remainingLength & ((sizeof(ulong) * 4) - 1);
            dstBatchEndPtr = dstPtr + (remainingLength - batchRemainder);
            remainingLength = batchRemainder;

            while (dstPtr < dstBatchEndPtr)
            {
                var d00 = *(ulong*)(firstKeyPtr + (sizeof(ulong) * 0));
                var d01 = *(ulong*)(firstKeyPtr + (sizeof(ulong) * 1));
                var d02 = *(ulong*)(firstKeyPtr + (sizeof(ulong) * 2));
                var d03 = *(ulong*)(firstKeyPtr + (sizeof(ulong) * 3));

                firstKeyPtr += sizeof(ulong) * 4;

                for (var i = 1; i < srcKeyCount; i++)
                {
                    ref var keyStartPtr = ref srcKeyPtrs[i];

                    d00 = TBinaryOperator.Invoke(d00, *(ulong*)(keyStartPtr + (sizeof(ulong) * 0)));
                    d01 = TBinaryOperator.Invoke(d01, *(ulong*)(keyStartPtr + (sizeof(ulong) * 1)));
                    d02 = TBinaryOperator.Invoke(d02, *(ulong*)(keyStartPtr + (sizeof(ulong) * 2)));
                    d03 = TBinaryOperator.Invoke(d03, *(ulong*)(keyStartPtr + (sizeof(ulong) * 3)));

                    srcKeyPtrs[i] += sizeof(ulong) * 4;
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

                if (firstKeyPtr < srcKeyEndPtrs[0])
                {
                    d00 = *firstKeyPtr;
                    firstKeyPtr++;
                }

                for (var i = 1; i < srcKeyCount; i++)
                {
                    if (srcKeyPtrs[i] < srcKeyEndPtrs[i])
                    {
                        d00 = TBinaryOperator.Invoke(d00, *srcKeyPtrs[i]);
                        srcKeyPtrs[i]++;
                    }
                    else if (typeof(TBinaryOperator) == typeof(BitwiseAndOperator))
                    {
                        d00 = 0;
                    }
                }

                *dstPtr++ = d00;
            }

            static void Vectorized256(ref byte* firstKeyPtr, byte** srcStartPtrs, int srcKeyCount, ref byte* dstPtr, byte* dstBatchEndPtr)
            {
                while (dstPtr < dstBatchEndPtr)
                {
                    var d00 = Vector256.Load(firstKeyPtr + (Vector256<byte>.Count * 0));
                    var d01 = Vector256.Load(firstKeyPtr + (Vector256<byte>.Count * 1));
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

                        var s00 = Vector256.Load(keyStartPtr + (Vector256<byte>.Count * 0));
                        var s01 = Vector256.Load(keyStartPtr + (Vector256<byte>.Count * 1));
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

            static void Vectorized128(ref byte* firstKeyPtr, byte** srcStartPtrs, int srcKeyCount, ref byte* dstPtr, byte* dstBatchEndPtr)
            {
                while (dstPtr < dstBatchEndPtr)
                {
                    var d00 = Vector128.Load(firstKeyPtr + (Vector128<byte>.Count * 0));
                    var d01 = Vector128.Load(firstKeyPtr + (Vector128<byte>.Count * 1));
                    var d02 = Vector128.Load(firstKeyPtr + (Vector128<byte>.Count * 2));
                    var d03 = Vector128.Load(firstKeyPtr + (Vector128<byte>.Count * 3));
                    var d04 = Vector128.Load(firstKeyPtr + (Vector128<byte>.Count * 4));
                    var d05 = Vector128.Load(firstKeyPtr + (Vector128<byte>.Count * 5));
                    var d06 = Vector128.Load(firstKeyPtr + (Vector128<byte>.Count * 6));
                    var d07 = Vector128.Load(firstKeyPtr + (Vector128<byte>.Count * 7));

                    firstKeyPtr += Vector128<byte>.Count * 8;

                    for (var i = 1; i < srcKeyCount; i++)
                    {
                        ref var keyStartPtr = ref srcStartPtrs[i];

                        var s00 = Vector128.Load(keyStartPtr + (Vector128<byte>.Count * 0));
                        var s01 = Vector128.Load(keyStartPtr + (Vector128<byte>.Count * 1));
                        var s02 = Vector128.Load(keyStartPtr + (Vector128<byte>.Count * 2));
                        var s03 = Vector128.Load(keyStartPtr + (Vector128<byte>.Count * 3));
                        var s04 = Vector128.Load(keyStartPtr + (Vector128<byte>.Count * 4));
                        var s05 = Vector128.Load(keyStartPtr + (Vector128<byte>.Count * 5));
                        var s06 = Vector128.Load(keyStartPtr + (Vector128<byte>.Count * 6));
                        var s07 = Vector128.Load(keyStartPtr + (Vector128<byte>.Count * 7));

                        d00 = TBinaryOperator.Invoke(d00, s00);
                        d01 = TBinaryOperator.Invoke(d01, s01);
                        d02 = TBinaryOperator.Invoke(d02, s02);
                        d03 = TBinaryOperator.Invoke(d03, s03);
                        d04 = TBinaryOperator.Invoke(d04, s04);
                        d05 = TBinaryOperator.Invoke(d05, s05);
                        d06 = TBinaryOperator.Invoke(d06, s06);
                        d07 = TBinaryOperator.Invoke(d07, s07);

                        keyStartPtr += Vector128<byte>.Count * 8;
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