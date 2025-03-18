// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Numerics.Tensors;
using System.Runtime.Intrinsics;
using Garnet.common;
using Garnet.common.Numerics;

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
                    var firstKeyLength = checked((int)(srcEndPtrs[0] - srcStartPtrs[0]));
                    TensorPrimitives.OnesComplement(new ReadOnlySpan<byte>(srcStartPtrs[0], firstKeyLength), new Span<byte>(dstPtr, dstLen));
                    break;
                case (byte)BitmapOperation.AND:
                    InvokeMultiKeyBitwise<BitwiseAndOperator>(dstPtr, dstLen, srcStartPtrs, srcEndPtrs, srcKeyCount, minSize);
                    break;
                case (byte)BitmapOperation.OR:
                    InvokeMultiKeyBitwise<BitwiseOrOperator>(dstPtr, dstLen, srcStartPtrs, srcEndPtrs, srcKeyCount, minSize);
                    break;
                case (byte)BitmapOperation.XOR:
                    InvokeMultiKeyBitwise<BitwiseXorOperator>(dstPtr, dstLen, srcStartPtrs, srcEndPtrs, srcKeyCount, minSize);
                    break;
                default:
                    throw new GarnetException("Unsupported BitOp command");
            }
            return true;
        }

        /// <summary>
        /// Invokes bitwise binary operation for multiple keys.
        /// </summary>
        /// <typeparam name="TBinaryOperator">The binary operator type to compute bitwise</typeparam>
        /// <param name="dstPtr">Output buffer to write BitOp result</param>
        /// <param name="dstLength">Output buffer length.</param>
        /// <param name="srcStartPtrs">Pointer to start of bitmap sources.</param>
        /// <param name="srcEndPtrs">Pointer to end of bitmap sources</param>
        /// <param name="srcKeyCount">Number of source keys.</param>
        /// <param name="minLength">Minimum length of source bitmaps.</param>
        private static void InvokeMultiKeyBitwise<TBinaryOperator>(byte* dstPtr, int dstLength, byte** srcStartPtrs, byte** srcEndPtrs, int srcKeyCount, int minLength)
            where TBinaryOperator : struct, IBinaryOperator
        {
            var dstEndPtr = dstPtr + dstLength;

            long remainingLength = minLength;
            long batchRemainder = minLength;
            byte* dstBatchEndPtr;

            ref var firstKeyPtr = ref srcStartPtrs[0];

            if (Vector256.IsHardwareAccelerated && Vector256<byte>.IsSupported)
            {
                // Vectorized: 32 bytes x 8
                batchRemainder = remainingLength & ((Vector256<byte>.Count * 8) - 1);
                dstBatchEndPtr = dstPtr + (remainingLength - batchRemainder);
                remainingLength = batchRemainder;

                Vectorized256(ref firstKeyPtr, srcStartPtrs, srcKeyCount, ref dstPtr, dstBatchEndPtr);
            }
            else if (Vector128.IsHardwareAccelerated && Vector128<byte>.IsSupported)
            {
                // Vectorized: 16 bytes x 8
                batchRemainder = remainingLength & ((Vector128<byte>.Count * 8) - 1);
                dstBatchEndPtr = dstPtr + (remainingLength - batchRemainder);
                remainingLength = batchRemainder;

                Vectorized128(ref firstKeyPtr, srcStartPtrs, srcKeyCount, ref dstPtr, dstBatchEndPtr);
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
                    ref var keyStartPtr = ref srcStartPtrs[i];

                    d00 = TBinaryOperator.Invoke(d00, *(ulong*)(keyStartPtr + (sizeof(ulong) * 0)));
                    d01 = TBinaryOperator.Invoke(d01, *(ulong*)(keyStartPtr + (sizeof(ulong) * 1)));
                    d02 = TBinaryOperator.Invoke(d02, *(ulong*)(keyStartPtr + (sizeof(ulong) * 2)));
                    d03 = TBinaryOperator.Invoke(d03, *(ulong*)(keyStartPtr + (sizeof(ulong) * 3)));

                    srcStartPtrs[i] += sizeof(ulong) * 4;
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