// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;

namespace Garnet.common.Numerics
{
    public static unsafe partial class TensorPrimitives
    {
        /// <summary>x &amp; y</summary>
        public readonly struct BitwiseAndOperator<T> : IBinaryOperator<T> where T : IBitwiseOperators<T, T, T>
        {
            public static T Invoke(T x, T y) => x & y;
            public static Vector128<T> Invoke(Vector128<T> x, Vector128<T> y) => x & y;
            public static Vector256<T> Invoke(Vector256<T> x, Vector256<T> y) => x & y;
            public static Vector512<T> Invoke(Vector512<T> x, Vector512<T> y) => x & y;
        }

        /// <summary>x | y</summary>
        public readonly struct BitwiseOrOperator<T> : IBinaryOperator<T> where T : IBitwiseOperators<T, T, T>
        {
            public static T Invoke(T x, T y) => x | y;
            public static Vector128<T> Invoke(Vector128<T> x, Vector128<T> y) => x | y;
            public static Vector256<T> Invoke(Vector256<T> x, Vector256<T> y) => x | y;
            public static Vector512<T> Invoke(Vector512<T> x, Vector512<T> y) => x | y;
        }

        /// <summary>x ^ y</summary>
        public readonly struct BitwiseXorOperator<T> : IBinaryOperator<T> where T : IBitwiseOperators<T, T, T>
        {
            public static T Invoke(T x, T y) => x ^ y;
            public static Vector128<T> Invoke(Vector128<T> x, Vector128<T> y) => x ^ y;
            public static Vector256<T> Invoke(Vector256<T> x, Vector256<T> y) => x ^ y;
            public static Vector512<T> Invoke(Vector512<T> x, Vector512<T> y) => x ^ y;
        }

        /// <summary>Operator that takes two input values and returns a single value.</summary>
        public interface IBinaryOperator<T>
        {
            static abstract T Invoke(T x, T y);
            static abstract Vector128<T> Invoke(Vector128<T> x, Vector128<T> y);
            static abstract Vector256<T> Invoke(Vector256<T> x, Vector256<T> y);
            static abstract Vector512<T> Invoke(Vector512<T> x, Vector512<T> y);
        }

        // TODO: Remove, no attempt to use yet in this PR
        public static void UnsafeInvokeOperator<T, TBinaryOperator>(
            T* xPtr, T* yPtr, T* dPtr, int length)
            where T : unmanaged
            where TBinaryOperator : struct, IBinaryOperator<T>
        {
            // Since every branch has a cost and since that cost is
            // essentially lost for larger inputs, we do branches
            // in a way that allows us to have the minimum possible
            // for small sizes

            nuint remainder = (uint)length;

            if (Vector512.IsHardwareAccelerated && Vector512<T>.IsSupported)
            {
                if (remainder >= (uint)Vector512<T>.Count)
                {
                    Vectorized512(ref xPtr, ref yPtr, ref dPtr, remainder);
                }
                else
                {
                    // We have less than a vector and so we can only handle this as scalar. To do this
                    // efficiently, we simply have a small jump table and fallthrough. So we get a simple
                    // length check, single jump, and then linear execution.

                    VectorizedSmall(ref xPtr, ref yPtr, ref dPtr, remainder);
                }

                return;
            }

            if (Vector256.IsHardwareAccelerated && Vector256<T>.IsSupported)
            {
                if (remainder >= (uint)Vector256<T>.Count)
                {
                    Vectorized256(ref xPtr, ref yPtr, ref dPtr, remainder);
                }
                else
                {
                    // We have less than a vector and so we can only handle this as scalar. To do this
                    // efficiently, we simply have a small jump table and fallthrough. So we get a simple
                    // length check, single jump, and then linear execution.

                    VectorizedSmall(ref xPtr, ref yPtr, ref dPtr, remainder);
                }

                return;
            }

            if (Vector128.IsHardwareAccelerated && Vector128<T>.IsSupported)
            {
                if (remainder >= (uint)Vector128<T>.Count)
                {
                    Vectorized128(ref xPtr, ref yPtr, ref dPtr, remainder);
                }
                else
                {
                    // We have less than a vector and so we can only handle this as scalar. To do this
                    // efficiently, we simply have a small jump table and fallthrough. So we get a simple
                    // length check, single jump, and then linear execution.

                    VectorizedSmall(ref xPtr, ref yPtr, ref dPtr, remainder);
                }

                return;
            }

            // This is the software fallback when no acceleration is available
            // It requires no branches to hit

            SoftwareFallback(xPtr, yPtr, dPtr, remainder);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static void SoftwareFallback(T* xPtr, T* yPtr, T* dPtr, nuint length)
            {
                for (nuint i = 0; i < length; i++)
                {
                    *(dPtr + i) = TBinaryOperator.Invoke(*(xPtr + i), *(yPtr + i));
                }
            }

            static void Vectorized128(ref T* xPtr, ref T* yPtr, ref T* dPtr, nuint remainder)
            {
                ref T* dPtrBeg = ref dPtr;

                // Preload the beginning and end so that overlapping accesses don't negatively impact the data

                Vector128<T> beg = TBinaryOperator.Invoke(Vector128.Load(xPtr), Vector128.Load(yPtr));
                Vector128<T> end = TBinaryOperator.Invoke(Vector128.Load(xPtr + remainder - (uint)Vector128<T>.Count),
                                                          Vector128.Load(yPtr + remainder - (uint)Vector128<T>.Count));

                if (remainder > (uint)(Vector128<T>.Count * 8))
                {
                    // We need to the ensure the underlying data can be aligned and only align
                    // it if it can. It is possible we have an unaligned ref, in which case we
                    // can never achieve the required SIMD alignment.

                    bool canAlign = ((nuint)dPtr % (nuint)sizeof(T)) == 0;

                    if (canAlign)
                    {
                        // Compute by how many elements we're misaligned and adjust the pointers accordingly
                        //
                        // Noting that we are only actually aligning dPtr. This is because unaligned stores
                        // are more expensive than unaligned loads and aligning both is significantly more
                        // complex.

                        nuint misalignment = ((uint)sizeof(Vector128<T>) - ((nuint)dPtr % (uint)sizeof(Vector128<T>))) / (uint)sizeof(T);

                        xPtr += misalignment;
                        yPtr += misalignment;
                        dPtr += misalignment;

                        Debug.Assert(((nuint)dPtr % (uint)sizeof(Vector128<T>)) == 0);

                        remainder -= misalignment;
                    }

                    Vector128<T> vector1;
                    Vector128<T> vector2;
                    Vector128<T> vector3;
                    Vector128<T> vector4;

                    while (remainder >= (uint)(Vector128<T>.Count * 8))
                    {
                        // We load, process, and store the first four vectors

                        vector1 = TBinaryOperator.Invoke(Vector128.Load(xPtr + (uint)(Vector128<T>.Count * 0)),
                                                         Vector128.Load(yPtr + (uint)(Vector128<T>.Count * 0)));
                        vector2 = TBinaryOperator.Invoke(Vector128.Load(xPtr + (uint)(Vector128<T>.Count * 1)),
                                                         Vector128.Load(yPtr + (uint)(Vector128<T>.Count * 1)));
                        vector3 = TBinaryOperator.Invoke(Vector128.Load(xPtr + (uint)(Vector128<T>.Count * 2)),
                                                         Vector128.Load(yPtr + (uint)(Vector128<T>.Count * 2)));
                        vector4 = TBinaryOperator.Invoke(Vector128.Load(xPtr + (uint)(Vector128<T>.Count * 3)),
                                                         Vector128.Load(yPtr + (uint)(Vector128<T>.Count * 3)));

                        vector1.Store(dPtr + (uint)(Vector128<T>.Count * 0));
                        vector2.Store(dPtr + (uint)(Vector128<T>.Count * 1));
                        vector3.Store(dPtr + (uint)(Vector128<T>.Count * 2));
                        vector4.Store(dPtr + (uint)(Vector128<T>.Count * 3));

                        // We load, process, and store the next four vectors

                        vector1 = TBinaryOperator.Invoke(Vector128.Load(xPtr + (uint)(Vector128<T>.Count * 4)),
                                                         Vector128.Load(yPtr + (uint)(Vector128<T>.Count * 4)));
                        vector2 = TBinaryOperator.Invoke(Vector128.Load(xPtr + (uint)(Vector128<T>.Count * 5)),
                                                         Vector128.Load(yPtr + (uint)(Vector128<T>.Count * 5)));
                        vector3 = TBinaryOperator.Invoke(Vector128.Load(xPtr + (uint)(Vector128<T>.Count * 6)),
                                                         Vector128.Load(yPtr + (uint)(Vector128<T>.Count * 6)));
                        vector4 = TBinaryOperator.Invoke(Vector128.Load(xPtr + (uint)(Vector128<T>.Count * 7)),
                                                         Vector128.Load(yPtr + (uint)(Vector128<T>.Count * 7)));

                        vector1.Store(dPtr + (uint)(Vector128<T>.Count * 4));
                        vector2.Store(dPtr + (uint)(Vector128<T>.Count * 5));
                        vector3.Store(dPtr + (uint)(Vector128<T>.Count * 6));
                        vector4.Store(dPtr + (uint)(Vector128<T>.Count * 7));

                        // We adjust the source and destination references, then update
                        // the count of remaining elements to process.

                        xPtr += (uint)(Vector128<T>.Count * 8);
                        yPtr += (uint)(Vector128<T>.Count * 8);
                        dPtr += (uint)(Vector128<T>.Count * 8);

                        remainder -= (uint)(Vector128<T>.Count * 8);
                    }
                }

                // Process the remaining [Count, Count * 8] elements via a jump table
                //
                // Unless the original length was an exact multiple of Count, then we'll
                // end up reprocessing a couple elements in case 1 for end. We'll also
                // potentially reprocess a few elements in case 0 for beg, to handle any
                // data before the first aligned address.

                nuint endIndex = remainder;
                remainder = (remainder + (uint)(Vector128<T>.Count - 1)) & (nuint)(-Vector128<T>.Count);

                switch (remainder / (uint)Vector128<T>.Count)
                {
                    case 8:
                        {
                            Vector128<T> vector = TBinaryOperator.Invoke(Vector128.Load(xPtr + remainder - (uint)(Vector128<T>.Count * 8)),
                                                                         Vector128.Load(yPtr + remainder - (uint)(Vector128<T>.Count * 8)));
                            vector.Store(dPtr + remainder - (uint)(Vector128<T>.Count * 8));
                            goto case 7;
                        }

                    case 7:
                        {
                            Vector128<T> vector = TBinaryOperator.Invoke(Vector128.Load(xPtr + remainder - (uint)(Vector128<T>.Count * 7)),
                                                                         Vector128.Load(yPtr + remainder - (uint)(Vector128<T>.Count * 7)));
                            vector.Store(dPtr + remainder - (uint)(Vector128<T>.Count * 7));
                            goto case 6;
                        }

                    case 6:
                        {
                            Vector128<T> vector = TBinaryOperator.Invoke(Vector128.Load(xPtr + remainder - (uint)(Vector128<T>.Count * 6)),
                                                                         Vector128.Load(yPtr + remainder - (uint)(Vector128<T>.Count * 6)));
                            vector.Store(dPtr + remainder - (uint)(Vector128<T>.Count * 6));
                            goto case 5;
                        }

                    case 5:
                        {
                            Vector128<T> vector = TBinaryOperator.Invoke(Vector128.Load(xPtr + remainder - (uint)(Vector128<T>.Count * 5)),
                                                                         Vector128.Load(yPtr + remainder - (uint)(Vector128<T>.Count * 5)));
                            vector.Store(dPtr + remainder - (uint)(Vector128<T>.Count * 5));
                            goto case 4;
                        }

                    case 4:
                        {
                            Vector128<T> vector = TBinaryOperator.Invoke(Vector128.Load(xPtr + remainder - (uint)(Vector128<T>.Count * 4)),
                                                                         Vector128.Load(yPtr + remainder - (uint)(Vector128<T>.Count * 4)));
                            vector.Store(dPtr + remainder - (uint)(Vector128<T>.Count * 4));
                            goto case 3;
                        }

                    case 3:
                        {
                            Vector128<T> vector = TBinaryOperator.Invoke(Vector128.Load(xPtr + remainder - (uint)(Vector128<T>.Count * 3)),
                                                                         Vector128.Load(yPtr + remainder - (uint)(Vector128<T>.Count * 3)));
                            vector.Store(dPtr + remainder - (uint)(Vector128<T>.Count * 3));
                            goto case 2;
                        }

                    case 2:
                        {
                            Vector128<T> vector = TBinaryOperator.Invoke(Vector128.Load(xPtr + remainder - (uint)(Vector128<T>.Count * 2)),
                                                                         Vector128.Load(yPtr + remainder - (uint)(Vector128<T>.Count * 2)));
                            vector.Store(dPtr + remainder - (uint)(Vector128<T>.Count * 2));
                            goto case 1;
                        }

                    case 1:
                        {
                            // Store the last block, which includes any elements that wouldn't fill a full vector
                            end.Store(dPtr + endIndex - (uint)Vector128<T>.Count);
                            goto case 0;
                        }

                    case 0:
                        {
                            // Store the first block, which includes any elements preceding the first aligned block
                            beg.Store(dPtrBeg);
                            break;
                        }
                }
            }

            static void Vectorized256(ref T* xPtr, ref T* yPtr, ref T* dPtr, nuint remainder)
            {
                ref T* dPtrBeg = ref dPtr;

                // Preload the beginning and end so that overlapping accesses don't negatively impact the data

                Vector256<T> beg = TBinaryOperator.Invoke(Vector256.Load(xPtr), Vector256.Load(yPtr));
                Vector256<T> end = TBinaryOperator.Invoke(Vector256.Load(xPtr + remainder - (uint)Vector256<T>.Count),
                                                          Vector256.Load(yPtr + remainder - (uint)Vector256<T>.Count));

                if (remainder > (uint)(Vector256<T>.Count * 8))
                {
                    // We need to the ensure the underlying data can be aligned and only align
                    // it if it can. It is possible we have an unaligned ref, in which case we
                    // can never achieve the required SIMD alignment.

                    bool canAlign = ((nuint)dPtr % (nuint)sizeof(T)) == 0;

                    if (canAlign)
                    {
                        // Compute by how many elements we're misaligned and adjust the pointers accordingly
                        //
                        // Noting that we are only actually aligning dPtr. This is because unaligned stores
                        // are more expensive than unaligned loads and aligning both is significantly more
                        // complex.

                        nuint misalignment = ((uint)sizeof(Vector256<T>) - ((nuint)dPtr % (uint)sizeof(Vector256<T>))) / (uint)sizeof(T);

                        xPtr += misalignment;
                        yPtr += misalignment;
                        dPtr += misalignment;

                        Debug.Assert(((nuint)dPtr % (uint)sizeof(Vector256<T>)) == 0);

                        remainder -= misalignment;
                    }

                    Vector256<T> vector1;
                    Vector256<T> vector2;
                    Vector256<T> vector3;
                    Vector256<T> vector4;

                    while (remainder >= (uint)(Vector256<T>.Count * 8))
                    {
                        // We load, process, and store the first four vectors

                        vector1 = TBinaryOperator.Invoke(Vector256.Load(xPtr + (uint)(Vector256<T>.Count * 0)),
                                                         Vector256.Load(yPtr + (uint)(Vector256<T>.Count * 0)));
                        vector2 = TBinaryOperator.Invoke(Vector256.Load(xPtr + (uint)(Vector256<T>.Count * 1)),
                                                         Vector256.Load(yPtr + (uint)(Vector256<T>.Count * 1)));
                        vector3 = TBinaryOperator.Invoke(Vector256.Load(xPtr + (uint)(Vector256<T>.Count * 2)),
                                                         Vector256.Load(yPtr + (uint)(Vector256<T>.Count * 2)));
                        vector4 = TBinaryOperator.Invoke(Vector256.Load(xPtr + (uint)(Vector256<T>.Count * 3)),
                                                         Vector256.Load(yPtr + (uint)(Vector256<T>.Count * 3)));

                        vector1.Store(dPtr + (uint)(Vector256<T>.Count * 0));
                        vector2.Store(dPtr + (uint)(Vector256<T>.Count * 1));
                        vector3.Store(dPtr + (uint)(Vector256<T>.Count * 2));
                        vector4.Store(dPtr + (uint)(Vector256<T>.Count * 3));

                        // We load, process, and store the next four vectors

                        vector1 = TBinaryOperator.Invoke(Vector256.Load(xPtr + (uint)(Vector256<T>.Count * 4)),
                                                         Vector256.Load(yPtr + (uint)(Vector256<T>.Count * 4)));
                        vector2 = TBinaryOperator.Invoke(Vector256.Load(xPtr + (uint)(Vector256<T>.Count * 5)),
                                                         Vector256.Load(yPtr + (uint)(Vector256<T>.Count * 5)));
                        vector3 = TBinaryOperator.Invoke(Vector256.Load(xPtr + (uint)(Vector256<T>.Count * 6)),
                                                         Vector256.Load(yPtr + (uint)(Vector256<T>.Count * 6)));
                        vector4 = TBinaryOperator.Invoke(Vector256.Load(xPtr + (uint)(Vector256<T>.Count * 7)),
                                                         Vector256.Load(yPtr + (uint)(Vector256<T>.Count * 7)));

                        vector1.Store(dPtr + (uint)(Vector256<T>.Count * 4));
                        vector2.Store(dPtr + (uint)(Vector256<T>.Count * 5));
                        vector3.Store(dPtr + (uint)(Vector256<T>.Count * 6));
                        vector4.Store(dPtr + (uint)(Vector256<T>.Count * 7));

                        // We adjust the source and destination references, then update
                        // the count of remaining elements to process.

                        xPtr += (uint)(Vector256<T>.Count * 8);
                        yPtr += (uint)(Vector256<T>.Count * 8);
                        dPtr += (uint)(Vector256<T>.Count * 8);

                        remainder -= (uint)(Vector256<T>.Count * 8);
                    }
                }

                // Process the remaining [Count, Count * 8] elements via a jump table
                //
                // Unless the original length was an exact multiple of Count, then we'll
                // end up reprocessing a couple elements in case 1 for end. We'll also
                // potentially reprocess a few elements in case 0 for beg, to handle any
                // data before the first aligned address.

                nuint endIndex = remainder;
                remainder = (remainder + (uint)(Vector256<T>.Count - 1)) & (nuint)(-Vector256<T>.Count);

                switch (remainder / (uint)Vector256<T>.Count)
                {
                    case 8:
                        {
                            Vector256<T> vector = TBinaryOperator.Invoke(Vector256.Load(xPtr + remainder - (uint)(Vector256<T>.Count * 8)),
                                                                         Vector256.Load(yPtr + remainder - (uint)(Vector256<T>.Count * 8)));
                            vector.Store(dPtr + remainder - (uint)(Vector256<T>.Count * 8));
                            goto case 7;
                        }

                    case 7:
                        {
                            Vector256<T> vector = TBinaryOperator.Invoke(Vector256.Load(xPtr + remainder - (uint)(Vector256<T>.Count * 7)),
                                                                         Vector256.Load(yPtr + remainder - (uint)(Vector256<T>.Count * 7)));
                            vector.Store(dPtr + remainder - (uint)(Vector256<T>.Count * 7));
                            goto case 6;
                        }

                    case 6:
                        {
                            Vector256<T> vector = TBinaryOperator.Invoke(Vector256.Load(xPtr + remainder - (uint)(Vector256<T>.Count * 6)),
                                                                         Vector256.Load(yPtr + remainder - (uint)(Vector256<T>.Count * 6)));
                            vector.Store(dPtr + remainder - (uint)(Vector256<T>.Count * 6));
                            goto case 5;
                        }

                    case 5:
                        {
                            Vector256<T> vector = TBinaryOperator.Invoke(Vector256.Load(xPtr + remainder - (uint)(Vector256<T>.Count * 5)),
                                                                         Vector256.Load(yPtr + remainder - (uint)(Vector256<T>.Count * 5)));
                            vector.Store(dPtr + remainder - (uint)(Vector256<T>.Count * 5));
                            goto case 4;
                        }

                    case 4:
                        {
                            Vector256<T> vector = TBinaryOperator.Invoke(Vector256.Load(xPtr + remainder - (uint)(Vector256<T>.Count * 4)),
                                                                         Vector256.Load(yPtr + remainder - (uint)(Vector256<T>.Count * 4)));
                            vector.Store(dPtr + remainder - (uint)(Vector256<T>.Count * 4));
                            goto case 3;
                        }

                    case 3:
                        {
                            Vector256<T> vector = TBinaryOperator.Invoke(Vector256.Load(xPtr + remainder - (uint)(Vector256<T>.Count * 3)),
                                                                         Vector256.Load(yPtr + remainder - (uint)(Vector256<T>.Count * 3)));
                            vector.Store(dPtr + remainder - (uint)(Vector256<T>.Count * 3));
                            goto case 2;
                        }

                    case 2:
                        {
                            Vector256<T> vector = TBinaryOperator.Invoke(Vector256.Load(xPtr + remainder - (uint)(Vector256<T>.Count * 2)),
                                                                         Vector256.Load(yPtr + remainder - (uint)(Vector256<T>.Count * 2)));
                            vector.Store(dPtr + remainder - (uint)(Vector256<T>.Count * 2));
                            goto case 1;
                        }

                    case 1:
                        {
                            // Store the last block, which includes any elements that wouldn't fill a full vector
                            end.Store(dPtr + endIndex - (uint)Vector256<T>.Count);
                            goto case 0;
                        }

                    case 0:
                        {
                            // Store the first block, which includes any elements preceding the first aligned block
                            beg.Store(dPtrBeg);
                            break;
                        }
                }
            }

            static void Vectorized512(ref T* xPtr, ref T* yPtr, ref T* dPtr, nuint remainder)
            {
                ref T* dPtrBeg = ref dPtr;

                // Preload the beginning and end so that overlapping accesses don't negatively impact the data

                Vector512<T> beg = TBinaryOperator.Invoke(Vector512.Load(xPtr), Vector512.Load(yPtr));
                Vector512<T> end = TBinaryOperator.Invoke(Vector512.Load(xPtr + remainder - (uint)Vector512<T>.Count),
                                                          Vector512.Load(yPtr + remainder - (uint)Vector512<T>.Count));

                if (remainder > (uint)(Vector512<T>.Count * 8))
                {
                    // We need to the ensure the underlying data can be aligned and only align
                    // it if it can. It is possible we have an unaligned ref, in which case we
                    // can never achieve the required SIMD alignment.

                    bool canAlign = ((nuint)dPtr % (nuint)sizeof(T)) == 0;

                    if (canAlign)
                    {
                        // Compute by how many elements we're misaligned and adjust the pointers accordingly
                        //
                        // Noting that we are only actually aligning dPtr. This is because unaligned stores
                        // are more expensive than unaligned loads and aligning both is significantly more
                        // complex.

                        nuint misalignment = ((uint)sizeof(Vector512<T>) - ((nuint)dPtr % (uint)sizeof(Vector512<T>))) / (uint)sizeof(T);

                        xPtr += misalignment;
                        yPtr += misalignment;
                        dPtr += misalignment;

                        Debug.Assert(((nuint)dPtr % (uint)sizeof(Vector512<T>)) == 0);

                        remainder -= misalignment;
                    }

                    Vector512<T> vector1;
                    Vector512<T> vector2;
                    Vector512<T> vector3;
                    Vector512<T> vector4;

                    while (remainder >= (uint)(Vector512<T>.Count * 8))
                    {
                        // We load, process, and store the first four vectors

                        vector1 = TBinaryOperator.Invoke(Vector512.Load(xPtr + (uint)(Vector512<T>.Count * 0)),
                                                         Vector512.Load(yPtr + (uint)(Vector512<T>.Count * 0)));
                        vector2 = TBinaryOperator.Invoke(Vector512.Load(xPtr + (uint)(Vector512<T>.Count * 1)),
                                                         Vector512.Load(yPtr + (uint)(Vector512<T>.Count * 1)));
                        vector3 = TBinaryOperator.Invoke(Vector512.Load(xPtr + (uint)(Vector512<T>.Count * 2)),
                                                         Vector512.Load(yPtr + (uint)(Vector512<T>.Count * 2)));
                        vector4 = TBinaryOperator.Invoke(Vector512.Load(xPtr + (uint)(Vector512<T>.Count * 3)),
                                                         Vector512.Load(yPtr + (uint)(Vector512<T>.Count * 3)));

                        vector1.Store(dPtr + (uint)(Vector512<T>.Count * 0));
                        vector2.Store(dPtr + (uint)(Vector512<T>.Count * 1));
                        vector3.Store(dPtr + (uint)(Vector512<T>.Count * 2));
                        vector4.Store(dPtr + (uint)(Vector512<T>.Count * 3));

                        // We load, process, and store the next four vectors

                        vector1 = TBinaryOperator.Invoke(Vector512.Load(xPtr + (uint)(Vector512<T>.Count * 4)),
                                                         Vector512.Load(yPtr + (uint)(Vector512<T>.Count * 4)));
                        vector2 = TBinaryOperator.Invoke(Vector512.Load(xPtr + (uint)(Vector512<T>.Count * 5)),
                                                         Vector512.Load(yPtr + (uint)(Vector512<T>.Count * 5)));
                        vector3 = TBinaryOperator.Invoke(Vector512.Load(xPtr + (uint)(Vector512<T>.Count * 6)),
                                                         Vector512.Load(yPtr + (uint)(Vector512<T>.Count * 6)));
                        vector4 = TBinaryOperator.Invoke(Vector512.Load(xPtr + (uint)(Vector512<T>.Count * 7)),
                                                         Vector512.Load(yPtr + (uint)(Vector512<T>.Count * 7)));

                        vector1.Store(dPtr + (uint)(Vector512<T>.Count * 4));
                        vector2.Store(dPtr + (uint)(Vector512<T>.Count * 5));
                        vector3.Store(dPtr + (uint)(Vector512<T>.Count * 6));
                        vector4.Store(dPtr + (uint)(Vector512<T>.Count * 7));

                        // We adjust the source and destination references, then update
                        // the count of remaining elements to process.

                        xPtr += (uint)(Vector512<T>.Count * 8);
                        yPtr += (uint)(Vector512<T>.Count * 8);
                        dPtr += (uint)(Vector512<T>.Count * 8);

                        remainder -= (uint)(Vector512<T>.Count * 8);
                    }
                }

                // Process the remaining [Count, Count * 8] elements via a jump table
                //
                // Unless the original length was an exact multiple of Count, then we'll
                // end up reprocessing a couple elements in case 1 for end. We'll also
                // potentially reprocess a few elements in case 0 for beg, to handle any
                // data before the first aligned address.

                nuint endIndex = remainder;
                remainder = (remainder + (uint)(Vector512<T>.Count - 1)) & (nuint)(-Vector512<T>.Count);

                switch (remainder / (uint)Vector512<T>.Count)
                {
                    case 8:
                        {
                            Vector512<T> vector = TBinaryOperator.Invoke(Vector512.Load(xPtr + remainder - (uint)(Vector512<T>.Count * 8)),
                                                                         Vector512.Load(yPtr + remainder - (uint)(Vector512<T>.Count * 8)));
                            vector.Store(dPtr + remainder - (uint)(Vector512<T>.Count * 8));
                            goto case 7;
                        }

                    case 7:
                        {
                            Vector512<T> vector = TBinaryOperator.Invoke(Vector512.Load(xPtr + remainder - (uint)(Vector512<T>.Count * 7)),
                                                                         Vector512.Load(yPtr + remainder - (uint)(Vector512<T>.Count * 7)));
                            vector.Store(dPtr + remainder - (uint)(Vector512<T>.Count * 7));
                            goto case 6;
                        }

                    case 6:
                        {
                            Vector512<T> vector = TBinaryOperator.Invoke(Vector512.Load(xPtr + remainder - (uint)(Vector512<T>.Count * 6)),
                                                                         Vector512.Load(yPtr + remainder - (uint)(Vector512<T>.Count * 6)));
                            vector.Store(dPtr + remainder - (uint)(Vector512<T>.Count * 6));
                            goto case 5;
                        }

                    case 5:
                        {
                            Vector512<T> vector = TBinaryOperator.Invoke(Vector512.Load(xPtr + remainder - (uint)(Vector512<T>.Count * 5)),
                                                                         Vector512.Load(yPtr + remainder - (uint)(Vector512<T>.Count * 5)));
                            vector.Store(dPtr + remainder - (uint)(Vector512<T>.Count * 5));
                            goto case 4;
                        }

                    case 4:
                        {
                            Vector512<T> vector = TBinaryOperator.Invoke(Vector512.Load(xPtr + remainder - (uint)(Vector512<T>.Count * 4)),
                                                                         Vector512.Load(yPtr + remainder - (uint)(Vector512<T>.Count * 4)));
                            vector.Store(dPtr + remainder - (uint)(Vector512<T>.Count * 4));
                            goto case 3;
                        }

                    case 3:
                        {
                            Vector512<T> vector = TBinaryOperator.Invoke(Vector512.Load(xPtr + remainder - (uint)(Vector512<T>.Count * 3)),
                                                                         Vector512.Load(yPtr + remainder - (uint)(Vector512<T>.Count * 3)));
                            vector.Store(dPtr + remainder - (uint)(Vector512<T>.Count * 3));
                            goto case 2;
                        }

                    case 2:
                        {
                            Vector512<T> vector = TBinaryOperator.Invoke(Vector512.Load(xPtr + remainder - (uint)(Vector512<T>.Count * 2)),
                                                                         Vector512.Load(yPtr + remainder - (uint)(Vector512<T>.Count * 2)));
                            vector.Store(dPtr + remainder - (uint)(Vector512<T>.Count * 2));
                            goto case 1;
                        }

                    case 1:
                        {
                            // Store the last block, which includes any elements that wouldn't fill a full vector
                            end.Store(dPtr + endIndex - (uint)Vector512<T>.Count);
                            goto case 0;
                        }

                    case 0:
                        {
                            // Store the first block, which includes any elements preceding the first aligned block
                            beg.Store(dPtrBeg);
                            break;
                        }
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static void VectorizedSmall(ref T* xPtr, ref T* yPtr, ref T* dPtr, nuint remainder)
            {
                if (sizeof(T) == 1)
                {
                    VectorizedSmall1(ref xPtr, ref yPtr, ref dPtr, remainder);
                }
                else if (sizeof(T) == 2)
                {
                    VectorizedSmall2(ref xPtr, ref yPtr, ref dPtr, remainder);
                }
                else if (sizeof(T) == 4)
                {
                    VectorizedSmall4(ref xPtr, ref yPtr, ref dPtr, remainder);
                }
                else
                {
                    Debug.Assert(sizeof(T) == 8);
                    VectorizedSmall8(ref xPtr, ref yPtr, ref dPtr, remainder);
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static void VectorizedSmall1(ref T* xPtr, ref T* yPtr, ref T* dPtr, nuint remainder)
            {
                Debug.Assert(sizeof(T) == 1);

                switch (remainder)
                {
                    // Two Vector256's worth of data, with at least one element overlapping.
                    case 63:
                    case 62:
                    case 61:
                    case 60:
                    case 59:
                    case 58:
                    case 57:
                    case 56:
                    case 55:
                    case 54:
                    case 53:
                    case 52:
                    case 51:
                    case 50:
                    case 49:
                    case 48:
                    case 47:
                    case 46:
                    case 45:
                    case 44:
                    case 43:
                    case 42:
                    case 41:
                    case 40:
                    case 39:
                    case 38:
                    case 37:
                    case 36:
                    case 35:
                    case 34:
                    case 33:
                        {
                            Debug.Assert(Vector256.IsHardwareAccelerated);

                            Vector256<T> beg = TBinaryOperator.Invoke(Vector256.Load(xPtr), Vector256.Load(yPtr));
                            Vector256<T> end = TBinaryOperator.Invoke(Vector256.Load(xPtr + remainder - (uint)Vector256<T>.Count),
                                                                      Vector256.Load(yPtr + remainder - (uint)Vector256<T>.Count));

                            beg.Store(dPtr);
                            end.Store(dPtr + remainder - (uint)Vector256<T>.Count);

                            break;
                        }

                    // One Vector256's worth of data.
                    case 32:
                        {
                            Debug.Assert(Vector256.IsHardwareAccelerated);

                            Vector256<T> beg = TBinaryOperator.Invoke(Vector256.Load(xPtr), Vector256.Load(yPtr));
                            beg.Store(dPtr);

                            break;
                        }

                    // Two Vector128's worth of data, with at least one element overlapping.
                    case 31:
                    case 30:
                    case 29:
                    case 28:
                    case 27:
                    case 26:
                    case 25:
                    case 24:
                    case 23:
                    case 22:
                    case 21:
                    case 20:
                    case 19:
                    case 18:
                    case 17:
                        {
                            Debug.Assert(Vector128.IsHardwareAccelerated);

                            Vector128<T> beg = TBinaryOperator.Invoke(Vector128.Load(xPtr), Vector128.Load(yPtr));
                            Vector128<T> end = TBinaryOperator.Invoke(Vector128.Load(xPtr + remainder - (uint)Vector128<T>.Count),
                                                                      Vector128.Load(yPtr + remainder - (uint)Vector128<T>.Count));

                            beg.Store(dPtr);
                            end.Store(dPtr + remainder - (uint)Vector128<T>.Count);

                            break;
                        }

                    // One Vector128's worth of data.
                    case 16:
                        {
                            Debug.Assert(Vector128.IsHardwareAccelerated);

                            Vector128<T> beg = TBinaryOperator.Invoke(Vector128.Load(xPtr), Vector128.Load(yPtr));
                            beg.Store(dPtr);

                            break;
                        }

                    // Cases that are smaller than a single vector. No SIMD; just jump to the length and fall through each
                    // case to unroll the whole processing.
                    case 15:
                        *(dPtr + 14) = TBinaryOperator.Invoke(*(xPtr + 14), *(yPtr + 14));
                        goto case 14;

                    case 14:
                        *(dPtr + 13) = TBinaryOperator.Invoke(*(xPtr + 13), *(yPtr + 13));
                        goto case 13;

                    case 13:
                        *(dPtr + 12) = TBinaryOperator.Invoke(*(xPtr + 12), *(yPtr + 12));
                        goto case 12;

                    case 12:
                        *(dPtr + 11) = TBinaryOperator.Invoke(*(xPtr + 11), *(yPtr + 11));
                        goto case 11;

                    case 11:
                        *(dPtr + 10) = TBinaryOperator.Invoke(*(xPtr + 10), *(yPtr + 10));
                        goto case 10;

                    case 10:
                        *(dPtr + 9) = TBinaryOperator.Invoke(*(xPtr + 9), *(yPtr + 9));
                        goto case 9;

                    case 9:
                        *(dPtr + 8) = TBinaryOperator.Invoke(*(xPtr + 8), *(yPtr + 8));
                        goto case 8;

                    case 8:
                        *(dPtr + 7) = TBinaryOperator.Invoke(*(xPtr + 7), *(yPtr + 7));
                        goto case 7;

                    case 7:
                        *(dPtr + 6) = TBinaryOperator.Invoke(*(xPtr + 6), *(yPtr + 6));
                        goto case 6;

                    case 6:
                        *(dPtr + 5) = TBinaryOperator.Invoke(*(xPtr + 5), *(yPtr + 5));
                        goto case 5;

                    case 5:
                        *(dPtr + 4) = TBinaryOperator.Invoke(*(xPtr + 4), *(yPtr + 4));
                        goto case 4;

                    case 4:
                        *(dPtr + 3) = TBinaryOperator.Invoke(*(xPtr + 3), *(yPtr + 3));
                        goto case 3;

                    case 3:
                        *(dPtr + 2) = TBinaryOperator.Invoke(*(xPtr + 2), *(yPtr + 2));
                        goto case 2;

                    case 2:
                        *(dPtr + 1) = TBinaryOperator.Invoke(*(xPtr + 1), *(yPtr + 1));
                        goto case 1;

                    case 1:
                        *dPtr = TBinaryOperator.Invoke(*xPtr, *yPtr);
                        goto case 0;

                    case 0:
                        break;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static void VectorizedSmall2(ref T* xPtr, ref T* yPtr, ref T* dPtr, nuint remainder)
            {
                Debug.Assert(sizeof(T) == 2);

                switch (remainder)
                {
                    // Two Vector256's worth of data, with at least one element overlapping.
                    case 31:
                    case 30:
                    case 29:
                    case 28:
                    case 27:
                    case 26:
                    case 25:
                    case 24:
                    case 23:
                    case 22:
                    case 21:
                    case 20:
                    case 19:
                    case 18:
                    case 17:
                        {
                            Debug.Assert(Vector256.IsHardwareAccelerated);

                            Vector256<T> beg = TBinaryOperator.Invoke(Vector256.Load(xPtr), Vector256.Load(yPtr));
                            Vector256<T> end = TBinaryOperator.Invoke(Vector256.Load(xPtr + remainder - (uint)Vector256<T>.Count),
                                                                      Vector256.Load(yPtr + remainder - (uint)Vector256<T>.Count));

                            beg.Store(dPtr);
                            end.Store(dPtr + remainder - (uint)Vector256<T>.Count);

                            break;
                        }

                    // One Vector256's worth of data.
                    case 16:
                        {
                            Debug.Assert(Vector256.IsHardwareAccelerated);

                            Vector256<T> beg = TBinaryOperator.Invoke(Vector256.Load(xPtr), Vector256.Load(yPtr));
                            beg.Store(dPtr);

                            break;
                        }

                    // Two Vector128's worth of data, with at least one element overlapping.
                    case 15:
                    case 14:
                    case 13:
                    case 12:
                    case 11:
                    case 10:
                    case 9:
                        {
                            Debug.Assert(Vector128.IsHardwareAccelerated);

                            Vector128<T> beg = TBinaryOperator.Invoke(Vector128.Load(xPtr), Vector128.Load(yPtr));
                            Vector128<T> end = TBinaryOperator.Invoke(Vector128.Load(xPtr + remainder - (uint)Vector128<T>.Count),
                                                                      Vector128.Load(yPtr + remainder - (uint)Vector128<T>.Count));

                            beg.Store(dPtr);
                            end.Store(dPtr + remainder - (uint)Vector128<T>.Count);

                            break;
                        }

                    // One Vector128's worth of data.
                    case 8:
                        {
                            Debug.Assert(Vector128.IsHardwareAccelerated);

                            Vector128<T> beg = TBinaryOperator.Invoke(Vector128.Load(xPtr), Vector128.Load(yPtr));
                            beg.Store(dPtr);

                            break;
                        }

                    // Cases that are smaller than a single vector. No SIMD; just jump to the length and fall through each
                    // case to unroll the whole processing.
                    case 7:
                        *(dPtr + 6) = TBinaryOperator.Invoke(*(xPtr + 6), *(yPtr + 6));
                        goto case 6;

                    case 6:
                        *(dPtr + 5) = TBinaryOperator.Invoke(*(xPtr + 5), *(yPtr + 5));
                        goto case 5;

                    case 5:
                        *(dPtr + 4) = TBinaryOperator.Invoke(*(xPtr + 4), *(yPtr + 4));
                        goto case 4;

                    case 4:
                        *(dPtr + 3) = TBinaryOperator.Invoke(*(xPtr + 3), *(yPtr + 3));
                        goto case 3;

                    case 3:
                        *(dPtr + 2) = TBinaryOperator.Invoke(*(xPtr + 2), *(yPtr + 2));
                        goto case 2;

                    case 2:
                        *(dPtr + 1) = TBinaryOperator.Invoke(*(xPtr + 1), *(yPtr + 1));
                        goto case 1;

                    case 1:
                        *dPtr = TBinaryOperator.Invoke(*xPtr, *yPtr);
                        goto case 0;

                    case 0:
                        break;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static void VectorizedSmall4(ref T* xPtr, ref T* yPtr, ref T* dPtr, nuint remainder)
            {
                Debug.Assert(sizeof(T) == 4);

                switch (remainder)
                {
                    case 15:
                    case 14:
                    case 13:
                    case 12:
                    case 11:
                    case 10:
                    case 9:
                        {
                            Debug.Assert(Vector256.IsHardwareAccelerated);

                            Vector256<T> beg = TBinaryOperator.Invoke(Vector256.Load(xPtr), Vector256.Load(yPtr));
                            Vector256<T> end = TBinaryOperator.Invoke(Vector256.Load(xPtr + remainder - (uint)Vector256<T>.Count),
                                                                      Vector256.Load(yPtr + remainder - (uint)Vector256<T>.Count));

                            beg.Store(dPtr);
                            end.Store(dPtr + remainder - (uint)Vector256<T>.Count);

                            break;
                        }

                    case 8:
                        {
                            Debug.Assert(Vector256.IsHardwareAccelerated);

                            Vector256<T> beg = TBinaryOperator.Invoke(Vector256.Load(xPtr), Vector256.Load(yPtr));
                            beg.Store(dPtr);

                            break;
                        }

                    case 7:
                    case 6:
                    case 5:
                        {
                            Debug.Assert(Vector128.IsHardwareAccelerated);

                            Vector128<T> beg = TBinaryOperator.Invoke(Vector128.Load(xPtr), Vector128.Load(yPtr));
                            Vector128<T> end = TBinaryOperator.Invoke(Vector128.Load(xPtr + remainder - (uint)Vector128<T>.Count),
                                                                      Vector128.Load(yPtr + remainder - (uint)Vector128<T>.Count));

                            beg.Store(dPtr);
                            end.Store(dPtr + remainder - (uint)Vector128<T>.Count);

                            break;
                        }

                    case 4:
                        {
                            Debug.Assert(Vector128.IsHardwareAccelerated);

                            Vector128<T> beg = TBinaryOperator.Invoke(Vector128.Load(xPtr), Vector128.Load(yPtr));
                            beg.Store(dPtr);

                            break;
                        }

                    case 3:
                        {
                            *(dPtr + 2) = TBinaryOperator.Invoke(*(xPtr + 2), *(yPtr + 2));
                            goto case 2;
                        }

                    case 2:
                        {
                            *(dPtr + 1) = TBinaryOperator.Invoke(*(xPtr + 1), *(yPtr + 1));
                            goto case 1;
                        }

                    case 1:
                        {
                            *dPtr = TBinaryOperator.Invoke(*xPtr, *yPtr);
                            goto case 0;
                        }

                    case 0:
                        {
                            break;
                        }
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static void VectorizedSmall8(ref T* xPtr, ref T* yPtr, ref T* dPtr, nuint remainder)
            {
                Debug.Assert(sizeof(T) == 8);

                switch (remainder)
                {
                    case 7:
                    case 6:
                    case 5:
                        {
                            Debug.Assert(Vector256.IsHardwareAccelerated);

                            Vector256<T> beg = TBinaryOperator.Invoke(Vector256.Load(xPtr), Vector256.Load(yPtr));
                            Vector256<T> end = TBinaryOperator.Invoke(Vector256.Load(xPtr + remainder - (uint)Vector256<T>.Count),
                                                                      Vector256.Load(yPtr + remainder - (uint)Vector256<T>.Count));

                            beg.Store(dPtr);
                            end.Store(dPtr + remainder - (uint)Vector256<T>.Count);

                            break;
                        }

                    case 4:
                        {
                            Debug.Assert(Vector256.IsHardwareAccelerated);

                            Vector256<T> beg = TBinaryOperator.Invoke(Vector256.Load(xPtr), Vector256.Load(yPtr));
                            beg.Store(dPtr);

                            break;
                        }

                    case 3:
                        {
                            Debug.Assert(Vector128.IsHardwareAccelerated);

                            Vector128<T> beg = TBinaryOperator.Invoke(Vector128.Load(xPtr), Vector128.Load(yPtr));
                            Vector128<T> end = TBinaryOperator.Invoke(Vector128.Load(xPtr + remainder - (uint)Vector128<T>.Count),
                                                                      Vector128.Load(yPtr + remainder - (uint)Vector128<T>.Count));

                            beg.Store(dPtr);
                            end.Store(dPtr + remainder - (uint)Vector128<T>.Count);

                            break;
                        }

                    case 2:
                        {
                            Debug.Assert(Vector128.IsHardwareAccelerated);

                            Vector128<T> beg = TBinaryOperator.Invoke(Vector128.Load(xPtr), Vector128.Load(yPtr));
                            beg.Store(dPtr);

                            break;
                        }

                    case 1:
                        {
                            *dPtr = TBinaryOperator.Invoke(*xPtr, *yPtr);
                            goto case 0;
                        }

                    case 0:
                        {
                            break;
                        }
                }
            }
        }
    }
}