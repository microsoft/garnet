// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Numerics;
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
    }
}