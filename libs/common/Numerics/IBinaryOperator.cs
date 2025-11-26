// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Numerics;
using System.Runtime.Intrinsics;

namespace Garnet.common.Numerics
{
    /// <summary>Operator that takes two input values and returns a single value.</summary>
    public interface IBinaryOperator
    {
        /// <summary>
        /// Computes the binary operation of two scalar values.
        /// </summary>
        static abstract T Invoke<T>(T x, T y) where T : IBinaryInteger<T>;

        /// <summary>
        /// Computes the binary operation of two vectors.
        /// </summary>
        static abstract Vector128<byte> Invoke(Vector128<byte> x, Vector128<byte> y);

        /// <inheritdoc cref="Invoke(Vector128{byte}, Vector128{byte})"/>
        static abstract Vector256<byte> Invoke(Vector256<byte> x, Vector256<byte> y);

        /// <inheritdoc cref="Invoke(Vector128{byte}, Vector128{byte})"/>
        static abstract Vector512<byte> Invoke(Vector512<byte> x, Vector512<byte> y);
    }

    /// <summary><c>x &amp; y</c></summary>
    public readonly struct BitwiseAndOperator : IBinaryOperator
    {
        /// <inheritdoc/>
        public static T Invoke<T>(T x, T y) where T : IBinaryInteger<T> => x & y;
        /// <inheritdoc/>
        public static Vector128<byte> Invoke(Vector128<byte> x, Vector128<byte> y) => x & y;
        /// <inheritdoc/>
        public static Vector256<byte> Invoke(Vector256<byte> x, Vector256<byte> y) => x & y;
        /// <inheritdoc/>
        public static Vector512<byte> Invoke(Vector512<byte> x, Vector512<byte> y) => x & y;
    }

    /// <summary><c>x | y</c></summary>
    public readonly struct BitwiseOrOperator : IBinaryOperator
    {
        /// <inheritdoc/>
        public static T Invoke<T>(T x, T y) where T : IBinaryInteger<T> => x | y;
        /// <inheritdoc/>
        public static Vector128<byte> Invoke(Vector128<byte> x, Vector128<byte> y) => x | y;
        /// <inheritdoc/>
        public static Vector256<byte> Invoke(Vector256<byte> x, Vector256<byte> y) => x | y;
        /// <inheritdoc/>
        public static Vector512<byte> Invoke(Vector512<byte> x, Vector512<byte> y) => x | y;
    }

    /// <summary><c>x ^ y</c></summary>
    public readonly struct BitwiseXorOperator : IBinaryOperator
    {
        /// <inheritdoc/>
        public static T Invoke<T>(T x, T y) where T : IBinaryInteger<T> => x ^ y;
        /// <inheritdoc/>
        public static Vector128<byte> Invoke(Vector128<byte> x, Vector128<byte> y) => x ^ y;
        /// <inheritdoc/>
        public static Vector256<byte> Invoke(Vector256<byte> x, Vector256<byte> y) => x ^ y;
        /// <inheritdoc/>
        public static Vector512<byte> Invoke(Vector512<byte> x, Vector512<byte> y) => x ^ y;
    }

    /// <summary><c>x &amp; ~y</c></summary>
    public readonly struct BitwiseAndNotOperator : IBinaryOperator
    {
        /// <inheritdoc/>
        public static T Invoke<T>(T x, T y) where T : IBinaryInteger<T> => x & ~y;
        /// <inheritdoc/>
        public static Vector128<byte> Invoke(Vector128<byte> x, Vector128<byte> y) => x & ~y;
        /// <inheritdoc/>
        public static Vector256<byte> Invoke(Vector256<byte> x, Vector256<byte> y) => x & ~y;
        /// <inheritdoc/>
        public static Vector512<byte> Invoke(Vector512<byte> x, Vector512<byte> y) => x & ~y;
    }
}