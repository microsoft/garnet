// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Tsavorite.core
{
    internal static class TsavoriteEqualityComparer
    {
        public static ITsavoriteEqualityComparer<T> Get<T>()
        {
            if (typeof(T) == typeof(string))
                return (ITsavoriteEqualityComparer<T>)(object)StringTsavoriteEqualityComparer.Instance;
            else if (typeof(T) == typeof(byte[]))
                return (ITsavoriteEqualityComparer<T>)(object)ByteArrayTsavoriteEqualityComparer.Instance;
            else if (typeof(T) == typeof(long))
                return (ITsavoriteEqualityComparer<T>)(object)LongTsavoriteEqualityComparer.Instance;
            else if (typeof(T) == typeof(int))
                return (ITsavoriteEqualityComparer<T>)(object)IntTsavoriteEqualityComparer.Instance;
            else if (typeof(T) == typeof(Guid))
                return (ITsavoriteEqualityComparer<T>)(object)GuidTsavoriteEqualityComparer.Instance;
            else if (typeof(T) == typeof(SpanByte))
                return (ITsavoriteEqualityComparer<T>)(object)SpanByteComparer.Instance;
            else
            {
                Debug.WriteLine("***WARNING*** Creating default Tsavorite key equality comparer based on potentially slow EqualityComparer<Key>.Default."
                               + "To avoid this, provide a comparer (ITsavoriteEqualityComparer<Key>) as an argument to Tsavorite's constructor, or make Key implement the interface ITsavoriteEqualityComparer<Key>");
                return DefaultTsavoriteEqualityComparer<T>.Instance;
            }
        }
    }

    /// <summary>
    /// Deterministic equality comparer for strings
    /// </summary>
    public sealed class StringTsavoriteEqualityComparer : ITsavoriteEqualityComparer<string>
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly StringTsavoriteEqualityComparer Instance = new();

        /// <inheritdoc />
        public bool Equals(ref string key1, ref string key2)
        {
            // Use locals in case the record space is cleared.
            string k1 = key1, k2 = key2;
            return (k1 is null || k2 is null) ? false : k1 == k2;
        }

        /// <inheritdoc />
        public unsafe long GetHashCode64(ref string key)
        {
            // Use locals in case the record space is cleared.
            string k = key;
            if (k is null)
                return 0;

            fixed (char* c = k)
            {
                return Utility.HashBytes((byte*)c, key.Length * sizeof(char));
            }
        }
    }

    /// <summary>
    /// Deterministic equality comparer for longs
    /// </summary>
    public sealed class LongTsavoriteEqualityComparer : ITsavoriteEqualityComparer<long>
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly LongTsavoriteEqualityComparer Instance = new();

        /// <inheritdoc />
        public bool Equals(ref long k1, ref long k2) => k1 == k2;

        /// <inheritdoc />
        public long GetHashCode64(ref long k) => Utility.GetHashCode(k);
    }

    /// <summary>
    /// Deterministic equality comparer for longs
    /// </summary>
    public sealed class IntTsavoriteEqualityComparer : ITsavoriteEqualityComparer<int>
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly IntTsavoriteEqualityComparer Instance = new();

        /// <inheritdoc />
        public bool Equals(ref int k1, ref int k2) => k1 == k2;

        /// <inheritdoc />
        public long GetHashCode64(ref int k) => Utility.GetHashCode(k);
    }

    /// <summary>
    /// Deterministic equality comparer for longs
    /// </summary>
    public sealed class GuidTsavoriteEqualityComparer : ITsavoriteEqualityComparer<Guid>
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly GuidTsavoriteEqualityComparer Instance = new();

        /// <inheritdoc />
        public bool Equals(ref Guid k1, ref Guid k2) => k1 == k2;

        /// <inheritdoc />
        public unsafe long GetHashCode64(ref Guid k)
        {
            var _k = k;
            var pGuid = (long*)&_k;
            return pGuid[0] ^ pGuid[1];
        }
    }

    /// <summary>
    /// Deterministic equality comparer for byte[]
    /// </summary>
    public sealed class ByteArrayTsavoriteEqualityComparer : ITsavoriteEqualityComparer<byte[]>
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly ByteArrayTsavoriteEqualityComparer Instance = new();

        /// <inheritdoc />
        public bool Equals(ref byte[] key1, ref byte[] key2) => key1.AsSpan().SequenceEqual(key2);

        /// <inheritdoc />
        public unsafe long GetHashCode64(ref byte[] key)
        {
            // Use locals in case the record space is cleared.
            byte[] k = key;
            if (k is null)
                return 0;

            fixed (byte* b = k)
            {
                return Utility.HashBytes(b, k.Length);
            }
        }
    }

    /// <summary>
    /// Low-performance Tsavorite equality comparer wrapper around EqualityComparer.Default
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal sealed class DefaultTsavoriteEqualityComparer<T> : ITsavoriteEqualityComparer<T>
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly DefaultTsavoriteEqualityComparer<T> Instance = new();

        private static readonly EqualityComparer<T> DefaultEC = EqualityComparer<T>.Default;

        /// <inheritdoc />
        public bool Equals(ref T k1, ref T k2) => DefaultEC.Equals(k1, k2);

        /// <inheritdoc />
        public long GetHashCode64(ref T k) => Utility.GetHashCode(DefaultEC.GetHashCode(k));
    }
}