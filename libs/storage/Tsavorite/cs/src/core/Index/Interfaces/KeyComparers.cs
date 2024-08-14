// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Tsavorite.core
{
    internal static class KeyComparers
    {
        public static IKeyComparer<T> Get<T>()
        {
            if (typeof(T) == typeof(string))
                return (IKeyComparer<T>)(object)StringKeyComparer.Instance;
            else if (typeof(T) == typeof(byte[]))
                return (IKeyComparer<T>)(object)ByteArrayKeyComparer.Instance;
            else if (typeof(T) == typeof(long))
                return (IKeyComparer<T>)(object)LongKeyComparer.Instance;
            else if (typeof(T) == typeof(int))
                return (IKeyComparer<T>)(object)IntKeyComparer.Instance;
            else if (typeof(T) == typeof(Guid))
                return (IKeyComparer<T>)(object)GuidKeyComparer.Instance;
            else if (typeof(T) == typeof(SpanByte))
                return (IKeyComparer<T>)(object)SpanByteComparer.Instance;
            else
            {
                Debug.WriteLine("***WARNING*** Creating default Tsavorite key equality comparer based on potentially slow EqualityComparer<TKey>.Default."
                               + "To avoid this, provide a comparer (ITsavoriteEqualityComparer<TKey>) as an argument to Tsavorite's constructor, or make Key implement the interface ITsavoriteEqualityComparer<TKey>");
                return DefaultKeyComparer<T>.Instance;
            }
        }
    }

    /// <summary>
    /// Deterministic equality comparer for strings
    /// </summary>
    public sealed class StringKeyComparer : IKeyComparer<string>
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly StringKeyComparer Instance = new();

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
    public sealed class LongKeyComparer : IKeyComparer<long>
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly LongKeyComparer Instance = new();

        /// <inheritdoc />
        public bool Equals(ref long k1, ref long k2) => k1 == k2;

        /// <inheritdoc />
        public long GetHashCode64(ref long k) => Utility.GetHashCode(k);
    }

    /// <summary>
    /// Deterministic equality comparer for longs
    /// </summary>
    public sealed class IntKeyComparer : IKeyComparer<int>
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly IntKeyComparer Instance = new();

        /// <inheritdoc />
        public bool Equals(ref int k1, ref int k2) => k1 == k2;

        /// <inheritdoc />
        public long GetHashCode64(ref int k) => Utility.GetHashCode(k);
    }

    /// <summary>
    /// Deterministic equality comparer for longs
    /// </summary>
    public sealed class GuidKeyComparer : IKeyComparer<Guid>
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly GuidKeyComparer Instance = new();

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
    public sealed class ByteArrayKeyComparer : IKeyComparer<byte[]>
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly ByteArrayKeyComparer Instance = new();

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
    /// No-op equality comparer for Empty (used by TsavoriteLog)
    /// </summary>
    public sealed class EmptyKeyComparer : IKeyComparer<Empty>
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly EmptyKeyComparer Instance = new();

        /// <inheritdoc />
        public bool Equals(ref Empty key1, ref Empty key2) => throw new NotImplementedException();

        /// <inheritdoc />
        public long GetHashCode64(ref Empty key) => throw new NotImplementedException();
    }

    /// <summary>
    /// Low-performance Tsavorite equality comparer wrapper around EqualityComparer.Default
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal sealed class DefaultKeyComparer<T> : IKeyComparer<T>
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly DefaultKeyComparer<T> Instance = new();

        private static readonly EqualityComparer<T> DefaultEC = EqualityComparer<T>.Default;

        /// <inheritdoc />
        public bool Equals(ref T k1, ref T k2) => DefaultEC.Equals(k1, k2);

        /// <inheritdoc />
        public long GetHashCode64(ref T k) => Utility.GetHashCode(DefaultEC.GetHashCode(k));
    }
}