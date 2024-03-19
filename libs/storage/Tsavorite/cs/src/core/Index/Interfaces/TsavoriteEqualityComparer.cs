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
            var t = typeof(T);
            if (t == typeof(string))
                return new StringTsavoriteEqualityComparer() as ITsavoriteEqualityComparer<T>;
            else if (t == typeof(byte[]))
                return new ByteArrayTsavoriteEqualityComparer() as ITsavoriteEqualityComparer<T>;
            else if (t == typeof(long))
                return new LongTsavoriteEqualityComparer() as ITsavoriteEqualityComparer<T>;
            else if (t == typeof(int))
                return new IntTsavoriteEqualityComparer() as ITsavoriteEqualityComparer<T>;
            else if (t == typeof(Guid))
                return new GuidTsavoriteEqualityComparer() as ITsavoriteEqualityComparer<T>;
            else if (t == typeof(SpanByte))
                return new SpanByteComparer() as ITsavoriteEqualityComparer<T>;
            else
            {
                Debug.WriteLine("***WARNING*** Creating default Tsavorite key equality comparer based on potentially slow EqualityComparer<Key>.Default."
                               + "To avoid this, provide a comparer (ITsavoriteEqualityComparer<Key>) as an argument to Tsavorite's constructor, or make Key implement the interface ITsavoriteEqualityComparer<Key>");
                return DefaultTsavoriteEqualityComparer<T>.Default;
            }
        }
    }

    /// <summary>
    /// Deterministic equality comparer for strings
    /// </summary>
    public sealed class StringTsavoriteEqualityComparer : ITsavoriteEqualityComparer<string>
    {
        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="key1"></param>
        /// <param name="key2"></param>
        /// <returns></returns>
        public bool Equals(ref string key1, ref string key2)
        {
            // Use locals in case the record space is cleared.
            string k1 = key1, k2 = key2;
            return (k1 is null || k2 is null) ? false : k1 == k2;
        }

        /// <summary>
        /// GetHashCode64
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
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
        /// Equals
        /// </summary>
        /// <param name="k1"></param>
        /// <param name="k2"></param>
        /// <returns></returns>
        public bool Equals(ref long k1, ref long k2) => k1 == k2;

        /// <summary>
        /// GetHashCode64
        /// </summary>
        /// <param name="k"></param>
        /// <returns></returns>
        public long GetHashCode64(ref long k) => Utility.GetHashCode(k);
    }

    /// <summary>
    /// Deterministic equality comparer for longs
    /// </summary>
    public sealed class IntTsavoriteEqualityComparer : ITsavoriteEqualityComparer<int>
    {
        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="k1"></param>
        /// <param name="k2"></param>
        /// <returns></returns>
        public bool Equals(ref int k1, ref int k2) => k1 == k2;

        /// <summary>
        /// GetHashCode64
        /// </summary>
        /// <param name="k"></param>
        /// <returns></returns>
        public long GetHashCode64(ref int k) => Utility.GetHashCode(k);
    }

    /// <summary>
    /// Deterministic equality comparer for longs
    /// </summary>
    public sealed class GuidTsavoriteEqualityComparer : ITsavoriteEqualityComparer<Guid>
    {
        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="k1"></param>
        /// <param name="k2"></param>
        /// <returns></returns>
        public bool Equals(ref Guid k1, ref Guid k2) => k1 == k2;

        /// <summary>
        /// GetHashCode64
        /// </summary>
        /// <param name="k"></param>
        /// <returns></returns>
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
        /// Equals
        /// </summary>
        /// <param name="key1"></param>
        /// <param name="key2"></param>
        /// <returns></returns>
        public bool Equals(ref byte[] key1, ref byte[] key2) => key1.AsSpan().SequenceEqual(key2);

        /// <summary>
        /// GetHashCode64
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
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
        public static readonly DefaultTsavoriteEqualityComparer<T> Default = new DefaultTsavoriteEqualityComparer<T>();

        private static readonly EqualityComparer<T> DefaultEC = EqualityComparer<T>.Default;

        public bool Equals(ref T k1, ref T k2)
        {
            return DefaultEC.Equals(k1, k2);
        }

        public long GetHashCode64(ref T k)
        {
            return Utility.GetHashCode(DefaultEC.GetHashCode(k));
        }
    }
}