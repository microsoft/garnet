// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Garnet.common;

namespace Garnet.cluster
{
    /// <summary>
    /// KEYS membership with one inline hash, promoted to a flat two-probe bitmap for additional hashes.
    /// The bitmap is unpinned and bounded at 128 KiB, independent of namespace collection capacity.
    /// Callers coordinate mutation and clearing with readers through migration epoch barriers.
    /// </summary>
    internal struct KeysMigrationFilter : IBloomFilter
    {
        const int BitmapBits = 1 << 20;
        const int BitMask = BitmapBits - 1;

        byte[] bitmap;
        ulong singleHash;
        bool hasEntries;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(ReadOnlySpan<byte> key, uint seed = 0)
        {
            var hash = HashUtils.MurmurHash2x64A(key, seed);
            var bits = bitmap;
            if (bits != null)
                AddHash(bits, hash);
            else if (!hasEntries)
                singleHash = hash;
            else if (singleHash != hash)
            {
                bits = new byte[BitmapBits >> 3];
                AddHash(bits, singleHash);
                AddHash(bits, hash);
                Volatile.Write(ref bitmap, bits);
            }
            hasEntries = true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static void AddHash(byte[] bits, ulong hash)
        {
            var first = (int)hash & BitMask;
            var second = (int)(hash >> 32) & BitMask;
            bits[first >> 3] |= (byte)(1 << (first & 7));
            bits[second >> 3] |= (byte)(1 << (second & 7));
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Contains(ulong hash)
        {
            var bits = Volatile.Read(ref bitmap);
            if (bits == null)
                return hasEntries && singleHash == hash;

            var first = (int)hash & BitMask;
            var second = (int)(hash >> 32) & BitMask;
            return (bits[first >> 3] & (1 << (first & 7))) != 0 &&
                (bits[second >> 3] & (1 << (second & 7))) != 0;
        }

        /// <inheritdoc/>
        public void Clear()
        {
            if (!hasEntries)
                return;
            if (bitmap != null)
                Array.Clear(bitmap);
            hasEntries = false;
        }
    }
}