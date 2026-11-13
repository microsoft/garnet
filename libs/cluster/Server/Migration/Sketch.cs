// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal class Sketch
    {
        readonly byte[] bitmap;
        readonly int size;
        public readonly ArgSliceVector argSliceVector;

        public List<(PinnedSpanByte, bool)> Keys { private set; get; }
        public SketchStatus Status { private set; get; }

        public Sketch(int keyCount = 1 << 20, int maxBatchSize = 1 << 18)
        {
            if (!(keyCount > 0 && (keyCount & (keyCount - 1)) == 0))
                throw new GarnetException($"{nameof(Sketch)} size should be power of 2!");
            size = keyCount;
            bitmap = GC.AllocateArray<byte>(keyCount >> 3, pinned: true);
            Status = SketchStatus.INITIALIZING;
            Keys = [];
            argSliceVector = new(maxBatchSize);
        }

        #region sketchMethods

        public bool TryHashAndStore(ReadOnlySpan<byte> key)
        {
            if (!argSliceVector.TryAddItem(key))
                return false;

            AddHash(HashUtils.MurmurHash2x64A(key));

            return true;
        }

        public bool TryHashAndStore(ReadOnlySpan<byte> ns, ReadOnlySpan<byte> key)
        {
            Debug.Assert(ns.Length is (sizeof(byte) or sizeof(uint)), "Longer namespaces not yet supported");

            if (!argSliceVector.TryAddItem(ns, key))
                return false;

            var nsRaw = VectorManager.ExtractContextFromNamespaces(ns);

            AddHash(HashUtils.MurmurHash2x64A(key, seed: (uint)nsRaw));

            return true;
        }

        /// <summary>
        /// Hash key to bloomfilter and store it for future use (NOTE: Use only with KEYS option)
        /// </summary>
        /// <param name="key"></param>
        public void HashAndStore(PinnedSpanByte key)
        {
            AddHash(HashUtils.MurmurHash2x64A(key.ReadOnlySpan));
            Keys.Add((key, false));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void AddHash(ulong keyHash)
        {
            SetBit((int)keyHash & (size - 1));
            SetBit((int)(keyHash >> 32) & (size - 1));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SetBit(int slot) => bitmap[slot >> 3] |= (byte)(1 << (slot & 7));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool IsSet(int slot) => (bitmap[slot >> 3] & (1 << (slot & 7))) != 0;

        /// <summary>
        /// Probe sketch using a precomputed key hash.
        /// </summary>
        /// <param name="keyHash">Unmasked MurmurHash2x64A key hash.</param>
        /// <param name="status"></param>
        /// <returns></returns>
        public bool Probe(ulong keyHash, out SketchStatus status)
        {
            var exists = IsSet((int)keyHash & (size - 1)) &&
                IsSet((int)(keyHash >> 32) & (size - 1));
            status = exists ? Status : SketchStatus.INITIALIZING;
            return exists;
        }

        /// <summary>
        /// Clear keys from working set
        /// </summary>
        public void Clear()
        {
            argSliceVector.Clear();
            for (var i = 0; i < (size >> 3); i++)
                bitmap[i] = 0;
            Status = SketchStatus.INITIALIZING;
        }

        /// <summary>
        /// Set KeyMigrationStatus
        /// </summary>
        /// <param name="status"></param>
        public void SetStatus(SketchStatus status) => Status = status;
        #endregion
    }
}