// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
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

        public Sketch(int keyCount = 1 << 20)
        {
            if (!(keyCount > 0 && (keyCount & (keyCount - 1)) == 0))
                throw new GarnetException($"{nameof(Sketch)} size should be power of 2!");
            size = keyCount;
            bitmap = GC.AllocateArray<byte>(keyCount >> 3, pinned: true);
            Status = SketchStatus.INITIALIZING;
            Keys = [];
            argSliceVector = new();
        }

        #region sketchMethods

        public bool TryHashAndStore(ReadOnlySpan<byte> key)
        {
            if (!argSliceVector.TryAddItem(key))
                return false;

            var slot = (int)HashUtils.MurmurHash2x64A(key) & (size - 1);
            var byteOffset = slot >> 3;
            var bitOffset = slot & 7;
            bitmap[byteOffset] = (byte)(bitmap[byteOffset] | (1UL << bitOffset));

            return true;
        }

        /// <summary>
        /// Hash key to bloomfilter and store it for future use (NOTE: Use only with KEYS option)
        /// </summary>
        /// <param name="key"></param>
        public unsafe void HashAndStore(PinnedSpanByte key)
        {
            var slot = (int)HashUtils.MurmurHash2x64A(key.ReadOnlySpan) & (size - 1);
            var byteOffset = slot >> 3;
            var bitOffset = slot & 7;
            bitmap[byteOffset] = (byte)(bitmap[byteOffset] | (1UL << bitOffset));
            Keys.Add((key, false));
        }

        /// <summary>
        /// Probe sketch to check if key has been added
        /// </summary>
        /// <param name="key"></param>
        /// <param name="status"></param>
        /// <returns></returns>
        public unsafe bool Probe(PinnedSpanByte key, out SketchStatus status)
        {
            var slot = (int)HashUtils.MurmurHash2x64A(key.ToPointer(), key.Length) & (size - 1);
            var byteOffset = slot >> 3;
            var bitOffset = slot & 7;

            var exists = (bitmap[byteOffset] & (1UL << bitOffset)) > 0;
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