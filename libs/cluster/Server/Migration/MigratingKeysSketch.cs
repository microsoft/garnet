// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal class MigratingKeysSketch
    {
        readonly byte[] bitmap;
        readonly int size;

        public List<(ArgSlice, bool)> Keys { private set; get; }
        public SketchStatus Status { private set; get; }
        public long Count { get; private set; }

        public MigratingKeysSketch(int size = 1 << 20)
        {
            if (!(size > 0 && (size & (size - 1)) == 0))
                throw new GarnetException($"{nameof(MigratingKeysSketch)} size should be power of 2!");
            this.size = size;
            bitmap = new byte[size >> 3];
            Status = SketchStatus.INITIALIZING;
            Keys = [];
        }

        #region sketchMethods
        /// <summary>
        /// Hash key to bloomfilter and store it for future use (NOTE: Use only with KEYS option)
        /// </summary>
        /// <param name="key"></param>
        public unsafe void HashAndStore(ref ArgSlice key)
        {
            Hash(key.SpanByte.ToPointer(), key.Length);
            Keys.Add((key, false));
        }

        /// <summary>
        /// Hash key to bloomfilter
        /// </summary>
        /// <param name="key"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        public unsafe void Hash(byte* key, int length)
        {
            var slot = (int)HashUtils.MurmurHash2x64A(key, length) & (size - 1);
            var byteOffset = slot >> 3;
            var bitOffset = slot & 7;
            bitmap[byteOffset] = (byte)(bitmap[byteOffset] | (1UL << bitOffset));
        }

        /// <summary>
        /// Probe sketch to check if key has been added
        /// </summary>
        /// <param name="key"></param>
        /// <param name="status"></param>
        /// <returns></returns>
        public unsafe bool Probe(ref SpanByte key, out SketchStatus status)
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
            for (var i = 0; i < (size >> 3); i++)
                bitmap[i] = 0;
            Status = SketchStatus.INITIALIZING;
            Count = 0;
        }

        /// <summary>
        /// Set KeyMigrationStatus
        /// </summary>
        /// <param name="status"></param>
        public void SetStatus(SketchStatus status) => Status = status;
        #endregion
    }
}