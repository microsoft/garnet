// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Drawing;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// The core data structures of the core, used for dual Tsavorite operations
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    struct TsavoriteKernel
    {
        // Unioned fields
        [FieldOffset(0)]
        internal HashTable hashTable;
        [FieldOffset(0)]
        internal HashBucketLockTable lockTable;

        [FieldOffset(HashTable.Size)]
        internal LightEpoch epoch;

        public TsavoriteKernel(long size, int sector_size, ILogger logger = null)
        {
            if (!Utility.IsPowerOfTwo(size))
                throw new ArgumentException("Size {0} is not a power of 2");
            if (!Utility.Is32Bit(size))
                throw new ArgumentException("Size {0} is not 32-bit");

            hashTable = new(size, sector_size, logger);
            epoch = new LightEpoch();
        }

        internal void Dispose()
        {
            hashTable.Dispose();
            epoch.Dispose();
        }
    }
}
