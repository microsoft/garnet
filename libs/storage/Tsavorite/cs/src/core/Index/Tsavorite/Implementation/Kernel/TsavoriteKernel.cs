// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// The core data structures of the core, used for dual Tsavorite operations
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    public partial class TsavoriteKernel
    {
        // Unioned fields
        [FieldOffset(0)]
        internal HashTable hashTable;
        [FieldOffset(0)]
        internal HashBucketLockTable lockTable;

        [FieldOffset(HashTable.Size)]
        public LightEpoch Epoch;

        const int KeyLockMaxRetryAttempts = 1000;

        public TsavoriteKernel(long numBuckets, int sectorSize, ILogger logger = null)
        {
            if (!Utility.IsPowerOfTwo(numBuckets))
                throw new ArgumentException("Size {0} is not a power of 2");
            if (!Utility.Is32Bit(numBuckets))
                throw new ArgumentException("Size {0} is not 32-bit");

            hashTable = new(numBuckets, sectorSize, logger);
            Epoch = new LightEpoch();
        }

        public bool EnsureEpochProtected()
        {
            if (Epoch.ThisInstanceProtected())
                return false;
            Epoch.Resume();
            return true;
        }

#if false
        public Status EnterForRead<TKernelSession, TKeyLocker, TEpochGuard>(ref TKernelSession kernelSession, TKeyLocker keyLocker, TEpochGuard epochGuard)
        {
            epochGuard.Acquire(this.epoch);
            keyLocker.
        }
#endif

        internal void Dispose()
        {
            hashTable.Dispose();
            Epoch.Dispose();
        }
    }
}
