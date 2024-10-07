// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
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

        [FieldOffset(HashTable.Size + Constants.IntPtrSize)]
        internal ILogger Logger;

        const int KeyLockMaxRetryAttempts = 1000;

        public TsavoriteKernel(long numBuckets, int sectorSize, ILogger logger = null)
        {
            if (!Utility.IsPowerOfTwo(numBuckets))
                throw new ArgumentException("Size {0} is not a power of 2");
            if (!Utility.Is32Bit(numBuckets))
                throw new ArgumentException("Size {0} is not 32-bit");

            this.Logger = logger;
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

        /// <summary>
        /// Enter the kernel for a read operation on a single Tsavorite configuration or the first TsavoriteKV of a dual configuration.
        /// </summary>
        /// <remarks>
        /// This does epoch entry and the hash bucket lookup and transient lock (which is the only hash bucket lock unless a RETRY is needed, in which case transient locks are released then reacquired by the TsavoriteKV instance).
        /// </remarks>
        /// <returns><see cref="Status.Found"/> if the tag was found, else <see cref="Status.NotFound"/>. TODO Add Lock timeout and possible <see cref="Status.NotFound"/> return.</returns>
        public Status EnterForRead<TKernelSession, TKeyLocker, TEpochThread>(ref TKernelSession kernelSession, TKeyLocker keyLocker, TEpochThread epochThread, long keyHash, ushort partitionId, out HashEntryInfo hei)
            where TKernelSession : IKernelSession
            where TKeyLocker : ISessionLocker
            where TEpochThread : IEpochThread<TKernelSession>
        {
            epochThread.BeginUnsafe(ref kernelSession);
            hei = new(keyHash, partitionId);

            // CheckHashTableGrowth();

            if (!hashTable.FindTag(ref hei))
                return new(StatusCode.NotFound);

            while (!keyLocker.TryLockTransientShared(this, ref hei))
            {
                kernelSession.Refresh();
                Thread.Yield();
            }
            return new(StatusCode.Found);
        }

        /// <summary>
        /// Enter the kernel for a read operation on the second TsavoriteKV of a dual configuration.
        /// </summary>
        /// <remarks>
        /// This updates the hash bucket lookup. Epoch entry and hash bucket lock should have survived from EnterForReadDual1
        /// </remarks>
        /// <returns><see cref="Status.Found"/> if the tag was found, else <see cref="Status.NotFound"/></returns>
        public Status EnterForReadDual2(ushort partitionId, ref HashEntryInfo hei)
        {
            hei.Reset(partitionId);
            if (!hashTable.FindTag(ref hei))
                return new(StatusCode.NotFound);
            Debug.Assert(hei.HasTransientSLock, "Expected transient HEI SLock after Reset and FindTag");
            Debug.Assert(lockTable.GetLockState(ref hei).IsLockedShared, "Expected bucket SLock after Reset and FindTag");
            return new(StatusCode.Found);
        }

        /// <summary>
        /// Exits the kernel for this Read operation.
        /// </summary>
        /// <remarks>
        /// Releases the read lock on the hash bucket and releases the epoch.
        /// </remarks>
        public void ExitForRead<TKernelSession, TKeyLocker, TEpochThread>(ref TKernelSession kernelSession, TKeyLocker keyLocker, TEpochThread epochThread, ref HashEntryInfo hei)
            where TKernelSession : IKernelSession
            where TKeyLocker : ISessionLocker
            where TEpochThread : IEpochThread<TKernelSession>
        {
            keyLocker.UnlockTransientShared(this, ref hei, isRetry:false);
            if (Epoch.ThisInstanceProtected())
                epochThread.EndUnsafe(ref kernelSession);
        }

        /// <summary>
        /// Enter the kernel for an update operation on a single Tsavorite configuration or on the first TsavoriteKV of a dual configuration.
        /// </summary>
        /// <remarks>
        /// This does epoch entry and the hash bucket lookup and transient lock (which is the only hash bucket lock unless a RETRY is needed, in which case transient locks are released then reacquired by the TsavoriteKV instance).
        /// <para/>
        /// This is a strict update operation from the HashBucket perspective; if the tag is not found, we do not create it. It is used for operations like Delete or Expire. Operations that do in-place updates or RCU will know
        /// which store they are operating on and will call a different method.
        /// </remarks>
        /// <returns><see cref="Status.Found"/> if the tag was found, else <see cref="Status.NotFound"/>. TODO Add Lock timeout and possible <see cref="Status.NotFound"/> return.</returns>
        public Status EnterForUpdate<TKernelSession, TKeyLocker, TEpochThread>(ref TKernelSession kernelSession, TKeyLocker keyLocker, TEpochThread epochThread, long keyHash, ushort partitionId, out HashEntryInfo hei)
            where TKernelSession : IKernelSession
            where TKeyLocker : ISessionLocker
            where TEpochThread : IEpochThread<TKernelSession>
        {
            epochThread.BeginUnsafe(ref kernelSession);
            hei = new(keyHash, partitionId);

            // CheckHashTableGrowth();

            // We do FindTag, not FindOrCreateTag, here because we don't want to create the slot if the tag is not found.
            if (!hashTable.FindTag(ref hei))
                return new(StatusCode.NotFound);

            while (!keyLocker.TryLockTransientExclusive(this, ref hei))
            {
                kernelSession.Refresh();
                Thread.Yield();
            }
            return new(StatusCode.Found);
        }

        /// <summary>
        /// Enter the kernel for an update operation on the second TsavoriteKV of a dual configuration.
        /// </summary>
        /// <remarks>
        /// This is a strict update operation from the HashBucket perspective; if the tag is not found, we do not create it. It is used for operations like Delete or Expire. Operations that do in-place updates or RCU will know
        /// which store they are operating on and will call a different method.
        /// </remarks>
        public Status EnterForUpdateDual2(ushort partitionId, ref HashEntryInfo hei)
        {
            hei.Reset(partitionId);
            if (!hashTable.FindTag(ref hei))
                return new(StatusCode.NotFound);
            Debug.Assert(hei.HasTransientXLock, "Expected transient XLock after Reset and FindTag");
            return new(StatusCode.Found);
        }

        /// <summary>
        /// Exits the kernel for this Update operation.
        /// </summary>
        /// <remarks>
        /// Releases the read lock on the hash bucket and releases the epoch.
        /// </remarks>
        public void ExitForUpdate<TKernelSession, TKeyLocker, TEpochThread>(ref TKernelSession kernelSession, TKeyLocker keyLocker, TEpochThread epochThread, ref HashEntryInfo hei)
            where TKernelSession : IKernelSession
            where TKeyLocker : ISessionLocker
            where TEpochThread : IEpochThread<TKernelSession>
        {
            keyLocker.UnlockTransientShared(this, ref hei, isRetry: false);
            epochThread.EndUnsafe(ref kernelSession);
        }

        internal void Dispose()
        {
            hashTable.Dispose();
            Epoch.Dispose();
        }
    }
}
