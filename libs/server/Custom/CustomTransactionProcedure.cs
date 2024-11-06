// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Base class for transaction definition
    /// </summary>
    public abstract class CustomTransactionProcedure : CustomFunctions
    {
        internal ScratchBufferManager scratchBufferManager;
        internal TransactionManager txnManager;

        /// <summary>
        /// If enabled, transaction fails fast on key locking failure instead of waiting on lock
        /// </summary>
        public virtual bool FailFastOnKeyLockFailure => false;

        /// <summary>
        /// Timeout for acquiring key locks. 100 ms by default
        /// </summary>
        public virtual TimeSpan KeyLockTimeout => TimeSpan.FromMilliseconds(
#if DEBUG
            5000    /* Things are slower in Debug so give it more time */
#else
            100
#endif
        );

        /// <summary>
        /// Add specified key to the locking set
        /// </summary>
        /// <param name="key"></param>
        /// <param name="type"></param>
        /// <param name="isObject"></param>
        protected void AddKey(ArgSlice key, LockType type, bool isObject)
        {
            txnManager.SaveKeyEntryToLock(key, isObject, type);
            txnManager.VerifyKeyOwnership(key, type);
        }

        /// <summary>
        /// Rewind (pop) the last entry of scratch buffer (rewinding the current scratch buffer offset),
        /// if it contains the given ArgSlice
        /// </summary>
        protected bool RewindScratchBuffer(ref ArgSlice slice)
            => scratchBufferManager.RewindScratchBuffer(ref slice);

        /// <summary>
        /// Create ArgSlice in scratch buffer, from given ReadOnlySpan
        /// </summary>
        protected ArgSlice CreateArgSlice(ReadOnlySpan<byte> bytes)
            => scratchBufferManager.CreateArgSlice(bytes);

        /// <summary>
        /// Create ArgSlice in UTF8 format in scratch buffer, from given string
        /// </summary>
        protected ArgSlice CreateArgSlice(string str)
            => scratchBufferManager.CreateArgSlice(str);

        /// <summary>
        /// Prepare phase: define read/write set
        /// </summary>
        public abstract bool Prepare<TKeyLocker, TEpochGuard, TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetReadApi : IGarnetReadApi<TKeyLocker, TEpochGuard>;

        /// <summary>
        /// Main transaction: allowed to read and write (locks are already taken) and produce output
        /// </summary>
        public abstract void Main<TKeyLocker, TEpochGuard, TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>;

        /// <summary>
        /// Finalize transaction: runs after the transactions commits/aborts, allowed to read and write (non-transactionally) with per-key locks and produce output
        /// </summary>
        public virtual void Finalize<TKeyLocker, TEpochGuard, TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        { }
    }
}