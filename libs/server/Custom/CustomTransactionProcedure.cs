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
    public abstract class CustomTransactionProcedure : CustomProcedureBase
    {
        internal ScratchBufferAllocator scratchBufferAllocator;
        internal TransactionManager txnManager;
        internal int virtualSublogParticipantCount;
        internal ulong physicalSublogAccessVector;
        internal BitVector[] virtualSublogAccessVector = null;

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
        /// <param name="storeType"></param>
        protected void AddKey(PinnedSpanByte key, LockType type, StoreType storeType)
        {
            txnManager.AddTransactionStoreType(storeType);
            txnManager.SaveKeyEntryToLock(key, type);
            txnManager.VerifyKeyOwnership(key, type);
            txnManager.IterativeShardedLogAccess(key, this);
        }

        /// <summary>
        /// Rewind (pop) the last entry of scratch buffer (rewinding the current scratch buffer offset),
        /// if it contains the given ArgSlice
        /// </summary>
        protected bool RewindScratchBuffer(PinnedSpanByte slice)
            => scratchBufferAllocator.RewindScratchBuffer(ref slice);

        /// <summary>
        /// Create ArgSlice in scratch buffer, from given ReadOnlySpan
        /// </summary>
        protected PinnedSpanByte CreateArgSlice(ReadOnlySpan<byte> bytes)
            => scratchBufferAllocator.CreateArgSlice(bytes);

        /// <summary>
        /// Create ArgSlice in UTF8 format in scratch buffer, from given string
        /// </summary>
        protected PinnedSpanByte CreateArgSlice(string str)
            => scratchBufferAllocator.CreateArgSlice(str);

        /// <summary>
        /// Prepare phase: define read/write set
        /// </summary>
        public abstract bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
            where TGarnetReadApi : IGarnetReadApi;

        /// <summary>
        /// Main transaction: allowed to read and write (locks are already taken) and produce output
        /// </summary>
        public abstract void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
            where TGarnetApi : IGarnetApi;

        /// <summary>
        /// Finalize transaction: runs after the transactions commits/aborts, allowed to read and write (non-transactionally) with per-key locks and produce output
        /// NOTE: Finalize is considered post transaction processing and therefore is not executed at recovery time. Instead, the individual Tsavorite commands are logged and replayed through the AOF.
        /// If you are not using AOF for persistence then this is implementation detail you can ignore.
        /// </summary>
        public virtual void Finalize<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
            where TGarnetApi : IGarnetApi
        { }
    }
}