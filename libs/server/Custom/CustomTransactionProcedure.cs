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
        /// Get argument from input, at specified offset (starting from 0)
        /// </summary>
        /// <param name="input">Input as ArgSlice</param>
        /// <param name="offset">Current offset into input</param>
        /// <returns>Argument as a span</returns>
        protected static unsafe ArgSlice GetNextArg(ArgSlice input, ref int offset)
        {
            byte* result = null;
            int len = 0;

            byte* ptr = input.ptr + offset;
            byte* end = input.ptr + input.length;
            if (ptr < end && RespReadUtils.ReadPtrWithLengthHeader(ref result, ref len, ref ptr, end))
            {
                offset = (int)(ptr - input.ptr);
                return new ArgSlice { ptr = result, length = len };
            }
            return default;
        }

        /// <summary>
        /// Prepare phase: define read/write set
        /// </summary>
        public abstract bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
            where TGarnetReadApi : IGarnetReadApi;

        /// <summary>
        /// Main transaction: allowed to read and write (locks are already taken) and produce output
        /// </summary>
        public abstract void Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
            where TGarnetApi : IGarnetApi;

        /// <summary>
        /// Finalize transaction: runs after the transactions commits/aborts, allowed to read and write (non-transactionally) with per-key locks and produce output
        /// </summary>
        public virtual void Finalize<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
            where TGarnetApi : IGarnetApi
        { }
    }
}