// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    internal ref struct OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        internal readonly ref HashEntryInfo hei;
        internal RecordSource<TKey, TValue, TStoreFunctions, TAllocator> recSrc;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStackContext(ref HashEntryInfo hei)
        {
            this.hei = ref hei;
            ResetTransientLockTimeout();
        }

        /// <summary>
        /// Sets <see cref="recSrc"/> to the current <see cref="hei"/>.<see cref="HashEntryInfo.Address"/>, which is the address it had
        /// at the time of last retrieval from the hash table.
        /// </summary>
        /// <param name="srcLog">The TsavoriteKV's hlog</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetRecordSourceToHashEntry(AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator> srcLog) => recSrc.Set(hei.Address, srcLog);

        /// <summary>
        /// Sets <see cref="recSrc"/> to the current <see cref="hei"/>.<see cref="HashEntryInfo.CurrentAddress"/>, which is the current address
        /// in the hash table. This is the same effect as calling <see cref="HashTable.FindTag(ref HashEntryInfo)"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UpdateRecordSourceToCurrentHashEntry(AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator> hlog)
        {
            hei.SetToCurrent();
            SetRecordSourceToHashEntry(hlog);
        }

        /// <summary>
        /// If this is not <see cref="Constants.kInvalidAddress"/>, it is the logical Address allocated by CreateNewRecord*; if an exception
        /// occurs, this needs to be set invalid and non-tentative by the caller's 'finally' (to avoid another try/finally overhead).
        /// </summary>
        private long newLogicalAddress;

        private DateTime transientLockStartTime = DateTime.MinValue;

        internal void ResetTransientLockTimeout() => transientLockStartTime = DateTime.MinValue;

        internal bool IsTransientLockTimeout(TimeSpan timeout)
        {
            // This is called on failure of lock iteration, to avoid the overhead of DateTime.UtcNow on the success path.
            // So the first time through we set the start time rather than testing the timeout.
            var utcNow = DateTime.UtcNow;
            if (transientLockStartTime < utcNow)
            {
                transientLockStartTime = utcNow;
                return false;
            }
            return transientLockStartTime + timeout < utcNow;
        }

        /// <summary>
        /// Sets the new record to be handled on error recovery.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetNewRecord(long newRecordLogicalAddress) => newLogicalAddress = newRecordLogicalAddress;

        /// <summary>
        /// Called during normal operations when a record insertion fails, to set the new record invalid and non-tentative.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetNewRecordInvalid(ref RecordInfo newRecordInfo)
        {
            Debug.Assert(newRecordInfo.Invalid, "Records should be invalidated until successfully inserted");   // TODO: If this does not fire, remove the following line that sets it
            newRecordInfo.SetInvalid();
            newLogicalAddress = Constants.kInvalidAddress;
        }

        /// <summary>
        /// Called during normal operations when a record insertion succeeds, to set the new record non-tentative (permanent).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ClearNewRecord() => newLogicalAddress = Constants.kInvalidAddress;

        /// <summary>
        /// Called during InternalXxx 'finally' handler, to set the new record invalid if an exception or other error occurred.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void HandleNewRecordOnException(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store)
        {
            if (newLogicalAddress != Constants.kInvalidAddress)
            {
                store.SetRecordInvalid(newLogicalAddress);
                newLogicalAddress = Constants.kInvalidAddress;
            }
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            var delimiter = "    ";  // Environment.NewLine doesn't display in VS
            return $"hei: {hei}{delimiter}newLA {newLogicalAddress}{delimiter}recSrc: {recSrc}";
        }
    }
}