// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    internal struct RevivificationManager<TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        internal FreeRecordPool<TStoreFunctions, TAllocator> FreeRecordPool;
        internal readonly bool UseFreeRecordPool => FreeRecordPool is not null;

        internal RevivificationStats stats = new();

        internal bool IsEnabled => revivSuspendCount == 0;
        internal bool restoreDeletedRecordsIfBinIsFull;
        internal bool useFreeRecordPoolForCTT;

        internal double revivifiableFraction;

        internal int revivSuspendCount = -1;

        public void PauseRevivification() => Interlocked.Decrement(ref revivSuspendCount);

        public void ResumeRevivification() => Interlocked.Increment(ref revivSuspendCount);

        public RevivificationManager(TsavoriteKV<TStoreFunctions, TAllocator> store, RevivificationSettings revivSettings, LogSettings logSettings)
        {
            revivifiableFraction = revivSettings is null || revivSettings.RevivifiableFraction == RevivificationSettings.DefaultRevivifiableFraction
                ? logSettings.MutableFraction
                : revivSettings.RevivifiableFraction;

            if (revivSettings is null)
                return;

            revivSettings.Verify(logSettings.MutableFraction);
            if (!revivSettings.EnableRevivification)
                return;

            revivSuspendCount = 0;
            if (revivSettings.FreeRecordBins?.Length > 0)
            {
                FreeRecordPool = new FreeRecordPool<TStoreFunctions, TAllocator>(store, revivSettings);
                restoreDeletedRecordsIfBinIsFull = revivSettings.RestoreDeletedRecordsIfBinIsFull;
                useFreeRecordPoolForCTT = revivSettings.UseFreeRecordPoolForCopyToTail;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly long GetMinRevivifiableAddress(long tailAddress, long readOnlyAddress)
            => tailAddress - (long)((tailAddress - readOnlyAddress) * revivifiableFraction);

        // Method redirectors
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAdd(long logicalAddress, ref LogRecord logRecord, ref RevivificationStats revivStats)
            => UseFreeRecordPool && FreeRecordPool.TryAdd(logicalAddress, ref logRecord, ref revivStats);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryTake(in RecordSizeInfo sizeInfo, long minAddress, out long address, ref RevivificationStats revivStats)
        {
            if (UseFreeRecordPool)
                return FreeRecordPool.TryTake(in sizeInfo, minAddress, out address, ref revivStats);
            address = 0;
            return false;
        }

        public void Dispose()
        {
            if (UseFreeRecordPool)
                FreeRecordPool.Dispose();
        }
    }
}