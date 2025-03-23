// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    internal struct RevivificationManager<TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        internal FreeRecordPool<TStoreFunctions, TAllocator> FreeRecordPool;
        internal readonly bool UseFreeRecordPool => FreeRecordPool is not null;

        internal RevivificationStats stats = new();

        internal readonly bool IsEnabled = false;
        internal bool restoreDeletedRecordsIfBinIsFull;
        internal bool useFreeRecordPoolForCTT;

        internal double revivifiableFraction;

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
            IsEnabled = true;
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
        public bool TryTake(ref RecordSizeInfo sizeInfo, long minAddress, out long address, ref RevivificationStats revivStats)
        {
            if (UseFreeRecordPool)
                return FreeRecordPool.TryTake(ref sizeInfo, minAddress, out address, ref revivStats);
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