// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    internal struct RevivificationManager<TKey, TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        internal FreeRecordPool<TKey, TValue, TStoreFunctions, TAllocator> FreeRecordPool;
        internal readonly bool UseFreeRecordPool => FreeRecordPool is not null;

        internal RevivificationStats stats = new();

        internal bool IsEnabled => revivSuspendCount == 0;
        internal static int FixedValueLength => Unsafe.SizeOf<TValue>();
        internal bool restoreDeletedRecordsIfBinIsFull;
        internal bool useFreeRecordPoolForCTT;

        internal readonly bool IsFixedLength { get; }

        internal double revivifiableFraction;

        internal int revivSuspendCount = -1;

        public void PauseRevivification() => Interlocked.Decrement(ref revivSuspendCount);

        public void ResumeRevivification() => Interlocked.Increment(ref revivSuspendCount);

        public RevivificationManager(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, bool isFixedLen, RevivificationSettings revivSettings, LogSettings logSettings)
        {
            IsFixedLength = isFixedLen;
            revivifiableFraction = revivSettings is null || revivSettings.RevivifiableFraction == RevivificationSettings.DefaultRevivifiableFraction
                ? logSettings.MutableFraction
                : revivSettings.RevivifiableFraction;

            if (revivSettings is null)
                return;

            revivSettings.Verify(IsFixedLength, logSettings.MutableFraction);
            if (!revivSettings.EnableRevivification)
                return;

            revivSuspendCount = 0;
            if (revivSettings.FreeRecordBins?.Length > 0)
            {
                FreeRecordPool = new FreeRecordPool<TKey, TValue, TStoreFunctions, TAllocator>(store, revivSettings, IsFixedLength ? store.hlog.GetAverageRecordSize() : -1);
                restoreDeletedRecordsIfBinIsFull = revivSettings.RestoreDeletedRecordsIfBinIsFull;
                useFreeRecordPoolForCTT = revivSettings.UseFreeRecordPoolForCopyToTail;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long GetMinRevivifiableAddress(long tailAddress, long readOnlyAddress)
            => tailAddress - (long)((tailAddress - readOnlyAddress) * revivifiableFraction);

        // Method redirectors
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAdd(long logicalAddress, int size, ref RevivificationStats revivStats)
            => UseFreeRecordPool && FreeRecordPool.TryAdd(logicalAddress, size, ref revivStats);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAdd(long logicalAddress, long physicalAddress, int allocatedSize, ref RevivificationStats revivStats)
            => UseFreeRecordPool && FreeRecordPool.TryAdd(logicalAddress, physicalAddress, allocatedSize, ref revivStats);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryTake(int recordSize, long minAddress, out long address, ref RevivificationStats revivStats)
        {
            if (UseFreeRecordPool)
                return FreeRecordPool.TryTake(recordSize, minAddress, out address, ref revivStats);
            address = 0;
            return false;
        }

        public void Dispose()
        {
            if (UseFreeRecordPool)
            {
                FreeRecordPool.Dispose();
            }
        }
    }
}