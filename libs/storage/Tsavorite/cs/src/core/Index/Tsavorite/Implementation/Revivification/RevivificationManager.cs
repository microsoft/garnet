// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    internal struct RevivificationManager<Key, Value>
    {
        internal FreeRecordPool<Key, Value> FreeRecordPool;
        internal readonly bool UseFreeRecordPool => FreeRecordPool is not null;

        internal RevivificationStats stats = new();

        internal readonly bool IsEnabled = false;
        internal readonly int FixedValueLength => Unsafe.SizeOf<Value>();
        internal bool restoreDeletedRecordsIfBinIsFull;
        internal bool useFreeRecordPoolForCTT;

        internal readonly bool IsFixedLength { get; }

        internal double revivifiableFraction;

        public RevivificationManager(TsavoriteKV<Key, Value> store, bool isFixedLen, RevivificationSettings revivSettings, LogSettings logSettings)
        {
            IsFixedLength = isFixedLen;
            revivifiableFraction = revivSettings is null || revivSettings.RevivifiableFraction == RevivificationSettings.DefaultRevivifiableFraction
                ? logSettings.MutableFraction
                : revivSettings.RevivifiableFraction;

            if (revivSettings is null)
                return;
            if (revivSettings.EnableRevivification && !store.IsLocking)
                throw new TsavoriteException("Revivification requires ConcurrencyControlMode of LockTable or RecordIsolation");

            revivSettings.Verify(IsFixedLength, logSettings.MutableFraction);
            if (!revivSettings.EnableRevivification)
                return;
            IsEnabled = true;
            if (revivSettings.FreeRecordBins?.Length > 0)
            {
                FreeRecordPool = new FreeRecordPool<Key, Value>(store, revivSettings, IsFixedLength ? store.hlog.GetAverageRecordSize() : -1);
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