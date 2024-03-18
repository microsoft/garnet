// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    [StructLayout(LayoutKind.Explicit, Size = sizeof(long))]
    internal struct FreeRecord
    {
        internal const int kSizeBits = 64 - RecordInfo.kPreviousAddressBits;
        const int kSizeShiftInWord = RecordInfo.kPreviousAddressBits;

        const long kSizeMask = RevivificationBin.MaxInlineRecordSize - 1;
        const long kSizeMaskInWord = kSizeMask << kSizeShiftInWord;

        // This is the empty word we replace the current word with on Reads.
        private const long emptyWord = 0;

        // 'word' contains the reclaimable logicalAddress and the size of the record at that address.
        [FieldOffset(0)]
        private long word;

        internal const int StructSize = sizeof(long);

        public long Address
        {
            readonly get => word & RecordInfo.kPreviousAddressMaskInWord;
            set => word = (word & ~RecordInfo.kPreviousAddressMaskInWord) | (value & RecordInfo.kPreviousAddressMaskInWord);
        }

        public readonly int Size => (int)((word & kSizeMaskInWord) >> kSizeShiftInWord);

        /// <inheritdoc/>
        public override readonly string ToString() => $"address {Address}, size {Size}";

        internal readonly bool IsSet => word != emptyWord;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool Set(long address, long recordSize, long minAddress)
        {
            // If the record is empty or the address is below minAddress, set the new address into it.
            var oldRecord = this;
            if (oldRecord.IsSet && oldRecord.Address >= minAddress)
                return false;

            long newWord = (recordSize << kSizeShiftInWord) | (address & RecordInfo.kPreviousAddressMaskInWord);
            return Interlocked.CompareExchange(ref word, newWord, oldRecord.word) == oldRecord.word;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SetEmptyAtomic(long oldWord)
        {
            // Ignore the result; this is just to clear an obsolete value, so if another thread already updated it, that's by design.
            Interlocked.CompareExchange(ref word, emptyWord, oldWord);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryPeek<Key, Value>(long recordSize, TsavoriteKV<Key, Value> store, bool oversize, long minAddress, out int thisRecordSize)
        {
            FreeRecord oldRecord = this;
            thisRecordSize = 0;
            if (!oldRecord.IsSet)
                return false;
            if (oldRecord.Address < minAddress)
            {
                SetEmptyAtomic(oldRecord.word);
                return false;
            }
            var thisSize = oversize ? GetRecordSize(store, oldRecord.Address) : oldRecord.Size;
            if (thisSize < recordSize)
                return false;

            thisRecordSize = thisSize;
            return thisSize == recordSize;
        }

        internal struct TakeResult
        {
            internal bool isEmpty = true;
            internal bool addressOk = false;
            internal bool recordSizeOk = false;

            public TakeResult() { }

            internal readonly void MergeTo(ref RevivificationStats revivStats)
            {
                // An empty bin means to ignore some flags we initialize to true.
                if (isEmpty)
                    ++revivStats.takeEmptyBins;
                else
                {
                    if (!addressOk)
                        ++revivStats.takeAddressFailures;
                    if (!recordSizeOk)
                        ++revivStats.takeRecordSizeFailures;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryTake(int recordSize, long minAddress, out long address, ref TakeResult takeResult)
        {
            address = 0;

            FreeRecord oldRecord = this;
            while (true)
            {
                if (!oldRecord.IsSet)
                    return false;
                takeResult.isEmpty = false;
                if (oldRecord.Address < minAddress)
                    return false;
                else
                    takeResult.addressOk = true;
                if (oldRecord.Size < recordSize)
                    return false;
                else
                    takeResult.recordSizeOk = true;

                // If we're here, the record was set and size and address were adequate.
                if (Interlocked.CompareExchange(ref word, emptyWord, oldRecord.word) == oldRecord.word)
                {
                    address = oldRecord.Address;
                    return true;
                }

                // Failed to CAS. Loop again to see if someone else put in a different, but still good, record.
                oldRecord = this;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetRecordSize<Key, Value>(TsavoriteKV<Key, Value> store, long logicalAddress)
        {
            // Because this is oversize, we need hlog to get the length out of the record's value (it won't fit in FreeRecord.kSizeBits)
            long physicalAddress = store.hlog.GetPhysicalAddress(logicalAddress);
            return store.GetFreeRecordSize(physicalAddress, ref store.hlog.GetInfo(physicalAddress));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe bool TryTakeOversize<Key, Value>(long recordSize, long minAddress, TsavoriteKV<Key, Value> store, out long address, ref TakeResult takeResult)
        {
            address = 0;

            // The difference in this oversize version is that we delay checking size until after the CAS, because we have
            // go go the slow route of getting the physical address.
            FreeRecord oldRecord = this;
            while (true)
            {
                if (!oldRecord.IsSet)
                    return false;
                takeResult.isEmpty = false;
                if (oldRecord.Address < minAddress)
                    return false;
                else
                    takeResult.addressOk = true;

                // Because this is oversize, we need hlog to get the length out of the record's value (it won't fit in FreeRecord.kSizeBits)
                long physicalAddress = store.hlog.GetPhysicalAddress(oldRecord.Address);
                long thisSize = store.GetFreeRecordSize(physicalAddress, ref store.hlog.GetInfo(physicalAddress));

                if (thisSize < recordSize)
                    return false;
                else
                    takeResult.recordSizeOk = true;

                // If we're here, the record was set and size and address were adequate.
                if (Interlocked.CompareExchange(ref word, emptyWord, oldRecord.word) == oldRecord.word)
                {
                    address = oldRecord.Address;
                    return true;
                }

                // Failed to CAS. Loop again to see if someone else put in a different, but still good, record.
                oldRecord = this;
            }
        }
    }

    internal unsafe class FreeRecordBin
    {
        internal const int MinRecordsPerBin = 8;    // Make sure we have enough to be useful
        internal const int MinSegmentSize = MinRecordsPerBin;

        private readonly FreeRecord[] recordsArray;
        internal readonly int maxRecordSize, recordCount;
        protected readonly int minRecordSize, segmentSize, segmentCount, segmentRecordSizeIncrement;

        internal readonly FreeRecord* records;

        protected readonly int bestFitScanLimit;

        internal bool isEmpty = true;

        public int MaxRecordSize => maxRecordSize;

        /// <inheritdoc/>
        public override string ToString()
        {
            string scanStr = bestFitScanLimit switch
            {
                RevivificationBin.BestFitScanAll => "ScanAll",
                RevivificationBin.UseFirstFit => "FirstFit",
                _ => bestFitScanLimit.ToString()
            };
            return $"isEmpty {isEmpty}, recSizes {minRecordSize}..{maxRecordSize}, recSizeInc {segmentRecordSizeIncrement}, #recs {recordCount}; segments: segSize {segmentSize}, #segs {segmentCount}; scanLimit {scanStr}";
        }

        internal FreeRecordBin(ref RevivificationBin binDef, int prevBinRecordSize, bool isFixedLength)
        {
            // If the record size range is too much for the number of records in the bin, we must allow multiple record sizes per segment.
            // prevBinRecordSize is already verified to be a multiple of 8.
            var bindefRecordSize = RoundUp(binDef.RecordSize, 8);
            if (isFixedLength || bindefRecordSize == prevBinRecordSize + 8)
            {
                bestFitScanLimit = RevivificationBin.UseFirstFit;

                segmentSize = RoundUp(binDef.NumberOfRecords, MinSegmentSize);
                segmentCount = 1;
                segmentRecordSizeIncrement = 1;  // For the division and multiplication in GetSegmentStart
                minRecordSize = maxRecordSize = isFixedLength ? prevBinRecordSize : bindefRecordSize;
            }
            else
            {
                bestFitScanLimit = binDef.BestFitScanLimit;

                // minRecordSize is already verified to be a multiple of 8.
                var sizeRange = bindefRecordSize - prevBinRecordSize;

                segmentCount = sizeRange / 8;
                segmentSize = (int)Math.Ceiling(binDef.NumberOfRecords / (double)segmentCount);

                if (segmentSize >= MinSegmentSize)
                    segmentSize = RoundUp(segmentSize, MinSegmentSize);
                else
                {
                    segmentSize = MinSegmentSize;
                    segmentCount = (int)Math.Ceiling(binDef.NumberOfRecords / (double)segmentSize);
                }

                segmentRecordSizeIncrement = RoundUp(sizeRange / segmentCount, 8);
                maxRecordSize = prevBinRecordSize + segmentRecordSizeIncrement * segmentCount;
                minRecordSize = prevBinRecordSize + segmentRecordSizeIncrement;
            }
            recordCount = segmentSize * segmentCount;

            // Overallocate the GCHandle by one cache line so we have room to offset the returned pointer to make it cache-aligned.
            recordsArray = GC.AllocateArray<FreeRecord>(recordCount + Constants.kCacheLineBytes / FreeRecord.StructSize, pinned: true);
            long p = (long)Unsafe.AsPointer(ref recordsArray[0]);

            // Force the pointer to align to cache boundary.
            records = (FreeRecord*)RoundUp(p, Constants.kCacheLineBytes);
            if (bestFitScanLimit > recordCount)
                bestFitScanLimit = recordCount;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetSegmentStart(int recordSize)
        {
            // recordSize and segmentSizeIncrement are rounded up to 8, unless IsFixedLength in which case segmentSizeIncrement is 1.
            // sizeOffset will be negative if we are searching the next-highest bin.
            var sizeOffset = recordSize - minRecordSize;
            if (sizeOffset < 0)
                sizeOffset = 0;
            var segmentIndex = sizeOffset / segmentRecordSizeIncrement;
            Debug.Assert(segmentIndex >= 0 && segmentIndex < segmentCount, $"Internal error: Segment index ({segmentIndex}) must be >= 0 && < segmentCount ({segmentCount})");
            return segmentSize * segmentIndex;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private FreeRecord* GetRecord(int recordIndex) => records + (recordIndex >= recordCount ? recordIndex - recordCount : recordIndex);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAdd<Key, Value>(long address, int recordSize, TsavoriteKV<Key, Value> store, long minAddress, ref RevivificationStats revivStats)
        {
            var segmentStart = GetSegmentStart(recordSize);

            for (var ii = 0; ii < recordCount; ++ii)
            {
                FreeRecord* record = GetRecord(segmentStart + ii);
                if (record->Set(address, recordSize, minAddress))
                {
                    ++revivStats.successfulAdds;
                    isEmpty = false;
                    return true;
                }
            }
            ++revivStats.failedAdds;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryTake<Key, Value>(int recordSize, long minAddress, TsavoriteKV<Key, Value> store, out long address, ref RevivificationStats revivStats)
            => TryTake(recordSize, minAddress, store, oversize: false, out address, ref revivStats);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryTake<Key, Value>(int recordSize, long minAddress, TsavoriteKV<Key, Value> store, bool oversize, out long address, ref RevivificationStats revivStats)
        {
            if (isEmpty)
            {
                address = 0;
                return false;
            }
            return (bestFitScanLimit == RevivificationBin.UseFirstFit)
                        ? TryTakeFirstFit(recordSize, minAddress, store, oversize, out address, ref revivStats)
                        : TryTakeBestFit(recordSize, minAddress, store, oversize, out address, ref revivStats);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryTakeFirstFit<Key, Value>(int recordSize, long minAddress, TsavoriteKV<Key, Value> store, bool oversize, out long address, ref RevivificationStats revivStats)
        {
            var segmentStart = GetSegmentStart(recordSize);

            int retryCount = recordCount;
            FreeRecord.TakeResult takeResult = new();
            while (true)
            {
                for (var ii = 0; ii < recordCount; ++ii)
                {
                    FreeRecord* record = GetRecord(segmentStart + ii);
                    if (oversize ? record->TryTakeOversize(recordSize, minAddress, store, out address, ref takeResult) : record->TryTake(recordSize, minAddress, out address, ref takeResult))
                    {
                        takeResult.MergeTo(ref revivStats);
                        return true;
                    }
                }
                if (takeResult.isEmpty || (retryCount >>= 1) < RevivificationBin.MinRecordsPerBin)
                    break;
            }

            takeResult.MergeTo(ref revivStats);
            address = 0;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryTakeBestFit<Key, Value>(int recordSize, long minAddress, TsavoriteKV<Key, Value> store, bool oversize, out long address, ref RevivificationStats revivStats)
        {
            // Retry as long as we find a candidate, but reduce the best fit scan limit each retry.
            int localBestFitScanLimit = bestFitScanLimit;
            var segmentStart = GetSegmentStart(recordSize);

            FreeRecord.TakeResult takeResult = new();
            while (true)
            {
                int bestFitSize = int.MaxValue;         // Comparison is "if record.Size < bestFitSize", hence initialized to int.MaxValue
                int bestFitIndex = -1;                  // Will be compared to >= 0 on exit from the best-fit scan loop
                int firstFitIndex = int.MaxValue;       // Subtracted from loop control var and tested for >= bestFitScanLimit; int.MaxValue produces a negative result

                FreeRecord* record;
                for (var ii = 0; ii < recordCount; ++ii)
                {
                    // For best-fit we must peek first without taking.
                    record = GetRecord(segmentStart + ii);
                    if (record->TryPeek(recordSize, store, oversize, minAddress, out var thisRecordSize))
                    {
                        bestFitIndex = ii;      // Found exact match
                        break;
                    }

                    if (thisRecordSize > 0 && thisRecordSize < bestFitSize)
                    {
                        bestFitIndex = ii;      // We have a better fit.
                        bestFitSize = thisRecordSize;
                        if (firstFitIndex == int.MaxValue)
                            firstFitIndex = ii;
                    }
                    if (ii - firstFitIndex >= localBestFitScanLimit)
                        break;
                }

                if (bestFitIndex < 0)
                {
                    takeResult.MergeTo(ref revivStats);
                    address = 0;    // No candidate found
                    return false;
                }

                record = GetRecord(segmentStart + bestFitIndex);
                if (oversize ? record->TryTakeOversize(recordSize, minAddress, store, out address, ref takeResult) : record->TryTake(recordSize, minAddress, out address, ref takeResult))
                {
                    takeResult.MergeTo(ref revivStats);
                    return true;
                }

                // We found a candidate but CAS failed. Reduce the best fit scan length and continue.
                localBestFitScanLimit /= 2;
                if (localBestFitScanLimit <= 1)
                    return TryTakeFirstFit(recordSize, minAddress, store, oversize, out address, ref revivStats);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ScanForEmpty<TKey, TValue>(FreeRecordPool<TKey, TValue> recordPool, CancellationToken cancellationToken)
        {
            // Add() always sets isEmpty to false and we do not clear isEmpty on Take() because that could lead to more lost "isEmpty = false".
            // So this routine is called only if the bin is marked not-empty.
            for (var ii = 0; ii < recordCount; ++ii)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;
                FreeRecord record = *(records + ii);
                if (record.IsSet)
                {
                    // Still not empty; only the CheckEmptyWorker thread should set this.isEmpty, so the value should not have changed.
                    // Don't set it spuriously, to avoid unnecessary cache-coherency overhead.
                    Debug.Assert(!isEmpty, "Should never have a bin marked Empty when there are records added");
                    return;
                }
            }
            isEmpty = true;
        }

        public void Dispose()
        {
            // Currently nothing
        }
    }

    internal unsafe class FreeRecordPool<Key, Value> : IDisposable
    {
        internal readonly TsavoriteKV<Key, Value> store;
        internal readonly FreeRecordBin[] bins;

        internal int numberOfBinsToSearch;
        internal bool IsFixedLength;

        internal readonly int[] sizeIndexArray;
        private readonly int* sizeIndex;
        private readonly int numBins;

        internal readonly CheckEmptyWorker<Key, Value> checkEmptyWorker;

        /// <inheritdoc/>
        public override string ToString()
            => $"isFixedLen {IsFixedLength}, numBins {numBins}, searchNextBin {numberOfBinsToSearch}, checkEmptyWorker: {checkEmptyWorker}";

        internal FreeRecordPool(TsavoriteKV<Key, Value> store, RevivificationSettings settings, int fixedRecordLength)
        {
            this.store = store;
            IsFixedLength = fixedRecordLength > 0;

            checkEmptyWorker = new(this);

            if (IsFixedLength)
            {
                numBins = 1;
                bins = new[] { new FreeRecordBin(ref settings.FreeRecordBins[0], fixedRecordLength, isFixedLength: true) };
                return;
            }

            // First create the "size index": a cache-aligned vector of int bin sizes. This way searching for the bin
            // for a record size will stay in a single cache line (unless there are more than 16 bins).
            var sizeIndexCount = RoundUp(settings.FreeRecordBins.Length * sizeof(int), Constants.kCacheLineBytes) / sizeof(int);

            // Overallocate the GCHandle by one cache line so we have room to offset the returned pointer to make it cache-aligned.
            sizeIndexArray = GC.AllocateArray<int>(sizeIndexCount + Constants.kCacheLineBytes / sizeof(int), pinned: true);
            long p = (long)Unsafe.AsPointer(ref sizeIndexArray[0]);

            // Force the pointer to align to cache boundary.
            long p2 = RoundUp(p, Constants.kCacheLineBytes);
            sizeIndex = (int*)p2;

            // Create the bins.
            List<FreeRecordBin> binList = new();
            int prevBinRecordSize = RevivificationBin.MinRecordSize - 8;      // The minimum record size increment is 8, so the first bin will set this to MinRecordSize or more
            for (var ii = 0; ii < settings.FreeRecordBins.Length; ++ii)
            {
                if (prevBinRecordSize >= settings.FreeRecordBins[ii].RecordSize)
                    continue;
                FreeRecordBin bin = new(ref settings.FreeRecordBins[ii], prevBinRecordSize, isFixedLength: false);
                sizeIndex[binList.Count] = bin.maxRecordSize;
                binList.Add(bin);
                prevBinRecordSize = bin.maxRecordSize;
            }
            bins = binList.ToArray();
            numBins = bins.Length;
            numberOfBinsToSearch = settings.NumberOfBinsToSearch;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool GetBinIndex(int size, out int binIndex)
        {
            Debug.Assert(!IsFixedLength, "Should only search bins if !IsFixedLength");

            // Sequential search in the sizeIndex for the requested size.
            for (var ii = 0; ii < numBins; ++ii)
            {
                if (sizeIndex[ii] >= size)
                {
                    binIndex = ii;
                    return true;
                }
            }
            binIndex = -1;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAdd(long logicalAddress, int size, ref RevivificationStats revivStats)
        {
            var minAddress = store.GetMinRevivifiableAddress();
            int binIndex = 0;
            if (logicalAddress < minAddress || (!IsFixedLength && !GetBinIndex(size, out binIndex)))
                return false;
            if (!bins[binIndex].TryAdd(logicalAddress, size, store, minAddress, ref revivStats))
                return false;

            // We've added a record, so now start the worker thread that periodically checks to see if Take() has emptied the bins.
            checkEmptyWorker.Start();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAdd(long logicalAddress, long physicalAddress, int allocatedSize, ref RevivificationStats revivStats)
        {
            var minAddress = store.GetMinRevivifiableAddress();
            if (logicalAddress < minAddress)
            {
                ++revivStats.failedAdds;
                return false;
            }
            var recordInfo = store.hlog.GetInfo(physicalAddress);
            recordInfo.TrySeal(invalidate: true);
            store.SetFreeRecordSize(physicalAddress, ref recordInfo, allocatedSize);
            bool result = TryAdd(logicalAddress, allocatedSize, ref revivStats);

            if (result)
                ++revivStats.successfulAdds;
            else
                ++revivStats.failedAdds;
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryTake(int recordSize, long minAddress, out long address, ref RevivificationStats revivStats)
        {
            address = 0;
            bool result = false;
            if (IsFixedLength)
                result = bins[0].TryTake(recordSize, minAddress, store, out address, ref revivStats);
            else if (GetBinIndex(recordSize, out int index))
            {
                // Try to Take from the initial bin and if unsuccessful, try the next-highest bin if requested.
                result = bins[index].TryTake(recordSize, minAddress, store, oversize: sizeIndex[index] > RevivificationBin.MaxInlineRecordSize, out address, ref revivStats);
                for (int ii = 0; !result && ii < numberOfBinsToSearch && index < numBins - 1; ++ii)
                    result = bins[++index].TryTake(recordSize, minAddress, store, oversize: sizeIndex[index] > RevivificationBin.MaxInlineRecordSize, out address, ref revivStats);
            }

            if (result)
                ++revivStats.successfulTakes;
            else
                ++revivStats.failedTakes;
            return result;
        }

        internal void ScanForEmpty(CancellationToken cancellationToken)
        {
            foreach (var bin in bins)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;
                if (!bin.isEmpty)
                    bin.ScanForEmpty(this, cancellationToken);
            }
        }

        public void Dispose()
        {
            foreach (var bin in bins)
                bin.Dispose();
            checkEmptyWorker.Dispose();
        }
    }
}