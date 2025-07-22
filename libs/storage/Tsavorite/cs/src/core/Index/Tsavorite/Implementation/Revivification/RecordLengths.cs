// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    using static Utility;

    public unsafe partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long GetMinRevivifiableAddress()
            => RevivificationManager.GetMinRevivifiableAddress(hlogBase.GetTailAddress(), hlogBase.ReadOnlyAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetValueOffset(long physicalAddress, ref TValue recordValue) => (int)((long)Unsafe.AsPointer(ref recordValue) - physicalAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe int* GetExtraValueLengthPointer(ref TValue recordValue, int usedValueLength)
        {
            Debug.Assert(RoundUp(usedValueLength, sizeof(int)) == usedValueLength, "GetLiveFullValueLengthPointer: usedValueLength should have int-aligned length");
            return (int*)((long)Unsafe.AsPointer(ref recordValue) + usedValueLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe void SetExtraValueLength(ref TValue recordValue, ref RecordInfo recordInfo, int usedValueLength, int fullValueLength)
        {
            if (RevivificationManager.IsFixedLength)
                recordInfo.ClearHasFiller();
            else
                SetVarLenExtraValueLength(ref recordValue, ref recordInfo, usedValueLength, fullValueLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void SetVarLenExtraValueLength(ref TValue recordValue, ref RecordInfo recordInfo, int usedValueLength, int fullValueLength)
        {
            usedValueLength = RoundUp(usedValueLength, sizeof(int));
            Debug.Assert(fullValueLength >= usedValueLength, $"SetFullValueLength: usedValueLength {usedValueLength} cannot be > fullValueLength {fullValueLength}");
            int extraValueLength = fullValueLength - usedValueLength;
            if (extraValueLength >= sizeof(int))
            {
                var extraValueLengthPtr = GetExtraValueLengthPointer(ref recordValue, usedValueLength);
                Debug.Assert(*extraValueLengthPtr == 0 || *extraValueLengthPtr == extraValueLength, "existing ExtraValueLength should be 0 or the same value");

                // We always store the "extra" as the difference between the aligned usedValueLength and the fullValueLength.
                // However, the UpdateInfo structures use the unaligned usedValueLength; aligned usedValueLength is not visible to the user.
                *extraValueLengthPtr = extraValueLength;
                recordInfo.SetHasFiller();
                return;
            }
            recordInfo.ClearHasFiller();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal (int usedValueLength, int fullValueLength, int fullRecordLength) GetRecordLengths(long physicalAddress, ref TValue recordValue, ref RecordInfo recordInfo)
        {
            // FixedLen may be GenericAllocator which does not point physicalAddress to the actual record location, so calculate fullRecordLength via GetAverageRecordSize().
            if (RevivificationManager.IsFixedLength)
                return (RevivificationManager<TKey, TValue, TStoreFunctions, TAllocator>.FixedValueLength, RevivificationManager<TKey, TValue, TStoreFunctions, TAllocator>.FixedValueLength, hlog.GetAverageRecordSize());

            int usedValueLength, fullValueLength, allocatedSize, valueOffset = GetValueOffset(physicalAddress, ref recordValue);
            if (recordInfo.HasFiller)
            {
                usedValueLength = hlog.GetValueLength(ref recordValue);
                var alignedUsedValueLength = RoundUp(usedValueLength, sizeof(int));
                fullValueLength = alignedUsedValueLength + *GetExtraValueLengthPointer(ref recordValue, alignedUsedValueLength);
                Debug.Assert(fullValueLength >= usedValueLength, $"GetLengthsFromFiller: fullValueLength {fullValueLength} should be >= usedValueLength {usedValueLength}");
                allocatedSize = valueOffset + fullValueLength;
            }
            else
            {
                // Live VarLen record with no stored sizes; we always have a Key and Value (even if defaults). Return the full record length (including recordInfo and Key).
                (int actualSize, allocatedSize) = hlog.GetRecordSize(physicalAddress);
                usedValueLength = actualSize - valueOffset;
                fullValueLength = allocatedSize - valueOffset;
            }

            Debug.Assert(usedValueLength >= 0, $"GetLiveRecordLengths: usedValueLength {usedValueLength}");
            Debug.Assert(fullValueLength >= 0, $"GetLiveRecordLengths: fullValueLength {fullValueLength}");
            Debug.Assert(allocatedSize >= 0, $"GetLiveRecordLengths: fullRecordLength {allocatedSize}");
            return (usedValueLength, fullValueLength, allocatedSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (int usedValueLength, int fullValueLength) GetNewValueLengths(int actualSize, int allocatedSize, long newPhysicalAddress, ref TValue recordValue)
        {
            // Called after a new record is allocated
            if (RevivificationManager.IsFixedLength)
                return (RevivificationManager<TKey, TValue, TStoreFunctions, TAllocator>.FixedValueLength, RevivificationManager<TKey, TValue, TStoreFunctions, TAllocator>.FixedValueLength);

            int valueOffset = GetValueOffset(newPhysicalAddress, ref recordValue);
            int usedValueLength = actualSize - valueOffset;
            int fullValueLength = allocatedSize - valueOffset;
            Debug.Assert(usedValueLength >= 0, $"GetNewValueLengths: usedValueLength {usedValueLength}");
            Debug.Assert(fullValueLength >= 0, $"GetNewValueLengths: fullValueLength {fullValueLength}");
            Debug.Assert(fullValueLength >= RoundUp(usedValueLength, sizeof(int)), $"GetNewValueLengths: usedValueLength {usedValueLength} cannot be > fullValueLength {fullValueLength}");

            return (usedValueLength, fullValueLength);
        }

        // A "free record" is one on the FreeList.
        #region FreeRecords

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetFreeRecordSize(long physicalAddress, ref RecordInfo recordInfo, int allocatedSize)
        {
            // Skip the valuelength calls if we are not VarLen.
            if (RevivificationManager.IsFixedLength)
            {
                recordInfo.ClearHasFiller();
                return;
            }

            // Store the full value length. Defer clearing the Key until the record is revivified (it may never be).
            ref TValue recordValue = ref hlog.GetValue(physicalAddress);
            int usedValueLength = hlog.GetValueLength(ref recordValue);
            int fullValueLength = allocatedSize - GetValueOffset(physicalAddress, ref recordValue);
            SetVarLenExtraValueLength(ref recordValue, ref recordInfo, usedValueLength, fullValueLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetFreeRecordSize(long physicalAddress, ref RecordInfo recordInfo)
            => RevivificationManager.IsFixedLength
                ? hlog.GetAverageRecordSize()
                : GetRecordLengths(physicalAddress, ref hlog.GetValue(physicalAddress), ref recordInfo).fullRecordLength;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static void ClearExtraValueSpace(ref RecordInfo recordInfo, ref TValue recordValue, int usedValueLength, int fullValueLength)
        {
            // SpanByte's implementation of GetAndInitializeValue does not clear the space after usedValueLength. This may be
            // considerably less than the previous value length, so we clear it here before DisposeForRevivification. This space
            // includes the extra value length if Filler is set, so we must clear the space before clearing the Filler bit so
            // log-scan traversal does not see nonzero values past Value (it's fine if we see the Filler and extra length is 0).
            int extraValueLength = fullValueLength - usedValueLength;   // do not round up usedValueLength; we must clear all extra bytes
            if (extraValueLength > 0)
            {
                // Even though this says "SpanByte" it is just a utility function to zero space; no actual SpanByte instance is assumed
                SpanByte.Clear((byte*)Unsafe.AsPointer(ref recordValue) + usedValueLength, extraValueLength);
            }
            recordInfo.ClearHasFiller();
        }

        // Do not try to inline this; it causes TryAllocateRecord to bloat and slow
        bool TryTakeFreeRecord<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, int requiredSize, ref int allocatedSize, int newKeySize, long minRevivAddress,
                    out long logicalAddress, out long physicalAddress)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Caller checks for UseFreeRecordPool
            if (RevivificationManager.TryTake(allocatedSize, minRevivAddress, out logicalAddress, ref sessionFunctions.Ctx.RevivificationStats))
            {
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                ref RecordInfo recordInfo = ref hlog.GetInfo(physicalAddress);
                Debug.Assert(recordInfo.IsSealed, "TryTakeFreeRecord: recordInfo should still have the revivification Seal");

                // If IsFixedLengthReviv, the allocatedSize will be unchanged
                if (!RevivificationManager.IsFixedLength)
                {
                    var (usedValueLength, fullValueLength, fullRecordLength) = GetRecordLengths(physicalAddress, ref hlog.GetValue(physicalAddress), ref recordInfo);

                    // ClearExtraValueSpace has already been called (at freelist-add time) to zero the end of the value space between used and full value lengths and clear the Filler.
                    // Now we use the newKeySize to find out how much space is actually required.
                    var valueOffset = fullRecordLength - fullValueLength;
                    var requiredValueLength = requiredSize - valueOffset;
                    var minValueLength = requiredValueLength < usedValueLength ? requiredValueLength : usedValueLength;
                    ref var recordValue = ref hlog.GetValue(physicalAddress);
                    Debug.Assert(valueOffset == (long)Unsafe.AsPointer(ref recordValue) - physicalAddress);

                    // Clear any no-longer-needed space, then call DisposeForRevivification again with newKeySize so SpanByte can be efficient about zeroinit.
                    ClearExtraValueSpace(ref recordInfo, ref recordValue, minValueLength, fullValueLength);
                    storeFunctions.DisposeRecord(ref hlog.GetKey(physicalAddress), ref recordValue, DisposeReason.RevivificationFreeList, newKeySize);

                    Debug.Assert(fullRecordLength >= allocatedSize, $"TryTakeFreeRecord: fullRecordLength {fullRecordLength} should be >= allocatedSize {allocatedSize}");
                    allocatedSize = fullRecordLength;
                }

                // Preserve the Sealed bit due to checkpoint/recovery; see RecordInfo.WriteInfo.
                return true;
            }

            // No free record available.
            logicalAddress = physicalAddress = default;
            return false;
        }

        #endregion FreeRecords

        // TombstonedRecords are in the tag chain with the tombstone bit set (they are not in the freelist). They preserve the key (they mark that key as deleted,
        // which is important if there is a subsequent record for that key), and store the full Value length after the used value data (if there is room).
        #region TombstonedRecords

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetTombstoneAndExtraValueLength(ref TValue recordValue, ref RecordInfo recordInfo, int usedValueLength, int fullValueLength)
        {
            recordInfo.SetTombstone();
            if (RevivificationManager.IsFixedLength)
            {
                recordInfo.ClearHasFiller();
                return;
            }

            Debug.Assert(usedValueLength == hlog.GetValueLength(ref recordValue));
            SetVarLenExtraValueLength(ref recordValue, ref recordInfo, usedValueLength, fullValueLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal (bool ok, int usedValueLength) TryReinitializeTombstonedValue<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref RecordInfo srcRecordInfo, ref TKey key, ref TValue recordValue, int requiredSize, (int usedValueLength, int fullValueLength, int allocatedSize) recordLengths, long physicalAddress)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (RevivificationManager.IsFixedLength || recordLengths.allocatedSize < requiredSize)
                return (false, recordLengths.usedValueLength);

            // Zero the end of the value space between required and full value lengths and clear the Filler.
            var valueOffset = recordLengths.allocatedSize - recordLengths.fullValueLength;
            var requiredValueLength = requiredSize - valueOffset;
            var minValueLength = requiredValueLength < recordLengths.usedValueLength ? requiredValueLength : recordLengths.usedValueLength;

            // clears out the minimum space possible. So let's say we are shrinking our usage from 8 bytes to 3 bytes. This will clear only the bytes 4-8.
            // if we are expanding this will not clear anything.
            ClearExtraValueSpace(ref srcRecordInfo, ref recordValue, minValueLength, recordLengths.fullValueLength);

            srcRecordInfo.ClearTombstone();

            // for SpanByte, this will set the new length (payload + metadata).
            hlog.GetAndInitializeValue(physicalAddress, physicalAddress + requiredSize);
            // since the above sets the Length, we can use the below to get TotalSize which represents the UsedLength of a record
            var newUsedValueLength = hlog.GetValueLength(ref recordValue);

            // potentially sets filler, if the used value length is going to be under the full length by more than 4 bytes.
            SetExtraValueLength(ref recordValue, ref srcRecordInfo, usedValueLength: newUsedValueLength, recordLengths.fullValueLength);

            return (true, newUsedValueLength);
        }

        #endregion TombstonedRecords
    }
}