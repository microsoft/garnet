// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>The in-memory record on the log: header, key, value, and optional fields</summary>
    /// <remarks>The space is laid out as:
    ///     <list>
    ///     <item>[RecordInfo][SpanByte key][SpanByte value][DBId?][ETag?][Expiration?][FillerLength]</item>
    ///     </list>
    /// This lets us get to the key and value without intermediate computations to account for the optional fields.
    /// </remarks>
    public unsafe struct StringLogRecord
    {
        public readonly LogRecordBase RecBase;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal StringLogRecord(long physicalAddress) => RecBase = new(physicalAddress);

        #region IReadOnlyRecord
        /// <inheritdoc/>
        public readonly ref RecordInfo InfoRef => ref RecBase.InfoRef;
        /// <inheritdoc/>
        public readonly RecordInfo Info => RecBase.Info;
        /// <inheritdoc/>
        public readonly SpanByte Key => RecBase.Key;
        /// <inheritdoc/>
        public readonly SpanByte ValueSpan => Value;
        /// <inheritdoc/>
        public readonly IHeapObject ValueObject => throw new TsavoriteException("StringLogRecord does not have Object values");
        /// <inheritdoc/>
        public readonly int DBId => RecBase.GetDBId(ValueLen);
        /// <inheritdoc/>
        public readonly long ETag => RecBase.GetETag(ValueLen);
        /// <inheritdoc/>
        public readonly long Expiration => RecBase.GetExpiration(ValueLen);
        #endregion //IReadOnlyRecord

        /// <summary>The readonly span of the value; may be inline-serialized or out-of-line overflow</summary>
        public readonly SpanByte Value => *(SpanByte*)RecBase.ValueAddress;

        /// <summary>The span of the value; may be inline-serialized or out-of-line overflow</summary>
        public readonly ref SpanByte ValueRef => ref *(SpanByte*)RecBase.ValueAddress;

        internal readonly int ValueLen => Value.TotalInlineSize;

        public readonly int RecordSize => RecBase.GetRecordSize(ValueLen);
        public readonly (int actualSize, int allocatedSize) RecordSizes => RecBase.GetRecordSizes(ValueLen);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetValueLength(int newValueLen)
        {
            // Do nothing if no size change. Growth and extraLen may be negative if shrinking.
            var growth = newValueLen - ValueLen;
            if (growth == 0)
                return true;

            var recordLen = RecBase.GetRecordSize(ValueLen);
            var maxLen = recordLen + RecBase.GetFillerLen(ValueLen);
            var availableSpace = maxLen - recordLen;

            var optLen = RecBase.OptionalLength;
            var optStartAddress = RecBase.GetOptionalStartAddress(ValueLen);
            var fillerLenAddress = RecBase.ValueAddress + ValueLen + optLen;
            var extraLen = availableSpace - growth;

            if (growth > 0)
            {
                // We're growing. See if there is enough space for the requested growth of Value.
                if (growth > availableSpace)
                    return false;

                // Preserve zero-init by:
                //  - Zeroing out FillerLen (this will leave only zeroes all the way to the next record, as there is nothing past FillerLen in this record).
                //      - We must do this here in case there is not enough room for filler after the growth.
                if (RecBase.Info.HasFiller)
                    *(int*)fillerLenAddress = 0;
                //  - Zeroing out optionals is *not* necessary as LogRecordBase.GetRecordSize bases its calculation on Info bits.
                //  - Update the value length
                ValueRef.Length += growth;
                //  - Set the new (reduced) ExtraValueLength if there is still space for it.
                if (extraLen >= LogRecordBase.FillerLenSize)
                {
                    fillerLenAddress += growth;
                    *(int*)fillerLenAddress = extraLen;
                }
                else
                    RecBase.InfoRef.ClearHasFiller();

                // Put the optionals in their new locations (MemoryCopy handles overlap). Let the caller's value update overwrite the previous optional space.
                Buffer.MemoryCopy((byte*)optStartAddress, (byte*)(optStartAddress + growth), optLen, optLen);

                return true;
            }

            // We're shrinking. Preserve zero-init by:
            //  - Store off optionals. We don't need to store FillerLen because we have to recalculate it anyway.
            var saveBuf = stackalloc byte[optLen]; // Garanteed not to be large
            Buffer.MemoryCopy((byte*)optStartAddress, saveBuf, optLen, optLen);
            //  - Zeroing FillerLen and the optionals space
            if (RecBase.Info.HasFiller)
                *(int*)fillerLenAddress = 0;
            Unsafe.InitBlockUnaligned((byte*)optStartAddress, 0, (uint)optLen);
            //  - Shrinking the value and zeroing unused space
            ValueRef.ShrinkSerializedLength(newValueLen);
            //  - Set the new (increased) ExtraValueLength first, then the optionals
            if (extraLen >= LogRecordBase.FillerLenSize)
                *(int*)fillerLenAddress = extraLen;
            Buffer.MemoryCopy((byte*)(optStartAddress + growth), saveBuf, optLen, optLen);

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetValue(SpanByte value)
        {
            if (!TrySetValueLength(ValueLen))
                return false;
            value.CopyTo(ref ValueRef);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetETag(long eTag) => RecBase.TrySetETag(ValueLen, eTag);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void RemoveETag() => RecBase.RemoveETag(ValueLen);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetExpiration(long expiration) => RecBase.TrySetExpiration(ValueLen, expiration);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void RemoveExpiration() => RecBase.RemoveExpiration(ValueLen);

        /// <inheritdoc/>
        public override readonly string ToString() => RecBase.ToString(ValueLen, Value.ToShortString(20));
    }
}