// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>The record on the log: header, key, value, and optional fields</summary>
    /// <remarks>The space is laid out as:
    ///     <list>
    ///     <item>[RecordInfo][SpanByte key][SpanByte value][DBId?][ETag?][Expiration?][FillerLength]</item>
    ///     </list>
    /// This lets us get to the key and value without intermediate computations to account for the optional fields.
    /// </remarks>
    public unsafe struct StringLogRecord(long physicalAddress)
    {
        internal readonly LogRecordBase recBase = new(physicalAddress);

        /// <summary>The value object id (index into the object values array)</summary>
        public readonly ref SpanByte ValueRef => ref *(SpanByte*)(physicalAddress + recBase.ValueOffset);

        public readonly int RecordSize => recBase.GetRecordSize(ValueRef.TotalSize);

        public readonly bool TrySetValueLength(int newValueLen)
        {
            // Do nothing if no size change. Growth and extraLen may be negative if shrinking.
            var growth = newValueLen - ValueRef.TotalSize;
            if (growth == 0)
                return true;

            var recordLen = recBase.GetRecordSize(ValueRef.TotalSize);
            var maxLen = recordLen + recBase.GetFillerLen(ValueRef.TotalSize);
            var availableSpace = maxLen - recordLen;

            var optLen = recBase.OptionalLength;
            var optStartAddress = recBase.GetOptionalStartAddress(ValueRef.TotalSize);
            var fillerLenAddress = recBase.physicalAddress + recBase.ValueOffset + ValueRef.TotalSize + optLen;
            var extraLen = availableSpace - growth;

            if (growth > 0)
            {
                // TODO: We're growing. Evaluate whether RecordSize plus ExtraValueLen allows the growth of Value
                if (growth > availableSpace)
                    return false;

                // Preserve zero-init by:
                //  - Zeroing out FillerLen (this will leave only zeroes all the way to the next record, as there is nothing past FillerLen in this record).
                //      - We must do this here in case there is not enough room for filler after the growth.
                if (recBase.Info.HasFiller)
                    *(int*)fillerLenAddress = 0;
                //  - Zeroing out optionals is *not* necessary as LogRecordBase.GetRecordSize bases its calculation on Info bits.
                //  - Update the value length
                ValueRef.Length += growth;
                //  - Set the new (reduced) ExtraValueLength if there is still space for it.
                if (extraLen >= Constants.FillerLenSize)
                {
                    fillerLenAddress += growth;
                    *(int*)fillerLenAddress = extraLen;
                }
                else
                    recBase.InfoRef.ClearHasFiller();

                // Put the optionals in their new locations (MemoryCopy handles overlap). Let the caller's value update overwrite the previous optional space.
                Buffer.MemoryCopy((byte*)optStartAddress, (byte*)(optStartAddress + growth), optLen, optLen);

                return true;
            }

            // We're shrinking. Preserve zero-init by:
            //  - Store off optionals. We don't need to store FillerLen because we have to recalculate it anyway.
            var saveBuf = stackalloc byte[optLen]; // Garanteed not to be large
            Buffer.MemoryCopy((byte*)optStartAddress, saveBuf, optLen, optLen);
            //  - Zeroing FillerLen and the optionals space
            if (recBase.Info.HasFiller)
                *(int*)fillerLenAddress = 0;
            Unsafe.InitBlockUnaligned((byte*)optStartAddress, 0, (uint)optLen);
            //  - Shrinking the value and zeroing unused space
            ValueRef.ShrinkSerializedLength(newValueLen);
            //  - Set the new (increased) ExtraValueLength first, then the optionals
            if (extraLen >= Constants.FillerLenSize)
                *(int*)fillerLenAddress = extraLen;
            Buffer.MemoryCopy((byte*)(optStartAddress + growth), saveBuf, optLen, optLen);

            return true;
        }

        /// <inheritdoc/>
        public override readonly string ToString() => recBase.ToString(ValueRef.TotalSize, ValueRef.ToShortString(20));
    }
}