// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;

namespace Tsavorite.core
{
    /// <summary>
    /// Downlevel object-log length decode, retained only for reading databases written before the length-hint format. New records are
    /// never written in this encoding.
    /// <para>In this encoding the on-disk overflow/object length is split across two locations: the low
    /// <see cref="RecordDataHeader.kKeyLengthBits"/> / <see cref="RecordDataHeader.kValueLengthBits"/> bits live in the RDH
    /// KeyLength/ValueLength field, and the next 32 bits are in the objectId slot at keyAddress/valueAddress. The object-log stream
    /// carries no length framing. The record's ObjectLogPosition word has the
    /// <see cref="ObjectLogFilePositionInfo.kReuseObjectIdForSizeBit"/> flag SET; that flag is the per-record discriminator that selects
    /// this decode on read (only reachable while recovering a downlevel checkpoint).</para>
    /// </summary>
    public unsafe partial struct LogRecord : ISourceLogRecord
    {
        /// <summary>
        /// Returns the object-log start position and the split (RDH-field low bits + objectId-slot high 32 bits) key/value lengths.
        /// </summary>
        /// <param name="keyLength">Outputs key length; set for an overflow key.</param>
        /// <param name="valueObjectLength">Outputs value length; set for an overflow or object value.</param>
        /// <returns>The object-log position word for this record, with flag bits masked off (segment+offset only).</returns>
        internal readonly ulong GetObjectLogRecordStartPositionAndLengths_v21(out int keyLength, out ulong valueObjectLength)
        {
            var dataHeader = DataHeader;
            if (dataHeader.KeyIsOverflow)
            {
                var (_ /*kLen*/, keyAddress) = dataHeader.GetKeyFieldInfo(physicalAddress);
                // Combine the RDH low bits with the next 32 bits from the objectId slot at keyAddress.
                var keyHighBits = (ulong)(uint)*(int*)keyAddress;
                var combinedKeyLength = (keyHighBits << RecordDataHeader.kKeyLengthBits) | (ulong)(uint)dataHeader.GetKeyLengthRaw();
                Debug.Assert(combinedKeyLength <= int.MaxValue, $"Key length {combinedKeyLength} exceeds int.MaxValue");
                keyLength = (int)combinedKeyLength;
            }
            else // KeyIsInline is true; keyLength will be ignored
                keyLength = 0;

            var (valueLength, valueAddress) = dataHeader.GetValueFieldInfo(physicalAddress);
            if (!dataHeader.ValueIsInline)
            {
                var valueHighBits = (ulong)(uint)*(int*)valueAddress;
                valueObjectLength = (valueHighBits << RecordDataHeader.kValueLengthBits) | (ulong)(uint)dataHeader.GetValueLengthRaw();
            }
            else // ValueIsInline is true; valueLength will be ignored
            {
                valueObjectLength = 0;
                if (dataHeader.RecordIsInline) // If the record is fully inline, we should not be called here
                {
                    Debug.Fail("Cannot call GetObjectLogRecordStartPositionAndLengths_v21 for an inline record");
                    return 0;
                }
            }

            // Read the position word; mask off flag bits to return just segment+offset.
            var word = *(ulong*)GetObjectLogPositionAddress(GetOptionalStartAddress());
            return word & ObjectLogFilePositionInfo.SegmentAndOffsetMask;
        }
    }
}
