// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;
    using static VarbyteLengthUtility;

    /// <summary>
    /// Struct for information about the key and the fields and their sizes in a record.
    /// </summary>
    public struct RecordSizeInfo
    {
        /// <summary>The value length and whether optional fields are present.</summary>
        public RecordFieldInfo FieldInfo;

        /// <summary>Whether the key was within the inline max key length. Set automatically by Tsavorite based on <see cref="RecordFieldInfo.KeySize"/> key size.</summary>
        public bool KeyIsInline;

        /// <summary>Whether the value was within the inline max value length.</summary>
        public bool ValueIsInline;

        /// <summary>Varbyte indicator word, containing the Indicator Byte as well as the key and value lengths; see <see cref="VarbyteLengthUtility"/>.</summary>
        public long IndicatorWord;

        /// <summary>Number of bytes in key length; see <see cref="VarbyteLengthUtility"/>.</summary>
        public int KeyLengthBytes;

        /// <summary>Number of bytes in value length; see <see cref="VarbyteLengthUtility"/>.</summary>
        public int ValueLengthBytes;

        /// <summary>Whether the value was specified to be an object.</summary>
        public readonly bool ValueIsObject => FieldInfo.ValueIsObject;

        /// <summary>Whether the key is an overflow allocation.</summary>
        public readonly bool KeyIsOverflow => !KeyIsInline;

        /// <summary>Whether the value is an overflow allocation.</summary>
        public readonly bool ValueIsOverflow => !ValueIsInline && !ValueIsObject;

        /// <summary>Returns the inline length of the key (the amount it will take in the record).</summary>
        public readonly int InlineKeySize => KeyIsInline ? FieldInfo.KeySize : ObjectIdMap.ObjectIdSize;

        /// <summary>Returns the inline length of the value (the amount it will take in the record).</summary>
        public readonly int InlineValueSize => ValueIsInline ? FieldInfo.ValueSize : ObjectIdMap.ObjectIdSize;

        /// <summary>The max inline value size if this is a record in the string log.</summary>
        public int MaxInlineValueSize { readonly get; internal set; }

        /// <summary>The inline size of the record (in the main log). If Key and/or Value are overflow (or value is Object),
        /// then their contribution to inline length is just <see cref="ObjectIdMap.ObjectIdSize"/>.</summary>
        public int ActualInlineRecordSize { readonly get; internal set; }

        /// <summary>The inline size of the record rounded up to <see cref="RecordInfo"/> alignment.</summary>
        public int AllocatedInlineRecordSize { readonly get; internal set; }

        /// <summary>Size to allocate for ETag if it will be included, else 0.</summary>
        public readonly int ETagSize => FieldInfo.HasETag ? LogRecord.ETagSize : 0;

        /// <summary>Size to allocate for Expiration if it will be included, else 0.</summary>
        public readonly int ExpirationSize => FieldInfo.HasExpiration ? LogRecord.ExpirationSize : 0;

        /// <summary>If true, this record specification has non-inline Key or Value or both, so will require the object log.</summary>
        public readonly bool RecordHasObjects => !KeyIsInline || !ValueIsInline;

        /// <summary>Size to allocate for Expiration if it will be included, else 0.</summary>
        public readonly int ObjectLogPositionSize => RecordHasObjects ? LogRecord.ObjectLogPositionSize : 0;

        /// <summary>Size to allocate for all optional fields that will be included; possibly 0.</summary>
        public readonly int OptionalSize => ETagSize + ExpirationSize + ObjectLogPositionSize;

        /// <summary>Whether these values are set (default instances are used for Delete internally, for example).</summary>
        public readonly bool IsSet => AllocatedInlineRecordSize != 0;

        internal void CalculateSizes(int keySize, int valueSize)
        {
            // Varbyte lengths. Add optionalSize to the effective value size when calculating valueLengthBytes so the value can grow if optionals are removed
            // (otherwise the filler-related calculations would require additional logic to constrain value size to the # of bytes we calculate here).
            IndicatorWord = ConstructInlineVarbyteLengthWord(keySize, valueSize, flagBits: 0, out KeyLengthBytes, out ValueLengthBytes);

            // Record
            var numVarbytes = NumIndicatorBytes + KeyLengthBytes + ValueLengthBytes;
            ActualInlineRecordSize = RecordInfo.Size + numVarbytes + keySize + valueSize + OptionalSize;
            AllocatedInlineRecordSize = RoundUp(ActualInlineRecordSize, Constants.kRecordAlignment);
        }

        /// <summary>Gets the value length currently in the record (e.g. before being updated with FieldInfo.ValueSize).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly unsafe int GetValueInlineLength(long recordPhysicalAddress)
            => (int)ReadVarbyteLength(ValueLengthBytes, (byte*)(recordPhysicalAddress + RecordInfo.Size + NumIndicatorBytes + KeyLengthBytes));

        /// <summary>Gets the Key address in the record.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly unsafe long GetKeyAddress(long recordPhysicalAddress)
            => recordPhysicalAddress + RecordInfo.Size + NumIndicatorBytes + KeyLengthBytes + ValueLengthBytes;

        /// <summary>Gets the Value address in the record.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly unsafe long GetValueAddress(long recordPhysicalAddress)
            => recordPhysicalAddress + RecordInfo.Size + NumIndicatorBytes + KeyLengthBytes + ValueLengthBytes + InlineKeySize;

        /// <summary>
        /// Called from Upsert or RMW methods for Span Values with the actual data size of the update value; ensures consistency between the Get*FieldInfo methods and the actual update methods.
        /// Usually called directly to save the cost of calculating actualDataSize twice (in Get*FieldInfo and the actual update methods).
        /// </summary>
        [Conditional("DEBUG")]
        public static void AssertValueDataLength(int dataSize, in RecordSizeInfo sizeInfo)
        {
            Debug.Assert(sizeInfo.FieldInfo.ValueSize == dataSize, $"Mismatch between expected value size {sizeInfo.FieldInfo.ValueSize} and actual value size {dataSize}");
        }

        /// <summary>Called from Upsert or RMW methods with the final record info; ensures consistency between the Get*FieldInfo methods and the actual update methods./// </summary>
        [Conditional("DEBUG")]
        public void AssertOptionals(RecordInfo recordInfo, bool checkETag = true, bool checkExpiration = true)
        {
            if (checkETag)
                Debug.Assert(FieldInfo.HasETag == recordInfo.HasETag, $"Mismatch between expected HasETag {FieldInfo.HasETag} and actual ETag {recordInfo.HasETag}");
            if (checkExpiration)
                Debug.Assert(FieldInfo.HasExpiration == recordInfo.HasExpiration, $"Mismatch between expected HasExpiration {FieldInfo.HasExpiration} and actual HasExpiration {recordInfo.HasExpiration}");
        }

        /// <inheritdoc/>
        public override readonly string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            return $"[{FieldInfo}] | KeyIsInl {bstr(KeyIsInline)}, ValIsInl {bstr(ValueIsInline)}, ValIsObj {bstr(ValueIsObject)}, ActRecSize {ActualInlineRecordSize}, AllocRecSize {AllocatedInlineRecordSize}, OptSize {OptionalSize}";
        }
    }
}