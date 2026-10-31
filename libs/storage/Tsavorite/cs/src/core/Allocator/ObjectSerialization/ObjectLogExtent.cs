// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Framing of the extent an object-log position addresses, and the record hints that size its first read.
    /// </summary>
    /// <remarks>
    /// A position addresses either a single data chunk (the record's only out-of-line component) or, when its
    /// <see cref="ObjectLogPositionInfo.kHasDirectoryMask"/> bit is set, the record's top directory record. Either way the
    /// addressed extent is framed one of two ways:
    /// <list type="bullet">
    /// <item><b>Exact</b>: a payload of at most <see cref="ExactCutoff"/> bytes is stored headerless, and the record's hint carries
    /// its exact length.</item>
    /// <item><b>Length-prefixed</b>: a larger payload is preceded by a 4-byte length, and the record's hint carries a 4 KB page count
    /// that sizes the first read. The prefix supplies the exact length, so no read depends on parsing another chunk first.</item>
    /// </list>
    /// Extents reached through a directory entry, which are data chunks and spine directory records, carry no prefix: the entry holds
    /// their exact length.
    /// </remarks>
    internal static class ObjectLogExtent
    {
        /// <summary>Size of the length prefix on a payload larger than <see cref="ExactCutoff"/>.</summary>
        internal const int LengthPrefixSize = sizeof(int);

        /// <summary>Largest payload stored headerless with its exact length in the record's hint.</summary>
        internal const int ExactCutoff = RecordDataHeader.kOutOfLineExactSizeCutoff;

        /// <summary>Unit of a non-exact hint's page count.</summary>
        internal const int HintPageSize = RecordDataHeader.kFlushPageSize;

        /// <summary>The value-hint value meaning "large": the extent is at least this many hint pages, and the first read covers
        /// <see cref="LargeInitialRead"/> bytes (clamped to the page and tail) before the prefix supplies the rest.</summary>
        internal const int LargeValueHint = ObjectIdMap.MaxObjectIdSizeHint;

        /// <summary>First read for a value whose hint is <see cref="LargeValueHint"/>: a full write chunk and its prefix, so a single
        /// chunk of any size the writer produces is read in one IO.</summary>
        internal const int LargeInitialRead = IStreamBuffer.BufferSize + LengthPrefixSize;

        /// <summary>Whether a payload of <paramref name="payloadLength"/> bytes is stored headerless.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsExact(long payloadLength) => payloadLength <= ExactCutoff;

        /// <summary>Bytes the extent occupies on the log for a payload of <paramref name="payloadLength"/> bytes.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long ExtentLength(long payloadLength) => IsExact(payloadLength) ? payloadLength : LengthPrefixSize + payloadLength;

        /// <summary>The objectId-slot hint for a key slot that sizes an extent with a payload of <paramref name="payloadLength"/> bytes.</summary>
        /// <param name="payloadLength">The extent's payload length.</param>
        /// <param name="rdhKeyLengthBits">The raw RDH KeyLength value: the page count's high bits, or the physical slot size when exact.</param>
        /// <param name="isExact">Whether the hint is the exact payload length.</param>
        /// <returns>The low 9 bits of the page count, or the exact length.</returns>
        internal static int EncodeKeyHint(long payloadLength, out int rdhKeyLengthBits, out bool isExact)
            => RecordDataHeader.ComputeOverflowKeySizeHint(payloadLength, ExtentLength(payloadLength), out rdhKeyLengthBits, out isExact);

        /// <summary>The objectId-slot hint for a value slot that sizes an extent with a payload of <paramref name="payloadLength"/> bytes.</summary>
        /// <param name="payloadLength">The extent's payload length.</param>
        /// <param name="isExact">Whether the hint is the exact payload length.</param>
        /// <returns>The exact length, a page count, or <see cref="LargeValueHint"/>.</returns>
        internal static int EncodeValueHint(long payloadLength, out bool isExact)
            => RecordDataHeader.ComputeObjectIdValueSizeHint(payloadLength, ExtentLength(payloadLength), out isExact);

        /// <summary>The first read for an extent described by a key slot: its exact length, or its exact page extent.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long KeyInitialRead(int rdhKeyLengthBits, int sizeHint, bool isExact)
            => (long)RecordDataHeader.DecodeOverflowKeyInitialReadExtent(rdhKeyLengthBits, sizeHint, isExact);

        /// <summary>The first read for an extent described by a value slot: its exact length, its page extent, or
        /// <see cref="LargeInitialRead"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long ValueInitialRead(int sizeHint, bool isExact)
        {
            if (isExact)
                return (uint)sizeHint;
            return sizeHint >= LargeValueHint ? LargeInitialRead : (long)(uint)sizeHint * HintPageSize;
        }
    }

    /// <summary>
    /// What a record's object-log write produced: its position word and the hints to stamp into its objectId slots.
    /// </summary>
    internal struct ObjectLogWriteResult
    {
        /// <summary>The position word: the extent's address, <see cref="ObjectLogPositionInfo.kHasDirectoryMask"/> when it is a directory
        /// record, and the exact-size flag of the slot that sizes it.</summary>
        internal ulong PositionWord;

        /// <summary>The key slot's hint, meaningful when the key is overflow.</summary>
        internal int KeySizeHint;

        /// <summary>The raw RDH KeyLength value for an overflow key: the key hint's page-count high bits, or the physical slot size.</summary>
        internal int KeyRdhLengthBits;

        /// <summary>The value slot's hint, meaningful when the value is out of line.</summary>
        internal int ValueSizeHint;

        /// <summary>Bytes the value object serialized to; zero for an overflow value or no value.</summary>
        internal ulong ValueObjectLength;

        /// <summary>Whether the position addresses a directory record.</summary>
        internal readonly bool HasDirectory => (PositionWord & ObjectLogPositionInfo.kHasDirectoryMask) != 0;

        /// <summary>The extent's address.</summary>
        internal readonly long Address => (long)(PositionWord & ObjectLogPositionInfo.AddressMask);

        /// <inheritdoc/>
        public override readonly string ToString()
            => $"Address {Address}; HasDirectory {HasDirectory}; KeyExact {(PositionWord & ObjectLogPositionInfo.kKeyIsExactSizeMask) != 0}; "
             + $"ValueExact {(PositionWord & ObjectLogPositionInfo.kValueIsExactSizeMask) != 0}; KeyHint {KeySizeHint}/{KeyRdhLengthBits}; "
             + $"ValueHint {ValueSizeHint}; ObjectLength {ValueObjectLength}";
    }

    /// <summary>
    /// What a reader needs from a record to find its out-of-line components: its position word, which components are out of line,
    /// and the hints stamped into its objectId slots.
    /// </summary>
    internal struct ObjectLogRecordRef
    {
        /// <summary>The position word as stamped, flags included.</summary>
        internal ulong PositionWord;

        /// <summary>Whether the key is an overflow key.</summary>
        internal bool KeyIsOverflow;

        /// <summary>Whether the value is an overflow value.</summary>
        internal bool ValueIsOverflow;

        /// <summary>Whether the value is an object.</summary>
        internal bool ValueIsObject;

        /// <summary>The key slot's hint.</summary>
        internal int KeySizeHint;

        /// <summary>The raw RDH KeyLength value of an overflow key.</summary>
        internal int KeyRdhLengthBits;

        /// <summary>The value slot's hint.</summary>
        internal int ValueSizeHint;

        /// <summary>Whether the value is out of line.</summary>
        internal readonly bool ValueIsOutOfLine => ValueIsOverflow || ValueIsObject;

        /// <summary>Whether the position is unstamped, so the record has no object-log bytes to read.</summary>
        internal readonly bool IsUnstamped => ObjectLogPositionInfo.WordIsUnstamped(PositionWord);

        /// <summary>Whether the addressed extent is headerless and occupies no bytes.</summary>
        internal readonly bool ExtentIsEmpty
            => (PositionWord & (KeyIsOverflow ? ObjectLogPositionInfo.kKeyIsExactSizeMask : ObjectLogPositionInfo.kValueIsExactSizeMask)) != 0
                && (KeyIsOverflow ? KeySizeHint : ValueSizeHint) == 0;

        /// <summary>Build the reference a reader needs from a write's result, as a record stamped with it would carry.</summary>
        internal static ObjectLogRecordRef FromWriteResult(in ObjectLogWriteResult result, bool keyIsOverflow, bool valueIsOverflow, bool valueIsObject)
            => new()
            {
                PositionWord = result.PositionWord,
                KeyIsOverflow = keyIsOverflow,
                ValueIsOverflow = valueIsOverflow,
                ValueIsObject = valueIsObject,
                KeySizeHint = result.KeySizeHint,
                KeyRdhLengthBits = result.KeyRdhLengthBits,
                ValueSizeHint = result.ValueSizeHint,
            };
    }
}