// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Represents the information about the segment and offset of a location within the object log file.
    /// </summary>
    internal struct ObjectLogFilePositionInfo
    {
        /// <summary>Indicates the word has not been set.</summary>
        internal const ulong NotSet = ulong.MaxValue;

        /// <summary>Number of bits in <see cref="word"/> used for segment + offset (low bits). 60 bits gives a 1 EB Object Log size range.</summary>
        internal const int NumSegmentAndOffsetBits = 60;

        /// <summary>Mask for the segment + offset portion of <see cref="word"/>.</summary>
        internal const ulong SegmentAndOffsetMask = (1UL << NumSegmentAndOffsetBits) - 1;

        // ── Flag bits in the top 4 bits of word (bits 60-63) ─────────────────────────

        /// <summary>Bit position of the <c>ReuseObjectIdForSize</c> flag in <see cref="word"/>.
        /// When set, the on-disk overflow/object length uses the legacy split encoding: (RDH KeyLength/ValueLength field low bits) +
        /// (objectId slot at keyAddress/valueAddress high 32 bits), with no length framing in the object-log stream. When clear, the record
        /// uses the objectId-hint format (the authoritative length comes from the object-log stream framing).
        /// The flag is the per-record discriminator selecting the legacy read/decode path.</summary>
        internal const int kReuseObjectIdForSizeBit = 63;
        internal const ulong kReuseObjectIdForSizeMask = 1UL << kReuseObjectIdForSizeBit;

        /// <summary>Bit position of the <c>KeyIsExactSize</c> flag in <see cref="word"/>.
        /// When set, the out-of-line KEY's exact byte length (&lt;= <see cref="ObjectIdMap.MaxObjectIdSizeHint"/>) is stored in the top
        /// <see cref="ObjectIdMap.ObjectIdSizeHintBits"/> bits of the objectId slot at keyAddress (no leading ChunkHeader precedes the key
        /// bytes). When clear, the key is headered/chunked and its length comes from the object-log stream framing. See
        /// website/docs/dev/objectlog-serialization.md.</summary>
        internal const int kKeyIsExactSizeBit = 61;
        internal const ulong kKeyIsExactSizeMask = 1UL << kKeyIsExactSizeBit;

        /// <summary>Bit position of the <c>ValueIsExactSize</c> flag in <see cref="word"/>.
        /// When set, the out-of-line VALUE's exact byte length (&lt;= <see cref="ObjectIdMap.MaxObjectIdSizeHint"/>) is stored in the top
        /// <see cref="ObjectIdMap.ObjectIdSizeHintBits"/> bits of the objectId slot at valueAddress (no leading ChunkHeader precedes the
        /// value bytes). When clear, the value is headered/chunked and its length comes from the object-log stream framing.</summary>
        internal const int kValueIsExactSizeBit = 60;
        internal const ulong kValueIsExactSizeMask = 1UL << kValueIsExactSizeBit;

        /// <summary>Mask for the reserved flag bit 62 (future use).</summary>
        internal const ulong kReservedFlagsMask = 0x1UL << 62;

        /// <summary>Object log segment size bits</summary>
        internal int SegmentSizeBits;

        /// <summary>The word containing the data.</summary>
        internal ulong word;

        internal readonly bool IsSet => SegmentSizeBits != 0;
        internal readonly bool HasData => word != 0 && word != NotSet;

        /// <summary>
        /// Default initialization; leaves IsSet false. ObjectLogFilePositionInfo must be instantiated by new(), not default; we don't have arrays of this,
        /// and fields are initalized with some overload of new().
        /// </summary>
        public ObjectLogFilePositionInfo()
        {
            SegmentSizeBits = 0;
            word = NotSet;
        }

        /// <summary>
        /// Initialize the ObjectLogFilePositionInfo with the given word (containing segment and offset) and segment size bits.
        /// </summary>
        internal ObjectLogFilePositionInfo(ulong word, int segSizeBits)
        {
            SegmentSizeBits = segSizeBits;
            this.word = word;
        }

        internal readonly void Serialize(StreamWriter writer)
        {
            writer.WriteLine(SegmentSizeBits);
            writer.WriteLine(word);
        }

        internal void Deserialize(StreamReader reader)
        {
            var value = reader.ReadLine();
            SegmentSizeBits = int.Parse(value);
            value = reader.ReadLine();
            word = ulong.Parse(value);
        }

        /// <summary>Set the <c>ReuseObjectIdForSize</c> flag bit on the position word pointed to by <paramref name="wordPtr"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe void SetReuseObjectIdForSize(ulong* wordPtr) => *wordPtr |= kReuseObjectIdForSizeMask;

        /// <summary>Read the <c>ReuseObjectIdForSize</c> flag bit on the position word pointed to by <paramref name="wordPtr"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe bool GetReuseObjectIdForSize(ulong* wordPtr) => (*wordPtr & kReuseObjectIdForSizeMask) != 0;

        /// <summary>Set the <c>KeyIsExactSize</c> flag bit on the position word pointed to by <paramref name="wordPtr"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe void SetKeyIsExactSize(ulong* wordPtr) => *wordPtr |= kKeyIsExactSizeMask;

        /// <summary>Read the <c>KeyIsExactSize</c> flag bit on the position word pointed to by <paramref name="wordPtr"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe bool GetKeyIsExactSize(ulong* wordPtr) => (*wordPtr & kKeyIsExactSizeMask) != 0;

        /// <summary>Set the <c>ValueIsExactSize</c> flag bit on the position word pointed to by <paramref name="wordPtr"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe void SetValueIsExactSize(ulong* wordPtr) => *wordPtr |= kValueIsExactSizeMask;

        /// <summary>Read the <c>ValueIsExactSize</c> flag bit on the position word pointed to by <paramref name="wordPtr"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe bool GetValueIsExactSize(ulong* wordPtr) => (*wordPtr & kValueIsExactSizeMask) != 0;

        /// <summary>The offset within the current <see cref="SegmentId"/>.</summary>
        public ulong Offset
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get
            {
                var mask = (ulong)(1L << SegmentSizeBits) - 1L;
                return word & mask;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                var mask = (ulong)(1L << SegmentSizeBits) - 1L;
                Debug.Assert((value & ~mask) <= SegmentSize, $"New Offset ({value & ~mask}) exceeds max segment size");
                word = (word & ~mask) | (value & mask);
            }
        }

        /// <summary>The current segment in the file.</summary>
        public int SegmentId
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get
            {
                var mask = (ulong)((1L << (NumSegmentAndOffsetBits - SegmentSizeBits)) - 1L);
                return (int)((word >> SegmentSizeBits) & mask);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                var mask = (ulong)((1L << (NumSegmentAndOffsetBits - SegmentSizeBits)) - 1L);
                word = (word & ~(mask << SegmentSizeBits)) | (((ulong)value & mask) << SegmentSizeBits);
            }
        }

        private readonly int MaxSegmentId
        {
            get
            {
                var seg = 1L << (NumSegmentAndOffsetBits - SegmentSizeBits);
                return seg > int.MaxValue ? int.MaxValue : (int)seg;
            }
        }

        public void Advance(ulong size)
        {
            // Does it fit in the current segment?
            var remaining = SegmentSize - Offset;
            if (size < remaining)
            {
                Offset += size;
                return;
            }

            // Note: If size == remaining, we will advance to the start of the next segment.
            size -= remaining;

            // Move to the next segment(s).
            long nextSegmentId = SegmentId + (int)(size / SegmentSize) + 1;
            if (nextSegmentId > MaxSegmentId)
                throw new InvalidDataException($"Advancing position by {size:N} bytes exceeds maximum object log segment.");

            SegmentId = (int)nextSegmentId;
            Offset = size & (SegmentSize - 1);
        }

        public void AdvanceToNextSegment()
        {
            long nextSegmentId = SegmentId + 1;
            if (nextSegmentId > MaxSegmentId)
                throw new InvalidDataException($"Advancing to next segment exceeds maximum object log segment.");
            SegmentId = (int)nextSegmentId;
            Offset = 0;
        }

        public readonly ulong CurrentAddress => ((ulong)SegmentId << SegmentSizeBits) | Offset;

        public static ulong operator -(ObjectLogFilePositionInfo left, ObjectLogFilePositionInfo right)
        {
            if (left.SegmentSizeBits != right.SegmentSizeBits)
                throw new InvalidDataException("Object-log positions must have the same segment size to compute distance.");
            if (left.CurrentAddress < right.CurrentAddress)
                throw new InvalidDataException("Object-log positions must be ordered and belong to the same address space to compute distance.");
            var segmentDiff = (ulong)(left.SegmentId - right.SegmentId);
            if (segmentDiff == 0)
                return left.Offset - right.Offset;
            return ((segmentDiff - 1) * left.SegmentSize) + (left.SegmentSize - right.Offset) + left.Offset;
        }

        public readonly ulong SegmentSize => 1UL << SegmentSizeBits;

        public readonly ulong RemainingSizeInSegment => SegmentSize - Offset;

        /// <inheritdoc/>
        public override readonly string ToString() => $"Segment# {SegmentId}; Offset {Offset:N0}; SegBits {SegmentSizeBits}; SegSize {SegmentSize:N0}; RemSize {RemainingSizeInSegment:N0}";
    }
}