// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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

        /// <summary>Maximum number of bytes to use for segment + offset. 7 bytes gives a 72PB Object Log size range.</summary>
        private const int NumSegmentAndOffsetBytes = 7;

        /// <summary>Maximum number of bits to use for segment + offset. 7 bytes gives a 72PB Object Log size range.</summary>
        private const int NumSegmentAndOffsetBits = NumSegmentAndOffsetBytes * sizeof(long);

        /// <summary>Maximum number of bytes to use for segment + offset. 7 bytes gives a 72PB Object Log size range.</summary>
        private const ulong SegmentAndOffsetMask = (1UL << NumSegmentAndOffsetBits) - 1;

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

        /// <summary>The high byte is combined with the Value object length stored in the Value field when serialized, yielding 40 bits or 1TB max single object size.</summary>
        public int ObjectSizeHighByte
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get { return (int)((word >> NumSegmentAndOffsetBits) & 0xFFUL); }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                if (value > byte.MaxValue)
                    throw new ArgumentOutOfRangeException(nameof(value), $"Object size high byte must be less than or equal to {byte.MaxValue}.");
                word = (word & ~(0xFFUL << NumSegmentAndOffsetBits)) | ((ulong)value << NumSegmentAndOffsetBits);
            }
        }

        /// <summary>The high byte is combined with the Value object length stored in the Value field when serialized, yielding 40 bits or 1TB max single object size.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe void SetObjectSizeHighByte(ulong* wordPtr, int value)
        {
            if (value > byte.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(value), $"Object size high byte must be less than or equal to {byte.MaxValue}.");
            *wordPtr = (*wordPtr & ~(0xFFUL << NumSegmentAndOffsetBits)) | ((ulong)value << NumSegmentAndOffsetBits);
        }

        /// <summary>The high byte is combined with the Value object length stored in the Value field when serialized, yielding 40 bits or 1TB max single object size.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe int GetObjectSizeHighByte(ulong* wordPtr) => (int)((*wordPtr >> NumSegmentAndOffsetBits) & 0xFFUL);

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
            Offset += size & (SegmentSize - 1);
        }

        public void AdvanceToNextSegment()
        {
            long nextSegmentId = SegmentId + 1;
            if (nextSegmentId > MaxSegmentId)
                throw new InvalidDataException($"Advancing to next segment exceeds maximum object log segment.");
            SegmentId = (int)nextSegmentId;
            Offset = 0;
        }

        public static ulong operator -(ObjectLogFilePositionInfo left, ObjectLogFilePositionInfo right)
        {
            Debug.Assert(left.SegmentSizeBits == right.SegmentSizeBits, "Segment size bits must match to compute distance");
            Debug.Assert((left.word & SegmentAndOffsetMask) >= (right.word & SegmentAndOffsetMask), "comparison position must be greater");
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