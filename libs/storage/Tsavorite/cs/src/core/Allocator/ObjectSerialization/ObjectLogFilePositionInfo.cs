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
    public struct ObjectLogFilePositionInfo
    {
        /// <summary>Indicates the word has not been set.</summary>
        internal const ulong NotSet = ulong.MaxValue;

        /// <summary>Object log segment size bits</summary>
        internal int SegmentSizeBits;

        /// <summary>The word containing the data.</summary>
        internal ulong word;

        internal readonly bool IsSet => SegmentSizeBits != 0;

        public ObjectLogFilePositionInfo()
        {
            SegmentSizeBits = 0;
            word = NotSet;
        }

        internal ObjectLogFilePositionInfo(int segSizeBits)
        {
            SegmentSizeBits = segSizeBits;
            word = NotSet;
        }

        internal ObjectLogFilePositionInfo(ulong word, int segSizeBits)
        {
            SegmentSizeBits = segSizeBits;
            this.word = word;
        }

        internal void Serialize(StreamWriter writer)
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
                Debug.Assert((value & ~mask) <= SegmentSize, $"New Offset ({(value & ~mask)}) exceeds max segment size");
                word = (word & ~mask) | (value & mask);
            }
        }

        /// <summary>The current segment in the file.</summary>
        public int SegmentId
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get
            {
                var mask = (ulong)((1L << ((sizeof(long) * 8) - SegmentSizeBits)) - 1L);
                return (int)((word >> SegmentSizeBits) & mask);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                var mask = (ulong)((1L << ((sizeof(long) * 8) - SegmentSizeBits)) - 1L);
                word = (word & (mask << SegmentSizeBits)) | (((ulong)value & mask) << SegmentSizeBits);
            }
        }

        public void Advance(ulong size)
        {
            var remaining = SegmentSize - Offset;
            if (size < remaining)
            {
                Offset += size;
                return;
            }

            // Note: If size == remaining, we advance to the start of the next segment.
            size -= remaining;
            SegmentId += (int)(size / SegmentSize) + 1;
            Offset += size & (SegmentSize - 1);
        }

        public void AdvanceToNextSegment()
        {
            SegmentId++;
            Offset = 0;
        }

        public static ulong operator -(ObjectLogFilePositionInfo left, ObjectLogFilePositionInfo right)
        {
            Debug.Assert(left.SegmentSizeBits == right.SegmentSizeBits, "Segment size bits must match to compute distance");
            Debug.Assert(left.word >= right.word, "comparison position must be greater");
            var segmentDiff = (ulong)(left.SegmentId - right.SegmentId);
            if (segmentDiff == 0)
                return left.Offset - right.Offset;
            return (segmentDiff - 1) * left.SegmentSize + (left.SegmentSize - right.Offset) + left.Offset;
        }

        public readonly ulong SegmentSize => 1UL << SegmentSizeBits;

        public readonly ulong RemainingSize => SegmentSize - Offset;

        /// <inheritdoc/>
        public override readonly string ToString() => $"Segment# {SegmentId}; Offset {Offset:N0}; SegBits {SegmentSizeBits}; SegSize {SegmentSize:N0}";
    }
}