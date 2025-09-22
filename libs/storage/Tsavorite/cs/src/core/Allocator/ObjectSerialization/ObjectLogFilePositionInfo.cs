// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Represents the information about the segment and offset of a location within the object log file.
    /// </summary>
    internal struct ObjectLogFilePositionInfo
    {
        internal const ulong PositionNotSet = ulong.MaxValue;

        /// <summary>Object log segment size bits</summary>
        internal int SegmentSizeBits;

        /// <summary>The word containing the data.</summary>
        internal ulong word = PositionNotSet;

        internal readonly bool IsSet => word != PositionNotSet;

        internal ObjectLogFilePositionInfo(int segSizeBits)
        {
            SegmentSizeBits = segSizeBits;
        }

        internal ObjectLogFilePositionInfo(ulong word, int segSizeBits)
        {
            SegmentSizeBits = segSizeBits;
            this.word = word;
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

        public void AdvanceToNextSegment()
        {
            SegmentId++;
            Offset = 0;
        }

        public readonly ulong SegmentSize => 1UL << SegmentSizeBits;

        public readonly ulong RemainingSize => SegmentSize - Offset;

        /// <inheritdoc/>
        public override readonly string ToString() => $"Segment {SegmentId}, Offset {Offset}, Bits {SegmentSizeBits}, Size {SegmentSize:N}";
    }
}
