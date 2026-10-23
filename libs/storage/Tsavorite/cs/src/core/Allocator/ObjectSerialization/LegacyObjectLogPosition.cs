// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// A location within a downlevel (cv7) object log file: a position word whose address bits are split into a segment id and an offset at
    /// the file's segment size.
    /// </summary>
    /// <remarks>
    /// Used only by <see cref="LegacyObjectLogReader{TStoreFunctions}"/> and the <see cref="CircularDiskReadBuffer"/> ring it reads through.
    /// The segment id and offset occupy exactly the <see cref="ObjectLogPositionInfo.AddressMask"/> bits, so <see cref="CurrentAddress"/>
    /// is the same flat address the current format uses.
    /// </remarks>
    internal struct LegacyObjectLogPosition
    {
        /// <summary>Object log segment size bits.</summary>
        internal int SegmentSizeBits;

        /// <summary>The position word.</summary>
        internal ulong word;

        internal readonly bool IsSet => SegmentSizeBits != 0;

        /// <summary>Whether this position carries a real object-log location.</summary>
        /// <remarks>Tests the unstamped encoding rather than comparing against <see cref="ObjectLogPositionInfo.NotSet"/>, so a legacy
        /// all-ones word is still recognized as carrying nothing.</remarks>
        internal readonly bool HasData => word != 0 && !ObjectLogPositionInfo.WordIsUnstamped(word);

        /// <summary>
        /// Default initialization; leaves IsSet false. A position must be instantiated by new(), not default; we don't have arrays of this,
        /// and fields are initialized with some overload of new().
        /// </summary>
        public LegacyObjectLogPosition()
        {
            SegmentSizeBits = 0;
            word = ObjectLogPositionInfo.NotSet;
        }

        /// <summary>
        /// Initialize the position with the given word (containing segment and offset) and segment size bits.
        /// </summary>
        internal LegacyObjectLogPosition(ulong word, int segSizeBits)
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
                var mask = (ulong)((1L << (ObjectLogPositionInfo.NumAddressBits - SegmentSizeBits)) - 1L);
                return (int)((word >> SegmentSizeBits) & mask);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                var mask = (ulong)((1L << (ObjectLogPositionInfo.NumAddressBits - SegmentSizeBits)) - 1L);
                word = (word & ~(mask << SegmentSizeBits)) | (((ulong)value & mask) << SegmentSizeBits);
            }
        }

        private readonly int MaxSegmentId
        {
            get
            {
                var seg = 1L << (ObjectLogPositionInfo.NumAddressBits - SegmentSizeBits);
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

        /// <summary>The position as a flat object-log address: <c>word &amp; <see cref="ObjectLogPositionInfo.AddressMask"/></c>.</summary>
        public readonly ulong CurrentAddress => ((ulong)SegmentId << SegmentSizeBits) | Offset;

        public static ulong operator -(LegacyObjectLogPosition left, LegacyObjectLogPosition right)
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