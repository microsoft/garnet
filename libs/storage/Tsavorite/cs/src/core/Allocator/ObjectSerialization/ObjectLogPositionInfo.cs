// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// A location within the object log: a flat logical address in the low bits of <see cref="word"/>, plus format flags above it.
    /// </summary>
    /// <remarks>
    /// The address is flat even though the current file-backed object log still reaches it through segment arithmetic: a segment id and
    /// offset occupy exactly the address bits, so <c>word &amp; <see cref="AddressMask"/></c> is the address either way. The segment
    /// members below exist only for that file arithmetic and are removed once the writer and reader address <see cref="ObjectLog"/>
    /// instead of a file.
    /// </remarks>
    internal struct ObjectLogPositionInfo
    {
        /// <summary>Indicates the word has not been set.</summary>
        internal const ulong NotSet = ulong.MaxValue;

        /// <summary>Number of low bits of <see cref="word"/> holding the address. 60 bits gives a 1 EB object-log range.</summary>
        internal const int NumAddressBits = 60;

        /// <summary>Mask for the address portion of <see cref="word"/>.</summary>
        /// <remarks>Public because it is the authority on how far an object-log address may reach, which
        /// <see cref="ObjectLog"/> enforces on allocation and the directory format uses to bound an entry's address field.</remarks>
        public const ulong AddressMask = (1UL << NumAddressBits) - 1;

        // ── Flag bits in the top 4 bits of word (bits 60-63) ─────────────────────────

        /// <summary>Bit position of an unused flag in <see cref="word"/>, available for a future format discriminator.
        /// <para>cv7 records set this bit to mark the downlevel split length encoding, but nothing in production reads it: recovery selects
        /// the downlevel-vs-current decode from the checkpoint metadata version (<see cref="HybridLogRecoveryInfo.UsesDownlevelObjectLog(int)"/>),
        /// and recovering a downlevel checkpoint up-converts every record -- clearing the bit, because flush rewrites the whole position word --
        /// before any page can be evicted, so a live read never encounters a downlevel record. It is written only by the test fixtures that
        /// synthesize cv7 record images, which also assert that cv8 recovery ignores and clears it; that keeps the bit proven available.</para></summary>
        internal const int kUnusedBit63 = 63;
        internal const ulong kUnusedBit63Mask = 1UL << kUnusedBit63;

        /// <summary>Bit position of the <c>HasDirectory</c> flag in <see cref="word"/>.
        /// <para>When set, the position addresses a <b>directory record</b> rather than a single data chunk: the object's bytes are
        /// spread over several chunks whose addresses and lengths the directory records. When clear -- the common case -- the position
        /// addresses one data chunk, which the reader streams in a single IO with no framing to parse beyond the entry header. The
        /// reader therefore learns the object's shape before issuing any IO.</para>
        /// <para>This is a separate bit from the position's stamped/unstamped state and from the exact-size flags, so a single-chunk
        /// object and a multi-chunk one are distinguished without consulting either.</para></summary>
        internal const int kHasDirectoryBit = 62;
        internal const ulong kHasDirectoryMask = 1UL << kHasDirectoryBit;

        /// <summary>Bit position of the <c>KeyIsExactSize</c> flag in <see cref="word"/>.
        /// When set, the out-of-line KEY's exact byte length (&lt;= <see cref="ObjectIdMap.MaxObjectIdSizeHint"/>) is stored in the top
        /// <see cref="ObjectIdMap.ObjectIdSizeHintBits"/> bits of the objectId slot at keyAddress (no leading ChunkHeader precedes the key
        /// bytes). When clear, the key is headered; its exact 4 KB-page-count read extent is split between raw RDH KeyLength high bits and
        /// objectId hint low bits, while its exact logical byte length comes from the object-log stream framing. See
        /// website/docs/dev/tsavorite/objectlog-serialization.md.</summary>
        internal const int kKeyIsExactSizeBit = 61;
        internal const ulong kKeyIsExactSizeMask = 1UL << kKeyIsExactSizeBit;

        /// <summary>Bit position of the <c>ValueIsExactSize</c> flag in <see cref="word"/>.
        /// When set, the out-of-line VALUE's exact byte length (&lt;= <see cref="ObjectIdMap.MaxObjectIdSizeHint"/>) is stored in the top
        /// <see cref="ObjectIdMap.ObjectIdSizeHintBits"/> bits of the objectId slot at valueAddress (no leading ChunkHeader precedes the
        /// value bytes). When clear, the value is headered/chunked and its length comes from the object-log stream framing.</summary>
        internal const int kValueIsExactSizeBit = 60;
        internal const ulong kValueIsExactSizeMask = 1UL << kValueIsExactSizeBit;

        /// <summary>Object log segment size bits.</summary>
        /// <remarks>File addressing only, and removed with the rest of the segment members once the writer and reader address
        /// <see cref="ObjectLog"/> instead of a file. It is not part of the position: the segment id and offset derived from it occupy
        /// exactly the <see cref="AddressMask"/> bits, so the word is the same either way.</remarks>
        internal int SegmentSizeBits;

        /// <summary>The word containing the data.</summary>
        internal ulong word;

        internal readonly bool IsSet => SegmentSizeBits != 0;
        internal readonly bool HasData => word != 0 && word != NotSet;

        /// <summary>Set the ObjectLogPosition at <paramref name="wordPtr"/> to the unstamped marker, preserving its flag bits.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void Unstamp(ulong* wordPtr) => *wordPtr |= AddressMask;

        /// <summary>Whether <paramref name="word"/> is an ObjectLogPosition that no flush ever stamped with a position.</summary>
        /// <remarks>
        /// The single place that knows the unstamped encoding; callers must not compare against <see cref="NotSet"/> themselves.
        /// <para>The marker lives entirely in the address bits -- all of them set, which no real position reaches because it
        /// would be the last addressable byte of a 1 EB object log. Keeping it out of the flag bits is what lets a record be
        /// unstamped without disturbing <c>KeyIsExactSize</c>/<c>ValueIsExactSize</c>, and leaves address 0 unambiguous: it is
        /// the first byte of the log, not "never stamped". The all-ones <see cref="NotSet"/> word satisfies this test too, so words
        /// written before the flags were separated still read as unstamped.</para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool WordIsUnstamped(ulong word) => (word & AddressMask) == AddressMask;

        /// <summary>Whether the ObjectLogPosition at <paramref name="wordPtr"/> was never stamped with a position.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe bool IsUnstamped(ulong* wordPtr) => WordIsUnstamped(*wordPtr);

        /// <summary>
        /// Default initialization; leaves IsSet false. ObjectLogPositionInfo must be instantiated by new(), not default; we don't have arrays of this,
        /// and fields are initalized with some overload of new().
        /// </summary>
        public ObjectLogPositionInfo()
        {
            SegmentSizeBits = 0;
            word = NotSet;
        }

        /// <summary>
        /// Initialize the ObjectLogPositionInfo with the given word (containing segment and offset) and segment size bits.
        /// </summary>
        internal ObjectLogPositionInfo(ulong word, int segSizeBits)
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

        /// <summary>Set the <c>HasDirectory</c> flag bit on the position word pointed to by <paramref name="wordPtr"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe void SetHasDirectory(ulong* wordPtr) => *wordPtr |= kHasDirectoryMask;

        /// <summary>Read the <c>HasDirectory</c> flag bit on the position word pointed to by <paramref name="wordPtr"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe bool GetHasDirectory(ulong* wordPtr) => (*wordPtr & kHasDirectoryMask) != 0;

        /// <summary>Whether <paramref name="word"/> addresses a directory record rather than a single data chunk.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool WordHasDirectory(ulong word) => (word & kHasDirectoryMask) != 0;

        /// <summary>The offset within the current <see cref="SegmentId"/>.</summary>
        /// <remarks>Part of the file-addressing group retired with the writer/reader port; see <see cref="SegmentSizeBits"/>.</remarks>
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
        /// <remarks>Part of the file-addressing group retired with the writer/reader port; see <see cref="SegmentSizeBits"/>.</remarks>
        public int SegmentId
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get
            {
                var mask = (ulong)((1L << (NumAddressBits - SegmentSizeBits)) - 1L);
                return (int)((word >> SegmentSizeBits) & mask);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                var mask = (ulong)((1L << (NumAddressBits - SegmentSizeBits)) - 1L);
                word = (word & ~(mask << SegmentSizeBits)) | (((ulong)value & mask) << SegmentSizeBits);
            }
        }

        private readonly int MaxSegmentId
        {
            get
            {
                var seg = 1L << (NumAddressBits - SegmentSizeBits);
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

        /// <summary>The position as a flat object-log address.</summary>
        /// <remarks>Equivalent to <c>word &amp; <see cref="AddressMask"/></c>: the segment id and offset below occupy exactly those
        /// bits. Computed from them only while the object log is a file; it becomes the mask once that is no longer true.</remarks>
        public readonly ulong CurrentAddress => ((ulong)SegmentId << SegmentSizeBits) | Offset;

        public static ulong operator -(ObjectLogPositionInfo left, ObjectLogPositionInfo right)
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