// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// The layout of a record's object-log position word: a flat object-log address in the low <see cref="NumAddressBits"/> bits, plus
    /// format flags above it.
    /// </summary>
    /// <remarks>
    /// A downlevel (cv7) object log is a file addressed by segment and offset; <see cref="LegacyObjectLogPosition"/> splits the same address
    /// bits that way for the cv7 reader.
    /// </remarks>
    internal static class ObjectLogPositionInfo
    {
        /// <summary>Number of low bits of a position word holding the address. 60 bits gives a 1 EB object-log range.</summary>
        internal const int NumAddressBits = 60;

        /// <summary>Mask for the address portion of a position word.</summary>
        /// <remarks>Public because it is the authority on how far an object-log address may reach, which
        /// <see cref="ObjectLog"/> enforces on allocation and the directory format uses to bound an entry's address field.</remarks>
        public const ulong AddressMask = (1UL << NumAddressBits) - 1;

        /// <summary>The word a position carries before any flush has stamped it: the unstamped address marker, with every flag clear.</summary>
        /// <remarks>
        /// <b>This is a 60-bit value, not all-ones.</b> It must leave the flag bits clear, because a flag is a claim about the position
        /// and an unstamped word makes no claims. When this was all-ones, every freshly initialized record read as "has a directory"
        /// and a protocol that claims a flag bit by setting it would find the bit already set -- a no-op rather than an ownership
        /// transition.
        /// <para>Words written before this was narrowed are still all-ones. They remain correctly recognized as unstamped, because
        /// every test for that goes through <see cref="WordIsUnstamped"/>, which looks only at the address bits. Flag <i>readers</i>
        /// must therefore reject an unstamped word rather than testing their bit in isolation -- see <see cref="WordHasDirectory"/>.
        /// Never compare a word against this constant for equality; use <see cref="WordIsUnstamped"/>.</para>
        /// </remarks>
        internal const ulong NotSet = AddressMask;

        // ── Flag bits in the top 4 bits of the word (bits 60-63) ─────────────────────────

        /// <summary>Bit position of an unused flag in a position word, available for a future format discriminator.
        /// <para>cv7 records set this bit to mark the downlevel split length encoding, but nothing in production reads it: recovery selects
        /// the downlevel-vs-current decode from the checkpoint metadata version (<see cref="HybridLogRecoveryInfo.UsesDownlevelObjectLog(int)"/>),
        /// and recovering a downlevel checkpoint up-converts every record -- clearing the bit, because flush rewrites the whole position word --
        /// before any page can be evicted, so a live read never encounters a downlevel record. It is written only by the test fixtures that
        /// synthesize cv7 record images, which also assert that cv8 recovery ignores and clears it; that keeps the bit proven available.</para></summary>
        internal const int kUnusedBit63 = 63;
        internal const ulong kUnusedBit63Mask = 1UL << kUnusedBit63;

        /// <summary>Bit position of the <c>HasDirectory</c> flag in a position word.
        /// <para>When set, the position addresses a <b>directory record</b> rather than a single data chunk: the record's out-of-line bytes
        /// are spread over several chunks whose addresses and lengths the directory records. When clear -- the common case -- the position
        /// addresses one data chunk, which the reader reads in a single IO with no framing to parse beyond its length prefix. The reader
        /// therefore learns the record's shape before issuing any IO.</para>
        /// <para>This is a separate bit from the position's stamped/unstamped state and from the exact-size flags, so a single-chunk
        /// record and a multi-chunk one are distinguished without consulting either.</para></summary>
        internal const int kHasDirectoryBit = 62;
        internal const ulong kHasDirectoryMask = 1UL << kHasDirectoryBit;

        /// <summary>Bit position of the <c>KeyIsExactSize</c> flag in a position word.
        /// <para>Meaningful when the key is overflow, in which case the key's objectId slot sizes the addressed extent (the key alone, or a
        /// directory). When set, the extent is headerless and the top <see cref="ObjectIdMap.ObjectIdSizeHintBits"/> bits of the key's objectId
        /// slot hold its exact byte length (&lt;= <see cref="ObjectIdMap.MaxObjectIdSizeHint"/>). When clear, the extent is length-prefixed,
        /// and its exact 4 KB page count is split between the raw RDH KeyLength high bits and the objectId hint low bits. See
        /// <see cref="ObjectLogExtent"/> and website/docs/dev/tsavorite/objectlog-serialization.md.</para></summary>
        internal const int kKeyIsExactSizeBit = 61;
        internal const ulong kKeyIsExactSizeMask = 1UL << kKeyIsExactSizeBit;

        /// <summary>Bit position of the <c>ValueIsExactSize</c> flag in a position word.
        /// <para>Meaningful when the key is not overflow, in which case the value's objectId slot sizes the addressed extent (the value alone,
        /// or a directory). When set, the extent is headerless and the top <see cref="ObjectIdMap.ObjectIdSizeHintBits"/> bits of the value's
        /// objectId slot hold its exact byte length (&lt;= <see cref="ObjectIdMap.MaxObjectIdSizeHint"/>). When clear, the extent is
        /// length-prefixed and the hint is its 4 KB page count, saturating at <see cref="ObjectLogExtent.LargeValueHint"/>.</para></summary>
        internal const int kValueIsExactSizeBit = 60;
        internal const ulong kValueIsExactSizeMask = 1UL << kValueIsExactSizeBit;

        /// <summary>Set the position word at <paramref name="wordPtr"/> to the unstamped marker, preserving its flag bits.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void Unstamp(ulong* wordPtr) => *wordPtr |= AddressMask;

        /// <summary>Whether <paramref name="word"/> is a position word that no flush ever stamped with a position.</summary>
        /// <remarks>
        /// The single place that knows the unstamped encoding; callers must not compare against <see cref="NotSet"/> themselves.
        /// <para>The marker lives entirely in the address bits -- all of them set, which no real position reaches because it
        /// would be the last addressable byte of a 1 EB object log. Keeping it out of the flag bits is what lets a record be
        /// unstamped without disturbing <c>KeyIsExactSize</c>/<c>ValueIsExactSize</c>, and leaves every real address unambiguous.
        /// Because only the address bits are examined, a legacy all-ones word reads as unstamped too.</para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool WordIsUnstamped(ulong word) => (word & AddressMask) == AddressMask;

        /// <summary>Whether the position word at <paramref name="wordPtr"/> was never stamped with a position.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe bool IsUnstamped(ulong* wordPtr) => WordIsUnstamped(*wordPtr);

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
        /// <remarks>Only meaningful on a word that is being, or has been, stamped with a position; see <see cref="WordHasDirectory"/>.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe void SetHasDirectory(ulong* wordPtr) => *wordPtr |= kHasDirectoryMask;

        /// <summary>Read the <c>HasDirectory</c> flag bit on the position word pointed to by <paramref name="wordPtr"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe bool GetHasDirectory(ulong* wordPtr) => WordHasDirectory(*wordPtr);

        /// <summary>Whether <paramref name="word"/> addresses a directory record rather than a single data chunk.</summary>
        /// <remarks>
        /// False whenever the word is unstamped, which is not merely a convenience: a never-stamped record's word may be a legacy
        /// all-ones value, flag bits included. A raw mask test would therefore report a directory on such a record. A directory cannot
        /// exist without a position, so "unstamped" and "has a directory" are mutually exclusive by construction.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool WordHasDirectory(ulong word) => !WordIsUnstamped(word) && (word & kHasDirectoryMask) != 0;
    }
}