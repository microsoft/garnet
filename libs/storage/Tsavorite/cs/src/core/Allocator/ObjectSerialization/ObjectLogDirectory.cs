// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// One entry of an object-log directory: the address and exact length of a single data chunk.
    /// </summary>
    /// <remarks>
    /// Twelve bytes, laid out as a 64-bit word carrying the address and its flags, followed by a 32-bit word carrying the length:
    /// <code>
    /// word0: bits  0..59  address of the chunk's first DATA byte
    ///        bit      60  IsDirectory
    ///        bits 61..63  spare
    /// word1: bits  0..26  data length, biased by one (see below)
    ///        bits 27..31  spare
    /// </code>
    /// <para>The address points at the data. A data chunk carries no framing of its own, so a reader issues exactly
    /// <see cref="Length"/> bytes of IO with nothing to parse. That is what removes the serial dependency the old format had, where
    /// read <i>k+1</i> could not be sized until read <i>k</i>'s framing had been parsed.</para>
    /// <para><b>Why the length is biased.</b> A chunk cannot exceed a page, so the largest possible chunk is a full page --
    /// <c>1 &lt;&lt; LogSettings.kMaxPageSizeBits</c>, which does not fit in that many bits. A zero length is meaningless (a chunk with
    /// no data is never recorded), so the field stores <c>length - 1</c> and covers the whole range 1 through one full page inclusive.
    /// Storing the length directly would reject exactly the full-page chunk that a record starting on a page boundary produces.</para>
    /// </remarks>
    internal readonly struct DirectoryEntry
    {
        /// <summary>Encoded size in bytes. This is the on-disk size, not <c>sizeof</c> the managed struct, which pads to 16.</summary>
        internal const int Size = 12;

        /// <summary>Number of low bits of <c>word1</c> holding the biased chunk length.</summary>
        internal const int NumLengthBits = LogSettings.kMaxPageSizeBits;

        /// <summary>Largest data length an entry can describe: one full page at the maximum page size.</summary>
        internal const int MaxLength = 1 << NumLengthBits;

        private const uint LengthMask = (1u << NumLengthBits) - 1;

        /// <summary>Bit position in <c>word0</c> of the flag marking this entry as pointing at another directory record rather than
        /// at data. Placed directly above the address, mirroring <see cref="ObjectLogPositionInfo"/>'s flags-above-address layout.</summary>
        internal const int kIsDirectoryBit = ObjectLogPositionInfo.NumAddressBits;
        internal const ulong kIsDirectoryMask = 1UL << kIsDirectoryBit;

        private readonly ulong word0;
        private readonly uint word1;

        /// <summary>Create an entry describing <paramref name="length"/> data bytes at <paramref name="address"/>.</summary>
        /// <param name="address">Address of the chunk's first data byte.</param>
        /// <param name="length">Exact number of data bytes at <paramref name="address"/>.</param>
        /// <param name="isDirectory">Whether the address is another directory record rather than data; see
        ///     <see cref="DirectoryRecordHeader"/> for how the spine uses this.</param>
        internal DirectoryEntry(long address, int length, bool isDirectory = false)
        {
            if ((ulong)address > ObjectLogPositionInfo.AddressMask)
                ThrowAddress(address);
            if (length <= 0 || length > MaxLength)
                ThrowLength(length);

            word0 = (ulong)address | (isDirectory ? kIsDirectoryMask : 0UL);
            word1 = (uint)(length - 1);
        }

        private DirectoryEntry(ulong word0, uint word1)
        {
            this.word0 = word0;
            this.word1 = word1;
        }

        /// <summary>Address of the chunk's first data byte.</summary>
        internal long Address => (long)(word0 & ObjectLogPositionInfo.AddressMask);

        /// <summary>Exact number of data bytes at <see cref="Address"/>.</summary>
        internal int Length => (int)(word1 & LengthMask) + 1;

        /// <summary>Whether <see cref="Address"/> is another directory record rather than data.</summary>
        internal bool IsDirectory => (word0 & kIsDirectoryMask) != 0;

        /// <summary>Write this entry's <see cref="Size"/> bytes at <paramref name="destination"/>.</summary>
        /// <remarks>Unaligned, because entries sit at arbitrary offsets in a page buffer.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe void WriteTo(byte* destination)
        {
            Unsafe.WriteUnaligned(destination, word0);
            Unsafe.WriteUnaligned(destination + sizeof(ulong), word1);
        }

        /// <summary>Read an entry from <paramref name="source"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe DirectoryEntry ReadFrom(byte* source)
            => new(Unsafe.ReadUnaligned<ulong>(source), Unsafe.ReadUnaligned<uint>(source + sizeof(ulong)));

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowAddress(long address)
            => throw new TsavoriteException($"Directory entry address {address:N0} exceeds the object-log address range of {ObjectLogPositionInfo.AddressMask:N0}");

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowLength(int length)
            => throw new TsavoriteException($"Directory entry length {length:N0} must be greater than zero and no more than {MaxLength:N0}");

        /// <inheritdoc/>
        public override string ToString() => $"Address {Address:N0}; Length {Length:N0}{(IsDirectory ? "; IsDirectory" : "")}";
    }

    /// <summary>Flags in a <see cref="DirectoryRecordHeader"/>.</summary>
    [Flags]
    internal enum DirectoryFlags : uint
    {
        None = 0,

        /// <summary>The final data chunk's bytes follow this record's entry vector, so reading the directory also yields them.</summary>
        /// <remarks>Set when the remaining data and the accumulated directory fit one allocation together. There is no convergence
        /// problem in deciding this: at that point both the remaining data size and the entry count are known, because the final chunk
        /// has not been appended yet.</remarks>
        DirectoryIsCombined = 1 << 0
    }

    /// <summary>
    /// The header of an object-log directory record, followed by <see cref="EntryCount"/> <see cref="DirectoryEntry"/> values and, when
    /// <see cref="DirectoryFlags.DirectoryIsCombined"/> is set, the final data chunk.
    /// </summary>
    /// <remarks>
    /// Entries cover every component portion in stream order. A chunk holding the end of the overflow key and the start of the value
    /// contributes two entries at the same address; a reader places each such entry after the ones before it at that address.
    /// <see cref="EntryCount"/> counts this record's entries, including a spine link, so it is not necessarily the chunk count.
    /// <see cref="DirectoryFlags.DirectoryIsCombined"/> only changes where the last chunk's bytes come from.
    /// <para><b>Multilevel directories are a backward spine.</b> When the entry list outgrows what one record can hold, it is written out
    /// as a directory record, the list is cleared, and the <b>first</b> entry of the fresh list points back at the record just written
    /// with <see cref="DirectoryEntry.IsDirectory"/> set. Always first and only, so decoding is trivial, depth is unbounded, and it costs
    /// no position-word bits and no format change. No flag is needed: a reader tests the first entry.</para>
    /// <para><see cref="KeyEntryCount"/> is a count over the <b>whole chain</b>, not over this record, and is meaningful only in the top
    /// record -- the one the position word addresses. Spine records carry zero. A reader walks the spine to collect every entry in order
    /// and then splits the sequence there: the first <see cref="KeyEntryCount"/> entries are the overflow key's, the rest the value's.
    /// Summing each side's lengths gives exact component sizes, so an overflow buffer is allocated once at the right size.</para>
    /// </remarks>
    internal readonly struct DirectoryRecordHeader
    {
        /// <summary>Encoded size in bytes.</summary>
        internal const int Size = 12;

        private readonly uint entryCount;
        private readonly uint keyEntryCount;
        private readonly uint flags;

        /// <summary>Create a directory record header.</summary>
        /// <param name="entryCount">Number of <see cref="DirectoryEntry"/> values in this record.</param>
        /// <param name="keyEntryCount">Entries belonging to the overflow key across the whole chain; zero in a spine record.</param>
        /// <param name="flags">Record flags.</param>
        internal DirectoryRecordHeader(int entryCount, int keyEntryCount, DirectoryFlags flags = DirectoryFlags.None)
        {
            if (entryCount <= 0)
                ThrowEntryCount(entryCount);
            if (keyEntryCount < 0)
                ThrowKeyEntryCount(keyEntryCount);

            this.entryCount = (uint)entryCount;
            this.keyEntryCount = (uint)keyEntryCount;
            this.flags = (uint)flags;
        }

        private DirectoryRecordHeader(uint entryCount, uint keyEntryCount, uint flags)
        {
            this.entryCount = entryCount;
            this.keyEntryCount = keyEntryCount;
            this.flags = flags;
        }

        /// <summary>Number of <see cref="DirectoryEntry"/> values immediately following this header.</summary>
        internal int EntryCount => (int)entryCount;

        /// <summary>Entries belonging to the overflow key, counted over the whole chain; zero in a spine record.</summary>
        internal int KeyEntryCount => (int)keyEntryCount;

        /// <summary>Record flags.</summary>
        internal DirectoryFlags Flags => (DirectoryFlags)flags;

        /// <summary>Whether the final data chunk's bytes follow this record's entry vector.</summary>
        internal bool IsCombined => (Flags & DirectoryFlags.DirectoryIsCombined) != 0;

        /// <summary>Write this header's <see cref="Size"/> bytes at <paramref name="destination"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe void WriteTo(byte* destination)
        {
            Unsafe.WriteUnaligned(destination, entryCount);
            Unsafe.WriteUnaligned(destination + sizeof(uint), keyEntryCount);
            Unsafe.WriteUnaligned(destination + (sizeof(uint) * 2), flags);
        }

        /// <summary>Read a header from <paramref name="source"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe DirectoryRecordHeader ReadFrom(byte* source)
            => new(Unsafe.ReadUnaligned<uint>(source),
                   Unsafe.ReadUnaligned<uint>(source + sizeof(uint)),
                   Unsafe.ReadUnaligned<uint>(source + (sizeof(uint) * 2)));

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowEntryCount(int entryCount)
            => throw new TsavoriteException($"Directory record entry count {entryCount:N0} must be greater than zero; a directory exists only when there is more than one chunk");

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowKeyEntryCount(int keyEntryCount)
            => throw new TsavoriteException($"Directory record key entry count {keyEntryCount:N0} must not be negative");

        /// <inheritdoc/>
        public override string ToString() => $"EntryCount {EntryCount:N0}; KeyEntryCount {KeyEntryCount:N0}; Flags {Flags}";
    }

    /// <summary>Layout arithmetic for object-log directory records.</summary>
    internal static class ObjectLogDirectory
    {
        /// <summary>Bytes needed for a directory record holding <paramref name="entryCount"/> entries, optionally with
        /// <paramref name="combinedDataLength"/> bytes of final chunk data appended.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int RecordSize(int entryCount, int combinedDataLength = 0)
            => DirectoryRecordHeader.Size + (entryCount * DirectoryEntry.Size) + combinedDataLength;

        /// <summary>How many entries a directory record of <paramref name="capacity"/> bytes can hold, which may be zero or negative
        /// when the capacity cannot even hold the header.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int MaxEntriesInRecord(int capacity)
            => (capacity - DirectoryRecordHeader.Size) / DirectoryEntry.Size;

        /// <summary>Offset within a directory record at which the entry vector starts.</summary>
        internal const int EntriesOffset = DirectoryRecordHeader.Size;

        /// <summary>Offset within a combined directory record at which the final chunk's data starts.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int CombinedDataOffset(int entryCount) => RecordSize(entryCount);
    }
}