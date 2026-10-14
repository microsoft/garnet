// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.Objects
{
    using static TestUtils;

    /// <summary>
    /// Covers the object-log directory encoding: <see cref="DirectoryEntry"/>, <see cref="DirectoryRecordHeader"/>, and the layout
    /// arithmetic they share. The behavioral matrix -- chunk-count boundaries, combined versus separate, spine engagement, page
    /// geometry -- belongs with the writer that produces those shapes.
    /// </summary>
    [TestFixture]
    internal class ObjectLogDirectoryTests : TestBase
    {
        [TearDown]
        public void TearDown() => OnTearDown();

        /// <summary>Round-trip an entry through its encoded bytes at an unaligned offset, which is where entries actually live.</summary>
        static DirectoryEntry RoundTrip(DirectoryEntry entry)
        {
            unsafe
            {
                // Three leading bytes so word0 lands unaligned, as it does mid-record.
                var buffer = stackalloc byte[3 + DirectoryEntry.Size];
                entry.WriteTo(buffer + 3);
                return DirectoryEntry.ReadFrom(buffer + 3);
            }
        }

        #region DirectoryEntry
        [Test, Category(ObjectLogCategory), Category(SmokeTestCategory)]
        public void EntryRoundTripsAddressLengthAndFlag(
            [Values(0L, 1L, 4096L, 0x0ABCDEF012345L, (long)ObjectLogPositionInfo.AddressMask)] long address,
            [Values(1, 2, 4096, DirectoryEntry.MaxLength)] int length,
            [Values] bool isDirectory)
        {
            var decoded = RoundTrip(new DirectoryEntry(address, length, isDirectory));

            Assert.That(decoded.Address, Is.EqualTo(address));
            Assert.That(decoded.Length, Is.EqualTo(length));
            Assert.That(decoded.IsDirectory, Is.EqualTo(isDirectory));
        }

        /// <summary>The address field is the same width as the position word's, so one definition bounds both.</summary>
        [Test, Category(ObjectLogCategory)]
        public void EntryAddressRangeMatchesThePositionWord()
        {
            Assert.That(DirectoryEntry.kIsDirectoryBit, Is.EqualTo(ObjectLogPositionInfo.NumAddressBits),
                "IsDirectory must sit directly above the address, as the position word's flags do");

            var max = (long)ObjectLogPositionInfo.AddressMask;
            Assert.That(RoundTrip(new DirectoryEntry(max, 1)).Address, Is.EqualTo(max));
            _ = Assert.Throws<TsavoriteException>(() => _ = new DirectoryEntry(max + 1, 1));
        }

        /// <summary>
        /// The largest possible chunk is a full page, which is why the length field is biased by one: a record starting exactly on a
        /// page boundary produces a full-page chunk, and storing the length directly would reject it.
        /// </summary>
        [Test, Category(ObjectLogCategory)]
        public void EntryLengthRangeCoversAFullPageChunk()
        {
            Assert.That(DirectoryEntry.NumLengthBits, Is.EqualTo(LogSettings.kMaxPageSizeBits));
            Assert.That(DirectoryEntry.MaxLength, Is.EqualTo(1 << LogSettings.kMaxPageSizeBits),
                "A full-page chunk must be representable; ObjectLog writes no per-chunk entry header that would make it impossible");

            // The boundary, both sides of the bias.
            Assert.That(RoundTrip(new DirectoryEntry(0, DirectoryEntry.MaxLength)).Length, Is.EqualTo(DirectoryEntry.MaxLength));
            Assert.That(RoundTrip(new DirectoryEntry(0, 1)).Length, Is.EqualTo(1));

            _ = Assert.Throws<TsavoriteException>(() => _ = new DirectoryEntry(0, 0));
            _ = Assert.Throws<TsavoriteException>(() => _ = new DirectoryEntry(0, -1));
            _ = Assert.Throws<TsavoriteException>(() => _ = new DirectoryEntry(0, DirectoryEntry.MaxLength + 1));
        }

        /// <summary>Spare bits must read back as zero, so a later format can claim them and distinguish old entries from new.</summary>
        [Test, Category(ObjectLogCategory)]
        public unsafe void EntrySpareBitsAreZero()
        {
            var buffer = stackalloc byte[DirectoryEntry.Size];
            new DirectoryEntry((long)ObjectLogPositionInfo.AddressMask, DirectoryEntry.MaxLength, isDirectory: true).WriteTo(buffer);

            var word0 = *(ulong*)buffer;
            var word1 = *(uint*)(buffer + sizeof(ulong));

            Assert.That(word0 >> (DirectoryEntry.kIsDirectoryBit + 1), Is.EqualTo(0UL), "word0 bits 61..63 are spare");
            Assert.That(word1 >> DirectoryEntry.NumLengthBits, Is.EqualTo(0U), "word1 bits 27..31 are spare even at the maximum length");
        }

        /// <summary>Entries are written back to back, so the encoded size must be exactly the stride.</summary>
        [Test, Category(ObjectLogCategory)]
        public unsafe void EntriesPackWithoutGaps()
        {
            const int Count = 5;
            var buffer = stackalloc byte[Count * DirectoryEntry.Size];
            for (var ii = 0; ii < Count; ++ii)
                new DirectoryEntry(address: (ii + 1) * 1024, length: ii + 1).WriteTo(buffer + (ii * DirectoryEntry.Size));

            for (var ii = 0; ii < Count; ++ii)
            {
                var entry = DirectoryEntry.ReadFrom(buffer + (ii * DirectoryEntry.Size));
                Assert.That(entry.Address, Is.EqualTo((ii + 1) * 1024));
                Assert.That(entry.Length, Is.EqualTo(ii + 1));
            }
        }
        #endregion

        #region DirectoryRecordHeader
        [Test, Category(ObjectLogCategory), Category(SmokeTestCategory)]
        public unsafe void HeaderRoundTrips(
            [Values(1, 2, 87_000, int.MaxValue)] int entryCount,
            [Values(0, 1)] int keyEntryCount,
            [Values(DirectoryFlags.None, DirectoryFlags.DirectoryIsCombined)] DirectoryFlags flags)
        {
            var buffer = stackalloc byte[DirectoryRecordHeader.Size];
            new DirectoryRecordHeader(entryCount, keyEntryCount, flags).WriteTo(buffer);
            var decoded = DirectoryRecordHeader.ReadFrom(buffer);

            Assert.That(decoded.EntryCount, Is.EqualTo(entryCount));
            Assert.That(decoded.KeyEntryCount, Is.EqualTo(keyEntryCount));
            Assert.That(decoded.Flags, Is.EqualTo(flags));
            Assert.That(decoded.IsCombined, Is.EqualTo(flags == DirectoryFlags.DirectoryIsCombined));
        }

        /// <summary>A directory exists only when there is more than one chunk, so an empty one is a bug rather than a shape.</summary>
        [Test, Category(ObjectLogCategory)]
        public void HeaderRejectsNonsenseCounts()
        {
            _ = Assert.Throws<TsavoriteException>(() => _ = new DirectoryRecordHeader(0, 0));
            _ = Assert.Throws<TsavoriteException>(() => _ = new DirectoryRecordHeader(-1, 0));
            _ = Assert.Throws<TsavoriteException>(() => _ = new DirectoryRecordHeader(1, -1));
        }
        #endregion

        #region Layout arithmetic
        [Test, Category(ObjectLogCategory)]
        public void RecordSizeAccountsForHeaderEntriesAndCombinedData()
        {
            Assert.That(ObjectLogDirectory.RecordSize(1), Is.EqualTo(DirectoryRecordHeader.Size + DirectoryEntry.Size));
            Assert.That(ObjectLogDirectory.RecordSize(3), Is.EqualTo(DirectoryRecordHeader.Size + (3 * DirectoryEntry.Size)));
            Assert.That(ObjectLogDirectory.RecordSize(3, combinedDataLength: 100),
                Is.EqualTo(DirectoryRecordHeader.Size + (3 * DirectoryEntry.Size) + 100));

            Assert.That(ObjectLogDirectory.EntriesOffset, Is.EqualTo(DirectoryRecordHeader.Size));
            Assert.That(ObjectLogDirectory.CombinedDataOffset(3), Is.EqualTo(ObjectLogDirectory.RecordSize(3)));
        }

        /// <summary>
        /// The spine dumps the entry list once it outgrows a record. The capacity arithmetic must be exact at that boundary, including
        /// capacities too small to hold even the header.
        /// </summary>
        [Test, Category(ObjectLogCategory)]
        public void MaxEntriesInRecordIsExactAtItsBoundaries()
        {
            Assert.That(ObjectLogDirectory.MaxEntriesInRecord(DirectoryRecordHeader.Size), Is.EqualTo(0));
            Assert.That(ObjectLogDirectory.MaxEntriesInRecord(DirectoryRecordHeader.Size - 1), Is.LessThanOrEqualTo(0));
            Assert.That(ObjectLogDirectory.MaxEntriesInRecord(ObjectLogDirectory.RecordSize(1)), Is.EqualTo(1));
            Assert.That(ObjectLogDirectory.MaxEntriesInRecord(ObjectLogDirectory.RecordSize(1) - 1), Is.EqualTo(0));
            Assert.That(ObjectLogDirectory.MaxEntriesInRecord(ObjectLogDirectory.RecordSize(7)), Is.EqualTo(7));
            Assert.That(ObjectLogDirectory.MaxEntriesInRecord(ObjectLogDirectory.RecordSize(7) + DirectoryEntry.Size - 1), Is.EqualTo(7));

            // The order-of-magnitude claim the spine design rests on: about 87,000 entries per 1 MB record.
            Assert.That(ObjectLogDirectory.MaxEntriesInRecord(1 << 20), Is.InRange(87_000, 87_500));
        }
        #endregion

        #region Shape
        /// <summary>
        /// A reader walks a spine by testing the first entry, so an encoded chain must decode without any extra framing: header, then
        /// entries, with entry zero pointing back at the previous record.
        /// </summary>
        [Test, Category(ObjectLogCategory), Category(SmokeTestCategory)]
        public unsafe void ASpineDecodesByTestingTheFirstEntry()
        {
            const long LowerRecordAddress = 8192;
            var buffer = stackalloc byte[ObjectLogDirectory.RecordSize(3)];

            new DirectoryRecordHeader(entryCount: 3, keyEntryCount: 1).WriteTo(buffer);
            var entries = buffer + ObjectLogDirectory.EntriesOffset;
            new DirectoryEntry(LowerRecordAddress, length: 64, isDirectory: true).WriteTo(entries);
            new DirectoryEntry(address: 16384, length: 100).WriteTo(entries + DirectoryEntry.Size);
            new DirectoryEntry(address: 16484, length: 200).WriteTo(entries + (2 * DirectoryEntry.Size));

            var header = DirectoryRecordHeader.ReadFrom(buffer);
            Assert.That(header.EntryCount, Is.EqualTo(3));
            Assert.That(header.KeyEntryCount, Is.EqualTo(1));
            Assert.That(header.IsCombined, Is.False);

            var first = DirectoryEntry.ReadFrom(entries);
            Assert.That(first.IsDirectory, Is.True, "A spine is recognized by the first entry alone");
            Assert.That(first.Address, Is.EqualTo(LowerRecordAddress));

            for (var ii = 1; ii < header.EntryCount; ++ii)
                Assert.That(DirectoryEntry.ReadFrom(entries + (ii * DirectoryEntry.Size)).IsDirectory, Is.False,
                    "Only the first entry may be a spine link, so decoding needs no scan");
        }

        /// <summary>
        /// A combined record's last entry addresses bytes inside that same record. Self-referential, but computable when written,
        /// because the record's own address is known before it is filled.
        /// </summary>
        [Test, Category(ObjectLogCategory)]
        public unsafe void ACombinedRecordsLastEntryAddressesItsOwnTail()
        {
            const long RecordAddress = 65536;
            const int EntryCount = 2;
            const int FinalChunkLength = 48;

            var totalSize = ObjectLogDirectory.RecordSize(EntryCount, FinalChunkLength);
            var buffer = stackalloc byte[totalSize];

            var finalDataAddress = RecordAddress + ObjectLogDirectory.CombinedDataOffset(EntryCount);
            new DirectoryRecordHeader(EntryCount, keyEntryCount: 0, DirectoryFlags.DirectoryIsCombined).WriteTo(buffer);
            var entries = buffer + ObjectLogDirectory.EntriesOffset;
            new DirectoryEntry(address: 1024, length: 500).WriteTo(entries);
            new DirectoryEntry(finalDataAddress, FinalChunkLength).WriteTo(entries + DirectoryEntry.Size);

            var header = DirectoryRecordHeader.ReadFrom(buffer);
            Assert.That(header.IsCombined, Is.True);

            var last = DirectoryEntry.ReadFrom(entries + ((header.EntryCount - 1) * DirectoryEntry.Size));
            Assert.That(last.Address - RecordAddress, Is.EqualTo(ObjectLogDirectory.CombinedDataOffset(EntryCount)),
                "The final entry must address this record's own tail, so a reader that already holds the record needs no further IO");
            Assert.That(last.Address + last.Length - RecordAddress, Is.EqualTo(totalSize));
        }

        /// <summary>Entries cover every chunk, so summing each side's lengths gives exact component sizes with no discovery.</summary>
        [Test, Category(ObjectLogCategory)]
        public unsafe void EntryLengthsSumToExactComponentSizes()
        {
            int[] keyChunks = [300, 700];
            int[] valueChunks = [1000, 1000, 250];
            var entryCount = keyChunks.Length + valueChunks.Length;

            var buffer = stackalloc byte[ObjectLogDirectory.RecordSize(entryCount)];
            new DirectoryRecordHeader(entryCount, keyEntryCount: keyChunks.Length).WriteTo(buffer);
            var entries = buffer + ObjectLogDirectory.EntriesOffset;

            long address = 4096;
            var index = 0;
            foreach (var length in (int[])[.. keyChunks, .. valueChunks])
            {
                new DirectoryEntry(address, length).WriteTo(entries + (index * DirectoryEntry.Size));
                address += length;
                ++index;
            }

            var header = DirectoryRecordHeader.ReadFrom(buffer);
            var keyLength = 0;
            var valueLength = 0;
            for (var ii = 0; ii < header.EntryCount; ++ii)
            {
                var entry = DirectoryEntry.ReadFrom(entries + (ii * DirectoryEntry.Size));
                if (ii < header.KeyEntryCount)
                    keyLength += entry.Length;
                else
                    valueLength += entry.Length;
            }

            Assert.That(keyLength, Is.EqualTo(1000));
            Assert.That(valueLength, Is.EqualTo(2250));
        }
        #endregion
    }
}