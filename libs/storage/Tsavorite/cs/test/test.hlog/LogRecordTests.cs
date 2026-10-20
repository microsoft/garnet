// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.LogRecordTests
{
    using static Utility;

    /// <summary>
    /// This also tests <see cref="MultiLevelPageArray{TestObjectValue}"/> and <see cref="SimpleConcurrentStack{_int_}"/>,
    /// which in turn tests <see cref="MultiLevelPageArray{_int_}"/>.
    /// </summary>
    [TestFixture]
    unsafe class LogRecordTests : TestBase
    {
        long nativePointer;
        ObjectIdMap objectIdMap;
        SpanByteAndMemory sbamOutput;

#pragma warning disable IDE1006 // Naming Styles
        const int initialKeyLen = 10;
        const int initialValueLen = 40;
        const int initialVarbyteSize = RecordDataHeader.Size;
        const int initialOptionalSize = sizeof(long) * 2;

        const int maxInlineKeySize = 64;
        const int maxInlineValueSize = 128;

        const long initialETag = 1000;
        const long initialExpiration = 2000;
#pragma warning restore IDE1006 // Naming Styles

        int expectedInitialActualInlineRecordSize;
        int expectedInitialAllocatedInlineRecordSize;

        [SetUp]
        public void Setup()
        {
            expectedInitialActualInlineRecordSize = RecordInfo.Size + initialVarbyteSize + initialKeyLen + initialValueLen + initialOptionalSize;
            expectedInitialAllocatedInlineRecordSize = RoundUp(expectedInitialActualInlineRecordSize, Constants.kRecordAlignment);

            DeleteDirectory(MethodTestDir);
            objectIdMap = new();
        }

        [TearDown]
        public void TearDown()
        {
            objectIdMap.Clear();
            if (nativePointer != IntPtr.Zero)
            {
                NativeMemory.AlignedFree((void*)nativePointer);
                nativePointer = IntPtr.Zero;
            }
            sbamOutput.Dispose();
            DeleteDirectory(MethodTestDir);
        }

        static void UpdateRecordSizeInfo(ref RecordSizeInfo sizeInfo, int keySize = -1, int valueSize = -1)
        {
            if (keySize > 0)
                sizeInfo.FieldInfo.KeySize = keySize;
            if (valueSize > 0)
                sizeInfo.FieldInfo.ValueSize = valueSize;

            // Clear packed word since we are re-evaluating; CalculateSizes will set KeyLengthBytes/RecordLengthBytes.
            sizeInfo.word = 0;

            // Key
            if (sizeInfo.FieldInfo.KeySize <= maxInlineKeySize)
                sizeInfo.SetKeyIsInline();
            keySize = sizeInfo.KeyIsInline ? sizeInfo.FieldInfo.KeySize : ObjectIdMap.ObjectIdSize;

            // Value
            sizeInfo.MaxInlineValueSize = maxInlineValueSize;
            if (!sizeInfo.ValueIsObject && sizeInfo.FieldInfo.ValueSize <= maxInlineValueSize)
                sizeInfo.SetValueIsInline();
            valueSize = sizeInfo.ValueIsInline ? sizeInfo.FieldInfo.ValueSize : ObjectIdMap.ObjectIdSize;

            // Record
            sizeInfo.CalculateSizes(keySize, valueSize);
        }

        static int GetTotalFillerLength(in LogRecord logRecord) => logRecord.DataHeader.GetTotalFillerLength();

        PinnedSpanByte CreateSerializedRecord(int length, int keyLength = 1, int valueLength = 1)
        {
            nativePointer = (long)NativeMemory.AlignedAlloc(256, Constants.kCacheLineBytes);
            new Span<byte>((void*)nativePointer, 256).Fill(0xAA);
            *(RecordInfo*)nativePointer = RecordInfo.InitialValid;
            ref var header = ref *(RecordDataHeader*)(nativePointer + RecordInfo.Size);
            header = default;
            header.SetKeyAndValueInline();
            header.KeyLength = keyLength;
            header.ValueLength = valueLength;
            header.RecordType = 1;
            return PinnedSpanByte.FromPinnedPointer((byte*)nativePointer, length);
        }

        DiskLogRecord DeserializeRecord(PinnedSpanByte record) =>
            DiskLogRecord.Deserialize(record, null, objectIdMap, default(StoreFunctions<SpanByteComparer, SpanByteRecordTriggers>));

        [Test]
        [Category(LogRecordCategory)]
        public void DeserializeRejectsTruncatedFixedHeaders()
        {
            var record = CreateSerializedRecord(Constants.FixedHeaderSize);
            for (var length = 0; length < Constants.FixedHeaderSize; ++length)
            {
                record.Length = length;
                Assert.Throws<TsavoriteException>(() => DeserializeRecord(record));
                Assert.Throws<TsavoriteException>(() => DiskLogRecord.GetChunkedRecordInlineSize(record.ReadOnlySpan));
                Assert.Throws<TsavoriteException>(() => DiskLogRecord.CompleteDeserializeChunkedRecord(record, default, default, null, objectIdMap));
            }
        }

        [TestCase(1, 56, 0, false, false, 17)]
        [TestCase(1023, 0, 0, false, false, 24)]
        [TestCase(0, 16777215, 0, false, false, 24)]
        [TestCase(1, 1, 127, false, false, 24)]
        [TestCase(1, 1, 0, true, false, 24)]
        [TestCase(1, 1, 0, false, true, 24)]
        [TestCase(1, 1, 0, false, false, 18)]
        [Category(LogRecordCategory)]
        public void DeserializeRejectsTruncatedInlineComponents(int keyLength, int valueLength, int namespaceLength, bool hasETag, bool hasExpiration, int wireLength)
        {
            var record = CreateSerializedRecord(wireLength, keyLength, valueLength);
            ref var header = ref *(RecordDataHeader*)(nativePointer + RecordInfo.Size);
            if (namespaceLength != 0)
                header.SetNamespaceByteRaw((byte)(0x80 | namespaceLength));
            if (hasETag)
                header.SetHasETag();
            if (hasExpiration)
                header.SetHasExpiration();
            var before = new ReadOnlySpan<byte>((void*)nativePointer, 256).ToArray();

            Assert.Throws<TsavoriteException>(() => DeserializeRecord(record));
            Assert.That(new ReadOnlySpan<byte>((void*)nativePointer, 256).ToArray(), Is.EqualTo(before));
        }

        [TestCase(true)]
        [TestCase(false)]
        [Category(LogRecordCategory)]
        public void DeserializeRejectsInvalidHeaders(bool nullInfo)
        {
            var record = CreateSerializedRecord(24);
            if (nullInfo)
                *(RecordInfo*)nativePointer = default;
            else
                *(byte*)(nativePointer + RecordInfo.Size) |= 4; // ValueIsObject conflicts with ValueIsInline.

            Assert.Throws<TsavoriteException>(() => DeserializeRecord(record));
            Assert.Throws<TsavoriteException>(() => DiskLogRecord.GetChunkedRecordInlineSize(record.ReadOnlySpan));
            Assert.Throws<TsavoriteException>(() => DiskLogRecord.CompleteDeserializeChunkedRecord(record, default, default, null, objectIdMap));
        }

        [TestCase(false)]
        [TestCase(true)]
        [Category(LogRecordCategory)]
        public void DeserializeRejectsTruncatedObjectSlots(bool overflowKey)
        {
            var record = CreateSerializedRecord(24);
            ref var header = ref *(RecordDataHeader*)(nativePointer + RecordInfo.Size);
            if (overflowKey)
                header.SetKeyIsOverflow();
            else
                header.SetValueIsObject();

            Assert.Throws<TsavoriteException>(() => DeserializeRecord(record));
            Assert.Throws<TsavoriteException>(() => DiskLogRecord.CompleteDeserializeChunkedRecord(record, default, default, null, objectIdMap));
        }

        [TestCase(-1, 36)]
        [TestCase(int.MaxValue, 36)]
        [TestCase(1, 35)]
        [TestCase(0, 36)]
        [TestCase(0, 40)]
        [Category(LogRecordCategory)]
        public void DeserializeRejectsMalformedOverflowComponents(int lengthPrefix, int wireLength)
        {
            var record = CreateSerializedRecord(wireLength);
            ref var header = ref *(RecordDataHeader*)(nativePointer + RecordInfo.Size);
            header.SetKeyIsOverflow();
            header.SetValueIsOverflow();
            var inlineSize = header.GetAlignedComponentSum();
            BinaryPrimitives.WriteInt32LittleEndian(new Span<byte>((void*)(nativePointer + inlineSize), sizeof(int)), lengthPrefix);
            var before = new ReadOnlySpan<byte>((void*)(nativePointer + wireLength), 256 - wireLength).ToArray();

            Assert.Throws<TsavoriteException>(() => DeserializeRecord(record));
            Assert.That(header.RecordIsInline, Is.True, "Cleanup must clear the transient overflow state");
            Assert.That(new ReadOnlySpan<byte>((void*)(nativePointer + wireLength), 256 - wireLength).ToArray(), Is.EqualTo(before));
        }

        [Test]
        [Category(LogRecordCategory)]
        public void DeserializeAcceptsCompactedInlineRecordWithExplicitFiller()
        {
            var record = CreateSerializedRecord(24);
            ref var header = ref *(RecordDataHeader*)(nativePointer + RecordInfo.Size);
            header.FillerWords = RecordDataHeader.MaxFillerWords;

            using var deserialized = DeserializeRecord(record);
            Assert.That(deserialized.Key.ToArray(), Is.EqualTo(new byte[] { 0xAA }));
            Assert.That(deserialized.ValueSpan.ToArray(), Is.EqualTo(new byte[] { 0xAA }));
        }

        [TestCase(false)]
        [TestCase(true)]
        [Category(LogRecordCategory)]
        public void DeserializeAcceptsOverflowComponents(bool chunked)
        {
            var record = CreateSerializedRecord(46);
            ref var header = ref *(RecordDataHeader*)(nativePointer + RecordInfo.Size);
            header.SetKeyIsOverflow();
            header.SetValueIsOverflow();
            var inlineSize = DiskLogRecord.GetChunkedRecordInlineSize(record.ReadOnlySpan.Slice(0, Constants.FixedHeaderSize));
            var tail = new Span<byte>((void*)(nativePointer + inlineSize), 14);
            BinaryPrimitives.WriteInt32LittleEndian(tail, 3);
            tail.Slice(4, 3).Fill(0xAB);
            BinaryPrimitives.WriteInt32LittleEndian(tail.Slice(7), 3);
            tail.Slice(11, 3).Fill(0xCD);
            var keyOverflow = OverflowByteArray.AllocateData(3);
            var valueOverflow = OverflowByteArray.AllocateData(3);
            tail.Slice(4, 3).CopyTo(keyOverflow.Span);
            tail.Slice(11, 3).CopyTo(valueOverflow.Span);

            using var deserialized = chunked
                ? DiskLogRecord.CompleteDeserializeChunkedRecord(PinnedSpanByte.FromPinnedPointer((byte*)nativePointer, inlineSize),
                    keyOverflow, valueOverflow, null, objectIdMap)
                : DeserializeRecord(record);
            Assert.That(deserialized.Key.ToArray(), Is.EqualTo(new byte[] { 0xAB, 0xAB, 0xAB }));
            Assert.That(deserialized.ValueSpan.ToArray(), Is.EqualTo(new byte[] { 0xCD, 0xCD, 0xCD }));
        }

        [Test]
        [Category(LogRecordCategory), Category(SmokeTestCategory)]
        public void InlineHeaderTests()
        {
            const int maxRecordAllocation = (1 << 25) + (1 << 20);
            nativePointer = (long)NativeMemory.AlignedAlloc(maxRecordAllocation, Constants.kCacheLineBytes);

            // With fixed 8-byte RecordDataHeader, header size is always RecordDataHeader.Size (8)
            Assert.That(RecordDataHeader.Size, Is.EqualTo(8));

            int inputKeyLength = 16;
            var inputValueLength = 255;
            InitializeKeyAndValue(inputKeyLength, inputValueLength, exNameSpaceLength: 0);

            inputValueLength = 256;
            InitializeKeyAndValue(inputKeyLength, inputValueLength, exNameSpaceLength: 2);

            // Large key and value
            inputKeyLength = LogSettings.MaxInlineKeySizeLimit;
            inputValueLength = (1 << 16) - 1;
            InitializeKeyAndValue(inputKeyLength, inputValueLength, exNameSpaceLength: 4);

            inputKeyLength = 1024;
            inputValueLength = 1 << 16;
            InitializeKeyAndValue(inputKeyLength, inputValueLength, exNameSpaceLength: 7);

            // Small key
            inputKeyLength = LogSettings.MaxInlineKeySizeLimit / 2;
            inputValueLength = 1024;
            InitializeKeyAndValue(inputKeyLength, inputValueLength, exNameSpaceLength: 0);

            void InitializeKeyAndValue(int keyLength, int valueLength, int exNameSpaceLength)
            {
                // 8*3 is for optionals, including ETag and Expiration and ObjectLogPosition. And some extra buffer just to be safe for the test.
                Assert.That(keyLength + valueLength + exNameSpaceLength + RecordDataHeader.Size + 8 * 3 + 1024, Is.LessThanOrEqualTo(maxRecordAllocation));

                var sizeInfo = new RecordSizeInfo()
                {
                    FieldInfo = new RecordFieldInfo()
                    {
                        KeySize = keyLength,
                        ValueSize = valueLength,
                        ExtendedNamespaceSize = exNameSpaceLength
                    },
                };

                PopulateBoundarySizeInfo(ref sizeInfo, maxInlineKey: LogSettings.MaxInlineKeySizeLimit, maxInlineValue: LogSettings.MaxInlineValueSizeLimit);

                // Force these to their inline length
                if (!sizeInfo.KeyIsInline)
                    keyLength = ObjectIdMap.ObjectIdSize;
                if (!sizeInfo.ValueIsInline)
                    valueLength = ObjectIdMap.ObjectIdSize;

                var recordBaseAddress = nativePointer;
                ref var dataHeader = ref *(RecordDataHeader*)(recordBaseAddress + RecordInfo.Size);
                var headerLength = dataHeader.Initialize(in sizeInfo, out var keyAddress, out var namespaceAddress, out var valueAddress, recordBaseAddress);
                Assert.That(headerLength, Is.EqualTo(RecordDataHeader.Size));
                Assert.That(keyAddress, Is.EqualTo(recordBaseAddress + Constants.FixedHeaderSize + exNameSpaceLength));
                Assert.That(valueAddress, Is.EqualTo(keyAddress + keyLength));
                var (keyLengthBack, keyAddressBack) = dataHeader.GetKeyFieldInfo(recordBaseAddress);
                Assert.That(keyLengthBack, Is.EqualTo(keyLength));
                Assert.That(keyAddressBack, Is.EqualTo(keyAddress));
                var (valueLengthBack, valueAddressBack) = dataHeader.GetValueFieldInfo(recordBaseAddress);
                Assert.That(valueLengthBack, Is.EqualTo(valueLength));
                Assert.That(valueAddressBack, Is.EqualTo(valueAddress));

                if (exNameSpaceLength == 0)
                {
                    Assert.That(namespaceAddress, Is.EqualTo(recordBaseAddress + RecordInfo.Size + RecordDataHeader.NamespaceOffsetInHeader));
                }
                else
                {
                    Assert.That(namespaceAddress, Is.EqualTo(recordBaseAddress + Constants.FixedHeaderSize));
                }
            }
        }

        [TestCase(0, 38, 0, 0)]
        [TestCase(0, 37, 0, 1)]
        [TestCase(29, 41, 0, 0)]
        [TestCase(29, 37, 0, 4)]
        [TestCase(0, 38, 24, 0)]
        [TestCase(0, 7, 24, 7)]
        [TestCase(29, 41, 24, 0)]
        [TestCase(29, 7, 24, 2)]
        [Category(LogRecordCategory), Category(SmokeTestCategory)]
        public void RevivificationPreparationPreservesLiveFraming(int extendedNamespaceSize, int valueSize, int explicitFillerLength, int implicitFillerLength)
        {
            nativePointer = (long)NativeMemory.AlignedAlloc(512, Constants.kCacheLineBytes);
            NativeMemory.Clear((void*)nativePointer, 512);

            var oldSizeInfo = new RecordSizeInfo { FieldInfo = new() { KeySize = initialKeyLen, ValueSize = valueSize, ExtendedNamespaceSize = extendedNamespaceSize, RecordType = 7 } };
            UpdateRecordSizeInfo(ref oldSizeInfo);
            oldSizeInfo.AllocatedInlineRecordSize += explicitFillerLength;

            var newSizeInfo = new RecordSizeInfo { FieldInfo = new() { KeySize = initialKeyLen, ValueSize = 8 } };
            UpdateRecordSizeInfo(ref newSizeInfo);
            ref var dataHeader = ref *(RecordDataHeader*)(nativePointer + RecordInfo.Size);

            _ = dataHeader.Initialize(in oldSizeInfo, out _, out _, out _, nativePointer);
            var oldRecordLength = dataHeader.GetRecordLength();
            Assert.That(dataHeader.KeyLength, Is.EqualTo(oldSizeInfo.InlineKeySize));
            Assert.That(dataHeader.ValueLength, Is.EqualTo(oldSizeInfo.InlineValueSize));
            Assert.That(dataHeader.ExtendedNamespaceLength, Is.EqualTo(oldSizeInfo.FieldInfo.ExtendedNamespaceSize));
            Assert.That(dataHeader.RecordType, Is.EqualTo(oldSizeInfo.FieldInfo.RecordType));
            Assert.That(dataHeader.GetUnalignedComponentSum(), Is.EqualTo(oldSizeInfo.ActualInlineRecordSize));
            Assert.That(oldRecordLength, Is.EqualTo(oldSizeInfo.AllocatedInlineRecordSize));
            Assert.That(dataHeader.GetExplicitFillerLength(), Is.EqualTo(explicitFillerLength));
            var totalFillerLength = dataHeader.GetTotalFillerLength();
            Assert.That(totalFillerLength, Is.EqualTo(explicitFillerLength + implicitFillerLength));
            var expectedKeyLength = dataHeader.KeyLength;
            var expectedValueLength = dataHeader.ValueLength;
            var logRecord = new LogRecord(nativePointer);
            logRecord.PrepareForRevivification(ref newSizeInfo);

            Assert.That(dataHeader.GetRecordLength(), Is.EqualTo(oldRecordLength));
            Assert.That(dataHeader.KeyLength, Is.EqualTo(expectedKeyLength));
            Assert.That(dataHeader.ValueLength, Is.EqualTo(expectedValueLength));
            Assert.That(dataHeader.ExtendedNamespaceLength, Is.EqualTo(oldSizeInfo.FieldInfo.ExtendedNamespaceSize));
            Assert.That(dataHeader.GetExplicitFillerLength(), Is.EqualTo(explicitFillerLength));
            Assert.That(dataHeader.GetTotalFillerLength(), Is.EqualTo(totalFillerLength));
            Assert.That(dataHeader.RecordType, Is.EqualTo(oldSizeInfo.FieldInfo.RecordType));
            Assert.That(newSizeInfo.AllocatedInlineRecordSize, Is.EqualTo(oldRecordLength));

            Span<byte> key = stackalloc byte[initialKeyLen];
            key.Fill(0x42);
            logRecord.InitializeHeadersForNewRecord(inNewVersion: false, previousAddress: 64);
            logRecord.InitializeRecord(TestSpanByteKey.FromPinnedSpan(key), in newSizeInfo);
            Assert.That(dataHeader.GetRecordLength(), Is.EqualTo(oldRecordLength));
        }

        [Test]
        [Category(LogRecordCategory), Category(SmokeTestCategory)]
        //[Repeat(900)]
        public unsafe void InlineBasicTest()
        {
            Span<byte> key = stackalloc byte[initialKeyLen];
            Span<byte> value = stackalloc byte[initialValueLen];

            key.Fill(0x42);
            value.Fill(0x43);

            var sizeInfo = new RecordSizeInfo();
            InitializeRecord(TestSpanByteKey.FromPinnedSpan(key), value, ref sizeInfo, out var logRecord, out var expectedFillerLength, out long eTag, out long expiration);

            // Shrink
            var offset = 12;
            sizeInfo.FieldInfo.ValueSize = initialValueLen - offset;
            Assert.That(logRecord.TrySetContentLengths(in sizeInfo), Is.True);
            Assert.That(GetTotalFillerLength(logRecord), Is.EqualTo(expectedFillerLength + offset));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));

            // Grow within range
            offset = 6;
            sizeInfo.FieldInfo.ValueSize = initialValueLen - offset;
            Assert.That(logRecord.TrySetContentLengths(in sizeInfo), Is.True);
            Assert.That(GetTotalFillerLength(logRecord), Is.EqualTo(expectedFillerLength + offset));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));

            // Grow beyond range
            offset = -10;
            sizeInfo.FieldInfo.ValueSize = initialValueLen - offset;
            Assert.That(logRecord.TrySetContentLengths(in sizeInfo), Is.False);

            // Restore to original
            sizeInfo.FieldInfo.ValueSize = initialValueLen;
            Assert.That(logRecord.TrySetContentLengths(in sizeInfo), Is.True);
            Assert.That(GetTotalFillerLength(logRecord), Is.EqualTo(expectedFillerLength));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));

            // Remove ETag and verify Expiration is the same and filler has grown.
            Assert.That(logRecord.RemoveETag(), Is.True);
            Assert.That(logRecord.DataHeader.HasETag, Is.False);
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
            Assert.That(GetTotalFillerLength(logRecord), Is.EqualTo(expectedFillerLength + LogRecord.ETagSize));

            // Restore ETag and verify Expiration is the same and filler has grown.
            eTag += 10;
            Assert.That(logRecord.TrySetETag(eTag), Is.True);
            Assert.That(logRecord.DataHeader.HasETag, Is.True);
            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
            Assert.That(GetTotalFillerLength(logRecord), Is.EqualTo(expectedFillerLength));

            // Remove Expiration and verify ETag is the same and filler has grown.
            Assert.That(logRecord.RemoveExpiration(), Is.True);
            Assert.That(logRecord.DataHeader.HasExpiration, Is.False);
            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(GetTotalFillerLength(logRecord), Is.EqualTo(expectedFillerLength + LogRecord.ExpirationSize));

            // Restore Expiration and verify ETag is the same and filler has grown.
            expiration += 20;
            Assert.That(logRecord.TrySetExpiration(expiration), Is.True);
            Assert.That(logRecord.DataHeader.HasExpiration, Is.True);
            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
            Assert.That(GetTotalFillerLength(logRecord), Is.EqualTo(expectedFillerLength));
        }

        [Test]
        [Category(LogRecordCategory), Category(SmokeTestCategory)]
        //[Repeat(900)]
        public unsafe void ConversionTest()
        {
            Span<byte> key = stackalloc byte[initialKeyLen];
            Span<byte> value = stackalloc byte[initialValueLen];
            Span<byte> overflowValue = stackalloc byte[maxInlineValueSize + 12];

            key.Fill(0x42);
            value.Fill(0x43);
            overflowValue.Fill(0x53);

            var sizeInfo = new RecordSizeInfo();
            InitializeRecord(TestSpanByteKey.FromPinnedSpan(key), value, ref sizeInfo, out var logRecord, out var expectedFillerLength, out long eTag, out long expiration);

            // Convert to overflow. Because objectIdSize is 4 bytes our value space will shrink by the original value data size less 4 bytes, but we will use 8 bytes for ObjectLogLogPosition.
            var offset = value.Length - 4 - LogRecord.ObjectLogPositionSize;
            ConvertToOverflow(overflowValue, ref sizeInfo, ref logRecord, expectedFillerLength, eTag, expiration, offset);
            RestoreToOriginal(value, ref sizeInfo, ref logRecord, expectedFillerLength, eTag, expiration);

            // Convert to Object. Because objectIdSize is the same as InlineLengthPrefixSize, we can reuse the same offset as above.
            ConvertToObject(ref sizeInfo, ref logRecord, expectedFillerLength, eTag, expiration, offset);
            RestoreToOriginal(value, ref sizeInfo, ref logRecord, expectedFillerLength, eTag, expiration);

            // Convert to overflow, then to object, then back to overflow and back to original
            ConvertToOverflow(overflowValue, ref sizeInfo, ref logRecord, expectedFillerLength, eTag, expiration, offset);
            ConvertToObject(ref sizeInfo, ref logRecord, expectedFillerLength, eTag, expiration, offset);
            ConvertToOverflow(overflowValue, ref sizeInfo, ref logRecord, expectedFillerLength, eTag, expiration, offset);
            RestoreToOriginal(value, ref sizeInfo, ref logRecord, expectedFillerLength, eTag, expiration);
        }

        // ── Max inline key/value size limit tests ─────────────────────────────────────
        //
        // These verify the boundary between inline (key/value bytes stored directly in the record) and overflow
        // (key/value stored on the heap via OverflowByteArray; record holds only an objectId slot).
        //   - "AtLimit" tests use a size exactly equal to LogSettings.MaxInline*SizeLimit and verify the field stays inline.
        //   - "AboveLimit" tests use a size one byte larger and verify the field becomes overflow.
        // The decision logic mirrors ObjectAllocatorImpl.PopulateRecordSizeInfo: KeyIsInline iff KeySize <= maxInlineKeySize.

        /// <summary>Apply the inline/overflow decision (mirrors ObjectAllocatorImpl.PopulateRecordSizeInfo) for a given
        /// max-inline-key and max-inline-value setting, then call CalculateSizes.</summary>
        static void PopulateBoundarySizeInfo(ref RecordSizeInfo sizeInfo, int maxInlineKey, int maxInlineValue)
        {
            sizeInfo.word = 0;
            if (sizeInfo.FieldInfo.KeySize <= maxInlineKey)
                sizeInfo.SetKeyIsInline();
            var keySize = sizeInfo.KeyIsInline ? sizeInfo.FieldInfo.KeySize : ObjectIdMap.ObjectIdSize;

            sizeInfo.MaxInlineValueSize = maxInlineValue;
            if (!sizeInfo.ValueIsObject && sizeInfo.FieldInfo.ValueSize <= maxInlineValue)
                sizeInfo.SetValueIsInline();
            var valueSize = sizeInfo.ValueIsInline ? sizeInfo.FieldInfo.ValueSize : ObjectIdMap.ObjectIdSize;

            sizeInfo.CalculateSizes(keySize, valueSize);
        }

        [Test]
        [Category(LogRecordCategory), Category(SmokeTestCategory)]
        public void MaxInlineKeyAtLimitStaysInline()
        {
            // A key of EXACTLY MaxInlineKeySizeLimit bytes must remain inline.
            const int keyLength = LogSettings.MaxInlineKeySizeLimit;
            const int valueLength = 32;

            var sizeInfo = new RecordSizeInfo()
            {
                FieldInfo = new() { KeySize = keyLength, ValueSize = valueLength }
            };
            PopulateBoundarySizeInfo(ref sizeInfo, maxInlineKey: LogSettings.MaxInlineKeySizeLimit, maxInlineValue: LogSettings.MaxInlineValueSizeLimit);

            Assert.That(sizeInfo.KeyIsInline, Is.True, "Key at MaxInlineKeySizeLimit should be inline");
            Assert.That(sizeInfo.ValueIsInline, Is.True);

            nativePointer = (long)NativeMemory.AlignedAlloc((nuint)sizeInfo.AllocatedInlineRecordSize, Constants.kCacheLineBytes);
            ref var dataHeader = ref *(RecordDataHeader*)(nativePointer + RecordInfo.Size);
            _ = dataHeader.Initialize(in sizeInfo, out var keyAddress, out _ /*nsAddr*/, out var valueAddress, nativePointer);

            // After Initialize: KeyIsInline bit set; raw KeyLength is the full keyLength; key/value addresses correct.
            Assert.That(dataHeader.KeyIsInline, Is.True);
            Assert.That(dataHeader.GetKeyLengthRaw(), Is.EqualTo(keyLength));
            Assert.That(dataHeader.KeyLength, Is.EqualTo(keyLength));
            var (keyLengthBack, keyAddressBack) = dataHeader.GetKeyFieldInfo(nativePointer);
            Assert.That(keyLengthBack, Is.EqualTo(keyLength));
            Assert.That(keyAddressBack, Is.EqualTo(keyAddress));
            Assert.That(valueAddress, Is.EqualTo(keyAddress + keyLength), "ValueAddress must immediately follow the inline key bytes");
        }

        [Test]
        [Category(LogRecordCategory), Category(SmokeTestCategory)]
        public void KeyAboveMaxInlineKeyBecomesOverflow()
        {
            // A key ONE BYTE ABOVE MaxInlineKeySizeLimit must become overflow.
            const int keyLength = LogSettings.MaxInlineKeySizeLimit + 1;
            const int valueLength = 32;

            var sizeInfo = new RecordSizeInfo()
            {
                FieldInfo = new() { KeySize = keyLength, ValueSize = valueLength }
            };
            PopulateBoundarySizeInfo(ref sizeInfo, maxInlineKey: LogSettings.MaxInlineKeySizeLimit, maxInlineValue: LogSettings.MaxInlineValueSizeLimit);

            Assert.That(sizeInfo.KeyIsInline, Is.False, "Key above MaxInlineKeySizeLimit must be overflow");
            Assert.That(sizeInfo.InlineKeySize, Is.EqualTo(ObjectIdMap.ObjectIdSize), "Overflow keys occupy only an ObjectIdSize slot inline");
            Assert.That(sizeInfo.ValueIsInline, Is.True);

            nativePointer = (long)NativeMemory.AlignedAlloc((nuint)sizeInfo.AllocatedInlineRecordSize, Constants.kCacheLineBytes);
            ref var dataHeader = ref *(RecordDataHeader*)(nativePointer + RecordInfo.Size);
            _ = dataHeader.Initialize(in sizeInfo, out var keyAddress, out _ /*nsAddr*/, out var valueAddress, nativePointer);

            // After Initialize: KeyIsInline=false; KeyLength property returns ObjectIdSize for non-inline.
            Assert.That(dataHeader.KeyIsInline, Is.False);
            Assert.That(dataHeader.KeyIsOverflow, Is.True);
            Assert.That(dataHeader.KeyLength, Is.EqualTo(ObjectIdMap.ObjectIdSize), "Property getter returns ObjectIdSize for non-inline keys");
            var (keyLengthBack, keyAddressBack) = dataHeader.GetKeyFieldInfo(nativePointer);
            Assert.That(keyLengthBack, Is.EqualTo(ObjectIdMap.ObjectIdSize));
            Assert.That(keyAddressBack, Is.EqualTo(keyAddress));
            Assert.That(valueAddress, Is.EqualTo(keyAddress + ObjectIdMap.ObjectIdSize), "ValueAddress must immediately follow the overflow key's objectId slot");
        }

        [Test]
        [Category(LogRecordCategory), Category(SmokeTestCategory)]
        public void MaxInlineValueAtLimitStaysInline()
        {
            // A value of EXACTLY MaxInlineValueSizeLimit bytes must remain inline.
            // This allocates ~16 MB of native memory for the record (24-bit ValueLength field); allowable in a unit test.
            const int keyLength = 32;
            const int valueLength = LogSettings.MaxInlineValueSizeLimit;

            var sizeInfo = new RecordSizeInfo()
            {
                FieldInfo = new() { KeySize = keyLength, ValueSize = valueLength }
            };
            PopulateBoundarySizeInfo(ref sizeInfo, maxInlineKey: LogSettings.MaxInlineKeySizeLimit, maxInlineValue: LogSettings.MaxInlineValueSizeLimit);

            Assert.That(sizeInfo.KeyIsInline, Is.True);
            Assert.That(sizeInfo.ValueIsInline, Is.True, "Value at MaxInlineValueSizeLimit should be inline");

            nativePointer = (long)NativeMemory.AlignedAlloc((nuint)sizeInfo.AllocatedInlineRecordSize, Constants.kCacheLineBytes);
            ref var dataHeader = ref *(RecordDataHeader*)(nativePointer + RecordInfo.Size);
            _ = dataHeader.Initialize(in sizeInfo, out var keyAddress, out _ /*nsAddr*/, out var valueAddress, nativePointer);

            // After Initialize: ValueIsInline bit set; raw ValueLength is the full valueLength.
            Assert.That(dataHeader.ValueIsInline, Is.True);
            Assert.That(dataHeader.GetValueLengthRaw(), Is.EqualTo(valueLength));
            Assert.That(dataHeader.ValueLength, Is.EqualTo(valueLength));
            var (valueLengthBack, valueAddressBack) = dataHeader.GetValueFieldInfo(nativePointer);
            Assert.That(valueLengthBack, Is.EqualTo(valueLength));
            Assert.That(valueAddressBack, Is.EqualTo(valueAddress));
            Assert.That(valueAddress, Is.EqualTo(keyAddress + keyLength));
        }

        [Test]
        [Category(LogRecordCategory), Category(SmokeTestCategory)]
        public void ValueAboveMaxInlineValueBecomesOverflow()
        {
            // A value ONE BYTE ABOVE MaxInlineValueSizeLimit must become overflow.
            const int keyLength = 32;
            const int valueLength = LogSettings.MaxInlineValueSizeLimit + 1;

            var sizeInfo = new RecordSizeInfo()
            {
                FieldInfo = new() { KeySize = keyLength, ValueSize = valueLength }
            };
            PopulateBoundarySizeInfo(ref sizeInfo, maxInlineKey: LogSettings.MaxInlineKeySizeLimit, maxInlineValue: LogSettings.MaxInlineValueSizeLimit);

            Assert.That(sizeInfo.KeyIsInline, Is.True);
            Assert.That(sizeInfo.ValueIsInline, Is.False, "Value above MaxInlineValueSizeLimit must be overflow");
            Assert.That(sizeInfo.InlineValueSize, Is.EqualTo(ObjectIdMap.ObjectIdSize), "Overflow values occupy only an ObjectIdSize slot inline");

            nativePointer = (long)NativeMemory.AlignedAlloc((nuint)sizeInfo.AllocatedInlineRecordSize, Constants.kCacheLineBytes);
            ref var dataHeader = ref *(RecordDataHeader*)(nativePointer + RecordInfo.Size);
            _ = dataHeader.Initialize(in sizeInfo, out var keyAddress, out _ /*nsAddr*/, out var valueAddress, nativePointer);

            // After Initialize: ValueIsInline=false; ValueIsOverflow=true; ValueLength property returns ObjectIdSize.
            Assert.That(dataHeader.ValueIsInline, Is.False);
            Assert.That(dataHeader.ValueIsOverflow, Is.True);
            Assert.That(dataHeader.ValueIsObject, Is.False);
            Assert.That(dataHeader.ValueLength, Is.EqualTo(ObjectIdMap.ObjectIdSize), "Property getter returns ObjectIdSize for non-inline values");
            var (valueLengthBack, valueAddressBack) = dataHeader.GetValueFieldInfo(nativePointer);
            Assert.That(valueLengthBack, Is.EqualTo(ObjectIdMap.ObjectIdSize));
            Assert.That(valueAddressBack, Is.EqualTo(valueAddress));
            Assert.That(valueAddress, Is.EqualTo(keyAddress + keyLength));
        }

        private void InitializeRecord<TKey>(TKey key, Span<byte> value, ref RecordSizeInfo sizeInfo, out LogRecord logRecord, out long expectedFillerLength, out long eTag, out long expiration)
            where TKey : IKey, allows ref struct
        {
            sizeInfo.FieldInfo = new()
            {
                KeySize = initialKeyLen,
                ValueSize = initialValueLen,
                HasETag = true,
                HasExpiration = true
            };

            UpdateRecordSizeInfo(ref sizeInfo);
            Assert.That(sizeInfo.ActualInlineRecordSize, Is.EqualTo(expectedInitialActualInlineRecordSize));
            Assert.That(sizeInfo.AllocatedInlineRecordSize, Is.EqualTo(expectedInitialAllocatedInlineRecordSize));
            Assert.That(sizeInfo.KeyIsInline, Is.True);
            Assert.That(sizeInfo.ValueIsInline, Is.True);

            nativePointer = (long)NativeMemory.AlignedAlloc((nuint)sizeInfo.AllocatedInlineRecordSize, Constants.kCacheLineBytes);
            logRecord = new LogRecord(nativePointer, objectIdMap) { InfoRef = RecordInfo.InitialValid };
            logRecord.InitializeRecord(key, in sizeInfo);

            // InitializeValue
            Assert.That(logRecord.ValueSpan.Length, Is.EqualTo(initialValueLen));

            expectedFillerLength = logRecord.AllocatedSize - logRecord.ActualSize;
            Assert.That(GetTotalFillerLength(logRecord), Is.EqualTo(expectedFillerLength));

            Assert.That(logRecord.TrySetValueSpanAndPrepareOptionals(value, in sizeInfo), Is.True);

            // Setting the ValueSpan included setting the optionals, so the logRecord's internal FillerLength should have been adjusted for them
            expectedFillerLength -= LogRecord.ETagSize + LogRecord.ExpirationSize;
            Assert.That(GetTotalFillerLength(logRecord), Is.EqualTo(expectedFillerLength));

            Assert.That(logRecord.DataHeader.ValueIsInline, Is.True);
            Assert.That(logRecord.DataHeader.ValueIsOverflow, Is.False);
            Assert.That(logRecord.DataHeader.ValueIsObject, Is.False);
            Assert.That(logRecord.ValueSpan.Length, Is.EqualTo(value.Length));
            Assert.That(logRecord.ValueSpan.Slice(0, sizeof(int)).AsRef<int>(), Is.EqualTo(0x43434343));

            eTag = initialETag;
            Assert.That(logRecord.TrySetETag(eTag), Is.True);
            Assert.That(GetTotalFillerLength(logRecord), Is.EqualTo(expectedFillerLength)); // Should not have changed

            expiration = initialExpiration;
            Assert.That(logRecord.TrySetExpiration(expiration), Is.True);
            Assert.That(GetTotalFillerLength(logRecord), Is.EqualTo(expectedFillerLength)); // Should not have changed

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
        }

        private static void ConvertToOverflow(Span<byte> overflowValue, ref RecordSizeInfo sizeInfo, ref LogRecord logRecord, long expectedFillerLength, long eTag, long expiration, int offset)
        {
            sizeInfo.FieldInfo.ValueSize = overflowValue.Length;
            sizeInfo.FieldInfo.ValueIsObject = false;
            UpdateRecordSizeInfo(ref sizeInfo);

            Assert.That(logRecord.TrySetValueSpanAndPrepareOptionals(overflowValue, in sizeInfo), Is.True);

            Assert.That(logRecord.DataHeader.ValueIsInline, Is.False);
            Assert.That(logRecord.DataHeader.ValueIsOverflow, Is.True);
            Assert.That(logRecord.ValueSpan.Length, Is.EqualTo(overflowValue.Length));
            Assert.That(logRecord.ValueSpan.Slice(0, sizeof(int)).AsRef<int>(), Is.EqualTo(0x53535353));

            Assert.That(GetTotalFillerLength(logRecord), Is.EqualTo(expectedFillerLength + offset));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
        }

        private static void ConvertToObject(ref RecordSizeInfo sizeInfo, ref LogRecord logRecord, long expectedFillerLength, long eTag, long expiration, int offset)
        {
            sizeInfo.FieldInfo.ValueSize = ObjectIdMap.ObjectIdSize;
            sizeInfo.FieldInfo.ValueIsObject = true;
            UpdateRecordSizeInfo(ref sizeInfo);

            var valueObject = new TestObjectValue() { value = 0x63636363 };
            Assert.That(logRecord.TrySetValueObjectAndPrepareOptionals(valueObject, in sizeInfo), Is.True);

            Assert.That(logRecord.DataHeader.ValueIsInline, Is.False);
            Assert.That(logRecord.DataHeader.ValueIsOverflow, Is.False);
            Assert.That(logRecord.DataHeader.ValueIsObject, Is.True);
            Assert.That(((TestObjectValue)logRecord.ValueObject).value, Is.EqualTo(0x63636363));

            Assert.That(GetTotalFillerLength(logRecord), Is.EqualTo(expectedFillerLength + offset));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
        }

        private static void RestoreToOriginal(Span<byte> value, ref RecordSizeInfo sizeInfo, ref LogRecord logRecord, long expectedFillerLength, long eTag, long expiration)
        {
            sizeInfo.FieldInfo.ValueSize = initialValueLen;
            sizeInfo.FieldInfo.ValueIsObject = false;
            UpdateRecordSizeInfo(ref sizeInfo);

            Assert.That(logRecord.TrySetValueSpanAndPrepareOptionals(value, in sizeInfo), Is.True);

            Assert.That(logRecord.DataHeader.ValueIsInline, Is.True);
            Assert.That(logRecord.DataHeader.ValueIsOverflow, Is.False);
            Assert.That(logRecord.ValueSpan.Length, Is.EqualTo(value.Length));
            Assert.That(logRecord.ValueSpan.Slice(0, sizeof(int)).AsRef<int>(), Is.EqualTo(0x43434343));

            Assert.That(GetTotalFillerLength(logRecord), Is.EqualTo(expectedFillerLength));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
        }
    }
}