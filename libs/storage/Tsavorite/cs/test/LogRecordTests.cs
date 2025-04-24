// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.core.Utility;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    /// <summary>
    /// This also tests <see cref="MultiLevelPageArray{TestObjectValue}"/> and <see cref="SimpleConcurrentStack{_int_}"/>,
    /// which in turn tests <see cref="MultiLevelPageArray{_int_}"/>.
    /// </summary>
    [TestFixture]
    unsafe class LogRecordTests
    {
        long nativePointer;
        ObjectIdMap objectIdMap;
        SectorAlignedBufferPool bufferPool;
        SectorAlignedMemory recordBuffer;

        [SetUp]
        public void Setup()
        {
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
            recordBuffer?.Return();
            recordBuffer = null;
            bufferPool = null;
            DeleteDirectory(MethodTestDir);
        }

        const int initialKeyLen = 10;
        const int initialValueLen = 40;

        const int expectedInitialActualInlineRecordSize = 82;   // based on the initial values
        const int expectedInitialAllocatedInlineRecordSize = 88;

        const int maxInlineKeySize = 64;
        const int maxInlineValueSize = 128;

        const long initialETag = 1000;
        const long InitialExpiration = 2000;

        void UpdateRecordSizeInfo(ref RecordSizeInfo sizeInfo, int keySize = -1, int valueSize = -1)
        {
            if (keySize > 0)
                sizeInfo.FieldInfo.KeyDataSize = keySize;
            if (valueSize > 0)
                sizeInfo.FieldInfo.ValueDataSize = valueSize;

            // Key
            sizeInfo.KeyIsInline = sizeInfo.FieldInfo.KeyDataSize <= maxInlineKeySize;
            keySize = sizeInfo.KeyIsInline ? sizeInfo.FieldInfo.KeyDataSize + LogField.InlineLengthPrefixSize : ObjectIdMap.ObjectIdSize;

            // Value
            sizeInfo.MaxInlineValueSpanSize = maxInlineValueSize;
            sizeInfo.ValueIsInline = !sizeInfo.ValueIsObject && sizeInfo.FieldInfo.ValueDataSize <= maxInlineValueSize;
            valueSize = sizeInfo.ValueIsInline ? sizeInfo.FieldInfo.ValueDataSize + LogField.InlineLengthPrefixSize : ObjectIdMap.ObjectIdSize;

            // Record
            sizeInfo.ActualInlineRecordSize = RecordInfo.GetLength() + keySize + valueSize + sizeInfo.OptionalSize;
            sizeInfo.AllocatedInlineRecordSize = RoundUp(sizeInfo.ActualInlineRecordSize, Constants.kRecordAlignment);
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
            InitializeRecord(key, value, ref sizeInfo, out var logRecord, out var expectedFillerLengthAddress, out var expectedFillerLength, out long eTag, out long expiration);

            // Shrink
            var offset = 12;
            sizeInfo.FieldInfo.ValueDataSize = initialValueLen - offset;
            Assert.That(logRecord.TrySetValueLength(ref sizeInfo), Is.True);

            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress - offset));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength + offset));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));

            // Grow within range
            offset = 6;
            sizeInfo.FieldInfo.ValueDataSize = initialValueLen - offset;
            Assert.That(logRecord.TrySetValueLength(ref sizeInfo), Is.True);

            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress - offset));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength + offset));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));

            // Grow beyond range
            offset = -10;
            sizeInfo.FieldInfo.ValueDataSize = initialValueLen - offset;
            Assert.That(logRecord.TrySetValueLength(ref sizeInfo), Is.False);

            // Restore to original
            sizeInfo.FieldInfo.ValueDataSize = initialValueLen;
            Assert.That(logRecord.TrySetValueLength(ref sizeInfo), Is.True);

            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));

            // Remove ETag and verify Expiration is the same and filler has grown.
            Assert.That(logRecord.RemoveETag(), Is.True);
            Assert.That(logRecord.Info.HasETag, Is.False);
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress - LogRecord.ETagSize));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength + LogRecord.ETagSize));

            // Restore ETag and verify Expiration is the same and filler has grown.
            eTag += 10;
            Assert.That(logRecord.TrySetETag(eTag), Is.True);
            Assert.That(logRecord.Info.HasETag, Is.True);
            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));

            // Remove Expiration and verify ETag is the same and filler has grown.
            Assert.That(logRecord.RemoveExpiration(), Is.True);
            Assert.That(logRecord.Info.HasExpiration, Is.False);
            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress - LogRecord.ExpirationSize));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength + LogRecord.ExpirationSize));

            // Restore Expiration and verify ETag is the same and filler has grown.
            expiration += 20;
            Assert.That(logRecord.TrySetExpiration(expiration), Is.True);
            Assert.That(logRecord.Info.HasExpiration, Is.True);
            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));
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
            InitializeRecord(key, value, ref sizeInfo, out var logRecord, out var expectedFillerLengthAddress, out var expectedFillerLength, out long eTag, out long expiration);

            // Convert to overflow. Because objectIdSize is the same as InlineLengthPrefixSize, our value space will shrink by the original value data size.
            var offset = value.Length;
            ConvertToOverflow(overflowValue, ref sizeInfo, ref logRecord, expectedFillerLengthAddress, expectedFillerLength, eTag, expiration, offset);
            RestoreToOriginal(value, ref sizeInfo, ref logRecord, expectedFillerLengthAddress, expectedFillerLength, eTag, expiration);

            // Convert to Object. Because objectIdSize is the same as InlineLengthPrefixSize, we can reuse the same offset as above.
            ConvertToObject(ref sizeInfo, ref logRecord, expectedFillerLengthAddress, expectedFillerLength, eTag, expiration, offset);
            RestoreToOriginal(value, ref sizeInfo, ref logRecord, expectedFillerLengthAddress, expectedFillerLength, eTag, expiration);

            // Convert to overflow, then to object, then back to overflow and back to original
            ConvertToOverflow(overflowValue, ref sizeInfo, ref logRecord, expectedFillerLengthAddress, expectedFillerLength, eTag, expiration, offset);
            ConvertToObject(ref sizeInfo, ref logRecord, expectedFillerLengthAddress, expectedFillerLength, eTag, expiration, offset);
            ConvertToOverflow(overflowValue, ref sizeInfo, ref logRecord, expectedFillerLengthAddress, expectedFillerLength, eTag, expiration, offset);
            RestoreToOriginal(value, ref sizeInfo, ref logRecord, expectedFillerLengthAddress, expectedFillerLength, eTag, expiration);
        }

        [Test]
        [Category(LogRecordCategory), Category(SmokeTestCategory)]
        //[Repeat(900)]
        public unsafe void SerializationTest()
        {
            Span<byte> key = stackalloc byte[initialKeyLen];
            Span<byte> value = stackalloc byte[initialValueLen];
            Span<byte> overflowValue = stackalloc byte[maxInlineValueSize + 12];

            key.Fill(0x42);
            value.Fill(0x43);
            overflowValue.Fill(0x53);

            // Local diskLogRecord. We keep recordBuffer outside DiskLogRecord for the reuse scenario.
            DiskLogRecord diskLogRecord;
            var valueSerializer = new TestObjectValue.Serializer();
            bufferPool = new(recordSize: 1, sectorSize: 512);

            var sizeInfo = new RecordSizeInfo();
            InitializeRecord(key, value, ref sizeInfo, out var logRecord, out var expectedFillerLengthAddress, out var expectedFillerLength, out long eTag, out long expiration);
            diskLogRecord = new();
            diskLogRecord.Serialize(ref logRecord, bufferPool, valueSerializer, ref recordBuffer);
            Assert.That(diskLogRecord.Info.RecordIsInline);
            // verify inline copy by checking SerializedSize
            Assert.That(diskLogRecord.GetSerializedLength(), Is.EqualTo(logRecord.ActualRecordSize));
            // verify getting the key and value - length and data; eTag; expiration
            Assert.That(diskLogRecord.Key.SequenceEqual(logRecord.Key));
            Assert.That(diskLogRecord.ValueSpan.SequenceEqual(logRecord.ValueSpan));
            Assert.That(!diskLogRecord.Info.ValueIsObject);
            Assert.That(diskLogRecord.ETag, Is.EqualTo(eTag));
            Assert.That(diskLogRecord.Expiration, Is.EqualTo(expiration));

            // From here down in this test we want diskLogRecord to be serialized in IndicatorByte format (varbyte).
            var optionalLength = 2 * sizeof(long);
            var expectedValueLengthBytes = 1;

            // Convert to overflow. Because objectIdSize is the same as InlineLengthPrefixSize, our value space will shrink by the original value data size.
            var offset = value.Length;
            ConvertToOverflow(overflowValue, ref sizeInfo, ref logRecord, expectedFillerLengthAddress, expectedFillerLength, eTag, expiration, offset);
            diskLogRecord = new();
            diskLogRecord.Serialize(ref logRecord, bufferPool, valueSerializer, ref recordBuffer);
            Assert.That(!diskLogRecord.Info.RecordIsInline);
            // verify out-of-line copy by checking SerializedSize
            Assert.That(diskLogRecord.GetSerializedLength(), Is.GreaterThan(logRecord.ActualRecordSize));
            // verify indicator byte
            Assert.That(diskLogRecord.Version, Is.EqualTo(0));
            // verify getting the key and value - length and data; eTag; expiration
            Assert.That(diskLogRecord.Key.Length, Is.EqualTo(key.Length));
            Assert.That(diskLogRecord.Key.SequenceEqual(logRecord.Key));
            Assert.That(diskLogRecord.ValueSpan.Length, Is.EqualTo(overflowValue.Length));
            Assert.That(diskLogRecord.ValueSpan.SequenceEqual(logRecord.ValueSpan));
            Assert.That(!diskLogRecord.Info.ValueIsObject);
            Assert.That(!diskLogRecord.Info.ValueIsInline, "To avoid issues with Info.RecordIsInline, varbyte-format DiskLogRecords do not set Info.ValueIsInline; see discussion in SerializeCommonVarByteFields");
            Assert.That(diskLogRecord.ETag, Is.EqualTo(eTag));
            Assert.That(diskLogRecord.Expiration, Is.EqualTo(expiration));

            // Convert to Object. Because objectIdSize is the same as InlineLengthPrefixSize, we can reuse the same offset as above.
            ConvertToObject(ref sizeInfo, ref logRecord, expectedFillerLengthAddress, expectedFillerLength, eTag, expiration, offset);

            // Now test different value sizes, using TestLargeObjectValue to test large objects.
            var largeValueSerializer = new TestLargeObjectValue.Serializer();
            for (var ii = 0; ii < sizeof(int); ++ii)
            {
                /////////////////////////////
                // Set up the LogRecord with the object.
                /////////////////////////////
                var valueDataSize = (1 << (ii * 8)) + 42;         // TODO: test long values
                var valueObject = new TestLargeObjectValue(valueDataSize);
                Array.Fill(valueObject.value, (byte)ii);
                sizeInfo.FieldInfo.ValueDataSize = ObjectIdMap.ObjectIdSize;
                sizeInfo.FieldInfo.ValueIsObject = true;
                UpdateRecordSizeInfo(ref sizeInfo);
                Assert.That(logRecord.TrySetValueObject(valueObject, ref sizeInfo), Is.True);

                expectedValueLengthBytes = 1;  // Non-serialized object so only a 1-byte "0" length
                var expectedKeyDataOffset = RecordInfo.GetLength() + 1 + 1 + expectedValueLengthBytes;    // IndicatorByte + key length byte
                var expectedKeyDataAddress = diskLogRecord.physicalAddress + expectedKeyDataOffset;

                /////////////////////////////
                // Serialize with a null object serializer to copy the object instance rather than serializing it into space in the record buffer.
                /////////////////////////////
                diskLogRecord = new();
                diskLogRecord.Serialize(ref logRecord, bufferPool, valueSerializer: null, ref recordBuffer);
                Assert.That(diskLogRecord.Version, Is.EqualTo(0));
                expectedKeyDataAddress = diskLogRecord.physicalAddress + expectedKeyDataOffset;

                var keyInfo = diskLogRecord.KeyInfo;
                Assert.That(keyInfo.length, Is.EqualTo(initialKeyLen));
                Assert.That(keyInfo.dataAddress, Is.EqualTo(expectedKeyDataAddress));

                var expectedSerializedValueLength = 0;      // The object instance was copied; no serialization was done so the length is zero.
                var valueInfo = diskLogRecord.ValueInfo;
                Assert.That(valueInfo.length, Is.EqualTo(expectedSerializedValueLength));
                Assert.That(valueInfo.dataAddress, Is.EqualTo(expectedKeyDataAddress + keyInfo.length));

                Assert.That(diskLogRecord.ETag, Is.EqualTo(eTag));
                Assert.That(diskLogRecord.Expiration, Is.EqualTo(expiration));

                Assert.That(diskLogRecord.ValueObject, Is.Not.Null);
                Assert.That(new Span<byte>(((TestLargeObjectValue)diskLogRecord.ValueObject).value).SequenceEqual(new Span<byte>(((TestLargeObjectValue)logRecord.ValueObject).value)));
                var expectedRecordSize = RoundUp(expectedKeyDataOffset + key.Length + expectedSerializedValueLength + optionalLength, Constants.kRecordAlignment);
                Assert.That(diskLogRecord.GetSerializedLength(), Is.EqualTo(expectedRecordSize));

                /////////////////////////////
                // Serialize with an object serializer to allocate space in the record buffer and serialize the object into it.
                /////////////////////////////
                diskLogRecord = new();
                diskLogRecord.Serialize(ref logRecord, bufferPool, largeValueSerializer, ref recordBuffer);
                Assert.That(diskLogRecord.Version, Is.EqualTo(0));

                expectedValueLengthBytes = ii + 1;  // Serialized object so the value length is used
                expectedKeyDataOffset = RecordInfo.GetLength() + 1 + 1 + expectedValueLengthBytes;    // IndicatorByte + key length byte
                expectedKeyDataAddress = diskLogRecord.physicalAddress + expectedKeyDataOffset;

                keyInfo = diskLogRecord.KeyInfo;
                Assert.That(keyInfo.length, Is.EqualTo(initialKeyLen));
                Assert.That(keyInfo.dataAddress, Is.EqualTo(expectedKeyDataAddress));

                valueInfo = diskLogRecord.ValueInfo;
                Assert.That(valueInfo.length, Is.EqualTo(valueObject.Size));
                Assert.That(valueInfo.dataAddress, Is.EqualTo(expectedKeyDataAddress + keyInfo.length));

                Assert.That(diskLogRecord.ETag, Is.EqualTo(eTag));
                Assert.That(diskLogRecord.Expiration, Is.EqualTo(expiration));

                Assert.That(diskLogRecord.Info.ValueIsObject);
                expectedSerializedValueLength = (int)valueObject.Size;
                expectedRecordSize = RoundUp(expectedKeyDataOffset + key.Length + expectedSerializedValueLength + optionalLength, Constants.kRecordAlignment);
                Assert.That(diskLogRecord.GetSerializedLength(), Is.EqualTo(expectedRecordSize));

                Assert.That(diskLogRecord.DeserializeValueObject(largeValueSerializer), Is.Not.Null);
                Assert.That(diskLogRecord.ValueObject, Is.Not.Null);
                Assert.That(new Span<byte>(((TestLargeObjectValue)diskLogRecord.ValueObject).value).SequenceEqual(new Span<byte>(((TestLargeObjectValue)logRecord.ValueObject).value)));
            }
        }

        private void InitializeRecord(Span<byte> key, Span<byte> value, ref RecordSizeInfo sizeInfo, out LogRecord logRecord, out long expectedFillerLengthAddress, out long expectedFillerLength, out long eTag, out long expiration)
        {
            sizeInfo.FieldInfo = new()
            {
                KeyDataSize = initialKeyLen,
                ValueDataSize = initialValueLen,
                HasETag = true,
                HasExpiration = true
            };

            UpdateRecordSizeInfo(ref sizeInfo);
            Assert.That(sizeInfo.ActualInlineRecordSize, Is.EqualTo(expectedInitialActualInlineRecordSize));
            Assert.That(sizeInfo.AllocatedInlineRecordSize, Is.EqualTo(expectedInitialAllocatedInlineRecordSize));
            Assert.That(sizeInfo.KeyIsInline, Is.True);
            Assert.That(sizeInfo.ValueIsInline, Is.True);

            nativePointer = (long)NativeMemory.AlignedAlloc((nuint)sizeInfo.AllocatedInlineRecordSize, Constants.kCacheLineBytes);
            long recordEndAddress = nativePointer + sizeInfo.AllocatedInlineRecordSize;

            logRecord = new LogRecord(nativePointer, objectIdMap);
            logRecord.InfoRef = default;
            logRecord.InfoRef.SetKeyIsInline();
            logRecord.InfoRef.SetValueIsInline();

            // SerializeKey
            var keySpan = LogField.SetInlineDataLength(logRecord.KeyAddress, key.Length);
            key.CopyTo(keySpan);
            _ = LogField.SetInlineDataLength(logRecord.ValueAddress, value.Length);

            // InitializeValue
            Assert.That(logRecord.ValueSpan.Length, Is.EqualTo(initialValueLen));

            // FillerLength is set after initialization of Value field, and must be done before actually setting the ValueSpan
            // (it ignores optionals as it's called before they're set up).
            logRecord.SetFillerLength(sizeInfo.AllocatedInlineRecordSize);

            expectedFillerLengthAddress = logRecord.ValueAddress + value.Length + LogField.InlineLengthPrefixSize;
            expectedFillerLength = recordEndAddress - expectedFillerLengthAddress;
            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));

            Assert.That(logRecord.TrySetValueSpan(value, ref sizeInfo), Is.True);

            Assert.That(logRecord.Info.ValueIsInline, Is.True);
            Assert.That(logRecord.Info.ValueIsOverflow, Is.False);
            Assert.That(logRecord.Info.ValueIsObject, Is.False);
            Assert.That(logRecord.ValueSpan.Length, Is.EqualTo(value.Length));
            Assert.That(logRecord.ValueSpan.Slice(0, sizeof(int)).AsRef<int>(), Is.EqualTo(0x43434343));

            // These should be the same still.
            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));

            eTag = initialETag;
            Assert.That(logRecord.TrySetETag(eTag), Is.True);

            expectedFillerLengthAddress += LogRecord.ETagSize;
            expectedFillerLength -= LogRecord.ETagSize;
            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));

            expiration = InitialExpiration;
            Assert.That(logRecord.TrySetExpiration(expiration), Is.True);
            Assert.That(logRecord.ETag, Is.EqualTo(eTag));

            expectedFillerLengthAddress += LogRecord.ExpirationSize;
            expectedFillerLength -= LogRecord.ExpirationSize;
            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));
        }

        private void ConvertToOverflow(Span<byte> overflowValue, ref RecordSizeInfo sizeInfo, ref LogRecord logRecord, long expectedFillerLengthAddress, long expectedFillerLength, long eTag, long expiration, int offset)
        {
            sizeInfo.FieldInfo.ValueDataSize = overflowValue.Length;
            sizeInfo.FieldInfo.ValueIsObject = false;
            UpdateRecordSizeInfo(ref sizeInfo);

            Assert.That(logRecord.TrySetValueSpan(overflowValue, ref sizeInfo), Is.True);

            Assert.That(logRecord.Info.ValueIsInline, Is.False);
            Assert.That(logRecord.Info.ValueIsOverflow, Is.True);
            Assert.That(logRecord.ValueSpan.Length, Is.EqualTo(overflowValue.Length));
            Assert.That(logRecord.ValueSpan.Slice(0, sizeof(int)).AsRef<int>(), Is.EqualTo(0x53535353));

            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress - offset));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength + offset));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
        }

        private void ConvertToObject(ref RecordSizeInfo sizeInfo, ref LogRecord logRecord, long expectedFillerLengthAddress, long expectedFillerLength, long eTag, long expiration, int offset)
        {
            sizeInfo.FieldInfo.ValueDataSize = ObjectIdMap.ObjectIdSize;
            sizeInfo.FieldInfo.ValueIsObject = true;
            UpdateRecordSizeInfo(ref sizeInfo);

            var valueObject = new TestObjectValue() { value = 0x63636363 };
            Assert.That(logRecord.TrySetValueObject(valueObject, ref sizeInfo), Is.True);

            Assert.That(logRecord.Info.ValueIsInline, Is.False);
            Assert.That(logRecord.Info.ValueIsOverflow, Is.False);
            Assert.That(logRecord.Info.ValueIsObject, Is.True);
            Assert.That(((TestObjectValue)logRecord.ValueObject).value, Is.EqualTo(0x63636363));

            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress - offset));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength + offset));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
        }

        private void RestoreToOriginal(Span<byte> value, ref RecordSizeInfo sizeInfo, ref LogRecord logRecord, long expectedFillerLengthAddress, long expectedFillerLength, long eTag, long expiration)
        {
            sizeInfo.FieldInfo.ValueDataSize = initialValueLen;
            sizeInfo.FieldInfo.ValueIsObject = false;
            UpdateRecordSizeInfo(ref sizeInfo);

            Assert.That(logRecord.TrySetValueSpan(value, ref sizeInfo), Is.True);

            Assert.That(logRecord.Info.ValueIsInline, Is.True);
            Assert.That(logRecord.Info.ValueIsOverflow, Is.False);
            Assert.That(logRecord.ValueSpan.Length, Is.EqualTo(value.Length));
            Assert.That(logRecord.ValueSpan.Slice(0, sizeof(int)).AsRef<int>(), Is.EqualTo(0x43434343));

            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
        }
    }
}