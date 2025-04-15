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
            keySize = sizeInfo.KeyIsInline ? sizeInfo.FieldInfo.KeyDataSize + SpanField.InlineLengthPrefixSize : ObjectIdMap.ObjectIdSize;

            // Value
            sizeInfo.MaxInlineValueSpanSize = maxInlineValueSize;
            sizeInfo.ValueIsInline = !sizeInfo.ValueIsObject && sizeInfo.FieldInfo.ValueDataSize <= maxInlineValueSize;
            valueSize = sizeInfo.ValueIsInline ? sizeInfo.FieldInfo.ValueDataSize + SpanField.InlineLengthPrefixSize : ObjectIdMap.ObjectIdSize;

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

            DiskLogRecord diskLogRecord;
            var valueSerializer = new TestObjectValue.Serializer();

            var sizeInfo = new RecordSizeInfo();
            InitializeRecord(key, value, ref sizeInfo, out var logRecord, out var expectedFillerLengthAddress, out var expectedFillerLength, out long eTag, out long expiration);
            diskLogRecord = new();
            diskLogRecord.Serialize(ref logRecord, bufferPool, valueSerializer, ref recordBuffer);
            // verify inline copy by checking for SerializedSize
            // verify getting the key and value - length and data; eTag; expiration

            // Convert to overflow. Because objectIdSize is the same as InlineLengthPrefixSize, our value space will shrink by the original value data size.
            var offset = value.Length;
            ConvertToOverflow(overflowValue, ref sizeInfo, ref logRecord, expectedFillerLengthAddress, expectedFillerLength, eTag, expiration, offset);
            diskLogRecord = new();
            diskLogRecord.Serialize(ref logRecord, bufferPool, valueSerializer, ref recordBuffer);
            // Verify indicator byte - this will be just one byte
            // calculate offsets and call KeyInfo, ValueInfo and compare their lengths and addresses
            // verify getting the key and value - length and data; eTag; expiration

            // Convert to Object. Because objectIdSize is the same as InlineLengthPrefixSize, we can reuse the same offset as above.
            ConvertToObject(ref sizeInfo, ref logRecord, expectedFillerLengthAddress, expectedFillerLength, eTag, expiration, offset);
            diskLogRecord = new();
            diskLogRecord.Serialize(ref logRecord, bufferPool, valueSerializer, ref recordBuffer);
            // Verify indicator byte - this will be just one byte
            // calculate offsets and call KeyInfo, ValueInfo and compare their lengths and addresses
            // verify getting the key and value - length and data; eTag; expiration

            // Test the 2-8 byte lengths
            for (var ii = 1; ii < 8; ++ii)
            {
                var valueObject = new TestLargeObjectValue(1 << (ii * 8 + 1));
                Array.Fill(valueObject.value, (byte)ii);
                var largeValueSerializer = new TestLargeObjectValue.Serializer();
                sizeInfo.FieldInfo.ValueDataSize = ObjectIdMap.ObjectIdSize;
                sizeInfo.FieldInfo.ValueIsObject = true;
                UpdateRecordSizeInfo(ref sizeInfo);
                Assert.That(logRecord.TrySetValueObject(valueObject, ref sizeInfo), Is.True);
                diskLogRecord = new();
                diskLogRecord.Serialize(ref logRecord, bufferPool, largeValueSerializer, ref recordBuffer);
                // Verify indicator byte - this will be 1-8 bytes
                // calculate offsets and call KeyInfo, ValueInfo and compare their lengths and addresses
                // verify getting the key and value - length and data; eTag; expiration
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
            var keySpan = SpanField.SetInlineDataLength(logRecord.KeyAddress, key.Length);
            key.CopyTo(keySpan);
            _ = SpanField.SetInlineDataLength(logRecord.ValueAddress, value.Length);

            // InitializeValue
            Assert.That(logRecord.ValueSpan.Length, Is.EqualTo(initialValueLen));

            // FillerLength is set after initialization of Value field, and must be done before actually setting the ValueSpan
            // (it ignores optionals as it's called before they're set up).
            logRecord.SetFillerLength(sizeInfo.AllocatedInlineRecordSize);

            expectedFillerLengthAddress = logRecord.ValueAddress + value.Length + SpanField.InlineLengthPrefixSize;
            expectedFillerLength = recordEndAddress - expectedFillerLengthAddress;
            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));

            Assert.That(logRecord.TrySetValueSpan(value, ref sizeInfo), Is.True);

            Assert.That(logRecord.Info.ValueIsInline, Is.True);
            Assert.That(logRecord.Info.ValueIsOverflow, Is.False);
            Assert.That(logRecord.Info.ValueIsObject, Is.False);
            Assert.That(logRecord.ValueSpan.Length, Is.EqualTo(value.Length));
            Assert.That(logRecord.ValueSpan.AsRef<int>(), Is.EqualTo(0x43434343));

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
            Assert.That(logRecord.ValueSpan.AsRef<int>(), Is.EqualTo(0x53535353));

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
            Assert.That(logRecord.ValueSpan.AsRef<int>(), Is.EqualTo(0x43434343));

            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
        }
    }
}