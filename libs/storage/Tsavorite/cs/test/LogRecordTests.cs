// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.LogRecordTests
{
    using static Utility;
    using static VarbyteLengthUtility;

    /// <summary>
    /// This also tests <see cref="MultiLevelPageArray{TestObjectValue}"/> and <see cref="SimpleConcurrentStack{_int_}"/>,
    /// which in turn tests <see cref="MultiLevelPageArray{_int_}"/>.
    /// </summary>
    [TestFixture]
    unsafe class LogRecordTests
    {
        long nativePointer;
        ObjectIdMap objectIdMap;
        SpanByteAndMemory sbamOutput;

#pragma warning disable IDE1006 // Naming Styles
        const int initialKeyLen = 10;
        const int initialValueLen = 40;
        const int initialVarbyteSize = MinLengthMetadataBytes;
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

            // Key
            sizeInfo.KeyIsInline = sizeInfo.FieldInfo.KeySize <= maxInlineKeySize;
            keySize = sizeInfo.KeyIsInline ? sizeInfo.FieldInfo.KeySize : ObjectIdMap.ObjectIdSize;

            // Value
            sizeInfo.MaxInlineValueSize = maxInlineValueSize;
            sizeInfo.ValueIsInline = !sizeInfo.ValueIsObject && sizeInfo.FieldInfo.ValueSize <= maxInlineValueSize;
            valueSize = sizeInfo.ValueIsInline ? sizeInfo.FieldInfo.ValueSize : ObjectIdMap.ObjectIdSize;

            // Record
            sizeInfo.CalculateSizes(keySize, valueSize);
        }

        [Test]
        [Category(LogRecordCategory), Category(SmokeTestCategory)]
        public unsafe void InlineHeaderTests()
        {
            const int maxRecordAllocation = (1 << 25) + (1 << 20);
            nativePointer = (long)NativeMemory.AlignedAlloc(maxRecordAllocation, Constants.kCacheLineBytes);

            Assert.That(RecordDataHeader.GetByteCount(0), Is.EqualTo(1));

            int inputKeyLength = 16;
            var inputValueLength = 1 << 8 - 1;

            // Test 1- and 2-byte valueLengthByte boundary with 1-keyLengthByte key
            Assert.That(RecordDataHeader.GetByteCount(inputValueLength), Is.EqualTo(1));
            InitializeKeyAndValue(inputKeyLength, inputValueLength, exNameSpaceLength: 0, out int keyLengthBytes, out int recordLengthBytes);
            Assert.That(keyLengthBytes, Is.EqualTo(1));
            Assert.That(recordLengthBytes, Is.EqualTo(1));

            inputValueLength = 1 << 8;
            Assert.That(RecordDataHeader.GetByteCount(inputValueLength), Is.EqualTo(2));
            InitializeKeyAndValue(inputKeyLength, inputValueLength, exNameSpaceLength: 2, out _ /*keyLengthBytes*/, out recordLengthBytes);
            Assert.That(recordLengthBytes, Is.EqualTo(2));

            // Test 2- and 3-byte valueLengthByte boundary with 2-keyLengthByte key
            inputKeyLength = inputValueLength = (1 << 16) - 1;
            Assert.That(RecordDataHeader.GetByteCount(inputValueLength), Is.EqualTo(2));
            InitializeKeyAndValue(inputKeyLength, inputValueLength, exNameSpaceLength: 4, out keyLengthBytes, out recordLengthBytes);
            Assert.That(keyLengthBytes, Is.EqualTo(2));
            Assert.That(recordLengthBytes, Is.EqualTo(3));  // We need an extra byte now

            inputValueLength = 1 << 16;
            Assert.That(RecordDataHeader.GetByteCount(inputValueLength), Is.EqualTo(3));
            InitializeKeyAndValue(inputKeyLength, inputValueLength, exNameSpaceLength: 7, out _ /*keyLengthBytes*/, out recordLengthBytes);
            Assert.That(recordLengthBytes, Is.EqualTo(3));

            // Test 3-byte valueLengthByte boundary with 3-keyLengthByte key, but the combination of keyLength and valueLength mean we need 4 bytes for recordLength.
            inputKeyLength = inputValueLength = (1 << 24) - 1024;
            Assert.That(RecordDataHeader.GetByteCount(inputValueLength), Is.EqualTo(3));
            InitializeKeyAndValue(inputKeyLength, inputValueLength, exNameSpaceLength: 0, out keyLengthBytes, out recordLengthBytes);
            Assert.That(keyLengthBytes, Is.EqualTo(3));
            Assert.That(recordLengthBytes, Is.EqualTo(4));  // Need an additional byte in recordLength

            // Test 4-byte valueLengthByte boundary with 4-keyLengthByte key, making the recordLength also 4 bytes
            inputKeyLength = inputValueLength = 1 << 24;
            Assert.That(RecordDataHeader.GetByteCount(inputValueLength), Is.EqualTo(4));
            InitializeKeyAndValue(inputKeyLength, inputValueLength, exNameSpaceLength: 0, out keyLengthBytes, out recordLengthBytes);
            Assert.That(keyLengthBytes, Is.EqualTo(4));
            Assert.That(recordLengthBytes, Is.EqualTo(4));

            void InitializeKeyAndValue(int keyLength, int valueLength, int exNameSpaceLength, out int keyLengthBytes, out int recordLengthBytes)
            {
                // 8*3 is for optionals, including ETag and Expiration and ObjectLogPosition. And some extra buffer just to be safe for the test.
                Assert.That(keyLength + valueLength + exNameSpaceLength + RecordDataHeader.MaxHeaderBytes + 8 * 3 + 1024, Is.LessThanOrEqualTo(maxRecordAllocation));

                var sizeInfo = new RecordSizeInfo()
                {
                    FieldInfo = new RecordFieldInfo()
                    {
                        KeySize = keyLength,
                        ValueSize = valueLength,
                        ExtendedNamespaceSize = exNameSpaceLength
                    },
                    KeyIsInline = true,
                    ValueIsInline = true,
                    MaxInlineValueSize = 1 << LogSettings.kMaxStringSizeBits
                };
                sizeInfo.CalculateSizes(sizeInfo.FieldInfo.KeySize, sizeInfo.FieldInfo.ValueSize);

                var dataHeader = new RecordDataHeader((byte*)nativePointer);
                var recordInfo = RecordInfo.InitialValid;
                var headerLength = dataHeader.Initialize(ref recordInfo, in sizeInfo, recordType: 0, out var keyAddress, out var valueAddress);
                (keyLengthBytes, recordLengthBytes) = dataHeader.DeconstructKVByteLengths(out var deconstructHeaderLength);
                Assert.That(headerLength, Is.EqualTo(RecordDataHeader.NumIndicatorBytes + keyLengthBytes + recordLengthBytes));
                Assert.That(deconstructHeaderLength, Is.EqualTo(headerLength));
                Assert.That(keyAddress, Is.EqualTo((long)nativePointer + headerLength + exNameSpaceLength));
                Assert.That(valueAddress, Is.EqualTo(keyAddress + keyLength));
                var (keyLengthBack, keyAddressBack) = dataHeader.GetKeyFieldInfo();
                Assert.That(keyLengthBack, Is.EqualTo(keyLength));
                Assert.That(keyAddressBack, Is.EqualTo(keyAddress));
                var (valueLengthBack, valueAddressBack) = dataHeader.GetValueFieldInfo(recordInfo);
                Assert.That(valueLengthBack, Is.EqualTo(valueLength));
                Assert.That(valueAddressBack, Is.EqualTo(valueAddress));
            }
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
            InitializeRecord(key, value, ref sizeInfo, out var logRecord, out var expectedFillerLength, out long eTag, out long expiration);

            // Shrink
            var offset = 12;
            sizeInfo.FieldInfo.ValueSize = initialValueLen - offset;
            Assert.That(logRecord.TrySetContentLengths(in sizeInfo), Is.True);
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength + offset));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));

            // Grow within range
            offset = 6;
            sizeInfo.FieldInfo.ValueSize = initialValueLen - offset;
            Assert.That(logRecord.TrySetContentLengths(in sizeInfo), Is.True);
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength + offset));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));

            // Grow beyond range
            offset = -10;
            sizeInfo.FieldInfo.ValueSize = initialValueLen - offset;
            Assert.That(logRecord.TrySetContentLengths(in sizeInfo), Is.False);

            // Restore to original
            sizeInfo.FieldInfo.ValueSize = initialValueLen;
            Assert.That(logRecord.TrySetContentLengths(in sizeInfo), Is.True);
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));

            // Remove ETag and verify Expiration is the same and filler has grown.
            Assert.That(logRecord.RemoveETag(), Is.True);
            Assert.That(logRecord.Info.HasETag, Is.False);
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength + LogRecord.ETagSize));

            // Restore ETag and verify Expiration is the same and filler has grown.
            eTag += 10;
            Assert.That(logRecord.TrySetETag(eTag), Is.True);
            Assert.That(logRecord.Info.HasETag, Is.True);
            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));

            // Remove Expiration and verify ETag is the same and filler has grown.
            Assert.That(logRecord.RemoveExpiration(), Is.True);
            Assert.That(logRecord.Info.HasExpiration, Is.False);
            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength + LogRecord.ExpirationSize));

            // Restore Expiration and verify ETag is the same and filler has grown.
            expiration += 20;
            Assert.That(logRecord.TrySetExpiration(expiration), Is.True);
            Assert.That(logRecord.Info.HasExpiration, Is.True);
            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
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
            InitializeRecord(key, value, ref sizeInfo, out var logRecord, out var expectedFillerLength, out long eTag, out long expiration);

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

        [Test]
        [Category(LogRecordCategory), Category(SmokeTestCategory)]
        public void CopyDiskLogRecordToLogRecord()
        {
            Assert.Ignore("TODO CopyDiskLogRecordToLogRecord");
        }

        [Test]
        [Category(LogRecordCategory), Category(SmokeTestCategory)]
        public void SerializeToMemoryPool()
        {
            Assert.Ignore("TODO SerializeToMemoryPool");
        }

        private void InitializeRecord(Span<byte> key, Span<byte> value, ref RecordSizeInfo sizeInfo, out LogRecord logRecord, out long expectedFillerLength, out long eTag, out long expiration)
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
            logRecord = new LogRecord(nativePointer, objectIdMap) { InfoRef = default };
            logRecord.InitializeRecord(key, in sizeInfo);

            // InitializeValue
            Assert.That(logRecord.ValueSpan.Length, Is.EqualTo(initialValueLen));

            expectedFillerLength = logRecord.AllocatedSize - logRecord.ActualSize;
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));

            Assert.That(logRecord.TrySetValueSpanAndPrepareOptionals(value, in sizeInfo), Is.True);

            // Now that we have set the ValueSpan it includes optionals, so FillerLength should have been adjusted for them
            expectedFillerLength -= LogRecord.ETagSize + LogRecord.ExpirationSize;
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));

            Assert.That(logRecord.Info.ValueIsInline, Is.True);
            Assert.That(logRecord.Info.ValueIsOverflow, Is.False);
            Assert.That(logRecord.Info.ValueIsObject, Is.False);
            Assert.That(logRecord.ValueSpan.Length, Is.EqualTo(value.Length));
            Assert.That(logRecord.ValueSpan.Slice(0, sizeof(int)).AsRef<int>(), Is.EqualTo(0x43434343));

            eTag = initialETag;
            Assert.That(logRecord.TrySetETag(eTag), Is.True);
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength)); // Should not have changed

            expiration = initialExpiration;
            Assert.That(logRecord.TrySetExpiration(expiration), Is.True);
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength)); // Should not have changed

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
        }

        private static void ConvertToOverflow(Span<byte> overflowValue, ref RecordSizeInfo sizeInfo, ref LogRecord logRecord, long expectedFillerLength, long eTag, long expiration, int offset)
        {
            sizeInfo.FieldInfo.ValueSize = overflowValue.Length;
            sizeInfo.FieldInfo.ValueIsObject = false;
            UpdateRecordSizeInfo(ref sizeInfo);

            Assert.That(logRecord.TrySetValueSpanAndPrepareOptionals(overflowValue, in sizeInfo), Is.True);

            Assert.That(logRecord.Info.ValueIsInline, Is.False);
            Assert.That(logRecord.Info.ValueIsOverflow, Is.True);
            Assert.That(logRecord.ValueSpan.Length, Is.EqualTo(overflowValue.Length));
            Assert.That(logRecord.ValueSpan.Slice(0, sizeof(int)).AsRef<int>(), Is.EqualTo(0x53535353));

            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength + offset));

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

            Assert.That(logRecord.Info.ValueIsInline, Is.False);
            Assert.That(logRecord.Info.ValueIsOverflow, Is.False);
            Assert.That(logRecord.Info.ValueIsObject, Is.True);
            Assert.That(((TestObjectValue)logRecord.ValueObject).value, Is.EqualTo(0x63636363));

            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength + offset));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
        }

        private static void RestoreToOriginal(Span<byte> value, ref RecordSizeInfo sizeInfo, ref LogRecord logRecord, long expectedFillerLength, long eTag, long expiration)
        {
            sizeInfo.FieldInfo.ValueSize = initialValueLen;
            sizeInfo.FieldInfo.ValueIsObject = false;
            UpdateRecordSizeInfo(ref sizeInfo);

            Assert.That(logRecord.TrySetValueSpanAndPrepareOptionals(value, in sizeInfo), Is.True);

            Assert.That(logRecord.Info.ValueIsInline, Is.True);
            Assert.That(logRecord.Info.ValueIsOverflow, Is.False);
            Assert.That(logRecord.ValueSpan.Length, Is.EqualTo(value.Length));
            Assert.That(logRecord.ValueSpan.Slice(0, sizeof(int)).AsRef<int>(), Is.EqualTo(0x43434343));

            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));
        }
    }
}