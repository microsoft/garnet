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
        const int initialVarbyteSize = 3;  // indicator byte, and 1 byte each for key and value len
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
            expectedInitialActualInlineRecordSize = RecordInfo.GetLength() + initialVarbyteSize + initialKeyLen + initialValueLen + initialOptionalSize;
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
        //[Repeat(900)]
        public unsafe void VarbyteWordTests()
        {
            long value;
            byte* ptr = (byte*)&value;
            long indicatorAddress = (long)ptr;

            Assert.That(GetByteCount(0), Is.EqualTo(1));

            int inputKeyLength = 16;
            var inputValueLength = 1 << 8 - 1;
            byte* keyPtr, valuePtr;

            // Test 1- and 2-byte valueLengthByte boundary with 1-keyLengthByte key
            Assert.That(GetByteCount(inputValueLength), Is.EqualTo(1));
            value = ConstructInlineVarbyteLengthWord(inputKeyLength, inputValueLength, hasFillerBit: 0, out int keyLengthBytes, out int valueLengthBytes);
            Assert.That(keyLengthBytes, Is.EqualTo(1));
            Assert.That(valueLengthBytes, Is.EqualTo(1));
            VerifyKeyAndValue();

            inputValueLength = 1 << 8;
            Assert.That(GetByteCount(inputValueLength), Is.EqualTo(2));
            value = ConstructInlineVarbyteLengthWord(inputKeyLength, inputValueLength, hasFillerBit: 0, out _ /*keyLengthBytes*/, out valueLengthBytes);
            Assert.That(valueLengthBytes, Is.EqualTo(2));
            VerifyKeyAndValue();

            // Test 2- and 3-byte valueLengthByte boundary with 2-keyLengthByte key
            inputKeyLength = inputValueLength = (1 << 16) - 1;
            Assert.That(GetByteCount(inputValueLength), Is.EqualTo(2));
            value = ConstructInlineVarbyteLengthWord(inputKeyLength, inputValueLength, hasFillerBit: 0, out keyLengthBytes, out valueLengthBytes);
            Assert.That(keyLengthBytes, Is.EqualTo(2));
            Assert.That(valueLengthBytes, Is.EqualTo(2));
            VerifyKeyAndValue();

            inputValueLength = 1 << 16;
            Assert.That(GetByteCount(inputValueLength), Is.EqualTo(3));
            value = ConstructInlineVarbyteLengthWord(inputKeyLength, inputValueLength, hasFillerBit: 0, out _ /*keyLengthBytes*/, out valueLengthBytes);
            Assert.That(valueLengthBytes, Is.EqualTo(3));
            VerifyKeyAndValue();

            // Test 3- and 4-byte valueLengthByte boundary with 3-keyLengthByte key
            inputKeyLength = inputValueLength = (1 << 24) - 1;
            Assert.That(GetByteCount(inputValueLength), Is.EqualTo(3));
            value = ConstructInlineVarbyteLengthWord(inputKeyLength, inputValueLength, hasFillerBit: 0, out keyLengthBytes, out valueLengthBytes);
            Assert.That(keyLengthBytes, Is.EqualTo(3));
            Assert.That(valueLengthBytes, Is.EqualTo(3));
            VerifyKeyAndValue();

            inputValueLength = 1 << 24;
            Assert.That(GetByteCount(inputValueLength), Is.EqualTo(4));
            value = ConstructInlineVarbyteLengthWord(inputKeyLength, inputValueLength, hasFillerBit: 0, out _ /*keyLengthBytes*/, out valueLengthBytes);
            Assert.That(valueLengthBytes, Is.EqualTo(4));
            VerifyKeyAndValue();

            // Test max ValueLength
            inputValueLength = int.MaxValue;
            Assert.That(GetByteCount(inputValueLength), Is.EqualTo(4));
            value = ConstructInlineVarbyteLengthWord(inputKeyLength, inputValueLength, hasFillerBit: 0, out _ /*keyLengthBytes*/, out valueLengthBytes);
            Assert.That(valueLengthBytes, Is.EqualTo(4));
            VerifyKeyAndValue();

            void VerifyKeyAndValue()
            {
                keyPtr = GetFieldPtr(indicatorAddress, isKey: true, out var keyLengthPtr, out var outputKeyLengthBytes, out var outputKeyLength);
                valuePtr = GetFieldPtr(indicatorAddress, isKey: false, out var valueLengthPtr, out var outputValueLengthBytes, out var outputValueLength);
                Assert.That(outputKeyLengthBytes, Is.EqualTo(keyLengthBytes));
                Assert.That(outputKeyLength, Is.EqualTo(inputKeyLength));
                Assert.That(outputValueLength, Is.EqualTo(inputValueLength));
                Assert.That((long)keyPtr, Is.EqualTo((long)(ptr + 1 + outputKeyLengthBytes + outputValueLengthBytes)));
                Assert.That((long)keyLengthPtr, Is.EqualTo((long)(ptr + 1)));
                Assert.That((long)valuePtr, Is.EqualTo((long)(keyPtr + outputKeyLength)));
                Assert.That((long)valueLengthPtr, Is.EqualTo((long)(keyLengthPtr + keyLengthBytes)));

                // Now test the word-based forms for in-memory use only
                if (inputValueLength <= int.MaxValue)
                {
                    // First verify reading from the pointer-update version.
                    long word = *(long*)indicatorAddress;
                    var keyLength2 = ReadVarbyteLengthInWord(word, precedingNumBytes: 0, keyLengthBytes);
                    var valueLength2 = ReadVarbyteLengthInWord(word, keyLengthBytes, valueLengthBytes);
                    Assert.That(keyLength2, Is.EqualTo(inputKeyLength));
                    Assert.That(valueLength2, Is.EqualTo(inputValueLength));

                    word = 0x7171717171717171;  // Initialize with a bit pattern to mask off
                    WriteVarbyteLengthInWord(ref word, inputKeyLength, precedingNumBytes: 0, keyLengthBytes);                   // Write key
                    WriteVarbyteLengthInWord(ref word, inputValueLength, precedingNumBytes: keyLengthBytes, valueLengthBytes);  // Write value
                    keyLength2 = ReadVarbyteLengthInWord(word, precedingNumBytes: 0, keyLengthBytes);
                    valueLength2 = ReadVarbyteLengthInWord(word, precedingNumBytes: keyLengthBytes, valueLengthBytes);
                    Assert.That(keyLength2, Is.EqualTo(inputKeyLength));
                    Assert.That(valueLength2, Is.EqualTo(inputValueLength));
                }
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
            InitializeRecord(key, value, ref sizeInfo, out var logRecord, out var expectedFillerLengthAddress, out var expectedFillerLength, out long eTag, out long expiration);

            // Shrink
            var offset = 12;
            sizeInfo.FieldInfo.ValueSize = initialValueLen - offset;
            Assert.That(logRecord.TrySetValueLength(in sizeInfo), Is.True);

            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress - offset));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength + offset));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));

            // Grow within range
            offset = 6;
            sizeInfo.FieldInfo.ValueSize = initialValueLen - offset;
            Assert.That(logRecord.TrySetValueLength(in sizeInfo), Is.True);

            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress - offset));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength + offset));

            Assert.That(logRecord.ETag, Is.EqualTo(eTag));
            Assert.That(logRecord.Expiration, Is.EqualTo(expiration));

            // Grow beyond range
            offset = -10;
            sizeInfo.FieldInfo.ValueSize = initialValueLen - offset;
            Assert.That(logRecord.TrySetValueLength(in sizeInfo), Is.False);

            // Restore to original
            sizeInfo.FieldInfo.ValueSize = initialValueLen;
            Assert.That(logRecord.TrySetValueLength(in sizeInfo), Is.True);

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

        private void InitializeRecord(Span<byte> key, Span<byte> value, ref RecordSizeInfo sizeInfo, out LogRecord logRecord, out long expectedFillerLengthAddress, out long expectedFillerLength, out long eTag, out long expiration)
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
            long recordEndAddress = nativePointer + sizeInfo.AllocatedInlineRecordSize;

            logRecord = new LogRecord(nativePointer, objectIdMap) { InfoRef = default };
            logRecord.InitializeRecord(key, in sizeInfo);

            // InitializeValue
            Assert.That(logRecord.ValueSpan.Length, Is.EqualTo(initialValueLen));

            expectedFillerLengthAddress = logRecord.physicalAddress + RecordInfo.GetLength() + initialVarbyteSize + key.Length + value.Length;  // no OptionalsLength
            expectedFillerLength = recordEndAddress - expectedFillerLengthAddress;
            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));

            Assert.That(logRecord.TrySetValueSpan(value, in sizeInfo), Is.True);

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

            expiration = initialExpiration;
            Assert.That(logRecord.TrySetExpiration(expiration), Is.True);
            Assert.That(logRecord.ETag, Is.EqualTo(eTag));

            expectedFillerLengthAddress += LogRecord.ExpirationSize;
            expectedFillerLength -= LogRecord.ExpirationSize;
            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));
        }

        private void ConvertToOverflow(Span<byte> overflowValue, ref RecordSizeInfo sizeInfo, ref LogRecord logRecord, long expectedFillerLengthAddress, long expectedFillerLength, long eTag, long expiration, int offset)
        {
            sizeInfo.FieldInfo.ValueSize = overflowValue.Length;
            sizeInfo.FieldInfo.ValueIsObject = false;
            UpdateRecordSizeInfo(ref sizeInfo);

            Assert.That(logRecord.TrySetValueSpan(overflowValue, in sizeInfo), Is.True);

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
            sizeInfo.FieldInfo.ValueSize = ObjectIdMap.ObjectIdSize;
            sizeInfo.FieldInfo.ValueIsObject = true;
            UpdateRecordSizeInfo(ref sizeInfo);

            var valueObject = new TestObjectValue() { value = 0x63636363 };
            Assert.That(logRecord.TrySetValueObject(valueObject, in sizeInfo), Is.True);

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
            sizeInfo.FieldInfo.ValueSize = initialValueLen;
            sizeInfo.FieldInfo.ValueIsObject = false;
            UpdateRecordSizeInfo(ref sizeInfo);

            Assert.That(logRecord.TrySetValueSpan(value, in sizeInfo), Is.True);

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