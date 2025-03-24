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

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir);
        }

        [TearDown]
        public void TearDown()
        {
            if (nativePointer != IntPtr.Zero)
            {
                NativeMemory.AlignedFree((void*)nativePointer);
                nativePointer = IntPtr.Zero;
            }
            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Category(LogRecordCategory), Category(SmokeTestCategory)]
        [Repeat(900)]
        public unsafe void LogRecordBasicTest()
        {
            const int initialKeyLen = 10;
            const int initialValueLen = 40;

            Span<byte> key = stackalloc byte[initialKeyLen];
            Span<byte> value = stackalloc byte[initialValueLen];

            key.Fill(0x42);
            value.Fill(0x43);

            var sizeInfo = new RecordSizeInfo()
            {
                FieldInfo = new ()
                {
                    KeyDataSize = initialKeyLen,
                    ValueDataSize = initialValueLen,
                    HasETag = true,
                    HasExpiration = true
                },

                KeyIsInline = true,

                MaxInlineValueSpanSize = 128,
                ValueIsInline = true
            };

            void UpdateRecordSizeInfo()
            {
                sizeInfo.ActualInlineRecordSize = RecordInfo.GetLength() + sizeInfo.FieldInfo.KeyDataSize + sizeInfo.FieldInfo.ValueDataSize + 2 * SpanField.FieldLengthPrefixSize + sizeInfo.OptionalSize;
                sizeInfo.AllocatedInlineRecordSize = RoundUp(sizeInfo.ActualInlineRecordSize, Constants.kRecordAlignment);
            }

            const int expectedInitialActualInlineRecordSize = 82;   // based on the initial values
            const int expectedInitialAllocatedInlineRecordSize = 88;

            UpdateRecordSizeInfo();
            Assert.That(sizeInfo.ActualInlineRecordSize, Is.EqualTo(expectedInitialActualInlineRecordSize));
            Assert.That(sizeInfo.AllocatedInlineRecordSize, Is.EqualTo(expectedInitialAllocatedInlineRecordSize));

            nativePointer = (long)NativeMemory.AlignedAlloc((nuint)sizeInfo.AllocatedInlineRecordSize, Constants.kCacheLineBytes);
            long recordEndAddress = nativePointer + sizeInfo.AllocatedInlineRecordSize;

            var logRecord = new LogRecord((long)nativePointer);
            logRecord.InfoRef = default;
            logRecord.InfoRef.SetKeyIsInline();
            logRecord.InfoRef.SetValueIsInline();

            var keySpan = SpanField.SetInlineDataLength(logRecord.KeyAddress, key.Length);
            key.CopyTo(keySpan);
            SpanField.SetInlineDataLength(logRecord.ValueAddress, value.Length);

            Assert.That(logRecord.ValueSpan.Length, Is.EqualTo(initialValueLen));

            // FillerLength is set after initialization of Value field, and must be done before actually setting the ValueSpan
            // (it ignores optionals as it's called before they're set up).
            logRecord.SetFillerLength(sizeInfo.AllocatedInlineRecordSize);
            var expectedFillerLengthAddress = logRecord.ValueAddress + value.Length + SpanField.FieldLengthPrefixSize;
            var expectedFillerLength = recordEndAddress - expectedFillerLengthAddress;
            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));

            Assert.That(logRecord.TrySetValueSpan(value, ref sizeInfo), Is.True);

            // These should be the same still.
            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));

            long eTag = 1000;
            long expiration = 2000;

            Assert.That(logRecord.TrySetETag(eTag), Is.True);

            expectedFillerLengthAddress += LogRecord.ETagSize;
            expectedFillerLength -= LogRecord.ETagSize;
            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));

            Assert.That(logRecord.TrySetExpiration(expiration), Is.True);
            Assert.That(logRecord.ETag, Is.EqualTo(eTag));

            expectedFillerLengthAddress += LogRecord.ExpirationSize;
            expectedFillerLength -= LogRecord.ExpirationSize;
            Assert.That(logRecord.GetFillerLengthAddress(), Is.EqualTo(expectedFillerLengthAddress));
            Assert.That(logRecord.GetFillerLength(), Is.EqualTo(expectedFillerLength));

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
    }
}