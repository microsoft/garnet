// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test.spanbyte
{
    [TestFixture]
    [Category("TsavoriteKV")]
    [Category("Smoke")]
    internal class SpanByteComparerTests
    {
        [Test]
        public void Equals_ReturnsTrue_ForIdenticalSpanBytes()
        {
            var left = SpanByte.FromPinnedSpan([1, 2, 3, 4]);
            var right = SpanByte.FromPinnedSpan([1, 2, 3, 4]);

            ClassicAssert.IsTrue(SpanByteComparer.Instance.Equals(ref left, ref right));
        }

        [Test]
        public void Equals_ReturnsFalse_ForDifferentPayloads()
        {
            var left = SpanByte.FromPinnedSpan([1, 2, 3, 4]);
            var right = SpanByte.FromPinnedSpan([1, 2, 3, 5]);

            ClassicAssert.IsFalse(SpanByteComparer.Instance.Equals(ref left, ref right));
        }

        [Test]
        public void Equals_ReturnsFalse_ForDifferentMetadataSizes()
        {
            Span<byte> data = [1, 2, 3, 4, 5, 6, 7, 8];
            var left = SpanByte.FromPinnedSpan(data);
            var right = SpanByte.FromPinnedSpan(data);

            left.ExtraMetadata = 1234; // sets MetadataSize to 8

            ClassicAssert.AreEqual(8, left.MetadataSize);
            ClassicAssert.AreEqual(0, right.MetadataSize);
            ClassicAssert.IsFalse(SpanByteComparer.Instance.Equals(ref left, ref right));
        }

        [Test]
        public void Equals_ReturnsTrue_SerializedAndUnserializedWithSamePayload()
        {
            Span<byte> payload = [1, 2, 3, 4];
            var unserialized = SpanByte.FromPinnedSpan(payload);

            Span<byte> serializedSpan = stackalloc byte[sizeof(int) + payload.Length];
            payload.CopyTo(serializedSpan.Slice(sizeof(int)));

            var serialized = SpanByte.Reinterpret(serializedSpan);

            ClassicAssert.AreNotEqual(unserialized.Serialized, serialized.Serialized);
            ClassicAssert.True(SpanByteComparer.Instance.Equals(ref serialized, ref unserialized));
        }

        [Test]
        public void Equals_ReturnsTrue_ForTwoEmpty()
        {
            var left = new SpanByte();
            var right = new SpanByte();

            ClassicAssert.True(SpanByteComparer.Instance.Equals(ref left, ref right));
        }

        [Test]
        public void Equals_ReturnsTrue_ForEmptyAndInvalid()
        {
            var invalid = new SpanByte();
            invalid.Invalid = true;

            var valid = new SpanByte();

            ClassicAssert.IsTrue(SpanByteComparer.Instance.Equals(ref invalid, ref valid));
        }
    }
}
