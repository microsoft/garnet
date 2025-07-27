// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>A byte[] wrapper that encodes offset from start in the first 2 bytes of the array and offset from end in the next 2 bytes.</summary>
    /// <remarks>Used primarily for sector-aligned reads directly into the overflow byte[]. The offsets are up to sector size, which is &lt;= 64k on NTFS systems.</remarks>
    internal struct OverflowByteArray
    {
        /// <summary>
        /// The maximum offset from start or end allowed by our 2-byte offsets.
        /// </summary>
        internal const int MaxInternalOffset = 64 * 1024;

        internal readonly byte[] Data { get; init; }

        internal readonly bool IsEmpty => Data is null;

        readonly int Offset => Unsafe.As<byte, ushort>(ref Data[0]);

        readonly int Length => Data.Length - Offset - Unsafe.As<byte, ushort>(ref Data[sizeof(ushort)]);

        internal readonly ReadOnlySpan<byte> ReadOnlySpan => Data.AsSpan().Slice(Offset, Length);
        internal readonly Span<byte> Span => Data.AsSpan().Slice(Offset, Length);

        internal OverflowByteArray(byte[] data) => Data = data;
        internal OverflowByteArray(byte[] data, int startOffset, int endOffset)
        {
            Data = data;
            Unsafe.As<byte, ushort>(ref Data[0]) = (ushort)startOffset;
            Unsafe.As<byte, ushort>(ref Data[sizeof(ushort)]) = (ushort)endOffset;
        }

        /// <summary>Increase the offset from the start, e.g. after having extracted the key that was read in the same IO operation as the value.</summary>
        /// <remarks>This is 'readonly' because it does not alter the <see cref="Data"/>> array field, only its contents.</remarks>
        internal readonly void AdjustOffsetFromStart(int increment) => Unsafe.As<byte, ushort>(ref Data[0]) += (ushort)increment;
        /// <summary>Increase the offset from the end, e.g. after having extracted the optionals that were read in the same IO operation as the value.</summary>
        /// <remarks>This is 'readonly' because it does not alter the <see cref="Data"/>> array field, only its contents.</remarks>
        internal readonly void AdjustOffsetFromEnd(int increment) => Unsafe.As<byte, ushort>(ref Data[2]) += (ushort)increment;

        internal OverflowByteArray(int length, int startOffset, int endOffset)
        {
            Debug.Assert(startOffset <= ushort.MaxValue, "startOffset must be less than or equal to ushort.MaxValue");
            Debug.Assert(endOffset <= ushort.MaxValue, "endOffset must be less than or equal to ushort.MaxValue");
            Data = new byte[length + (sizeof(ushort) * 2)];
            Unsafe.As<byte, ushort>(ref Data[0]) = (ushort)startOffset;
            Unsafe.As<byte, ushort>(ref Data[sizeof(ushort)]) = (ushort)endOffset;
        }

        internal readonly void SetOffsets(int offsetFromStart, int offsetFromEnd)
        {
            Debug.Assert(offsetFromStart > 0 && offsetFromStart < Data.Length - 1, "offsetFromStart is out of range");
            Debug.Assert(offsetFromEnd > 0 && offsetFromEnd < Data.Length - 1, "offsetFromEnd is out of range");
            Debug.Assert(offsetFromStart < offsetFromEnd, "offsetFromStart must be less than offsetFromEnd");
            Unsafe.As<byte, ushort>(ref Data[0]) = (ushort)offsetFromStart;
            Unsafe.As<byte, ushort>(ref Data[2]) = (ushort)offsetFromEnd;
        }

        internal readonly void ExtractOptionals(RecordInfo recordInfo, int valueLength, out long eTag, out long expiration)
        {
            var optionalOffset = 0;
            eTag = expiration = 0L;
            if (recordInfo.HasETag)
            {
                eTag = Unsafe.As<byte, long>(ref Span.Slice(valueLength, LogRecord.ETagSize)[0]);
                optionalOffset += LogRecord.ETagSize;
            }
            if (recordInfo.HasExpiration)
            {
                expiration = Unsafe.As<byte, long>(ref Span.Slice(valueLength + optionalOffset, LogRecord.ExpirationSize)[0]);
                optionalOffset += LogRecord.ExpirationSize;
            }
            AdjustOffsetFromEnd(optionalOffset);
        }

        internal static ReadOnlySpan<byte> AsReadOnlySpan(object value) => new OverflowByteArray(Unsafe.As<byte[]>(value)).ReadOnlySpan;
        internal static Span<byte> AsSpan(object value) => new OverflowByteArray(Unsafe.As<byte[]>(value)).Span;
    }
}
