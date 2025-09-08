﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    /// <summary>A byte[] wrapper that encodes start and end offsets of the actual data in the first sizeof(<see cref="OverflowHeader"/>) bytes in the array.</summary>
    /// <remarks>Used primarily for sector-aligned reads directly into the overflow byte[].</remarks>
    public struct OverflowByteArray
    {
        /// <summary>Define the header of an overflow allocation. Overflow allocations are typically large so use a full int to allow greater than 64k offsets,
        /// which makes it possible to read more information with a single IO and then copy it out to other destinations. Sector sizes may be up to 64k on NTFS systems,
        /// which is sizeof(ushort) bytes, so the use of full ints removes boundary concerns (e.g. reading a value followed by optional bytes may cross a sector boundary,
        /// in which case we need an end offset greater than a single sector).</summary>
        struct OverflowHeader
        {
            internal const int Size = 2 * sizeof(int);
            internal int startOffset, endOffset;
        }

        internal readonly byte[] Data { get; init; }

        internal readonly bool IsEmpty => Data is null;

        internal readonly int StartOffset => Unsafe.As<byte, OverflowHeader>(ref Data[0]).startOffset + OverflowHeader.Size;

        readonly int EndOffset => Unsafe.As<byte, OverflowHeader>(ref Data[0]).endOffset;

        internal readonly int Length => Data.Length - StartOffset - EndOffset;

        /// <summary>Span of data between offsets</summary>
        internal readonly ReadOnlySpan<byte> ReadOnlySpan => Data.AsSpan().Slice(StartOffset, Length);
        /// <summary>Span of data between offsets</summary>
        internal readonly Span<byte> Span => Data.AsSpan().Slice(StartOffset, Length);

        /// <summary>Span of all data, including before and after offsets; this is for aligned Read from the device.</summary>
        internal readonly Span<byte> AlignedReadSpan => Data.AsSpan().Slice(OverflowHeader.Size);

        /// <summary>Construct an <see cref="OverflowByteArray"/> from a byte[] allocated by <see cref="OverflowByteArray(int, int, int, bool)"/>.</summary>
        internal OverflowByteArray(byte[] data) => Data = data;

        internal OverflowByteArray(int length, int startOffset, int endOffset, bool zeroInit)
        {
            // Allocate with enough extra space for the metadata (offset from start and end)
            Data = !zeroInit 
                ? GC.AllocateUninitializedArray<byte>(length + OverflowHeader.Size) 
                : (new byte[length + OverflowHeader.Size]);
            ref var header = ref Unsafe.As<byte, OverflowHeader>(ref Data[0]);
            header.startOffset = startOffset;
            header.endOffset = endOffset;
        }

        /// <summary>Increase the offset from the start, e.g. after having extracted the key that was read in the same IO operation as the value.</summary>
        /// <remarks>This is 'readonly' because it does not alter the <see cref="Data"/> array field, only its contents.</remarks>
        internal readonly void AdjustOffsetFromStart(int increment) => Unsafe.As<byte, OverflowHeader>(ref Data[0]).startOffset += increment;
        /// <summary>Increase the offset from the end, e.g. after having extracted the optionals that were read in the same IO operation as the value.</summary>
        /// <remarks>This is 'readonly' because it does not alter the <see cref="Data"/>> array field, only its contents.</remarks>
        internal readonly void AdjustOffsetFromEnd(int increment) => Unsafe.As<byte, OverflowHeader>(ref Data[0]).endOffset += increment;

        internal readonly void SetOffsets(int offsetFromStart, int offsetFromEnd)
        {
            Debug.Assert(offsetFromStart > 0 && offsetFromStart < Data.Length - 1, "offsetFromStart is out of range");
            Debug.Assert(offsetFromEnd > 0 && offsetFromEnd < Data.Length - 1, "offsetFromEnd is out of range");
            Debug.Assert(offsetFromStart < offsetFromEnd, "offsetFromStart must be less than offsetFromEnd");
            ref var header = ref Unsafe.As<byte, OverflowHeader>(ref Data[0]);
            header.startOffset = offsetFromStart;
            header.endOffset = offsetFromEnd;
        }

        internal readonly void ExtractOptionalsAtEnd(RecordInfo recordInfo, int optionalLength, out RecordOptionals optionals)
        {
            var optionalOffset = Length - optionalLength;
            optionals = default;
            if (recordInfo.HasETag)
            {
                optionals.eTag = MemoryMarshal.Cast<byte, long>(ReadOnlySpan.Slice(optionalOffset, LogRecord.ETagSize))[0];
                optionalOffset += LogRecord.ETagSize;
            }
            if (recordInfo.HasExpiration)
                optionals.expiration = MemoryMarshal.Cast<byte, long>(ReadOnlySpan.Slice(optionalOffset, LogRecord.ExpirationSize))[0];
            AdjustOffsetFromEnd(Length - optionalLength);
        }

        /// <summary>Get the <see cref="ReadOnlySpan{_byte_}"/> of a byte[] allocated by <see cref="OverflowByteArray(int, int, int, bool)"/> constructor.</summary>
        internal static ReadOnlySpan<byte> AsReadOnlySpan(object value) => new OverflowByteArray(Unsafe.As<byte[]>(value)).ReadOnlySpan;

        /// <summary>Get the <see cref="Span{_byte_}"/> of a byte[] allocated by <see cref="OverflowByteArray(int, int, int, bool)"/> constructor.</summary>
        internal static Span<byte> AsSpan(object value) => new OverflowByteArray(Unsafe.As<byte[]>(value)).Span;
    }
}
