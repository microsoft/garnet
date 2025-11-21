// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    /// <summary>
    /// A key in <see cref="SpanByteYcsbBenchmark{TAllocator}"/>.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = DataSize)]
    public struct KeySpanByte
    {
        internal const int DataSize = 12;
        internal const int TotalSize = DataSize + (sizeof(int));

        /// <summary>The data of the key</summary>
        [FieldOffset(0)]
        public long value;

        /// <summary>
        /// This field is for kRecordAlignment of the key since Tsavorite no longer aligns key size (i.e. Value start) to <see cref="Constants.kRecordAlignment"/>.
        /// </summary>
        /// <remarks>
        /// Combined with the header length total of <see cref="VarbyteLengthUtility.MinLengthMetadataBytes"/> bytes, we get:
        /// [RecordInfo header no_extended_namespace keydata valuedata] = [8 + 5 (NumIndicatorBytes + 2 1-byte lengths) + 12 + 100 (kValueDataSize)] = 125, so the final record size is
        /// exactly aligned to two cache lines. To illustrate why this is important: during the conversion to <see cref="ReadOnlySpan{_byte_}"/>, the change in key
        /// alignment was not correctly accounted for; the record was 8 bytes shorter, and the next record's RecordInfo was in the final bytes of the previous record's
        /// cache line. This resulted in about a 10% slowdown.
        /// </remarks>
        [FieldOffset(sizeof(long))]
        public int padding1;

        /// <summary>
        /// Convert to string; Only call this for stack-based structs, not the ones in the *_keys vectors.
        /// </summary>
        public override readonly string ToString() => "{ " + value + " }";

        /// <summary>
        /// Represent the key as a <see cref="ReadOnlySpan{_byte_}"/>
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe ReadOnlySpan<byte> AsReadOnlySpan() => new(Unsafe.AsPointer(ref this), DataSize);
    }
}