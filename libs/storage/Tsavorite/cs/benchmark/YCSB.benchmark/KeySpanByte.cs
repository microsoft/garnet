// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    [StructLayout(LayoutKind.Explicit, Size = DataSize)]
    public struct KeySpanByte
    {
        public const int DataSize = 12;
        public const int TotalSize = DataSize + sizeof(int) * 2;

        /// <summary>The data of the key</summary>
        [FieldOffset(0)]
        public long value;

        /// <summary>
        /// This field is for kRecordAlignment of the key since Tsavorite no longer aligns key size (i.e. Value start) to <see cref="Constants.kRecordAlignment"/>.
        /// </summary>
        /// <remarks>
        /// Combined with the varbyte length total of 3 bytes (indicator, 1 byte for key length, 1 byte for value length), we get:
        /// [RecordInfo varbytelength keydata valuedata] = [8 + 3 + 20 + 96] = 127, so the final record size is exactly aligned to two cache lines.
        /// To illustrate why this is imporatant: during the conversion to <see cref="ReadOnlySpan{_byte_}"/>, the change in key alignment was not correctly
        /// accounted for; the record was 8 bytes shorter, and the next record's RecordInfo was in the final bytes of the previous record's cache line.
        /// This resulted in about a 10% slowdown.
        /// </remarks>
        [FieldOffset(sizeof(long))]
        public int padding1, padding2;

        // Only call this for stack-based structs, not the ones in the *_keys vectors
        public override readonly string ToString() => "{ " + value + " }";

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe ReadOnlySpan<byte> AsReadOnlySpan() => new(Unsafe.AsPointer(ref this), DataSize);
    }
}