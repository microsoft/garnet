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
    public struct KeySpanByte : IKey
    {
        internal const int DataSize = 16;

        /// <summary>The data of the key</summary>
        [FieldOffset(0)]
        public long value;

        /// <summary>
        /// These fields are for kRecordAlignment of the key since Tsavorite no longer aligns key size (i.e. Value start) to <see cref="Constants.kRecordAlignment"/>.
        /// The size remains the same as the previous key size for comparison purposes, making sure the large init_key and txn_key arrays use the same amount of memory.
        /// </summary>
        /// <remarks>
        /// Combined with the header length total of <see cref="RecordDataHeader.MinHeaderBytes"/> bytes, we get:
        ///     [RecordInfo header no_extended_namespace keydata valuedata]
        ///   = [8 + 5 (NumIndicatorBytes + 2 1-byte lengths) + 12 + 100 (see <see cref="SpanByteYcsbConstants.kValueDataSize"/>)] = 125
        /// which is rounded up to <see cref="Constants.kRecordAlignment"/> (8) so the final record size is exactly aligned to two cache lines.
        /// To illustrate why this is important: during the conversion to <see cref="ReadOnlySpan{_byte_}"/>, the change in key alignment was not correctly accounted for;
        /// the record was 8 bytes shorter, and the next record's RecordInfo was in the final bytes of the previous record's cache line. This resulted in about a 10% slowdown.
        /// </remarks>
        [FieldOffset(sizeof(long))]
        public int padding1, padding2;

        /// <summary>
        /// Convert to string; Only call this for stack-based structs, not the ones in the *_keys vectors.
        /// </summary>
        public override readonly string ToString() => "{ " + value + " }";

        /// <inheritdoc/>
        public readonly bool IsPinned => false;

        /// <inheritdoc/>
        public unsafe ReadOnlySpan<byte> KeyBytes
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new(Unsafe.AsPointer(ref this), sizeof(long));   // Not including the padding in the key bytes since it's not actually part of the key; it's just for alignment purposes.
        }

        /// <inheritdoc/>
        public readonly bool HasNamespace => false;

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> NamespaceBytes => [];
    }
}