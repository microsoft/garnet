// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Tsavorite.kvbench
{
    /// <summary>
    /// 11-byte key with the 8-byte payload at the END of the byte sequence.
    /// Layout in a Tsavorite record: hdr(13) + key(11) + value → value at offset 24 (8-byte aligned).
    /// Record size stays 120 (same as the 8-byte key with padding); no cache footprint growth.
    /// Uses <see cref="KvKeyComparer"/> to hash/compare only the trailing 8 payload bytes (not all 11).
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = DataSize)]
    public struct KvKey : IKey
    {
        internal const int DataSize = 11;
        internal const int PayloadOffset = DataSize - sizeof(long); // 3

        [FieldOffset(0)]
        public byte pad0, pad1, pad2;

        /// <summary>The 8-byte key payload — written by callers via <c>key.Value = ...</c>.</summary>
        [FieldOffset(PayloadOffset)]
        public long Value;

        public override readonly string ToString() => "{ " + Value + " }";

        public readonly bool IsPinned => false;

        /// <summary>All 11 bytes are exposed (padding included) so that copies into the log are byte-exact.
        /// Hashing/equality uses only the trailing payload — see <see cref="KvKeyComparer"/>.</summary>
        public unsafe ReadOnlySpan<byte> KeyBytes
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new(Unsafe.AsPointer(ref this), DataSize);
        }

        public readonly bool HasNamespace => false;

        public readonly ReadOnlySpan<byte> NamespaceBytes => [];
    }
}
