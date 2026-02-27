// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    /// <summary>
    /// An <see cref="IKey"/> that can be used in heap allocated contexts.
    /// </summary>
    public unsafe struct ConditionallyHoistedKey : IKey, IDisposable
    {
        private readonly byte* keyPtr;
        private readonly int keyLen;
        private SectorAlignedMemory keyMem;

        public readonly bool IsEmpty => keyLen == 0;

        /// <inheritdoc/>
        public readonly bool IsPinned
        => keyPtr != null;

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> KeyBytes
        {
            get
            {
                if (keyPtr != null)
                {
                    return new ReadOnlySpan<byte>(keyPtr, keyLen);
                }
                else if (keyMem != null)
                {
                    return keyMem.TotalValidSpan[..keyLen];
                }
                else
                {
                    return default;
                }
            }
        }

        private ConditionallyHoistedKey(byte* keyPtr, int keyLen)
        {
            this.keyPtr = keyPtr;
            this.keyMem = null;
            this.keyLen = keyLen;
        }

        private ConditionallyHoistedKey(SectorAlignedMemory keyArr, int keyLen)
        {
            this.keyPtr = null;
            this.keyMem = keyArr;
            this.keyLen = keyLen;
        }

        /// <inheritdoc/>
        public long GetKeyHashCode64()
            => SpanByteComparer.StaticGetHashCode64(KeyBytes);

        /// <inheritdoc/>
        public bool KeysEqual<TOther>(TOther other) where TOther :
            IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            => KeyBytes.SequenceEqual(other.KeyBytes);

        /// <inheritdoc/>
        public void Dispose()
        {
            keyMem?.Return();
            keyMem = null;
        }

        /// <summary>
        /// Create a new <see cref="ConditionallyHoistedKey"/>, copying bytes if needed.
        /// </summary>
        internal static ConditionallyHoistedKey Create<TKey>(TKey key, SectorAlignedBufferPool bufferPool)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif

        {
            var keyBytes = key.KeyBytes;

            if (keyBytes.IsEmpty || key.IsPinned)
            {
                return new((byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(keyBytes)), keyBytes.Length);
            }
            else
            {
                // TODO: This matches existing use, but is this correct?  Seems like we'd get a big record here
                var mem = bufferPool.Get(keyBytes.Length);
                keyBytes.CopyTo(mem.TotalValidSpan);

                return new(mem, keyBytes.Length);
            }
        }
    }
}
