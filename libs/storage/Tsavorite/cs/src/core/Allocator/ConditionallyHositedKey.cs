// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    /// <summary>
    /// An <see cref="IKey"/> that can be used in heap allocated contexts.
    /// </summary>
    public unsafe struct ConditionallyHoistedKey : IKey, IDisposable
    {
        private static ConditionallyHoistedKey Empty { get; } = new((byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference<byte>([])), 0);

        private readonly byte* keyPtr;
        private readonly int keyLen;
        private readonly byte* namespacePtr;
        private readonly int namespaceLen;
        private SectorAlignedMemory keyAndNamespaceMem;

        /// <inheritdoc/>
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
                else if (keyAndNamespaceMem != null)
                {
                    return keyAndNamespaceMem.TotalValidSpan[..keyLen];
                }
                else
                {
                    return default;
                }
            }
        }

        /// <inheritdoc/>
        public readonly bool HasNamespace => namespaceLen != 0;

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> NamespaceBytes
        {
            get
            {
                Debug.Assert(HasNamespace, "Should never be called if !HasNamespace");

                if (namespacePtr != null)
                {
                    return new ReadOnlySpan<byte>(namespacePtr, namespaceLen);
                }
                else if (keyAndNamespaceMem != null)
                {
                    return keyAndNamespaceMem.TotalValidSpan.Slice(keyLen, namespaceLen);
                }
                else
                {
                    return default;
                }
            }
        }

        private ConditionallyHoistedKey(byte* keyPtr, int keyLen)
        {
            keyAndNamespaceMem = null;
            namespaceLen = 0;

            this.keyPtr = keyPtr;
            this.keyLen = keyLen;
        }

        private ConditionallyHoistedKey(byte* keyPtr, int keyLen, byte* namespacePtr, int namespaceLen)
        {
            Debug.Assert(namespaceLen > 0, "Shouldn't use this constructor if namespace isn't set");

            keyAndNamespaceMem = null;

            this.keyPtr = keyPtr;
            this.keyLen = keyLen;

            this.namespacePtr = namespacePtr;
            this.namespaceLen = namespaceLen;
        }

        private ConditionallyHoistedKey(SectorAlignedMemory keyArr, int keyLen)
        {
            this.keyPtr = null;
            this.keyAndNamespaceMem = keyArr;
            this.keyLen = keyLen;
            this.namespaceLen = 0;
        }

        private ConditionallyHoistedKey(SectorAlignedMemory keyArr, int keyLen, int namespaceLen)
        {
            Debug.Assert(namespaceLen > 0, "Shouldn't use this constructor if namespace isn't set");

            this.keyPtr = null;
            this.keyAndNamespaceMem = keyArr;
            this.keyLen = keyLen;
            this.namespaceLen = namespaceLen;
        }

        /// <inheritdoc/>
        public bool KeysEqual<TOther>(TOther other) where TOther :
            IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            if (other.HasNamespace)
            {
                if (!HasNamespace)
                {
                    return false;
                }

                // Namespace must be considered alongside key
                return KeyBytes.SequenceEqual(other.KeyBytes) && NamespaceBytes.SequenceEqual(other.NamespaceBytes);
            }
            else
            {
                if (HasNamespace)
                {
                    return false;
                }

                // Namespace is known not set, ignore
                return KeyBytes.SequenceEqual(other.KeyBytes);
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            keyAndNamespaceMem?.Return();
            keyAndNamespaceMem = null;
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

            if (key.IsEmpty)
            {
                Debug.Assert(!key.HasNamespace, "Empty key should never have a namespace");
                return Empty;
            }

            var keyBytes = key.KeyBytes;

            if (key.IsPinned)
            {
                var keyPtr = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(keyBytes));

                if (key.HasNamespace)
                {
                    var namespaceBytes = key.NamespaceBytes;
                    var namespacePtr = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(namespaceBytes));

                    return new(
                        keyPtr,
                        keyBytes.Length,
                        namespacePtr,
                        namespaceBytes.Length
                    );
                }

                return new(keyPtr, keyBytes.Length);
            }
            else
            {
                // TODO: This matches existing use, but is this correct?  Seems like we'd get a big record here

                var recordMinLen = keyBytes.Length;
                if (key.HasNamespace)
                {
                    var namespaceBytes = key.NamespaceBytes;

                    recordMinLen += namespaceBytes.Length;

                    var mem = bufferPool.Get(keyBytes.Length);
                    keyBytes.CopyTo(mem.TotalValidSpan);
                    namespaceBytes.CopyTo(mem.TotalValidSpan[keyBytes.Length..]);

                    return new(mem, keyBytes.Length, namespaceBytes.Length);
                }
                else
                {
                    var mem = bufferPool.Get(keyBytes.Length);
                    keyBytes.CopyTo(mem.TotalValidSpan);

                    return new(mem, keyBytes.Length);
                }
            }
        }
    }
}