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
    /// <remarks>
    /// Carries key bytes via one of three storage modes selected at <see cref="Create{TKey}"/>:
    /// <list type="bullet">
    ///   <item><b>Pinned</b> — caller guarantees the key bytes are pinned for the
    ///     lifetime of this struct (used by pre-pinned RESP buffers). Stored as a raw
    ///     pointer; copy of this struct is safe because the pointer is to externally
    ///     pinned memory.</item>
    ///   <item><b>Inline</b> — key (and optional namespace) bytes fit in a fixed
    ///     <see cref="InlineCapacity"/>-byte buffer carried by the struct itself. No
    ///     heap allocation. Span accessors point into the receiver's own storage —
    ///     callers must not stash the span across struct copies (the span is implicitly
    ///     scoped to the receiver's lifetime, same contract as the pinned path).</item>
    ///   <item><b>Hoisted (pooled)</b> — key bytes are copied into a
    ///     <see cref="SectorAlignedMemory"/> rented from a pool. Used only when the key
    ///     exceeds <see cref="InlineCapacity"/>. <see cref="Dispose"/> returns the
    ///     buffer to the pool.</item>
    /// </list>
    /// The pending-IO path on small keys (8-byte hashes, 16-byte UUIDs, short RESP
    /// keys) hits the Inline branch and avoids the per-pending-read
    /// <c>SectorAlignedBufferPool.Get/Return</c> cycle that otherwise dominates GC on
    /// disk-bound workloads.
    /// </remarks>
    public unsafe struct ConditionallyHoistedKey : IKey, IDisposable
    {
        /// <summary>
        /// Maximum total bytes (key + namespace) that will be stored inline inside
        /// this struct without renting a <see cref="SectorAlignedMemory"/>. Sized to
        /// cover typical Garnet RESP small-key workloads (≤ 32 B) while keeping the
        /// containing structs (<c>PendingContext</c>, <see cref="AsyncIOContext"/>)
        /// compact enough that the per-op struct copy
        /// (Buffer.BulkMoveWithWriteBarrier) does not regress the hot path.
        /// </summary>
        internal const int InlineCapacity = 32;

        /// <summary>Fixed-size inline byte buffer used for the Inline storage mode.</summary>
        [InlineArray(InlineCapacity)]
        private struct InlineBuf
        {
            private byte _e0;
        }

        private static ConditionallyHoistedKey Empty { get; } = new((byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference<byte>([])), 0);

        private readonly byte* keyPtr;
        private int keyLen;
        private readonly byte* namespacePtr;
        private int namespaceLen;
        private SectorAlignedMemory keyAndNamespaceMem;
        private InlineBuf inlineBuf;
        private bool isInline;

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
                else if (isInline)
                {
                    // Inline layout matches Tsavorite's on-disk RecordDataHeader: namespace
                    // bytes (if any) precede the key bytes. Key starts at offset namespaceLen.
                    // Span points into the receiver's own inline storage. Lifetime is the
                    // receiver's lifetime; callers MUST consume the span before letting the
                    // receiver go out of scope (same contract as the pinned-pointer mode).
                    return MemoryMarshal.CreateReadOnlySpan(
                        ref Unsafe.Add(ref Unsafe.AsRef(in Unsafe.As<InlineBuf, byte>(ref Unsafe.AsRef(in inlineBuf))), namespaceLen),
                        keyLen);
                }
                else if (keyAndNamespaceMem != null)
                {
                    return keyAndNamespaceMem.TotalValidSpan.Slice(namespaceLen, keyLen);
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
                else if (isInline)
                {
                    // Namespace bytes are stored before the key bytes in the inline buffer
                    // (matches Tsavorite's on-disk RecordDataHeader extended-NS layout).
                    return MemoryMarshal.CreateReadOnlySpan(
                        ref Unsafe.AsRef(in Unsafe.As<InlineBuf, byte>(ref Unsafe.AsRef(in inlineBuf))),
                        namespaceLen);
                }
                else if (keyAndNamespaceMem != null)
                {
                    return keyAndNamespaceMem.TotalValidSpan[..namespaceLen];
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
            // Inline mode owns no external memory, so Dispose is a no-op there.
            keyAndNamespaceMem?.Return();
            keyAndNamespaceMem = null;
        }

        /// <summary>
        /// Create a new <see cref="ConditionallyHoistedKey"/>, copying bytes if needed.
        /// </summary>
        /// <remarks>
        /// Storage mode selection:
        /// <list type="bullet">
        ///   <item><see cref="IKey.IsPinned"/> → raw pointer (no copy, no alloc).</item>
        ///   <item>Else key (+ optional namespace) total bytes ≤ <see cref="InlineCapacity"/>
        ///     → inline buffer carried by this struct (no alloc).</item>
        ///   <item>Else → <see cref="SectorAlignedMemory"/> rented from <paramref name="bufferPool"/>
        ///     and freed by <see cref="Dispose"/>.</item>
        /// </list>
        /// </remarks>
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
                // Inline-storage fast path: copy small keys directly into the struct so the
                // pending-read path doesn't burn a SectorAlignedMemory.Get + Return + Array.Clear
                // cycle on every IO. Total budget is InlineCapacity bytes shared between key
                // and namespace. Layout matches Tsavorite's on-disk RecordDataHeader: namespace
                // bytes first, then key bytes.
                if (key.HasNamespace)
                {
                    var namespaceBytes = key.NamespaceBytes;
                    var totalLen = keyBytes.Length + namespaceBytes.Length;

                    if (totalLen <= InlineCapacity)
                    {
                        ConditionallyHoistedKey result = default;
                        result.keyLen = keyBytes.Length;
                        result.namespaceLen = namespaceBytes.Length;
                        result.isInline = true;
                        Span<byte> dst = result.inlineBuf;
                        namespaceBytes.CopyTo(dst);
                        keyBytes.CopyTo(dst[namespaceBytes.Length..]);
                        return result;
                    }

                    var mem = bufferPool.Get(totalLen);
                    namespaceBytes.CopyTo(mem.TotalValidSpan);
                    keyBytes.CopyTo(mem.TotalValidSpan[namespaceBytes.Length..]);

                    return new(mem, keyBytes.Length, namespaceBytes.Length);
                }
                else
                {
                    if (keyBytes.Length <= InlineCapacity)
                    {
                        ConditionallyHoistedKey result = default;
                        result.keyLen = keyBytes.Length;
                        result.isInline = true;
                        Span<byte> dst = result.inlineBuf;
                        keyBytes.CopyTo(dst);
                        return result;
                    }

                    var mem = bufferPool.Get(keyBytes.Length);
                    keyBytes.CopyTo(mem.TotalValidSpan);

                    return new(mem, keyBytes.Length);
                }
            }
        }
    }
}