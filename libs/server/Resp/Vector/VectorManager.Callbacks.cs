// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Methods which <see cref="DiskANNService"/> calls back into to interact with Garnet.
    /// </summary>
    public sealed partial class VectorManager
    {
        public unsafe
#if NET9_0_OR_GREATER
            ref
#endif
            struct VectorReadBatch : IReadArgBatch<VectorElementKey, VectorInput, VectorOutput>
        {
            public int Count { get; }

            public readonly ReadOnlySpan<PinnedSpanByte> Parameters
                => default;

            /// <summary>
            /// Per-term initial disk read size. The big, fixed-size records (FullVector, and the adjacency
            /// NeighborList) are sized to the active vector set's geometry (<see cref="SetActiveReadGeometry"/>)
            /// so each lands in a single IO regardless of dimension / M — and different vector sets get different
            /// optimal sizes. When the geometry is unset (paths that don't call SetActiveReadGeometry), FullVector
            /// uses the configured store/session size (<c>--initial-io-record-size</c>) and the other terms use the
            /// small default, avoiding over-reading a full-vector-sized block for a tiny record.
            /// </summary>
            public readonly int InitialIORecordSize
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    // Single thread-static read; the struct copy (3 ints) is cheaper than re-reading TLS per branch.
                    var geometry = ActiveReadGeometry;
                    switch (NamespaceBytes[0] & 7)
                    {
                        case DiskANNService.FullVector:
                            return geometry.FullVectorIOSize > 0 ? geometry.FullVectorIOSize : KVSettings.UseDefaultInitialIORecordSize;
                        case DiskANNService.NeighborList:
                            return geometry.NeighborListIOSize > 0 ? geometry.NeighborListIOSize : IStreamBuffer.DefaultInitialIORecordSize;
                        case DiskANNService.QuantizedVector:
                            return geometry.QuantizedVectorIOSize > 0 ? geometry.QuantizedVectorIOSize : IStreamBuffer.DefaultInitialIORecordSize;
                        default:
                            return IStreamBuffer.DefaultInitialIORecordSize;
                    }
                }
            }

            /// <summary>
            /// Per-term read-copy policy. The small per-element records (NeighborList adjacency, QuantizedVector,
            /// internal/external id maps) are copied back into memory on disk read — to <see cref="StubReadCopyTo"/>
            /// (the read cache when enabled, else the main-log tail) — so later hops and queries serve them from
            /// memory. The large raw FullVector and Attributes/Metadata are served from disk (CopyTo=None): quantized
            /// sets use the raw vector only for reranking, and for no-quant sets caching it yields no net gain once the
            /// working set exceeds the read cache, as copying these large records in costs more than the reads it saves.
            /// </summary>
            public readonly ReadCopyOptions ReadCopyOptions
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    switch (NamespaceBytes[0] & 7)
                    {
                        case DiskANNService.NeighborList:
                        case DiskANNService.QuantizedVector:
                        case DiskANNService.InternalIdMap:
                        case DiskANNService.ExternalIdMap:
                            return new ReadCopyOptions { CopyFrom = ReadCopyFrom.AllImmutable, CopyTo = ActiveThreadSession.vectorManager.StubReadCopyTo };
                        default:
                            return new ReadCopyOptions { CopyFrom = ReadCopyFrom.None, CopyTo = ReadCopyTo.None };
                    }
                }
            }

            private readonly ReadOnlySpan<byte> NamespaceBytes
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
#if NET9_0_OR_GREATER
                    return namespaceBytes;
#else
                    return new ReadOnlySpan<byte>(namespaceBytesPtr, namespaceBytesLen);
#endif
                }
            }

#if NET9_0_OR_GREATER
            private readonly ReadOnlySpan<byte> namespaceBytes;
#else
            private byte* namespaceBytesPtr;
            private int namespaceBytesLen;
#endif
            private readonly PinnedSpanByte lengthPrefixedKeys;

            public readonly delegate* unmanaged[Cdecl, SuppressGCTransition]<int, nint, nint, nuint, void> callback;
            public readonly nint callbackContext;

            private int currentIndex;

            private int currentLen;
            private byte* currentPtr;

            private bool hasPending;

            public VectorReadBatch(nint callback, nint callbackContext, uint keyCount, PinnedSpanByte lengthPrefixedKeys, ReadOnlySpan<byte> namespaceBytes)
            {
#if NET9_0_OR_GREATER
                this.namespaceBytes = namespaceBytes;
#else
                namespaceBytesPtr = (byte*)Unsafe.AsPointer(ref Unsafe.AsRef(in namespaceBytes[0]));
                namespaceBytesLen = namespaceBytes.Length;
#endif
                this.lengthPrefixedKeys = lengthPrefixedKeys;

                this.callback = (delegate* unmanaged[Cdecl, SuppressGCTransition]<int, nint, nint, nuint, void>)callback;
                this.callbackContext = callbackContext;

                currentIndex = 0;
                Count = (int)keyCount;

                currentPtr = this.lengthPrefixedKeys.ToPointer();
                currentLen = *(int*)currentPtr;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void AdvanceTo(int i)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");

                if (i == currentIndex)
                {
                    return;
                }

                // Most likely case, we're going one forward
                if (i == (currentIndex + 1))
                {
                    currentPtr += currentLen + sizeof(int); // Skip length prefix too

                    Debug.Assert(currentPtr < lengthPrefixedKeys.ToPointer() + lengthPrefixedKeys.Length, "About to access out of bounds data");

                    currentLen = *(int*)currentPtr;

                    currentIndex = i;

                    return;
                }

                // Next most likely case, we're going back to the start
                currentPtr = lengthPrefixedKeys.ToPointer();
                currentLen = *(int*)currentPtr;
                currentIndex = 0;

                if (i == 0)
                {
                    return;
                }

                SlowPath(ref this, i);

                // For the case where we're not just scanning or rolling back to 0, just iterate
                //
                // This should basically never happen
                [MethodImpl(MethodImplOptions.NoInlining)]
                static void SlowPath(ref VectorReadBatch self, int i)
                {
                    for (var subI = 1; subI <= i; subI++)
                    {
                        self.AdvanceTo(subI);
                    }
                }
            }

            /// <inheritdoc/>
            public void GetKey(int i, out VectorElementKey key)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");

                AdvanceTo(i);

                ReadOnlySpan<byte> keyBytes = new(currentPtr + 4, currentLen);

                key = new(NamespaceBytes, keyBytes);
            }

            /// <inheritdoc/>
            public readonly void GetInput(int i, out VectorInput input)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");

                input = default;
                input.CallbackContext = callbackContext;
                input.Callback = (nint)callback;
                input.Index = i;
            }

            /// <inheritdoc/>
            public readonly void GetOutput(int i, out VectorOutput output)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");

                // Don't care, won't be used
                Unsafe.SkipInit(out output);
            }

            /// <inheritdoc/>
            public readonly void SetOutput(int i, VectorOutput output)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");
            }

            /// <inheritdoc/>
            public void SetStatus(int i, Status status)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");

                hasPending |= status.IsPending;
            }

            internal readonly void CompletePending(ref VectorBasicContext objectContext)
            {
                if (hasPending)
                {
                    _ = objectContext.CompletePending(wait: true);
                }
            }
        }

        private unsafe delegate* unmanaged[Cdecl]<ulong, uint, nint, nuint, nint, nint, void> ReadCallbackPtr { get; } = &ReadCallbackUnmanaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, nint, nuint, byte> WriteCallbackPtr { get; } = &WriteCallbackUnmanaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, byte> DeleteCallbackPtr { get; } = &DeleteCallbackUnmanaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, nuint, nint, nint, byte> ReadModifyWriteCallbackPtr { get; } = &ReadModifyWriteCallbackUnmanaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, uint, byte> InlineFilterCallbackPtr { get; } = &FilterCallbackUnmanaged;

        /// <summary>
        /// Used to thread the active <see cref="StorageSession"/> across p/invoke and reverse p/invoke boundaries into DiskANN.
        /// 
        /// Not the most elegent option, but work so long as DiskANN remains single threaded.
        /// </summary>
        [ThreadStatic]
        internal static StorageSession ActiveThreadSession;

        /// <summary>
        /// Per-term initial disk-read sizes (in bytes) for the vector set currently being operated on, so each
        /// record is fetched in a single IO sized to its actual geometry. A field value of 0 means "not set" and
        /// the read falls back to the normal default.
        /// </summary>
        internal struct VectorReadGeometry
        {
            /// <summary>Initial disk-read size for the FullVector record (term 0).</summary>
            public int FullVectorIOSize;

            /// <summary>Initial disk-read size for the adjacency NeighborList record (term 1).</summary>
            public int NeighborListIOSize;

            /// <summary>Initial disk-read size for the QuantizedVector record (term 2).</summary>
            public int QuantizedVectorIOSize;
        }

        /// <summary>
        /// Per-term initial disk-read sizes for the vector set currently being operated on. Thread-static for the
        /// same reason as <see cref="ActiveThreadSession"/> (DiskANN runs single-threaded per operation): set on
        /// entry to a search/add once the index's dimensions / links are known (<see cref="SetActiveReadGeometry"/>)
        /// and reset to <c>default</c> when the index context is exited (<see cref="VectorSetLock.Dispose"/>) so a
        /// subsequent operation on a different set does not inherit stale sizes. Because the sizes are derived
        /// per-index, two vector sets with different dimensions or M get different (each optimal) sizes within the
        /// same Garnet instance.
        /// </summary>
        [ThreadStatic]
        internal static VectorReadGeometry ActiveReadGeometry;

        /// <summary>
        /// Destination for copying the small graph "stub" records (NeighborList adjacency, internal/external id
        /// maps, quantized vectors) back into memory when they are read from disk (see
        /// <see cref="VectorReadBatch.ReadCopyOptions"/>). Set from <see cref="GarnetServerOptions.EnableReadCache"/>
        /// at <see cref="VectorManager"/> construction: the read cache when it is enabled (the natural home for hot
        /// read-only data — separate, never flushed, LRU — so it doesn't pollute the writable main log), otherwise
        /// the main-log tail (still memory-resident). Per instance, so servers/databases with different read-cache
        /// settings in the same process do not clobber each other; reads reach it via
        /// <see cref="ActiveThreadSession"/>.<see cref="StorageSession.vectorManager"/>.
        /// </summary>
        internal readonly ReadCopyTo StubReadCopyTo;

        /// <summary>
        /// Per-record overhead (RecordInfo + key + length prefixes) added to the value size when computing the
        /// initial disk-read size, so the whole record lands in one IO. Generous; the read is sector-aligned downstream.
        /// </summary>
        private const int VectorRecordReadOverheadBytes = 64;

        /// <summary>
        /// Compute and stash the per-term initial disk-read sizes from the active vector set's geometry, so that
        /// <see cref="VectorReadBatch.InitialIORecordSize"/> can size each read to the record it is fetching.
        /// FullVector value = <paramref name="dimensions"/> * (full bytes-per-element for <paramref name="quantType"/>);
        /// QuantizedVector value = effective-dims * (quantized bits-per-element / 8); NeighborList value = <paramref name="numLinks"/> * sizeof(int).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void SetActiveReadGeometry(uint dimensions, uint numLinks, VectorQuantType quantType, uint reduceDims)
        {
            // The stored FullVector element size depends on the quantizer: the Redis quantizers (NoQuant/Bin/Q8)
            // store F32 (4 bytes/dim), while the extended X* quantizers store 1 byte/dim. See the format mapping in
            // VectorManager.TryGetEmbedding. Over-estimating only wastes bandwidth; under-estimating would force a
            // second IO, so this must match the actual stored size.
            var isBinaryQuant = quantType is VectorQuantType.Bin or VectorQuantType.XBin_I8 or VectorQuantType.XBin_U8;
            var fullVectorElementBytes = quantType is VectorQuantType.XNoQuant_U8 or VectorQuantType.XNoQuant_I8
                or VectorQuantType.XBin_I8 or VectorQuantType.XBin_U8
                ? 1
                : sizeof(float);

            // QuantizedVector reads (term 2, used for the approximate-distance pass on quantized sets) are sized to
            // the quantized width, which is much smaller than the full vector and differs by quantizer: the byte
            // quantizers (Q8) store 1 byte/dim, while the binary quantizers (Bin) pack 1 bit/dim. Quantization is
            // applied to the (optionally reduced) dimensions. Sizing per-quantizer avoids over-reading whole sectors
            // for the tiny binary records; the overhead covers any per-vector scale, and an under-read just self-
            // corrects with a second IO.
            var quantizedDims = reduceDims != 0 ? reduceDims : dimensions;
            var quantizedValueBytes = isBinaryQuant ? checked((int)quantizedDims + 7) / 8 : checked((int)quantizedDims);

            ActiveReadGeometry = new VectorReadGeometry
            {
                FullVectorIOSize = checked((int)dimensions * fullVectorElementBytes) + VectorRecordReadOverheadBytes,
                NeighborListIOSize = checked((int)numLinks * sizeof(int)) + VectorRecordReadOverheadBytes,
                QuantizedVectorIOSize = quantizedValueBytes + VectorRecordReadOverheadBytes,
            };
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe void ReadCallbackUnmanaged(
            ulong context,
            uint numKeys,
            nint keysData,
            nuint keysLength,
            nint dataCallback,
            nint dataCallbackContext
        )
        {
            // dataCallback takes: index, dataCallbackContext, data pointer, data length, and returns nothing

            Span<byte> nsBytes = stackalloc byte[sizeof(uint)];
            StoreContextInNamespace(context, ref nsBytes);

            var enumerable = new VectorReadBatch(dataCallback, dataCallbackContext, numKeys, PinnedSpanByte.FromPinnedPointer((byte*)keysData, (int)keysLength), nsBytes);

            ref var ctx = ref ActiveThreadSession.vectorBasicContext;

            ctx.ReadWithPrefetch(ref enumerable);

            enumerable.CompletePending(ref ctx);
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe byte WriteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength, nint writeData, nuint writeLength)
        {
            var keyWithNamespace = MakeVectorElementKey(context, keyData, keyLength);

            ref var ctx = ref ActiveThreadSession.vectorBasicContext;
            VectorInput input = new();
            input.AlignmentExpected = true;
            var valueSpan = SpanByte.FromPinnedPointer((byte*)writeData, (int)writeLength);
            VectorOutput outputSpan = new();

            var status = ctx.Upsert(keyWithNamespace, ref input, valueSpan, ref outputSpan);
            if (status.IsPending)
            {
                CompletePending(ref status, ref outputSpan, ref ctx);
            }

            return status.IsCompletedSuccessfully ? (byte)1 : default;
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static byte DeleteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength)
        {
            var keyWithNamespace = MakeVectorElementKey(context, keyData, keyLength);

            ref var ctx = ref ActiveThreadSession.vectorBasicContext;

            var status = ctx.Delete(keyWithNamespace);
            Debug.Assert(!status.IsPending, "Deletes should never go async");

            return status.IsCompletedSuccessfully && status.Found ? (byte)1 : default;
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static byte ReadModifyWriteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength, nuint writeLength, nint dataCallback, nint dataCallbackContext)
        {
            var keyWithNamespace = MakeVectorElementKey(context, keyData, keyLength);

            ref var ctx = ref ActiveThreadSession.vectorBasicContext;

            VectorInput input = default;
            input.Callback = dataCallback;
            input.CallbackContext = dataCallbackContext;
            input.WriteDesiredSize = (int)writeLength;

            var status = ctx.RMW(keyWithNamespace, ref input);
            if (status.IsPending)
            {
                VectorOutput ignored = new();

                CompletePending(ref status, ref ignored, ref ctx);
            }

            return status.IsCompletedSuccessfully ? (byte)1 : default;
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static byte FilterCallbackUnmanaged(ulong context, uint internalId)
        {
            return EvaluateCandidateFilter(context, internalId);
        }

        private static unsafe bool ReadSizeUnknown(ulong context, bool forceAlignment, ReadOnlySpan<byte> key, ref SpanByteAndMemory value)
        {
            Debug.Assert(context <= uint.MaxValue, "Contexts > 2^32-1 are not supported");

            Span<byte> nsBytes = stackalloc byte[sizeof(uint)];
            StoreContextInNamespace(context, ref nsBytes);

            VectorElementKey keyWithNamespace = new(nsBytes, key);

            ref var ctx = ref ActiveThreadSession.vectorBasicContext;

            while (true)
            {
                VectorInput input = new();
                input.ReadDesiredSize = -1;

                // Sometimes we read DiskANN written data from the .NET side
                // If that's the case, we need to pad for alignment even though .NET doesn't require it
                input.AlignmentExpected = forceAlignment;
                fixed (byte* ptr = value.Span)
                {
                    VectorOutput asSpanByte = new(ptr, value.Length);

                    var status = ctx.Read(keyWithNamespace, ref input, ref asSpanByte);
                    if (status.IsPending)
                    {
                        CompletePending(ref status, ref input, ref asSpanByte, ref ctx);
                    }

                    if (!status.Found)
                    {
                        value.Length = 0;
                        return false;
                    }

                    if (input.ReadDesiredSize > asSpanByte.SpanByteAndMemory.Length)
                    {
                        value.Memory?.Dispose();
                        var newAlloc = MemoryPool<byte>.Shared.Rent(input.ReadDesiredSize);
                        value = new(newAlloc, newAlloc.Memory.Length);
                        continue;
                    }

                    value.Length = asSpanByte.SpanByteAndMemory.Length;
                    return true;
                }
            }
        }

        /// <summary>
        /// Get a <see cref="SpanByte"/> which covers (keyData, keyLength), but has a namespace component based on <paramref name="context"/>.
        /// 
        /// Attempts to do this in place.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe VectorElementKey MakeVectorElementKey(ulong context, nint keyData, nuint keyLength)
        {
            // NOTE: DiskANN guarantees we have 4-bytes worth of unused data right before the key
            Span<byte> nsBytes = new(((byte*)keyData) - sizeof(uint), sizeof(uint));
            StoreContextInNamespace(context, ref nsBytes);

            ReadOnlySpan<byte> keyBytes = new((byte*)keyData, (int)keyLength);

            return new(nsBytes, keyBytes);
        }
    }
}