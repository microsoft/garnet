// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Methods which <see cref="DiskANNService"/> calls back into to interact with Garnet.
    /// </summary>
    public sealed partial class VectorManager
    {
        /// <summary>
        /// Per-record overhead (RecordInfo + key + length prefixes) added to the value size when computing the
        /// initial disk-read size, so the whole record lands in one IO. Generous; the read is sector-aligned downstream.
        /// </summary>
        private const int VectorRecordReadOverheadBytes = 64;

        public unsafe
#if NET9_0_OR_GREATER
            ref
#endif
            struct VectorReadBatch : IReadArgBatch<VectorElementKey, VectorInput, VectorOutput>
        {
            /// <summary>
            /// Total number of keys in batch.
            /// </summary>
            public int Count { get; }

            public readonly ReadOnlySpan<PinnedSpanByte> Parameters
                => default;

            /// <inheritdoc/>
            public readonly int InitialIORecordSize
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get;
            }

            /// <summary>
            /// Per-term read-copy policy. Records reused across hops and queries during traversal and rerank —
            /// NeighborList adjacency, QuantizedVector, the internal/external id maps, and the Metadata term — are
            /// copied back into memory on a disk read (to <see cref="stubReadCopyTo"/>: the read cache when enabled,
            /// else the main-log tail) so later reads serve them from memory, bounding disk traffic to the working
            /// set. The raw FullVector and Attributes are served from disk (CopyTo=None): the FullVector is read once
            /// per rerank candidate and is large, so for no-quant sets caching it yields no net gain once the working
            /// set exceeds memory.
            /// </summary>
            public readonly ReadCopyOptions ReadCopyOptions
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get;
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

            public VectorReadBatch(nint callback, nint callbackContext, uint keyCount, PinnedSpanByte lengthPrefixedKeys, ReadOnlySpan<byte> namespaceBytes, ReadCopyOptions readOpts, int initialRecordSizeHint)
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

                ReadCopyOptions = readOpts;
                InitialIORecordSize = initialRecordSizeHint;
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
                Debug.Assert((keyBytes.Length % 4) == 0, "Unaligned key provided by DiskANN");

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

        private unsafe delegate* unmanaged[Cdecl]<ulong, uint, uint, nint, nuint, nint, nint, void> ReadCallbackPtr { get; } = &ReadCallbackUnmanaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, nint, nuint, byte> WriteCallbackPtr { get; } = &WriteCallbackUnmanaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, byte> DeleteCallbackPtr { get; } = &DeleteCallbackUnmanaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, nuint, nint, nint, byte> ReadModifyWriteCallbackPtr { get; } = &ReadModifyWriteCallbackUnmanaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, byte> FilterCallbackPtr { get; } = &FilterCallbackUnmanaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, void> LogCallbackPtr { get; } = &LogCallbackUnmanaged;

        /// <summary>
        /// Used to thread the active <see cref="StorageSession"/> across p/invoke and reverse p/invoke boundaries into DiskANN.
        /// 
        /// Not the most elegent option, but work so long as DiskANN remains single threaded.
        /// </summary>
        [ThreadStatic]
        internal static StorageSession ActiveThreadSession;

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
        private readonly ReadCopyTo stubReadCopyTo;

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe void LogCallbackUnmanaged(ulong context, nint logMessage, nuint logMessageLength)
        {
            const int MaxVectorSetLogNameLength = 64;

            if (ActiveThreadSession == null)
            {
                // Can't do anything here
                return;
            }

            var msgUtf8Raw = new ReadOnlySpan<byte>((byte*)logMessage, (int)logMessageLength);
            var msg = Encoding.UTF8.GetString(msgUtf8Raw);

            var contextNoNs = context & ~(ContextStep - 1);
            var nsBits = context & (ContextStep - 1);

            var ns =
                nsBits switch
                {
                    DiskANNService.Attributes => nameof(DiskANNService.Attributes),
                    DiskANNService.ExternalIdMap => nameof(DiskANNService.ExternalIdMap),
                    DiskANNService.FullVector => nameof(DiskANNService.FullVector),
                    DiskANNService.InternalIdMap => nameof(DiskANNService.InternalIdMap),
                    DiskANNService.NeighborList => nameof(DiskANNService.NeighborList),
                    DiskANNService.QuantizedVector => nameof(DiskANNService.QuantizedVector),
                    _ => $"!!UNKNOWN ({nsBits})!!",
                };

            string vectorSet;
            int args;
            if (ActiveThreadSession.parseState.Count > 0)
            {
                args = ActiveThreadSession.parseState.Count;
                var probablyVectorSet = ActiveThreadSession.parseState.GetArgSliceByRef(0).ReadOnlySpan;

                if (probablyVectorSet.Length > MaxVectorSetLogNameLength)
                {
                    vectorSet = $"(Escaped for length ({probablyVectorSet.Length}): {SpanByte.ToShortString(probablyVectorSet, MaxVectorSetLogNameLength)})";
                }
                else
                {
                    try
                    {
                        vectorSet = Encoding.UTF8.GetString(probablyVectorSet);
                    }
                    catch
                    {
                        vectorSet = $"(Escaped non-utf8: {SpanByte.ToShortString(probablyVectorSet, MaxVectorSetLogNameLength)})";
                    }
                }
            }
            else
            {
                vectorSet = "";
                args = 0;
            }

            // TODO: It'd be nice to get the command in here as well
            ActiveThreadSession.vectorManager.logger?.LogWarning("DiskANN Log Message={msg}, Context={contextNoNs}, Namespace={ns}, VectorSet={vectorSet}, CommandArgsCount={args}", msg, contextNoNs, ns, vectorSet, args);
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe void ReadCallbackUnmanaged(
            ulong context,
            uint numKeys,
            uint valueLengthHint,
            nint keysData,
            nuint keysLength,
            nint dataCallback,
            nint dataCallbackContext
        )
        {
            // dataCallback takes: index, dataCallbackContext, data pointer, data length, and returns nothing

            Span<byte> nsBytes = stackalloc byte[sizeof(uint)];
            StoreContextInNamespace(context, ref nsBytes);

            // Calculate optimal read options for this batch
            var readCopyOptions =
                (context & (ContextStep - 1)) switch
                {
                    DiskANNService.NeighborList or DiskANNService.QuantizedVector or DiskANNService.InternalIdMap or DiskANNService.ExternalIdMap or DiskANNService.Metadata => new ReadCopyOptions { CopyFrom = ReadCopyFrom.AllImmutable, CopyTo = ActiveThreadSession.vectorManager.stubReadCopyTo },
                    _ => new ReadCopyOptions { CopyFrom = ReadCopyFrom.None, CopyTo = ReadCopyTo.None },
                };

            var valueLengthHintWithOverhead = valueLengthHint + VectorRecordReadOverheadBytes;

            var enumerable = new VectorReadBatch(dataCallback, dataCallbackContext, numKeys, PinnedSpanByte.FromPinnedPointer((byte*)keysData, (int)keysLength), nsBytes, readCopyOptions, (int)valueLengthHint);

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
            Debug.Assert((keyLength % 4) == 0, "Unaligned key provided by DiskANN");

            var keyWithNamespace = MakeVectorElementKey(context, keyData, keyLength);

            ref var ctx = ref ActiveThreadSession.vectorBasicContext;

            var status = ctx.Delete(keyWithNamespace);
            Debug.Assert(!status.IsPending, "Deletes should never go async");

            return status.IsCompletedSuccessfully && status.Found ? (byte)1 : default;
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static byte ReadModifyWriteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength, nuint writeLength, nint dataCallback, nint dataCallbackContext)
        {
            Debug.Assert((keyLength % 4) == 0, "Unaligned key provided by DiskANN");

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
        private static unsafe byte FilterCallbackUnmanaged(ulong context, nint valueData, nuint valueLength)
        {
            return EvaluateCandidateFilter(context, new ReadOnlySpan<byte>((byte*)valueData, (int)valueLength));
        }

        private static unsafe bool ReadSizeUnknown(ulong context, ReadOnlySpan<byte> key, ref SpanByteAndMemory value)
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