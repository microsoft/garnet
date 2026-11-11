// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Numerics;
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
        /// Delegate type for calls to <see cref="FilterCallbackManaged(ulong, nint, nuint)"/>.
        /// 
        /// Declares Cdecl calling convention for function pointer compatibility.
        /// </summary>
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate byte FilterCallbackDelegate(ulong context, nint attributeData, nuint attributeLength);

        /// <summary>
        /// Per-record overhead (RecordInfo + key + length prefixes) added to the value size when computing the
        /// initial disk-read size, so the whole record lands in one IO. Generous; the read is sector-aligned downstream.
        /// </summary>
        private const int VectorRecordReadOverheadBytes = 64;

        /// <summary>
        /// <see cref="IReadArgBatch{TKey, TInput, TOutput}"/> implementation for DiskANN batch reads.
        /// </summary>
        public unsafe ref struct VectorReadBatch : IReadArgBatch<VectorElementKey, VectorInput, VectorOutput>
        {
            /// <summary>
            /// Total number of keys in batch.
            /// </summary>
            public int Count { get; }

            /// <inheritdoc/>
            public readonly ReadOnlySpan<PinnedSpanByte> Parameters => default;

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
                    return namespaceBytes;
                }
            }

            private readonly ReadOnlySpan<byte> namespaceBytes;
            private readonly PinnedSpanByte lengthPrefixedKeys;

            public readonly delegate* unmanaged[Cdecl, SuppressGCTransition]<int, nint, nint, nuint, void> callback;
            public readonly nint callbackContext;

            private int currentIndex;

            private int currentLen;
            private byte* currentPtr;

            private bool hasPending;

            public VectorReadBatch(nint callback, nint callbackContext, uint keyCount, PinnedSpanByte lengthPrefixedKeys, ReadOnlySpan<byte> namespaceBytes, ReadCopyOptions readOpts, int initialRecordSizeHint)
            {
                this.namespaceBytes = namespaceBytes;
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
                key = new(NamespaceBytes, keyBytes);

                AssertKeyAlignment(in key);

                [Conditional("DEBUG")]
                static void AssertKeyAlignment(in VectorElementKey key)
                {
                    ulong context;
                    if (key.NamespaceBytes.Length == 1)
                    {
                        context = key.NamespaceBytes[0];
                    }
                    else if (key.NamespaceBytes.Length == 4)
                    {
                        context = BinaryPrimitives.ReadUInt32LittleEndian(key.NamespaceBytes);
                    }
                    else
                    {
                        Debug.Fail("Unexpected namespace length");
                        context = 0;
                    }

                    var keyLength = key.KeyBytes.Length;
                    Debug.Assert(((context & (ContextStep - 1)) is DiskANNService.InternalIdMap or DiskANNService.Attributes) || (keyLength % 4) == 0, "Unaligned key provided by DiskANN");
                }
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

        /// <summary>
        /// <see cref="IReadArgBatch{TKey, TInput, TOutput}"/> implementation for DiskANN batch reads, but setup to track following the shallow copy context chain.
        /// </summary>
        public unsafe ref struct VectorReadBatchWithContextChain : IReadArgBatch<VectorElementKey, VectorInput, VectorOutput>
        {
            /// <summary>
            /// Total number of keys in batch.
            /// </summary>
            public int Count { get; private set; }

            /// <inheritdoc/>
            public readonly ReadOnlySpan<PinnedSpanByte> Parameters => default;

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
                    return namespaceBytes;
                }
            }

            private readonly ReadOnlySpan<byte> namespaceBytes;
            private readonly PinnedSpanByte lengthPrefixedKeys;

            private readonly Span<ulong> foundBitmap;
            private readonly Span<ulong> pendingBitmapUpdates;

            public readonly delegate* unmanaged[Cdecl, SuppressGCTransition]<int, nint, nint, nuint, void> callback;
            public readonly nint callbackContext;

            /// <summary>
            /// Index into keys that <see cref="Count"/> and other <see cref="IReadArgBatch{TKey, TInput, TOutput}"/> methods take and return.
            /// 
            /// This is adjusted based on <see cref="foundBitmap"/> so we don't query keys previously found with this enumerable and an early namespace.
            /// </summary>
            private int publicCurrentIndex;

            /// <summary>
            /// Index in <see cref="lengthPrefixedKeys"/> stored keys.
            /// 
            /// This differs from <see cref="publicCurrentIndex"/> because it ignored <see cref="foundBitmap"/>.
            /// </summary>
            private int internalCurrentIndex;

            private readonly uint internalKeyCount;

            private byte* currentPtr;
            private int currentLen;

            private bool hasPending;

            public VectorReadBatchWithContextChain(nint callback, nint callbackContext, Span<ulong> foundBitmap, Span<ulong> bitmapScratch, uint keyCount, PinnedSpanByte lengthPrefixedKeys, ReadOnlySpan<byte> namespaceBytes, ReadCopyOptions readOpts, int initialRecordSizeHint)
            {
                Debug.Assert(bitmapScratch.Length >= foundBitmap.Length, "Scratch must be at least as large as tracking bitmap");

                this.namespaceBytes = namespaceBytes;
                this.lengthPrefixedKeys = lengthPrefixedKeys;
                this.foundBitmap = foundBitmap;
                pendingBitmapUpdates = bitmapScratch;

                this.foundBitmap.Clear();
                pendingBitmapUpdates[..this.foundBitmap.Length].Clear();

                this.callback = (delegate* unmanaged[Cdecl, SuppressGCTransition]<int, nint, nint, nuint, void>)callback;
                this.callbackContext = callbackContext;

                ReadCopyOptions = readOpts;
                InitialIORecordSize = initialRecordSizeHint;

                Count = (int)keyCount;
                internalKeyCount = keyCount;

                currentPtr = lengthPrefixedKeys.ptr;
                currentLen = currentPtr != null ? *(int*)currentPtr : 0;
            }

            /// <summary>
            /// Reset to start of internal keys, after adjusting to skip any keys already found.
            /// 
            /// Returns true if all keys have been found, and thus this enumerable is unusable.
            /// Returns false if there's at least one remaining key.
            /// </summary>
            public bool ResetAndAdjustCountForFound()
            {
                Debug.Assert(!hasPending, "Shouldn't reset before all pending operations complete");

                // Union pending changes into effective bitmap
                for (var i = 0; i < foundBitmap.Length; i++)
                {
                    foundBitmap[i] |= pendingBitmapUpdates[i];
                }

                pendingBitmapUpdates[..foundBitmap.Length].Clear();

                // Adjust Count for previously found keys
                Count = (int)internalKeyCount;
                for (var i = 0; i < foundBitmap.Length; i++)
                {
                    Count -= BitOperations.PopCount(foundBitmap[i]);
                }

                if (Count == 0)
                {
                    return true;
                }

                // Force a reset
                publicCurrentIndex = int.MaxValue;
                AdvanceTo(0);

                return false;
            }

            /// <summary>
            /// Mark a given public index as found  - does not take effect until next <see cref="ResetAndAdjustCountForFound"/> call.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void MarkPublicFound(int publicIndex)
            {
                AdvanceTo(publicIndex);

                MarkInternalFound(internalCurrentIndex);
            }

            /// <summary>
            /// Mark a given internal index as found - does not take effect until next <see cref="ResetAndAdjustCountForFound"/> call.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private readonly void MarkInternalFound(int internalIndex)
            {
                Debug.Assert(!PreviouslyFound(internalIndex), "Shouldn't be marking found twice");

                pendingBitmapUpdates[internalIndex / 64] |= 1UL << (internalIndex % 64);
            }

            /// <summary>
            /// Returns true if the key at the given internal index has previously been found, provided <see cref="ResetAndAdjustCountForFound"/> was called after that update.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private readonly bool PreviouslyFound(int internalIndex)
            {
                Debug.Assert(internalIndex >= 0 && internalIndex < internalKeyCount, "Shouldn't check for indexes beyond internal key count");

                return (foundBitmap[internalIndex / 64] & (1UL << (internalIndex % 64))) != 0;
            }

            /// <summary>
            /// Move <see cref="publicCurrentIndex"/>, <see cref="internalCurrentIndex"/>, <see cref="currentPtr"/> and <see cref="currentLen"/> to
            /// a given key indicated by <paramref name="toPublicIndex"/>.
            /// 
            /// Optimized for scanning forward, random access will be slower.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void AdvanceTo(int toPublicIndex)
            {
                if (toPublicIndex == publicCurrentIndex)
                {
                    return;
                }
                else if (toPublicIndex < publicCurrentIndex)
                {
                    // Start over from start
                    currentPtr = lengthPrefixedKeys.ToPointer();
                    currentLen = *(int*)currentPtr;
                    publicCurrentIndex = 0;
                    internalCurrentIndex = 0;

                    // Skip to first not yet found internal index
                    while (PreviouslyFound(internalCurrentIndex))
                    {
                        internalCurrentIndex++;
                        currentPtr += currentLen + sizeof(int); // Skip length prefix too

                        Debug.Assert(currentPtr < lengthPrefixedKeys.ToPointer() + lengthPrefixedKeys.Length, "About to access out of bounds data");

                        currentLen = *(int*)currentPtr;
                    }
                }

                // Scan forward until public index matches target
                while (publicCurrentIndex != toPublicIndex)
                {
                    // Advance one internal key
                    internalCurrentIndex++;
                    currentPtr += currentLen + sizeof(int); // Skip length prefix too

                    Debug.Assert(currentPtr < lengthPrefixedKeys.ToPointer() + lengthPrefixedKeys.Length, "About to access out of bounds data");

                    currentLen = *(int*)currentPtr;

                    // Keep skipping if we've already found this key
                    if (PreviouslyFound(internalCurrentIndex))
                    {
                        continue;
                    }

                    publicCurrentIndex++;
                }
            }

            /// <inheritdoc/>
            public void GetKey(int publicIndex, out VectorElementKey key)
            {
                Debug.Assert(publicIndex >= 0 && publicIndex < Count, "Trying to advance out of bounds");

                AdvanceTo(publicIndex);

                ReadOnlySpan<byte> keyBytes = new(currentPtr + 4, currentLen);
                key = new(NamespaceBytes, keyBytes);

                AssertKeyAlignment(in key);

                [Conditional("DEBUG")]
                static void AssertKeyAlignment(in VectorElementKey key)
                {
                    ulong context;
                    if (key.NamespaceBytes.Length == 1)
                    {
                        context = key.NamespaceBytes[0];
                    }
                    else if (key.NamespaceBytes.Length == 4)
                    {
                        context = BinaryPrimitives.ReadUInt32LittleEndian(key.NamespaceBytes);
                    }
                    else
                    {
                        Debug.Fail("Unexpected namespace length");
                        context = 0;
                    }

                    var keyLength = key.KeyBytes.Length;
                    Debug.Assert(((context & (ContextStep - 1)) is DiskANNService.InternalIdMap or DiskANNService.Attributes) || (keyLength % 4) == 0, "Unaligned key provided by DiskANN");
                }
            }

            /// <inheritdoc/>
            public void GetInput(int publicIndex, out VectorInput input)
            {
                Debug.Assert(publicIndex >= 0 && publicIndex < Count, "Trying to advance out of bounds");

                AdvanceTo(publicIndex);

                input = default;
                input.CallbackContext = callbackContext;
                input.Callback = (nint)callback;

                // Internal index, because that's the index DiskANN is expecting
                input.Index = internalCurrentIndex;
            }

            /// <inheritdoc/>
            public readonly void GetOutput(int publicIndex, out VectorOutput output)
            {
                Debug.Assert(publicIndex >= 0 && publicIndex < Count, "Trying to advance out of bounds");

                // Don't care, won't be used
                Unsafe.SkipInit(out output);
            }

            /// <inheritdoc/>
            public readonly void SetOutput(int publicIndex, VectorOutput output)
            {
                Debug.Assert(publicIndex >= 0 && publicIndex < Count, "Trying to advance out of bounds");
            }

            /// <inheritdoc/>
            public void SetStatus(int publicIndex, Status status)
            {
                Debug.Assert(publicIndex >= 0 && publicIndex < Count, "Trying to advance out of bounds");

                if (status.Found)
                {
                    MarkPublicFound(publicIndex);
                }

                hasPending |= status.IsPending;
            }

            /// <summary>
            /// Complete any pending operations that occurred during batch reads.
            /// </summary>
            internal void CompletePending(ref VectorBasicContext objectContext)
            {
                if (hasPending)
                {
                    var res = objectContext.CompletePendingWithOutputs(out var outputs, wait: true);
                    Debug.Assert(res, "Should have completed pending");

                    using (outputs)
                    {
                        while (outputs.Next())
                        {
                            var status = outputs.Current.Status;
                            if (status.Found)
                            {
                                // Index provided as part of VectorInput is the internal index
                                MarkInternalFound(outputs.Current.Input.Index);
                            }
                        }
                    }

                    hasPending = false;
                }
            }
        }

        private unsafe delegate* unmanaged[Cdecl]<ulong, uint, uint, nint, nuint, nint, nint, void> ReadCallbackPtr { get; } = &ReadCallbackUnmanaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, nint, nuint, byte> WriteCallbackPtr { get; } = &WriteCallbackUnmanaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, byte> DeleteCallbackPtr { get; } = &DeleteCallbackUnmanaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, nuint, nint, nint, byte> ReadModifyWriteCallbackPtr { get; } = &ReadModifyWriteCallbackUnmanaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, void> LogCallbackPtr { get; } = &LogCallbackUnmanaged;

        private static readonly FilterCallbackDelegate FilterCallbackObject = FilterCallbackManaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, byte> FilterCallbackPtr { get; } = (delegate* unmanaged[Cdecl]<ulong, nint, nuint, byte>)Marshal.GetFunctionPointerForDelegate(FilterCallbackObject);

        /// <summary>
        /// Used to thread the active <see cref="StorageSession"/> across p/invoke and reverse p/invoke boundaries into DiskANN.
        /// 
        /// Not the most elegent option, but work so long as DiskANN remains single threaded.
        /// </summary>
        [ThreadStatic]
        internal static StorageSession ActiveThreadSession;

        /// <summary>
        /// Used to thread the chain of backreferences from shallow copies across p/invoke and reverse p/invoke boundaries.
        /// 
        /// Tracks <see cref="ActiveThreadSession"/> closely, but unlike that it is not always set as not all Vector Sets
        /// start as shallow copies.
        /// </summary>
        [ThreadStatic]
        internal static ReadOnlyMemory<ulong> ActiveThreadShallowCopyBackReferences;

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

            if (ActiveThreadShallowCopyBackReferences.IsEmpty)
            {
                // Fast path when we're never going have to walk the shallow copy context chain
                var enumerable = new VectorReadBatch(dataCallback, dataCallbackContext, numKeys, PinnedSpanByte.FromPinnedPointer((byte*)keysData, (int)keysLength), nsBytes, readCopyOptions, (int)valueLengthHintWithOverhead);

                ref var ctx = ref ActiveThreadSession.vectorBasicContext;

                ctx.ReadWithPrefetch(ref enumerable);

                enumerable.CompletePending(ref ctx);
            }
            else
            {
                // Repeat the batch read until we exhaust the COW context chain or find all keys
                Span<ulong> notFoundBitmap = stackalloc ulong[(int)(keysLength / 64) + (keysLength % 64 == 0 ? 0 : 1)];
                Span<ulong> scratchBitmap = stackalloc ulong[notFoundBitmap.Length];

                var enumerable = new VectorReadBatchWithContextChain(dataCallback, dataCallbackContext, notFoundBitmap, scratchBitmap, numKeys, PinnedSpanByte.FromPinnedPointer((byte*)keysData, (int)keysLength), nsBytes, readCopyOptions, (int)valueLengthHintWithOverhead);

                ref var ctx = ref ActiveThreadSession.vectorBasicContext;
                var remainingContexts = ActiveThreadShallowCopyBackReferences.Span;

                while (true)
                {
                    ctx.ReadWithPrefetch(ref enumerable);

                    enumerable.CompletePending(ref ctx);

                    if (enumerable.ResetAndAdjustCountForFound() || remainingContexts.IsEmpty)
                    {
                        // Break if we got all hits OR we ran out of the COW chain
                        break;
                    }

                    // Move to next context in the COW
                    var nextRootContext = remainingContexts[0];
                    remainingContexts = remainingContexts[1..];

                    // Copy the same namespace bits in
                    var nextContext = nextRootContext | (context & (ContextStep - 1));

                    // Update namespace for the enumerable
                    StoreContextInNamespace(nextContext, ref nsBytes);
                }
            }
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe byte WriteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength, nint writeData, nuint writeLength)
        {
            Debug.Assert(((context & (ContextStep - 1)) is DiskANNService.InternalIdMap or DiskANNService.Attributes) || (keyLength % 4) == 0, "Unaligned key provided by DiskANN");

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

            RestoreKeyLengthPrefix(keyData, keyLength);

            if (status.IsCompletedSuccessfully)
            {
                return 1;
            }
            else
            {
                return 0;
            }
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static byte DeleteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength)
        {
            Debug.Assert(((context & (ContextStep - 1)) is DiskANNService.InternalIdMap or DiskANNService.Attributes) || (keyLength % 4) == 0, "Unaligned key provided by DiskANN");

            var keyWithNamespace = MakeVectorElementKey(context, keyData, keyLength);

            ref var ctx = ref ActiveThreadSession.vectorBasicContext;

            var status = ctx.Delete(keyWithNamespace);
            Debug.Assert(!status.IsPending, "Deletes should never go async");

            RestoreKeyLengthPrefix(keyData, keyLength);

            if (status.IsCompletedSuccessfully && status.Found)
            {
                return 1;
            }
            else
            {
                return 0;
            }
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static byte ReadModifyWriteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength, nuint writeLength, nint dataCallback, nint dataCallbackContext)
        {
            const int MaxCopyOnStackBytes = 1_024;

            Debug.Assert(((context & (ContextStep - 1)) is DiskANNService.InternalIdMap or DiskANNService.Attributes) || (keyLength % 4) == 0, "Unaligned key provided by DiskANN");

            var keyWithNamespace = MakeVectorElementKey(context, keyData, keyLength);

            ref var ctx = ref ActiveThreadSession.vectorBasicContext;

            VectorInput input = default;
            input.Callback = dataCallback;
            input.CallbackContext = dataCallbackContext;
            input.WriteDesiredSize = (int)writeLength;

            // If we're modifying a shallow copy, the RMW should _fail_ to create a new record until we consult the (already immutable) chain of contexts
            input.CopyOnWrite = !ActiveThreadShallowCopyBackReferences.IsEmpty ? VectorInput.CopyOnWriteOperation.DisallowCreate : default;

        retryInitialRMW:
            var status = ctx.RMW(keyWithNamespace, ref input);
            if (status.IsPending)
            {
                VectorOutput ignored = new();

                CompletePending(ref status, ref ignored, ref ctx);
            }

            // If we cancelled the RMW because of being a shallow copy, look through older contexts for the record (if any) to copy
            if (!ActiveThreadShallowCopyBackReferences.IsEmpty && status.IsCanceled)
            {
                Span<byte> copyDataSpan = stackalloc byte[MaxCopyOnStackBytes];
                var remainingContexts = ActiveThreadShallowCopyBackReferences.Span;

                while (!remainingContexts.IsEmpty)
                {
                    // Attempt to copy the value from a prior context
                    var effectiveNamespace = remainingContexts[0] | (context & (ContextStep - 1));
                    var copyFromKeyWithNamespace = MakeVectorElementKey(effectiveNamespace, keyData, keyLength);

                    VectorInput copyReadInput = default;
                    copyReadInput.ReadDesiredSize = (int)writeLength;

                    VectorOutput copyOutput = new(SpanByteAndMemory.FromPinnedSpan(copyDataSpan));
                    var copyReadRes = ctx.Read(copyFromKeyWithNamespace, ref copyReadInput, ref copyOutput);
                    if (copyReadRes.IsPending)
                    {
                        CompletePending(ref status, ref copyOutput, ref ctx);
                    }

                    if (copyReadRes.Found)
                    {
                        // If we found a record, we need to copy it _BUT_ we cannot overwrite any other record
                        // since we might be racing other redirected RMWs or Upserts

                        VectorInput copyWriteInput = default;
                        copyWriteInput.CopyOnWrite = VectorInput.CopyOnWriteOperation.DisallowOverwrite;

                        VectorOutput ignored = default;

                        var copyWriteRes = ctx.RMW(keyWithNamespace, ref copyWriteInput, ref copyOutput);
                        if (copyWriteRes.IsPending)
                        {
                            CompletePending(ref copyWriteRes, ref ignored, ref ctx);
                        }

                        copyOutput.SpanByteAndMemory.Dispose();

                        if (copyWriteRes.IsCompletedSuccessfully || copyWriteRes.IsCanceled)
                        {
                            // If we did the copy OR conflicted with another update, retry but with no COW chaining
                            input.CopyOnWrite = default;
                            goto retryInitialRMW;
                        }

                        // If something else went wrong, fail the RMW
                        RestoreKeyLengthPrefix(keyData, keyLength);
                        return 0;
                    }

                    // Didn't find anything to copy, move on to next context
                    remainingContexts = remainingContexts[1..];
                }

                // Found nothing to copy, redo the RMW with no COW chaining
                input.CopyOnWrite = default;
                goto retryInitialRMW;
            }

            RestoreKeyLengthPrefix(keyData, keyLength);

            if (status.IsCompletedSuccessfully)
            {
                return 1;
            }
            else
            {
                return 0;
            }
        }

        private static unsafe byte FilterCallbackManaged(ulong context, nint valueData, nuint valueLength)
        {
            // Called from Reader methods indirectly, so must support managed callers
            return EvaluateCandidateFilter(context, new ReadOnlySpan<byte>((byte*)valueData, (int)valueLength));
        }

        private static unsafe bool ReadSizeUnknown(ulong context, ReadOnlySpan<byte> key, ref SpanByteAndMemory value)
        {
            Debug.Assert(context <= uint.MaxValue, "Contexts > 2^32-1 are not supported");

            Span<byte> nsBytesRaw = stackalloc byte[sizeof(uint)];
            var nsBytes = nsBytesRaw;
            StoreContextInNamespace(context, ref nsBytes);

            VectorElementKey keyWithNamespace = new(nsBytes, key);

            ref var ctx = ref ActiveThreadSession.vectorBasicContext;

            var remainingContexts = ActiveThreadShallowCopyBackReferences.Span;

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
                        // If we have other contexts to check, walk through them before giving up
                        if (!remainingContexts.IsEmpty)
                        {
                            context = remainingContexts[0];
                            remainingContexts = remainingContexts[1..];
                            nsBytes = nsBytesRaw;
                            StoreContextInNamespace(context, ref nsBytes);
                            keyWithNamespace = new(nsBytes, key);

                            input.ReadDesiredSize = -1;

                            continue;
                        }

                        value.Length = 0;
                        return false;
                    }

                    // We got a hit, but we didn't allocate a large enough buffer - try again with the correct size
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

        /// <summary>
        /// Undoes clobbering performed by <see cref="MakeVectorElementKey(ulong, nint, nuint)"/>.
        /// 
        /// DiskANN requires we leave any passed data how we found it.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void RestoreKeyLengthPrefix(nint keyData, nuint keyLength)
        {
            // DiskANN gives us these 4-bytes before every key
            Span<byte> nsBytes = new(((byte*)keyData) - sizeof(uint), sizeof(uint));

            BinaryPrimitives.WriteInt32LittleEndian(nsBytes, (int)keyLength);
        }
    }
}