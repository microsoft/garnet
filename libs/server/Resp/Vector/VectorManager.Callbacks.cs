// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    /// <summary>
    /// Methods which <see cref="DiskANNService"/> calls back into to interact with Garnet.
    /// </summary>
    public sealed partial class VectorManager
    {
        public unsafe struct VectorReadBatch : IReadArgBatch<SpanByte, VectorInput, SpanByte>
        {
            public int Count { get; }

            private readonly ulong context;
            private readonly SpanByte lengthPrefixedKeys;

            public readonly unsafe delegate* unmanaged[Cdecl, SuppressGCTransition]<int, nint, nint, nuint, void> callback;
            public readonly nint callbackContext;

            private int currentIndex;

            private int currentLen;
            private byte* currentPtr;

            private bool hasPending;

            public VectorReadBatch(nint callback, nint callbackContext, ulong context, uint keyCount, SpanByte lengthPrefixedKeys)
            {
                this.context = context;
                this.lengthPrefixedKeys = lengthPrefixedKeys;

                this.callback = (delegate* unmanaged[Cdecl, SuppressGCTransition]<int, nint, nint, nuint, void>)callback;
                this.callbackContext = callbackContext;

                currentIndex = 0;
                Count = (int)keyCount;

                currentPtr = this.lengthPrefixedKeys.ToPointerWithMetadata();
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

                // Undo namespace mutation
                *(int*)currentPtr = currentLen;

                // Most likely case, we're going one forward
                if (i == (currentIndex + 1))
                {
                    currentPtr += currentLen + sizeof(int); // Skip length prefix too

                    Debug.Assert(currentPtr < lengthPrefixedKeys.ToPointerWithMetadata() + lengthPrefixedKeys.Length, "About to access out of bounds data");

                    currentLen = *currentPtr;

                    currentIndex = i;

                    return;
                }

                // Next most likely case, we're going back to the start
                currentPtr = lengthPrefixedKeys.ToPointerWithMetadata();
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
            public void GetKey(int i, out SpanByte key)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");

                AdvanceTo(i);

                key = SpanByte.FromPinnedPointer(currentPtr + 3, currentLen + 1);
                key.MarkNamespace();
                key.SetNamespaceInPayload((byte)context);
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
            public readonly void GetOutput(int i, out SpanByte output)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");

                // Don't care, won't be used
                Unsafe.SkipInit(out output);
            }

            /// <inheritdoc/>
            public readonly void SetOutput(int i, SpanByte output)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");
            }

            /// <inheritdoc/>
            public void SetStatus(int i, Status status)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");

                hasPending |= status.IsPending;
            }

            internal readonly void CompletePending<TContext>(ref TContext objectContext)
                where TContext : ITsavoriteContext<SpanByte, SpanByte, VectorInput, SpanByte, long, VectorSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            {
                // Undo mutations
                *(int*)currentPtr = currentLen;

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

        /// <summary>
        /// Used to thread the active <see cref="StorageSession"/> across p/invoke and reverse p/invoke boundaries into DiskANN.
        /// 
        /// Not the most elegent option, but work so long as DiskANN remains single threaded.
        /// </summary>
        [ThreadStatic]
        internal static StorageSession ActiveThreadSession;

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

            var enumerable = new VectorReadBatch(dataCallback, dataCallbackContext, context, numKeys, SpanByte.FromPinnedPointer((byte*)keysData, (int)keysLength));

            ref var ctx = ref ActiveThreadSession.vectorContext;

            ctx.ReadWithPrefetch(ref enumerable);

            enumerable.CompletePending(ref ctx);
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe byte WriteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength, nint writeData, nuint writeLength)
        {
            var keyWithNamespace = MarkDiskANNKeyWithNamespace(context, keyData, keyLength);

            ref var ctx = ref ActiveThreadSession.vectorContext;
            VectorInput input = default;
            var valueSpan = SpanByte.FromPinnedPointer((byte*)writeData, (int)writeLength);
            SpanByte outputSpan = default;

            var status = ctx.Upsert(ref keyWithNamespace, ref input, ref valueSpan, ref outputSpan);
            if (status.IsPending)
            {
                CompletePending(ref status, ref outputSpan, ref ctx);
            }

            return status.IsCompletedSuccessfully ? (byte)1 : default;
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe byte DeleteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength)
        {
            var keyWithNamespace = MarkDiskANNKeyWithNamespace(context, keyData, keyLength);

            ref var ctx = ref ActiveThreadSession.vectorContext;

            var status = ctx.Delete(ref keyWithNamespace);
            Debug.Assert(!status.IsPending, "Deletes should never go async");

            return status.IsCompletedSuccessfully && status.Found ? (byte)1 : default;
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe byte ReadModifyWriteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength, nuint writeLength, nint dataCallback, nint dataCallbackContext)
        {
            var keyWithNamespace = MarkDiskANNKeyWithNamespace(context, keyData, keyLength);

            ref var ctx = ref ActiveThreadSession.vectorContext;

            VectorInput input = default;
            input.Callback = dataCallback;
            input.CallbackContext = dataCallbackContext;
            input.WriteDesiredSize = (int)writeLength;

            var status = ctx.RMW(ref keyWithNamespace, ref input);
            if (status.IsPending)
            {
                SpanByte ignored = default;

                CompletePending(ref status, ref ignored, ref ctx);
            }

            return status.IsCompletedSuccessfully ? (byte)1 : default;
        }

        private static unsafe bool ReadSizeUnknown(ulong context, ReadOnlySpan<byte> key, ref SpanByteAndMemory value)
        {
            Span<byte> distinctKey = stackalloc byte[key.Length + 1];
            var keyWithNamespace = SpanByte.FromPinnedSpan(distinctKey);
            keyWithNamespace.MarkNamespace();
            keyWithNamespace.SetNamespaceInPayload((byte)context);
            key.CopyTo(keyWithNamespace.AsSpan());

            ref var ctx = ref ActiveThreadSession.vectorContext;

            while (true)
            {
                VectorInput input = new();
                input.ReadDesiredSize = -1;
                fixed (byte* ptr = value.AsSpan())
                {
                    SpanByte asSpanByte = new(value.Length, (nint)ptr);

                    var status = ctx.Read(ref keyWithNamespace, ref input, ref asSpanByte);
                    if (status.IsPending)
                    {
                        CompletePending(ref status, ref asSpanByte, ref ctx);
                    }

                    if (!status.Found)
                    {
                        value.Length = 0;
                        return false;
                    }

                    if (input.ReadDesiredSize > asSpanByte.Length)
                    {
                        value.Memory?.Dispose();
                        var newAlloc = MemoryPool<byte>.Shared.Rent(input.ReadDesiredSize);
                        value = new(newAlloc, newAlloc.Memory.Length);
                        continue;
                    }

                    value.Length = asSpanByte.Length;
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
        private static unsafe SpanByte MarkDiskANNKeyWithNamespace(ulong context, nint keyData, nuint keyLength)
        {
            // DiskANN guarantees we have 4-bytes worth of unused data right before the key
            var keyPtr = (byte*)keyData;
            var keyNamespaceByte = keyPtr - 1;

            // TODO: if/when namespace can be > 4-bytes, we'll need to copy here

            var keyWithNamespace = SpanByte.FromPinnedPointer(keyNamespaceByte, (int)(keyLength + 1));
            keyWithNamespace.MarkNamespace();
            keyWithNamespace.SetNamespaceInPayload((byte)context);

            return keyWithNamespace;
        }
    }
}