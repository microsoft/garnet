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

                Debug.Assert((currentLen % 4) == 0, "Keys must be 4-byte aligned to preserve value alignment");

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

                input = new()
                {
                    CallbackContext = callbackContext,
                    Callback = (nint)callback,
                    Index = i,
                };
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

#pragma warning disable IDE0302 // [...]-style collection initialization doesn't actually _guarantee_ stackalloc (or inline arrays), which we need here
            ReadOnlySpan<byte> nsBytes = stackalloc byte[1] { (byte)context };
#pragma warning restore IDE0302
            var enumerable = new VectorReadBatch(dataCallback, dataCallbackContext, numKeys, PinnedSpanByte.FromPinnedPointer((byte*)keysData, (int)keysLength), nsBytes);

            ref var ctx = ref ActiveThreadSession.vectorBasicContext;

            ctx.ReadWithPrefetch(ref enumerable);

            enumerable.CompletePending(ref ctx);
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe byte WriteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength, nint writeData, nuint writeLength)
        {
            Debug.Assert((keyLength % 4) == 0, "Key must be 4-byte aligned to preserve value alignment");

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
            Debug.Assert((keyLength % 4) == 0, "Key must be 4-byte aligned to preserve value alignment");

            var keyWithNamespace = MakeVectorElementKey(context, keyData, keyLength);

            ref var ctx = ref ActiveThreadSession.vectorBasicContext;

            var status = ctx.Delete(keyWithNamespace);
            Debug.Assert(!status.IsPending, "Deletes should never go async");

            return status.IsCompletedSuccessfully && status.Found ? (byte)1 : default;
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static byte ReadModifyWriteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength, nuint writeLength, nint dataCallback, nint dataCallbackContext)
        {
            Debug.Assert((keyLength % 4) == 0, "Key must be 4-byte aligned to preserve value alignment");

            var keyWithNamespace = MakeVectorElementKey(context, keyData, keyLength);

            ref var ctx = ref ActiveThreadSession.vectorBasicContext;

            VectorInput input = new()
            {
                Callback = dataCallback,
                CallbackContext = dataCallbackContext,
                WriteDesiredSize = (int)writeLength,
            };

            var status = ctx.RMW(keyWithNamespace, ref input);
            if (status.IsPending)
            {
                VectorOutput ignored = new();

                CompletePending(ref status, ref ignored, ref ctx);
            }

            return status.IsCompletedSuccessfully ? (byte)1 : default;
        }

        private static unsafe bool ReadSizeUnknown(ulong context, ReadOnlySpan<byte> key, ref SpanByteAndMemory value)
        {
            // We explicitly DO NOT check alignment here because we're always in managed code which doesn't care

#pragma warning disable IDE0302 // [...]-style collection initialization doesn't actually _guarantee_ stackalloc (or inline arrays), which we need here
            ReadOnlySpan<byte> nsBytes = stackalloc byte[1] { (byte)context };
#pragma warning restore IDE0302

            VectorElementKey keyWithNamespace = new(nsBytes, key);

            ref var ctx = ref ActiveThreadSession.vectorBasicContext;

            while (true)
            {
                VectorInput input = new()
                {
                    VariableSizedRead = true,
                };

                fixed (byte* ptr = value.Span)
                {
                    VectorOutput output = new(ptr, value.Length);

                    var status = ctx.Read(keyWithNamespace, ref input, ref output);
                    if (status.IsPending)
                    {
                        CompletePending(ref status, ref output, ref ctx);
                    }

                    if (!status.Found)
                    {
                        value.Length = 0;
                        return false;
                    }

                    var updateReadDesiredSize = output.UpdatedReadDesiredSize.GetValueOrDefault(-1);
                    if (updateReadDesiredSize > output.SpanByteAndMemory.Length)
                    {
                        value.Memory?.Dispose();
                        var newAlloc = MemoryPool<byte>.Shared.Rent(updateReadDesiredSize);
                        value = new(newAlloc, newAlloc.Memory.Length);
                        continue;
                    }

                    value.Length = output.SpanByteAndMemory.Length;
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
            Span<byte> nsBytes = new(((byte*)keyData) - 1, 1);
            nsBytes[0] = (byte)context;

            ReadOnlySpan<byte> keyBytes = new((byte*)keyData, (int)keyLength);

            return new(nsBytes, keyBytes);
        }
    }
}