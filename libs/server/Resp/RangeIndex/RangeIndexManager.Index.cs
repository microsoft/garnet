// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Garnet.server
{
    /// <summary>
    /// Stub struct and serialization methods for RangeIndex records.
    /// </summary>
    public sealed partial class RangeIndexManager
    {
        /// <summary>
        /// Fixed-size struct stored as a raw-byte value in Tsavorite's unified store.
        /// Contains BfTree configuration metadata and a native pointer to the live instance.
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Size = Size)]
        internal struct RangeIndexStub
        {
            internal const int Size = 51;

            /// <summary>Pointer to the live BfTreeService instance (managed object handle).</summary>
            [FieldOffset(0)]
            public nint TreeHandle;

            /// <summary>BfTree circular buffer size in bytes.</summary>
            [FieldOffset(8)]
            public ulong CacheSize;

            /// <summary>BfTree minimum record size.</summary>
            [FieldOffset(16)]
            public uint MinRecordSize;

            /// <summary>BfTree maximum record size.</summary>
            [FieldOffset(20)]
            public uint MaxRecordSize;

            /// <summary>BfTree maximum key length.</summary>
            [FieldOffset(24)]
            public uint MaxKeyLen;

            /// <summary>BfTree leaf page size.</summary>
            [FieldOffset(28)]
            public uint LeafPageSize;

            /// <summary>Storage backend: 0=Disk, 1=Memory.</summary>
            [FieldOffset(32)]
            public byte StorageBackend;

            /// <summary>Reserved flags byte.</summary>
            [FieldOffset(33)]
            public byte Flags;

            /// <summary>Serialization phase for checkpoint coordination.</summary>
            [FieldOffset(34)]
            public byte SerializationPhase;

            /// <summary>Process instance ID for stale pointer detection.</summary>
            [FieldOffset(35)]
            public Guid ProcessInstanceId;
        }

        /// <summary>
        /// Write a new stub into the value span of a LogRecord.
        /// Called from InitialUpdater in RMWMethods.cs.
        /// </summary>
        internal void CreateIndex(
            ulong cacheSize,
            uint minRecordSize,
            uint maxRecordSize,
            uint maxKeyLen,
            uint leafPageSize,
            byte storageBackend,
            nint treeHandle,
            Span<byte> valueSpan)
        {
            Debug.Assert(Unsafe.SizeOf<RangeIndexStub>() == RangeIndexStub.Size, "Constant stub size is incorrect");
            Debug.Assert(valueSpan.Length >= RangeIndexStub.Size, $"Value span too small: {valueSpan.Length} < {RangeIndexStub.Size}");

            ref var stub = ref Unsafe.As<byte, RangeIndexStub>(ref MemoryMarshal.GetReference(valueSpan));
            stub.TreeHandle = treeHandle;
            stub.CacheSize = cacheSize;
            stub.MinRecordSize = minRecordSize;
            stub.MaxRecordSize = maxRecordSize;
            stub.MaxKeyLen = maxKeyLen;
            stub.LeafPageSize = leafPageSize;
            stub.StorageBackend = storageBackend;
            stub.Flags = 0;
            stub.SerializationPhase = 0;
            stub.ProcessInstanceId = processInstanceId;
        }

        /// <summary>
        /// Get a readonly reference to the stub from a store value span (zero-copy).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ref readonly RangeIndexStub ReadIndex(ReadOnlySpan<byte> value)
            => ref Unsafe.As<byte, RangeIndexStub>(ref MemoryMarshal.GetReference(value));

        /// <summary>
        /// Update TreeHandle and ProcessInstanceId after recovery (old pointer is stale).
        /// </summary>
        internal void RecreateIndex(nint newTreeHandle, Span<byte> valueSpan)
        {
            ref var stub = ref Unsafe.As<byte, RangeIndexStub>(ref MemoryMarshal.GetReference(valueSpan));
            Debug.Assert(processInstanceId != stub.ProcessInstanceId,
                "Shouldn't recreate an index from the same process instance");
            stub.TreeHandle = newTreeHandle;
            stub.ProcessInstanceId = processInstanceId;
        }
    }
}