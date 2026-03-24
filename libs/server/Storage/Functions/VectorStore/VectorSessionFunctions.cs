// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Functions for operating against the Main Store, but for data stored as part of a Vector Set operation - not a RESP command.
    /// </summary>
    public readonly struct VectorSessionFunctions : ISessionFunctions<VectorInput, VectorOutput, long>
    {
        private const int ValueAlignmentBytes = 4;

        private readonly FunctionsState functionsState;

        /// <summary>
        /// Constructor
        /// </summary>
        internal VectorSessionFunctions(FunctionsState functionsState)
        {
            this.functionsState = functionsState;
        }

        #region Reads
        /// <inheritdoc/>
        public readonly bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref VectorInput input, ref VectorOutput output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            Debug.Assert(srcLogRecord.HasNamespace, "Should never write a non-namespaced value with VectorSessionFunctions");
            Debug.Assert(srcLogRecord.NamespaceBytes.Length == 1, "Variable length namespaces not supported");

            var value = srcLogRecord.ValueSpan;

            value = Align(value);

            unsafe
            {
                if (input.Callback != 0)
                {
                    var callback = (delegate* unmanaged[Cdecl, SuppressGCTransition]<int, nint, nint, nuint, void>)input.Callback;

                    var dataPtr = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(value));
                    var dataLen = (nuint)value.Length;

                    callback(input.Index, input.CallbackContext, dataPtr, dataLen);
                    return true;
                }
            }

            if (input.ReadDesiredSize > 0)
            {
                Debug.Assert(output.SpanByteAndMemory.Length >= value.Length, "Should always have space for vector point reads");

                output.SpanByteAndMemory.Length = value.Length;
                value.CopyTo(output.SpanByteAndMemory.Span);
            }
            else
            {
                input.ReadDesiredSize = value.Length;
                if (output.SpanByteAndMemory.Length >= value.Length)
                {
                    value.CopyTo(output.SpanByteAndMemory.Span);
                    output.SpanByteAndMemory.Length = value.Length;
                }
            }

            return true;
        }

        /// <inheritdoc/>
        public readonly void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref VectorInput input, ref VectorOutput output, long ctx, Status status, RecordMetadata recordMetadata)
        {
        }
        #endregion Reads

        #region Upserts
        /// <inheritdoc/>
        public readonly bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, ReadOnlySpan<byte> srcValue, ref VectorOutput output, ref UpsertInfo upsertInfo)
        => InPlaceWriter(ref logRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo);

        /// <inheritdoc/>
        public readonly bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, IHeapObject srcValue, ref VectorOutput output, ref UpsertInfo upsertInfo)
        => ObjectOperationsNotExpected<bool>();

        /// <inheritdoc/>
        public readonly bool InitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, in TSourceLogRecord inputLogRecord, ref VectorOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        => ObjectOperationsNotExpected<bool>();


        /// <inheritdoc/>
        public readonly bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, ReadOnlySpan<byte> newValue, ref VectorOutput output, ref UpsertInfo upsertInfo)
        {
            Debug.Assert(logRecord.HasNamespace, "Should never write a non-namespaced value with VectorSessionFunctions");
            Debug.Assert(logRecord.NamespaceBytes.Length == 1, "Variable length namespaces not supported");

            var rawValue = logRecord.ValueSpan;
            var value = Align(rawValue);

            newValue.CopyTo(value);

            return logRecord.TrySetContentLengths(rawValue.Length, in sizeInfo);
        }

        /// <inheritdoc/>
        public readonly bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, IHeapObject newValue, ref VectorOutput output, ref UpsertInfo upsertInfo)
        => ObjectOperationsNotExpected<bool>();

        /// <inheritdoc/>
        public readonly bool InPlaceWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, in TSourceLogRecord inputLogRecord, ref VectorOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        => ObjectOperationsNotExpected<bool>();
        #endregion Upserts

        #region RMWs
        #region Variable Length
        /// <summary>Length of resulting value object when performing RMW modification of value using given input</summary>
        public readonly RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref VectorInput input)
            where TSourceLogRecord : ISourceLogRecord
        {
            var value = srcLogRecord.ValueSpan;

            if (input.WriteDesiredSize < 0)
            {
                // Add to value, this is a dynamically sized type
                return new RecordFieldInfo() { KeySize = srcLogRecord.Key.Length, ValueSize = value.Length + (-input.WriteDesiredSize) };
            }

            // Constant size indicated
            return new RecordFieldInfo() { KeySize = srcLogRecord.Key.Length, ValueSize = input.WriteDesiredSize + ValueAlignmentBytes };
        }

        /// <summary>Initial expected length of value object when populated by RMW using given input</summary>
        public readonly RecordFieldInfo GetRMWInitialFieldInfo<TKey>(TKey key, ref VectorInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            var effectiveWriteDesiredSize = input.WriteDesiredSize;

            if (effectiveWriteDesiredSize < 0)
            {
                effectiveWriteDesiredSize = -effectiveWriteDesiredSize;
            }

            return new() { KeySize = key.KeyBytes.Length, ValueSize = effectiveWriteDesiredSize + ValueAlignmentBytes };
        }

        /// <summary>Length of value object, when populated by Upsert using given value and input</summary>
        public readonly RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, ReadOnlySpan<byte> value, ref VectorInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        => new() { KeySize = key.KeyBytes.Length, ValueSize = value.Length + ValueAlignmentBytes };

        /// <summary>Length of value object, when populated by Upsert using given value and input</summary>
        public readonly RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, IHeapObject value, ref VectorInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        => ObjectOperationsNotExpected<RecordFieldInfo>();

        /// <summary>Length of value object, when populated by Upsert using given log record</summary>
        public readonly RecordFieldInfo GetUpsertFieldInfo<TKey, TSourceLogRecord>(TKey key, in TSourceLogRecord inputLogRecord, ref VectorInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSourceLogRecord : ISourceLogRecord
        => ObjectOperationsNotExpected<RecordFieldInfo>();
        #endregion Variable Length

        #region InitialUpdater
        /// <inheritdoc/>
        public readonly bool NeedInitialUpdate<TKey>(TKey key, ref VectorInput input, ref VectorOutput output, ref RMWInfo rmwInfo)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            Debug.Assert(key.HasNamespace, "Should never write a non-namespaced value with VectorSessionFunctions");
            Debug.Assert(key.NamespaceBytes.Length == 1, "Variable length namespaces not supported");

            // Only needed when updating ContextMetadata or InProgressDeletes via RMW or the DiskANN RMW callback, all of which set WriteDesiredSize
            return input.WriteDesiredSize != 0;
        }

        /// <inheritdoc/>
        public readonly bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, ref VectorOutput output, ref RMWInfo rmwInfo)
        {
            Debug.Assert(logRecord.HasNamespace, "Should never write a non-namespaced value with VectorSessionFunctions");
            Debug.Assert(logRecord.NamespaceBytes.Length == 1, "Variable length namespaces not supported");

            var key = logRecord.Key;
            var rawValue = logRecord.ValueSpan;

            var value = Align(rawValue);

            if (input.Callback == 0)
            {
                Debug.Assert(logRecord.NamespaceBytes.Length == 1 && logRecord.NamespaceBytes[0] == VectorManager.MetadataNamespace, "Should never write a non-namespaced value with VectorSessionFunctions");

                if (key.Length == 0)
                {
                    // Operating on ContextMetadata

                    PinnedSpanByte newMetadataValue;
                    unsafe
                    {
                        newMetadataValue = PinnedSpanByte.FromPinnedPointer((byte*)input.CallbackContext, VectorManager.ContextMetadata.Size);
                    }

                    newMetadataValue.CopyTo(value);
                    return logRecord.TrySetContentLengths(rawValue.Length, in sizeInfo);
                }
                else
                {
                    // Operating on InProgressDeletes
                    Debug.Assert(input.CallbackContext != 0, "Should have data on VectorInput");
                    Debug.Assert(logRecord.NamespaceBytes.Length == 1 && logRecord.NamespaceBytes[0] == VectorManager.MetadataNamespace, "Should never write a non-namespaced value with VectorSessionFunctions");
                    Debug.Assert(key.Length == 1 && key[0] == 1, "Should be working on InProgressDeletes");

                    Span<byte> inProgressDeleteUpdateData;
                    bool adding;

                    unsafe
                    {
                        var len = BinaryPrimitives.ReadInt32LittleEndian(new Span<byte>((byte*)input.CallbackContext + sizeof(long), sizeof(int)));
                        adding = len > 0;
                        if (!adding)
                        {
                            len = -len;
                        }

                        inProgressDeleteUpdateData = new Span<byte>((byte*)input.CallbackContext, sizeof(ulong) + sizeof(int) + len);
                    }

                    if (!adding)
                    {
                        // We may be recovering and doing some optimistic deletes, but since we're creating... just ignore the op, it does nothing
                        rmwInfo.Action = RMWAction.CancelOperation;
                        return false;
                    }

                    var fits = VectorManager.TryUpdateInProgressDeletes(inProgressDeleteUpdateData, ref logRecord, in sizeInfo);
                    Debug.Assert(fits, "Initial size of record should have been correct for in progress deletes");

                    return true;
                }
            }
            else
            {
                Debug.Assert(input.WriteDesiredSize <= value.Length, "Insufficient space for initial update, this should never happen");

                // Must explicitly 0 before passing if we're doing an initial update
                value.Clear();

                value = Align(value);

                unsafe
                {
                    // Callback takes: dataCallbackContext, dataPtr, dataLength
                    var callback = (delegate* unmanaged[Cdecl, SuppressGCTransition]<nint, nint, nuint, void>)input.Callback;

                    var dataPtr = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(value));
                    var dataLen = (nuint)input.WriteDesiredSize;
                    callback(input.CallbackContext, dataPtr, dataLen);

                    return logRecord.TrySetContentLengths(rawValue.Length, in sizeInfo);
                }
            }
        }
        #endregion InitialUpdater

        #region CopyUpdater
        /// <inheritdoc/>
        public readonly bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref VectorInput input, ref VectorOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        => input.WriteDesiredSize != 0;

        /// <inheritdoc/>
        public readonly bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, ref VectorOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            Debug.Assert(srcLogRecord.HasNamespace, "Should never write a non-namespaced value with VectorSessionFunctions");
            Debug.Assert(srcLogRecord.NamespaceBytes.Length == 1, "Variable length namespaces not supported");

            var key = srcLogRecord.Key;
            var oldValue = srcLogRecord.ValueSpan;
            var newValue = dstLogRecord.ValueSpan;

            if (input.Callback == 0)
            {
                // We're doing a Metadata or InProgressDelete update

                Debug.Assert(srcLogRecord.NamespaceBytes[0] == VectorManager.MetadataNamespace, "Should be operating on special namespace");

                if (key.Length == 0)
                {
                    // Doing a Metadata update
                    Debug.Assert(srcLogRecord.ValueSpan.Length == VectorManager.ContextMetadata.Size, "Should be ContextMetadata");
                    Debug.Assert(dstLogRecord.ValueSpan.Length == VectorManager.ContextMetadata.Size, "Should be ContextMetadata");
                    Debug.Assert(input.CallbackContext != 0, "Should have data on VectorInput");

                    ref readonly var oldMetadata = ref MemoryMarshal.Cast<byte, VectorManager.ContextMetadata>(oldValue)[0];

                    PinnedSpanByte newMetadataValue;
                    unsafe
                    {
                        newMetadataValue = PinnedSpanByte.FromPinnedPointer((byte*)input.CallbackContext, VectorManager.ContextMetadata.Size);
                    }

                    ref readonly var newMetadata = ref MemoryMarshal.Cast<byte, VectorManager.ContextMetadata>(newMetadataValue.ReadOnlySpan)[0];

                    if (newMetadata.Version < oldMetadata.Version)
                    {
                        rmwInfo.Action = RMWAction.CancelOperation;
                        return false;
                    }

                    newMetadataValue.CopyTo(newValue);
                    return dstLogRecord.TrySetContentLengths(newMetadataValue.Length, in sizeInfo);
                }
                else
                {
                    // Doing an InProgressDelete update
                    Debug.Assert(input.CallbackContext != 0, "Should have data on VectorInput");
                    Debug.Assert(key.Length == 1 && key[0] == 1, "Should be working on InProgressDeletes");

                    Span<byte> inProgressDeleteUpdateData;
                    bool adding;

                    oldValue.CopyTo(newValue);

                    unsafe
                    {
                        var len = BinaryPrimitives.ReadInt32LittleEndian(new Span<byte>((byte*)input.CallbackContext + sizeof(long), sizeof(int)));
                        adding = len > 0;
                        if (!adding)
                        {
                            len = -len;
                        }

                        inProgressDeleteUpdateData = new Span<byte>((byte*)input.CallbackContext, sizeof(ulong) + sizeof(int) + len);
                    }

                    var fits = VectorManager.TryUpdateInProgressDeletes(inProgressDeleteUpdateData, ref dstLogRecord, in sizeInfo);
                    Debug.Assert(fits, "Copy update should have allocated enough space for in progress deletes");

                    return true;
                }
            }
            else
            {
                Debug.Assert(input.WriteDesiredSize <= newValue.Length, "Insufficient space for copy update, this should never happen");
                Debug.Assert(input.WriteDesiredSize <= oldValue.Length, "Insufficient space for copy update, this should never happen");

                oldValue.CopyTo(newValue);

                newValue = Align(newValue);

                unsafe
                {
                    // Callback takes: dataCallbackContext, dataPtr, dataLength
                    var callback = (delegate* unmanaged[Cdecl, SuppressGCTransition]<nint, nint, nuint, void>)input.Callback;

                    var dataPtr = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(newValue));
                    var dataLen = (nuint)input.WriteDesiredSize;

                    callback(input.CallbackContext, dataPtr, dataLen);
                }

                return true;
            }
        }
        #endregion CopyUpdater

        #region InPlaceUpdater
        /// <inheritdoc/>
        public readonly bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, ref VectorOutput output, ref RMWInfo rmwInfo)
        {
            Debug.Assert(logRecord.HasNamespace, "Should never write a non-namespaced value with VectorSessionFunctions");
            Debug.Assert(logRecord.NamespaceBytes.Length == 1, "Variable length namespaces not supported");

            var key = logRecord.Key;
            var value = logRecord.ValueSpan;

            if (input.Callback == 0)
            {
                // We're doing a Metadata or InProgressDelete update

                Debug.Assert(logRecord.NamespaceBytes.Length ==1 && logRecord.NamespaceBytes[0] == VectorManager.MetadataNamespace, "Should be operating on special namespace");

                if (key.Length == 0)
                {
                    // Doing a Metadata update
                    Debug.Assert(value.Length >= VectorManager.ContextMetadata.Size, "Should be ContextMetadata");
                    Debug.Assert(input.CallbackContext != 0, "Should have data on VectorInput");

                    ref readonly var oldMetadata = ref MemoryMarshal.Cast<byte, VectorManager.ContextMetadata>(value)[0];

                    PinnedSpanByte newMetadataValue;
                    unsafe
                    {
                        newMetadataValue = PinnedSpanByte.FromPinnedPointer((byte*)input.CallbackContext, VectorManager.ContextMetadata.Size);
                    }

                    ref readonly var newMetadata = ref MemoryMarshal.Cast<byte, VectorManager.ContextMetadata>(newMetadataValue.ReadOnlySpan)[0];

                    if (newMetadata.Version < oldMetadata.Version)
                    {
                        rmwInfo.Action = RMWAction.CancelOperation;
                        return false;
                    }

                    newMetadataValue.CopyTo(value);
                    return logRecord.TrySetContentLengths(value.Length, in sizeInfo);
                }
                else
                {
                    // Doing an InProgressDelete update
                    Debug.Assert(input.CallbackContext != 0, "Should have data on VectorInput");
                    Debug.Assert(key.Length == 1 && key[0] == 1, "Should be working on InProgressDeletes");

                    Span<byte> inProgressDeleteUpdateData;
                    bool adding;

                    unsafe
                    {
                        var len = BinaryPrimitives.ReadInt32LittleEndian(new Span<byte>((byte*)input.CallbackContext + sizeof(long), sizeof(int)));
                        adding = len > 0;
                        if (!adding)
                        {
                            len = -len;
                        }

                        inProgressDeleteUpdateData = new Span<byte>((byte*)input.CallbackContext, sizeof(ulong) + sizeof(int) + len);
                    }

                    return VectorManager.TryUpdateInProgressDeletes(inProgressDeleteUpdateData, ref logRecord, in sizeInfo);
                }
            }
            else
            {
                Debug.Assert(input.WriteDesiredSize <= value.Length, "Insufficient space for inplace update, this should never happen");

                value = Align(value);

                unsafe
                {
                    // Callback takes: dataCallbackContext, dataPtr, dataLength
                    var callback = (delegate* unmanaged[Cdecl, SuppressGCTransition]<nint, nint, nuint, void>)input.Callback;

                    var dataPtr = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(value));
                    var dataLen = (nuint)input.WriteDesiredSize;

                    callback(input.CallbackContext, dataPtr, dataLen);
                }

                return true;
            }
        }
        #endregion InPlaceUpdater

        /// <inheritdoc/>
        public readonly void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref VectorInput input, ref VectorOutput output, long ctx, Status status, RecordMetadata recordMetadata)
        {
        }
        #endregion RMWs

        #region Deletes
        /// <inheritdoc/>
        public readonly bool InitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            //Debug.Assert(key.MetadataSize == 1, "Should never delete a non-namespaced value with VectorSessionFunctions");

            functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
            return true;
        }

        /// <inheritdoc/>
        public readonly bool InPlaceDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        => InitialDeleter(ref logRecord, ref deleteInfo);
        #endregion Deletes

        #region Utilities
        /// <inheritdoc/>
        public readonly void ConvertOutputToHeap(ref VectorInput input, ref VectorOutput output)
        {
        }
        #endregion Utilities

        [DoesNotReturn]
        private static TReturn ObjectOperationsNotExpected<TReturn>([CallerMemberName] string callerName = null, [CallerLineNumber] int lineNum = -1)
        => throw new InvalidOperationException($"Object related operations are not expected, was: {callerName} on {lineNum}");

        // TODO: Remove all this alignment hackery

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe Span<byte> Align(Span<byte> maybeUnaligned)
        {
            Span<byte> ret;

            var leading = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(maybeUnaligned)) % 4;
            if (leading == 0)
            {
                ret = maybeUnaligned[..^ValueAlignmentBytes];
            }
            else
            {
                var skip = (int)(ValueAlignmentBytes - leading);
                var tail = ValueAlignmentBytes - skip;
                ret = maybeUnaligned[skip..^tail];
            }

            AssertAlignment(ret);
            return ret;
        }

        [Conditional("DEBUG")]
        private static unsafe void AssertAlignment(ReadOnlySpan<byte> aligned)
        {
            var ptr = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(aligned));
            Debug.Assert((ptr % ValueAlignmentBytes) == 0, "Must guarantee 4-byte alignment before invoking callback");
        }

#region Post operation callbacks
        /// <inheritdoc/>
        public readonly void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, ReadOnlySpan<byte> srcValue, ref VectorOutput output, ref UpsertInfo upsertInfo)
        {
        }

        /// <inheritdoc/>
        public readonly void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, IHeapObject srcValue, ref VectorOutput output, ref UpsertInfo upsertInfo)
        => ObjectOperationsNotExpected<bool>();

        /// <inheritdoc/>
        public readonly void PostInitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, in TSourceLogRecord inputLogRecord, ref VectorOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        => ObjectOperationsNotExpected<bool>();

        /// <inheritdoc/>
        public readonly void PostInitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, ref VectorOutput output, ref RMWInfo rmwInfo)
        {
        }

        /// <inheritdoc/>
        public readonly bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, ref VectorOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        => true;

        /// <inheritdoc/>
        public void PostUpsertOperation<TKey, TEpochAccessor>(TKey key, ref VectorInput input, ReadOnlySpan<byte> valueSpan, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        {
        }

        /// <inheritdoc/>
        public void PostUpsertOperation<TKey, TEpochAccessor>(TKey key, ref VectorInput input, IHeapObject valueObject, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        => ObjectOperationsNotExpected<bool>();

        /// <inheritdoc/>
        public void PostRMWOperation<TKey, TEpochAccessor>(TKey key, ref VectorInput input, ref RMWInfo rmwInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        {
        }

        /// <inheritdoc/>
        public void PostDeleteOperation<TKey, TEpochAccessor>(TKey key, ref DeleteInfo deleteInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        {
        }

        /// <inheritdoc/>
        public readonly void PostInitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
        }
        #endregion
    }
}