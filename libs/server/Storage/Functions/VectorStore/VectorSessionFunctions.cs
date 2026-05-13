// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Frozen;
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
        private readonly ReadSessionState readSessionState;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="functionsState"></param>
        /// <param name="readSessionState"></param>
        internal VectorSessionFunctions(FunctionsState functionsState, ReadSessionState readSessionState = null)
        {
            this.functionsState = functionsState;
            this.readSessionState = readSessionState;
        }

        #region Reads
        /// <inheritdoc/>
        public readonly bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref VectorInput input, ref VectorOutput output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            Debug.Assert(srcLogRecord.HasNamespace, "Should never write a non-namespaced value with VectorSessionFunctions");
            Debug.Assert(srcLogRecord.NamespaceBytes.Length == 1, "Variable length namespaces not supported");

            var value = AlignOrPin(in srcLogRecord, ref input, out var pin);
            try
            {
                if (input.IsMigrationRead)
                {
                    Debug.Assert(input.Callback == 0, "No callback expected");

                    // We can't ship the log record over because of alignment shenanigans
                    // TODO: When alignment is handled at the Tsavorite level, we CAN start shipping the log over like everything else

                    var neededSpace =
                        sizeof(int) + srcLogRecord.NamespaceBytes.Length +
                        sizeof(int) + srcLogRecord.KeyBytes.Length +
                        sizeof(int) + value.Length;

                    output.SpanByteAndMemory.EnsureHeapMemorySize(neededSpace);

                    var writeTo = output.SpanByteAndMemory.Span;

                    BinaryPrimitives.WriteInt32LittleEndian(writeTo, srcLogRecord.NamespaceBytes.Length);
                    writeTo = writeTo[sizeof(int)..];
                    srcLogRecord.NamespaceBytes.CopyTo(writeTo);
                    writeTo = writeTo[srcLogRecord.NamespaceBytes.Length..];

                    BinaryPrimitives.WriteInt32LittleEndian(writeTo, srcLogRecord.KeyBytes.Length);
                    writeTo = writeTo[sizeof(int)..];
                    srcLogRecord.KeyBytes.CopyTo(writeTo);
                    writeTo = writeTo[srcLogRecord.KeyBytes.Length..];

                    // Move value over _without_ any padding for alignment
                    BinaryPrimitives.WriteInt32LittleEndian(writeTo, value.Length);
                    writeTo = writeTo[sizeof(int)..];
                    value.CopyTo(writeTo);

                    return true;
                }

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
            finally
            {
                pin?.Free();
            }
        }

        /// <inheritdoc/>
        public readonly void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref VectorInput input, ref VectorOutput output, long ctx, Status status, RecordMetadata recordMetadata)
        {
        }
        #endregion Reads

        #region Upserts
        /// <inheritdoc/>
        public readonly bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, ReadOnlySpan<byte> srcValue, ref VectorOutput output, ref UpsertInfo upsertInfo)
        {
            Debug.Assert(logRecord.HasNamespace, "Should never write a non-namespaced value with VectorSessionFunctions");
            Debug.Assert(logRecord.NamespaceBytes.Length == 1, "Variable length namespaces not supported");

            var value = AlignOrPin(in logRecord, ref input, out var pin);
            try
            {
                srcValue.CopyTo(value);

                return logRecord.TrySetContentLengths(logRecord.ValueSpan.Length, in sizeInfo);
            }
            finally
            {
                pin?.Free();
            }
        }

        /// <inheritdoc/>
        public readonly bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, IHeapObject srcValue, ref VectorOutput output, ref UpsertInfo upsertInfo)
        => ObjectOperationsNotExpected<bool>();

        /// <inheritdoc/>
        public readonly bool InitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, in TSourceLogRecord inputLogRecord, ref VectorOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        => LogRecordOperationsNotExpected<bool>();

        /// <inheritdoc/>
        public readonly bool InPlaceWriter(ref LogRecord logRecord, ref VectorInput input, ReadOnlySpan<byte> newValue, ref VectorOutput output, ref UpsertInfo upsertInfo)
        {
            Debug.Assert(logRecord.HasNamespace, "Should never write a non-namespaced value with VectorSessionFunctions");
            Debug.Assert(logRecord.NamespaceBytes.Length == 1, "Variable length namespaces not supported");

            var value = AlignOrPin(in logRecord, ref input, out var pin);
            try
            {
                newValue.CopyTo(value);

                return true;
            }
            finally
            {
                pin?.Free();
            }
        }

        /// <inheritdoc/>
        public readonly bool InPlaceWriter(ref LogRecord logRecord, ref VectorInput input, IHeapObject newValue, ref VectorOutput output, ref UpsertInfo upsertInfo)
        => ObjectOperationsNotExpected<bool>();

        /// <inheritdoc/>
        public readonly bool InPlaceWriter<TSourceLogRecord>(ref LogRecord logRecord, ref VectorInput input, in TSourceLogRecord inputLogRecord, ref VectorOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        => LogRecordOperationsNotExpected<bool>();
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
                // Add to value, this is a dynamically sized type - which are only used from Garnet, not DiskANN
                return new RecordFieldInfo() { KeySize = srcLogRecord.Key.Length, ValueSize = value.Length + (-input.WriteDesiredSize) };
            }

            var needsAlignmentPadding = input.AlignmentExpected || input.Callback != 0;

            // Constant size indicated
            if (needsAlignmentPadding)
            {
                return new RecordFieldInfo() { KeySize = srcLogRecord.Key.Length, ValueSize = input.WriteDesiredSize + ValueAlignmentBytes };
            }
            else
            {
                return new RecordFieldInfo() { KeySize = srcLogRecord.Key.Length, ValueSize = input.WriteDesiredSize };
            }
        }

        /// <summary>Initial expected length of value object when populated by RMW using given input</summary>
        public readonly RecordFieldInfo GetRMWInitialFieldInfo<TKey>(TKey key, ref VectorInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            var effectiveWriteDesiredSize = input.WriteDesiredSize;

            var needsAlignmentPadding = input.AlignmentExpected || input.Callback != 0;

            if (effectiveWriteDesiredSize < 0)
            {
                effectiveWriteDesiredSize = -effectiveWriteDesiredSize;
            }

            if (!needsAlignmentPadding)
            {
                return new() { KeySize = key.KeyBytes.Length, ValueSize = effectiveWriteDesiredSize };
            }
            else
            {
                return new() { KeySize = key.KeyBytes.Length, ValueSize = effectiveWriteDesiredSize + ValueAlignmentBytes };
            }
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
        => new() { KeySize = key.KeyBytes.Length, ValueSize = inputLogRecord.ValueSpan.Length };
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
            var alignedValue = AlignOrPin(in logRecord, ref input, out var pin);

            try
            {

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

                        newMetadataValue.CopyTo(alignedValue);

                        return logRecord.TrySetContentLengths(logRecord.ValueSpan.Length, in sizeInfo);
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
                    Debug.Assert(input.WriteDesiredSize <= alignedValue.Length, "Insufficient space for initial update, this should never happen");

                    // Must explicitly 0 before passing if we're doing an initial update
                    alignedValue.Clear();

                    unsafe
                    {
                        // Callback takes: dataCallbackContext, dataPtr, dataLength
                        var callback = (delegate* unmanaged[Cdecl, SuppressGCTransition]<nint, nint, nuint, void>)input.Callback;

                        var dataPtr = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(alignedValue));
                        var dataLen = (nuint)input.WriteDesiredSize;
                        callback(input.CallbackContext, dataPtr, dataLen);

                        return logRecord.TrySetContentLengths(logRecord.ValueSpan.Length, in sizeInfo);
                    }
                }
            }
            finally
            {
                pin?.Free();
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

            var oldValueAligned = AlignOrPin(in srcLogRecord, ref input, out var srcPin);
            var newValueAligned = AlignOrPin(in dstLogRecord, ref input, out var dstPin);

            try
            {
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

                        ref readonly var oldMetadata = ref MemoryMarshal.Cast<byte, VectorManager.ContextMetadata>(oldValueAligned)[0];

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

                        newMetadataValue.CopyTo(newValueAligned);
                        return dstLogRecord.TrySetContentLengths(srcLogRecord.ValueSpan.Length, in sizeInfo);
                    }
                    else
                    {
                        // Doing an InProgressDelete update
                        Debug.Assert(input.CallbackContext != 0, "Should have data on VectorInput");
                        Debug.Assert(key.Length == 1 && key[0] == 1, "Should be working on InProgressDeletes");

                        Span<byte> inProgressDeleteUpdateData;
                        bool adding;

                        oldValueAligned.CopyTo(newValueAligned);

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
                    Debug.Assert(input.WriteDesiredSize <= newValueAligned.Length, "Insufficient space for copy update, this should never happen");
                    Debug.Assert(input.WriteDesiredSize <= oldValueAligned.Length, "Insufficient space for copy update, this should never happen");

                    oldValueAligned.CopyTo(newValueAligned);

                    unsafe
                    {
                        // Callback takes: dataCallbackContext, dataPtr, dataLength
                        var callback = (delegate* unmanaged[Cdecl, SuppressGCTransition]<nint, nint, nuint, void>)input.Callback;

                        var dataPtr = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(newValueAligned));
                        var dataLen = (nuint)input.WriteDesiredSize;

                        callback(input.CallbackContext, dataPtr, dataLen);
                    }

                    return true;

                }
            }
            finally
            {
                srcPin?.Free();
                dstPin?.Free();
            }
        }
        #endregion CopyUpdater

        #region InPlaceUpdater
        /// <inheritdoc/>
        public readonly bool InPlaceUpdater(ref LogRecord logRecord, ref VectorInput input, ref VectorOutput output, ref RMWInfo rmwInfo)
        {
            Debug.Assert(logRecord.HasNamespace, "Should never write a non-namespaced value with VectorSessionFunctions");
            Debug.Assert(logRecord.NamespaceBytes.Length == 1, "Variable length namespaces not supported");

            var key = logRecord.Key;

            var alignedValue = AlignOrPin(in logRecord, ref input, out var pin);
            try
            {
                if (input.Callback == 0)
                {
                    // We're doing a Metadata or InProgressDelete update

                    Debug.Assert(logRecord.NamespaceBytes.Length == 1 && logRecord.NamespaceBytes[0] == VectorManager.MetadataNamespace, "Should be operating on special namespace");

                    if (key.Length == 0)
                    {
                        // Doing a Metadata update
                        Debug.Assert(alignedValue.Length >= VectorManager.ContextMetadata.Size, "Should be ContextMetadata");
                        Debug.Assert(input.CallbackContext != 0, "Should have data on VectorInput");

                        ref readonly var oldMetadata = ref MemoryMarshal.Cast<byte, VectorManager.ContextMetadata>(alignedValue)[0];

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

                        newMetadataValue.CopyTo(alignedValue);
                        return true;
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

                        return true;
                    }
                }
                else
                {
                    Debug.Assert(input.WriteDesiredSize <= alignedValue.Length, "Insufficient space for inplace update, this should never happen");

                    unsafe
                    {
                        // Callback takes: dataCallbackContext, dataPtr, dataLength
                        var callback = (delegate* unmanaged[Cdecl, SuppressGCTransition]<nint, nint, nuint, void>)input.Callback;

                        var dataPtr = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(alignedValue));
                        var dataLen = (nuint)input.WriteDesiredSize;

                        callback(input.CallbackContext, dataPtr, dataLen);
                    }

                    return true;
                }
            }
            finally
            {
                pin?.Free();
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

        [DoesNotReturn]
        private static TReturn LogRecordOperationsNotExpected<TReturn>([CallerMemberName] string callerName = null, [CallerLineNumber] int lineNum = -1)
        => throw new InvalidOperationException($"LogRecord related operations are not expected, was: {callerName} on {lineNum}");

        // TODO: Remove all this alignment hackery when Tsavorite can enforce it

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe Span<byte> AlignOrPin<TSourceLogRecord>(in TSourceLogRecord logRecord, ref VectorInput input, out GCHandle? pin)
            where TSourceLogRecord : ISourceLogRecord
        {
            var maybeUnaligned = logRecord.ValueSpan;

            // Alignment is expected if we're passing to DiskANN or Garnet code explicitly requested it
            var inputRequiresAligment = input.AlignmentExpected || input.Callback != 0;

            if (inputRequiresAligment)
            {
                if (logRecord.IsPinnedValue)
                {
                    // LogRecord itself is in POH, but value might not be aligned so we need to do some checking

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

                    pin = null;
                    return ret;
                }
                else
                {
                    // Value isn't in log record, it's on the (presumably unpinned) heap as a byte[]
                    //
                    // This guarantees it's aligned, but it might move during any callback so pin

                    pin = logRecord.ValueOverflow.Pin();

                    // We over allocated (we don't know how Tsavorite is going to place the value in advance) so trim the extra allocation off the end.
                    var ret = maybeUnaligned[..^ValueAlignmentBytes];

                    AssertAlignment(ret);

                    return ret;
                }
            }
            else
            {
                pin = null;
                return maybeUnaligned;
            }
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
        => LogRecordOperationsNotExpected<bool>();

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

        /// <summary>
        /// Update the namespaces stored in <paramref name="readOutput"/> according to <see cref="FrozenDictionary"/>.
        /// 
        /// <paramref name="readInput"/> should have been used to populate <paramref name="readOutput"/> with a Tsavorite Read prior to this call.
        /// </summary>
        public static void UpdateMigratedElementNamespaces(FrozenDictionary<ulong, ulong> oldToNewNamespaces, ref VectorInput readInput, ref VectorOutput readOutput)
        {
            Debug.Assert(readInput.IsMigrationRead, "Unexpected input");

            // This should contain the results from the IsMigrationRead block in Reader
            var span = readOutput.SpanByteAndMemory.Span;

            var nsLen = BinaryPrimitives.ReadInt32LittleEndian(span);
            Debug.Assert(nsLen == 1, "Longer namespaces not supported");

            var oldNs = (ulong)span[sizeof(int)];

            if (!oldToNewNamespaces.TryGetValue(oldNs, out var newNs))
            {
                return;
            }

            Debug.Assert(newNs <= byte.MaxValue, "Namespace too large");

            span[sizeof(int)] = (byte)newNs;
        }

        /// <inheritdoc />
        public void BeforeConsistentReadCallback(long hash)
            => readSessionState?.BeforeConsistentReadKeyCallback(hash);

        /// <inheritdoc />
        public void AfterConsistentReadKeyCallback()
            => readSessionState?.AfterConsistentReadKeyCallback();

        /// <inheritdoc />
        public void BeforeConsistentReadKeyBatchCallback(ReadOnlySpan<PinnedSpanByte> parameters)
            => readSessionState?.BeforeConsistentReadKeyBatch(parameters);

        /// <inheritdoc />
        public bool AfterConsistentReadKeyBatchCallback(int keyCount)
            => readSessionState != null && readSessionState.AfterConsistentReadKeyBatch(keyCount);
    }
}