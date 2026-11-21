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

            var value = srcLogRecord.ValueSpan;

            if (input.IsMigrationRead)
            {
                Debug.Assert(input.Callback == 0, "No callback expected");

                // We can't ship the log record over because of alignment shenanigans
                // TODO: When alignment is handled at the Tsavorite level, we CAN start shipping the log over like everything else

                var neededSpace = VectorManager.GetMigratedElementKeySerializationSize(srcLogRecord.KeyBytes, value);

                output.SpanByteAndMemory.EnsureHeapMemorySize(neededSpace);

                VectorManager.SerializeMigratedElementKey(output.SpanByteAndMemory.Span, srcLogRecord.NamespaceBytes, srcLogRecord.KeyBytes, value);

                return true;
            }

            unsafe
            {
                if (input.Callback != 0)
                {
                    Debug.Assert(BitConverter.IsLittleEndian, "Code assumes little-endian for everything for now");
                    var isAttribute = (srcLogRecord.NamespaceBytes[0] & (VectorManager.ContextStep - 1)) == DiskANNService.Attributes;

                    nint dataPtr;
                    GCHandle? dataPin;

                    if (srcLogRecord.IsPinnedValue)
                    {
                        dataPin = null;
                    }
                    else
                    {
                        dataPin = srcLogRecord.ValueOverflow.Pin();
                    }

                    try
                    {

                        dataPtr = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(value));
                        var dataLen = (nuint)value.Length;

                        AssertAlignment(in srcLogRecord, dataPtr);

                        if (isAttribute)
                        {
                            // Attribute reads _may_ reverse p/invoke back into Garnet for filtering, so DO NOT suppress GC transition
                            var callback = (delegate* unmanaged[Cdecl]<int, nint, nint, nuint, void>)input.Callback;
                            callback(input.Index, input.CallbackContext, dataPtr, dataLen);
                        }
                        else
                        {
                            // All other read types are entirely serviced on the DiskANN side, so suppress GC transition
                            var callback = (delegate* unmanaged[Cdecl, SuppressGCTransition]<int, nint, nint, nuint, void>)input.Callback;
                            callback(input.Index, input.CallbackContext, dataPtr, dataLen);
                        }

                        return true;
                    }
                    finally
                    {
                        dataPin?.Free();
                    }
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
        {
            Debug.Assert(logRecord.HasNamespace, "Should never write a non-namespaced value with VectorSessionFunctions");

            var value = logRecord.ValueSpan;
            srcValue.CopyTo(value);

            return logRecord.TrySetContentLengths(logRecord.ValueSpan.Length, in sizeInfo);
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

            var sizeInfo = new RecordSizeInfo() { FieldInfo = GetUpsertFieldInfo(logRecord, newValue, ref input) };
            functionsState.storeWrapper.store.Log.PopulateRecordSizeInfo(ref sizeInfo);

            // Resize the value to the new payload; false means it can't grow in place, so Tsavorite reallocates via
            // InitialWriter. Without this the copy below would overflow on a grow or leave stale bytes on a shrink.
            if (!logRecord.TrySetContentLengths(sizeInfo.FieldInfo.ValueSize, in sizeInfo))
                return false;

            var value = logRecord.ValueSpan;

            newValue.CopyTo(value);

            return true;
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
        private enum PrepareResult : byte
        {
            NoChange = 0,
            Write = 1,
            Error = 2,
        }

        // Scratch can be reused for an updater fallback within one attempt, not across attempts.
        private const byte PreparedForInitial = 1;
        private const byte PreparedForCopy = 2;

        private static unsafe PrepareResult PrepareRmw(ref VectorInput input, nint current, nuint currentLength, ref RMWInfo rmwInfo)
        {
            var prepare = (delegate* unmanaged[Cdecl]<nint, nint, nuint, nuint*, byte>)input.PrepareCallback;
            nuint replacementLength = 0;
            var result = (PrepareResult)prepare(input.CallbackContext, current, currentLength, &replacementLength);
            if (result == PrepareResult.NoChange)
                return result;

            if (result == PrepareResult.Write && replacementLength > 0 && replacementLength <= int.MaxValue)
            {
                input.WriteDesiredSize = (int)replacementLength;
                return result;
            }

            // In-chain tombstone revival reports NotFound when NeedInitialUpdate declines, even for cancellation.
            input.PrepareFailed = true;
            rmwInfo.Action = RMWAction.CancelOperation;
            return PrepareResult.Error;
        }

        private static unsafe PrepareResult PrepareExistingRmw(ReadOnlySpan<byte> current, ref VectorInput input, ref RMWInfo rmwInfo)
        {
            // Pin overflow values too, and reserve a null current pointer for an absent record, not an empty span.
            fixed (byte* ptr = &MemoryMarshal.GetReference(current))
                return PrepareRmw(ref input, (nint)ptr, (nuint)current.Length, ref rmwInfo);
        }

        private static unsafe void WritePreparedReplacement(in LogRecord logRecord, Span<byte> value, in VectorInput input)
        {
            Debug.Assert(value.Length == input.WriteDesiredSize);

            // Only fill the destination. An initial/copy destination may still be abandoned if publishing it fails.
            var writer = (delegate* unmanaged[Cdecl, SuppressGCTransition]<nint, nint, nuint, void>)input.Callback;
            fixed (byte* ptr = value)
            {
                AssertAlignment(in logRecord, (nint)ptr);
                writer(input.CallbackContext, (nint)ptr, (nuint)value.Length);
            }
        }

        #region Variable Length
        /// <summary>Length of resulting value object when performing RMW modification of value using given input</summary>
        public readonly RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref VectorInput input)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (input.WriteDesiredSize < 0)
            {
                // Add to value, this is a dynamically sized type - which are only used from Garnet, not DiskANN
                return new() { KeySize = srcLogRecord.Key.Length, ValueSize = srcLogRecord.ValueSpan.Length + (-input.WriteDesiredSize), ExtendedNamespaceSize = RecordNamespace.GetExtendedNamespaceSize(in srcLogRecord) };
            }

            return new() { KeySize = srcLogRecord.Key.Length, ValueSize = input.WriteDesiredSize, ExtendedNamespaceSize = RecordNamespace.GetExtendedNamespaceSize(in srcLogRecord) };
        }

        /// <summary>Initial expected length of value object when populated by RMW using given input</summary>
        public readonly RecordFieldInfo GetRMWInitialFieldInfo<TKey>(TKey key, ref VectorInput input)
            where TKey : IKey, allows ref struct
        {
            var effectiveWriteDesiredSize = input.WriteDesiredSize;

            if (effectiveWriteDesiredSize < 0)
            {
                effectiveWriteDesiredSize = -effectiveWriteDesiredSize;
            }

            return new() { KeySize = key.KeyBytes.Length, ValueSize = effectiveWriteDesiredSize, ExtendedNamespaceSize = RecordNamespace.GetExtendedNamespaceSize(in key) };
        }

        /// <summary>Length of value object, when populated by Upsert using given value and input</summary>
        public readonly RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, ReadOnlySpan<byte> value, ref VectorInput input)
            where TKey : IKey, allows ref struct
        => new() { KeySize = key.KeyBytes.Length, ValueSize = value.Length, ExtendedNamespaceSize = RecordNamespace.GetExtendedNamespaceSize(in key) };

        /// <summary>Length of value object, when populated by Upsert using given value and input</summary>
        public readonly RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, IHeapObject value, ref VectorInput input)
            where TKey : IKey, allows ref struct
        => ObjectOperationsNotExpected<RecordFieldInfo>();

        /// <summary>Length of value object, when populated by Upsert using given log record</summary>
        public readonly RecordFieldInfo GetUpsertFieldInfo<TKey, TSourceLogRecord>(TKey key, in TSourceLogRecord inputLogRecord, ref VectorInput input)
            where TKey : IKey, allows ref struct
            where TSourceLogRecord : ISourceLogRecord
        => new() { KeySize = key.KeyBytes.Length, ValueSize = inputLogRecord.ValueSpan.Length, ExtendedNamespaceSize = RecordNamespace.GetExtendedNamespaceSize(in key) };
        #endregion Variable Length

        #region InitialUpdater
        /// <inheritdoc/>
        public readonly bool NeedInitialUpdate<TKey>(TKey key, ref VectorInput input, ref VectorOutput output, ref RMWInfo rmwInfo)
            where TKey : IKey, allows ref struct
        {
            Debug.Assert(key.HasNamespace, "Should never write a non-namespaced value with VectorSessionFunctions");

            if (input.PrepareCallback == 0)
                return input.WriteDesiredSize != 0;

            // A failed in-chain tombstone revival falls back to insertion with the same absent source.
            if (rmwInfo.UserData == PreparedForInitial)
                return true;

            if (PrepareRmw(ref input, 0, 0, ref rmwInfo) != PrepareResult.Write)
                return false;

            rmwInfo.UserData = PreparedForInitial;
            return true;
        }

        /// <inheritdoc/>
        public readonly bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, ref VectorOutput output, ref RMWInfo rmwInfo)
        {
            Debug.Assert(logRecord.HasNamespace, "Should never write a non-namespaced value with VectorSessionFunctions");

            if (input.PrepareCallback != 0)
            {
                WritePreparedReplacement(in logRecord, logRecord.ValueSpan, in input);
                return true;
            }

            var key = logRecord.Key;
            var alignedValue = logRecord.ValueSpan;

            if (input.Callback == 0)
            {
                Debug.Assert(logRecord.NamespaceBytes.Length == 1 && logRecord.NamespaceBytes[0] == VectorManager.MetadataNamespace, "Should never write a non-namespaced value with VectorSessionFunctions");
                Debug.Assert(key.Length == sizeof(int), "Should have int sized key for ContextMetadata");

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
                Debug.Assert(input.WriteDesiredSize <= alignedValue.Length, "Insufficient space for initial update, this should never happen");

                // Must explicitly 0 before passing if we're doing an initial update
                alignedValue.Clear();

                unsafe
                {
                    // Callback takes: dataCallbackContext, dataPtr, dataLength
                    var callback = (delegate* unmanaged[Cdecl, SuppressGCTransition]<nint, nint, nuint, void>)input.Callback;

                    var dataPtr = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(alignedValue));
                    var dataLen = (nuint)input.WriteDesiredSize;

                    AssertAlignment(in logRecord, dataPtr);
                    callback(input.CallbackContext, dataPtr, dataLen);

                    return logRecord.TrySetContentLengths(logRecord.ValueSpan.Length, in sizeInfo);
                }
            }
        }
        #endregion InitialUpdater

        #region CopyUpdater
        /// <inheritdoc/>
        public readonly bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref VectorInput input, ref VectorOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (input.PrepareCallback == 0)
                return input.WriteDesiredSize != 0;

            if (rmwInfo.UserData == PreparedForCopy)
                return true;

            // NeedCopyUpdate runs before sizing, and can decline or cancel without allocating a record.
            return PrepareExistingRmw(srcLogRecord.ValueSpan, ref input, ref rmwInfo) == PrepareResult.Write;
        }

        /// <inheritdoc/>
        public readonly bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, ref VectorOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            Debug.Assert(srcLogRecord.HasNamespace, "Should never write a non-namespaced value with VectorSessionFunctions");

            if (input.PrepareCallback != 0)
            {
                WritePreparedReplacement(in dstLogRecord, dstLogRecord.ValueSpan, in input);
                return true;
            }

            var key = srcLogRecord.Key;

            var oldValueAligned = srcLogRecord.ValueSpan;
            var newValueAligned = dstLogRecord.ValueSpan;

            if (input.Callback == 0)
            {
                // We're doing a Metadata update

                Debug.Assert(srcLogRecord.NamespaceBytes[0] == VectorManager.MetadataNamespace, "Should be operating on special namespace");
                Debug.Assert(key.Length == sizeof(int), "Should have int sized key for ContextMetadata");

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
                // WriteDesiredSize is ignored on CopyUpdate

                var srcCopy = oldValueAligned;
                var dstCopy = newValueAligned[..oldValueAligned.Length];

                srcCopy.CopyTo(dstCopy);

                unsafe
                {
                    // Callback takes: dataCallbackContext, dataPtr, dataLength
                    var callback = (delegate* unmanaged[Cdecl, SuppressGCTransition]<nint, nint, nuint, void>)input.Callback;

                    var dataPtr = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(newValueAligned));
                    var dataLen = (nuint)dstCopy.Length;

                    AssertAlignment(in srcLogRecord, dataPtr);
                    callback(input.CallbackContext, dataPtr, dataLen);
                }

                return true;

            }
        }
        #endregion CopyUpdater

        #region InPlaceUpdater
        /// <inheritdoc/>
        public readonly bool InPlaceUpdater(ref LogRecord logRecord, ref VectorInput input, ref VectorOutput output, ref RMWInfo rmwInfo)
        {
            Debug.Assert(logRecord.HasNamespace, "Should never write a non-namespaced value with VectorSessionFunctions");

            if (input.PrepareCallback != 0)
            {
                var current = logRecord.ValueSpan;
                switch (PrepareExistingRmw(current, ref input, ref rmwInfo))
                {
                    case PrepareResult.NoChange:
                        rmwInfo.RecordUnchanged = true;
                        return true;
                    case PrepareResult.Error:
                        return false;
                }

                if (input.WriteDesiredSize != current.Length)
                {
                    var sizeInfo = new RecordSizeInfo() { FieldInfo = GetRMWModifiedFieldInfo(in logRecord, ref input) };
                    functionsState.storeWrapper.store.Log.PopulateRecordSizeInfo(ref sizeInfo);
                    if (!logRecord.TrySetContentLengths(in sizeInfo))
                    {
                        // Tsavorite falls through to NeedCopyUpdate with the same source under the same lock.
                        rmwInfo.UserData = PreparedForCopy;
                        return false;
                    }
                    current = logRecord.ValueSpan;
                }

                WritePreparedReplacement(in logRecord, current, in input);
                return true;
            }

            var key = logRecord.Key;

            var alignedValue = logRecord.ValueSpan;

            if (input.Callback == 0)
            {
                // We're doing a Metadata update

                Debug.Assert(logRecord.NamespaceBytes.Length == 1 && logRecord.NamespaceBytes[0] == VectorManager.MetadataNamespace, "Should be operating on special namespace");

                // Doing a Metadata update
                Debug.Assert(alignedValue.Length >= VectorManager.ContextMetadata.Size, "Should be ContextMetadata");
                Debug.Assert(input.CallbackContext != 0, "Should have data on VectorInput");
                Debug.Assert(key.Length == sizeof(int), "Should have int sized key for ContextMetadata");

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
                Debug.Assert(input.WriteDesiredSize <= alignedValue.Length, "Insufficient space for inplace update, this should never happen");

                unsafe
                {
                    // Callback takes: dataCallbackContext, dataPtr, dataLength
                    var callback = (delegate* unmanaged[Cdecl, SuppressGCTransition]<nint, nint, nuint, void>)input.Callback;

                    var dataPtr = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(alignedValue));
                    var dataLen = (nuint)input.WriteDesiredSize;

                    AssertAlignment(in logRecord, dataPtr);
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

        [DoesNotReturn]
        private static TReturn LogRecordOperationsNotExpected<TReturn>([CallerMemberName] string callerName = null, [CallerLineNumber] int lineNum = -1)
        => throw new InvalidOperationException($"LogRecord related operations are not expected, was: {callerName} on {lineNum}");

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
            where TKey : IKey, allows ref struct
            where TEpochAccessor : IEpochAccessor
        {
        }

        /// <inheritdoc/>
        public void PostUpsertOperation<TKey, TEpochAccessor>(TKey key, ref VectorInput input, IHeapObject valueObject, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TKey : IKey, allows ref struct
            where TEpochAccessor : IEpochAccessor
        => ObjectOperationsNotExpected<bool>();

        /// <inheritdoc/>
        public void PostRMWOperation<TKey, TEpochAccessor>(TKey key, ref VectorInput input, ref RMWInfo rmwInfo, TEpochAccessor epochAccessor)
            where TKey : IKey, allows ref struct
            where TEpochAccessor : IEpochAccessor
        {
            // Pending RMW retries reuse RMWInfo. Do not reuse scratch prepared before a retry.
            rmwInfo.UserData = 0;
        }

        /// <inheritdoc/>
        public void PostDeleteOperation<TKey, TEpochAccessor>(TKey key, ref DeleteInfo deleteInfo, TEpochAccessor epochAccessor)
            where TKey : IKey, allows ref struct
            where TEpochAccessor : IEpochAccessor
        {
        }

        /// <inheritdoc/>
        public readonly void PostInitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
        }
        #endregion

        /// <inheritdoc />
        public void PreSingleKeyConsistentRead(long hash)
            => readSessionState?.PreSingleKeyConsistentRead(hash);

        /// <inheritdoc />
        public void PostSingleKeyConsistentReadCallback()
            => readSessionState?.PostSingleKeyConsistentReadCallback();

        /// <inheritdoc />
        public void PreBatchKeyConsistentReadCallback(ReadOnlySpan<PinnedSpanByte> parameters)
            => readSessionState?.PreBatchKeyConsistentReadCallback(parameters);

        /// <inheritdoc />
        public bool PostBatchKeyConsistentReadCallback(int keyCount)
            => readSessionState != null && readSessionState.PostBatchKeyConsistentReadCallback(keyCount);

        [Conditional("DEBUG")]
        private static void AssertAlignment<TSourceLogRecord>(in TSourceLogRecord record, nint dataPtr)
            where TSourceLogRecord : ISourceLogRecord
        {
            var nsBytes = record.NamespaceBytes;
            var ns = nsBytes.Length == 1 ? nsBytes[0] : BinaryPrimitives.ReadInt32LittleEndian(nsBytes);
            Debug.Assert((((ulong)ns & (VectorManager.ContextStep - 1)) is DiskANNService.InternalIdMap or DiskANNService.Attributes) || (dataPtr % 4) == 0, "About to pass unaligned value to DiskANN");
        }
    }
}