// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Functions for operating against the Main Store, but for data stored as part of a Vector Set operation - not a RESP command.
    /// </summary>
    public readonly struct VectorSessionFunctions : ISessionFunctions<SpanByte, SpanByte, VectorInput, SpanByte, long>
    {
        private readonly FunctionsState functionsState;

        /// <summary>
        /// Constructor
        /// </summary>
        internal VectorSessionFunctions(FunctionsState functionsState)
        {
            this.functionsState = functionsState;
        }

        #region Deletes
        /// <inheritdoc />
        public bool SingleDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
        {
            Debug.Assert(key.MetadataSize == 1, "Should never delete a non-namespaced value with VectorSessionFunctions");

            recordInfo.ClearHasETag();
            functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
            return true;
        }
        /// <inheritdoc />
        public bool ConcurrentDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
        {
            Debug.Assert(key.MetadataSize == 1, "Should never delete a non-namespaced value with VectorSessionFunctions");

            recordInfo.ClearHasETag();
            if (!deleteInfo.RecordInfo.Modified)
                functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
            return true;
        }
        /// <inheritdoc />
        public void PostSingleDeleter(ref SpanByte key, ref DeleteInfo deleteInfo) { }
        #endregion

        #region Reads
        /// <inheritdoc />
        public bool SingleReader(ref SpanByte key, ref VectorInput input, ref SpanByte value, ref SpanByte dst, ref ReadInfo readInfo)
        {
            Debug.Assert(key.MetadataSize == 1, "Should never read a non-namespaced value with VectorSessionFunctions");

            unsafe
            {
                if (input.Callback != 0)
                {
                    var callback = (delegate* unmanaged[Cdecl, SuppressGCTransition]<int, nint, nint, nuint, void>)input.Callback;

                    callback(input.Index, input.CallbackContext, (nint)value.ToPointer(), (nuint)value.Length);
                    return true;
                }
            }

            if (input.ReadDesiredSize > 0)
            {
                Debug.Assert(dst.Length >= value.Length, "Should always have space for vector point reads");

                dst.Length = value.Length;
                value.AsReadOnlySpan(functionsState.etagState.etagSkippedStart).CopyTo(dst.AsSpan());
            }
            else
            {
                input.ReadDesiredSize = value.Length;
                if (dst.Length >= value.Length)
                {
                    value.AsReadOnlySpan(functionsState.etagState.etagSkippedStart).CopyTo(dst.AsSpan());
                    dst.Length = value.Length;
                }
            }

            return true;
        }
        /// <inheritdoc />
        public bool ConcurrentReader(ref SpanByte key, ref VectorInput input, ref SpanByte value, ref SpanByte dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        => SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);

        /// <inheritdoc />
        public void ReadCompletionCallback(ref SpanByte key, ref VectorInput input, ref SpanByte output, long ctx, Status status, RecordMetadata recordMetadata)
        {
        }
        #endregion

        #region Initial Values
        /// <inheritdoc />
        public bool NeedInitialUpdate(ref SpanByte key, ref VectorInput input, ref SpanByte output, ref RMWInfo rmwInfo)
        {
            Debug.Assert(key.MetadataSize == 1, "Should never write a non-namespaced value with VectorSessionFunctions");

            // Only needed when updating ContextMetadata or InProgressDeletes via RMW or the DiskANN RMW callback, all of which set WriteDesiredSize
            return input.WriteDesiredSize != 0;
        }
        /// <inheritdoc />
        public bool InitialUpdater(ref SpanByte key, ref VectorInput input, ref SpanByte value, ref SpanByte output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            Debug.Assert(key.MetadataSize == 1, "Should never write a non-namespaced value with VectorSessionFunctions");

            if (input.Callback == 0)
            {
                Debug.Assert(key.GetNamespaceInPayload() == 0, "Should be operating on special namespace");

                if (key.LengthWithoutMetadata == 0)
                {
                    // Operating on ContextMetadata

                    SpanByte newMetadataValue;
                    unsafe
                    {
                        newMetadataValue = SpanByte.FromPinnedPointer((byte*)input.CallbackContext, VectorManager.ContextMetadata.Size);
                    }

                    return SpanByteFunctions<VectorInput, SpanByte, long>.DoSafeCopy(ref newMetadataValue, ref value, ref rmwInfo, ref recordInfo);
                }
                else
                {
                    // Operating on InProgressDeletes
                    Debug.Assert(input.CallbackContext != 0, "Should have data on VectorInput");
                    Debug.Assert(key.LengthWithoutMetadata == 1 && key.AsReadOnlySpan()[0] == 1, "Should be working on InProgressDeletes");

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

                    VectorManager.UpdateInProgressDeletes(inProgressDeleteUpdateData, ref value, ref recordInfo, ref rmwInfo);
                    return true;
                }
            }
            else
            {
                Debug.Assert(input.WriteDesiredSize <= value.LengthWithoutMetadata, "Insufficient space for initial update, this should never happen");

                rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);

                // Must explicitly 0 before passing if we're doing an initial update
                value.AsSpan().Clear();

                unsafe
                {
                    // Callback takes: dataCallbackContext, dataPtr, dataLength
                    var callback = (delegate* unmanaged[Cdecl, SuppressGCTransition]<nint, nint, nuint, void>)input.Callback;
                    callback(input.CallbackContext, (nint)value.ToPointer(), (nuint)input.WriteDesiredSize);

                    value.ShrinkSerializedLength(input.WriteDesiredSize);
                    value.Length = input.WriteDesiredSize;
                }

                return true;
            }
        }
        /// <inheritdoc />
        public void PostInitialUpdater(ref SpanByte key, ref VectorInput input, ref SpanByte value, ref SpanByte output, ref RMWInfo rmwInfo) { }
        #endregion

        #region Writes
        /// <inheritdoc />
        public bool SingleWriter(ref SpanByte key, ref VectorInput input, ref SpanByte src, ref SpanByte dst, ref SpanByte output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
        => ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, ref recordInfo);

        /// <inheritdoc />
        public void PostSingleWriter(ref SpanByte key, ref VectorInput input, ref SpanByte src, ref SpanByte dst, ref SpanByte output, ref UpsertInfo upsertInfo, WriteReason reason) { }
        /// <inheritdoc />
        public bool ConcurrentWriter(ref SpanByte key, ref VectorInput input, ref SpanByte src, ref SpanByte dst, ref SpanByte output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        {
            Debug.Assert(key.MetadataSize == 1, "Should never write a non-namespaced value with VectorSessionFunctions");

            return SpanByteFunctions<VectorInput, SpanByte, long>.DoSafeCopy(ref src, ref dst, ref upsertInfo, ref recordInfo, 0);
        }

        #endregion

        #region RMW
        /// <inheritdoc />
        public int GetRMWInitialValueLength(ref VectorInput input)
        {
            var effectiveWriteDesiredSize = input.WriteDesiredSize;

            if (effectiveWriteDesiredSize < 0)
            {
                effectiveWriteDesiredSize = -effectiveWriteDesiredSize;
            }

            return sizeof(byte) + sizeof(int) + effectiveWriteDesiredSize;
        }
        /// <inheritdoc />
        public int GetRMWModifiedValueLength(ref SpanByte value, ref VectorInput input)
        {
            if (input.WriteDesiredSize < 0)
            {
                // Add to value, this is a dynamically sized type
                return value.Length + (-input.WriteDesiredSize);
            }

            // Constant size indicated
            return sizeof(byte) + sizeof(int) + input.WriteDesiredSize;
        }

        /// <inheritdoc />
        public int GetUpsertValueLength(ref SpanByte value, ref VectorInput input)
        => sizeof(byte) + sizeof(int) + value.Length;

        /// <inheritdoc />
        public bool InPlaceUpdater(ref SpanByte key, ref VectorInput input, ref SpanByte value, ref SpanByte output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            Debug.Assert(key.MetadataSize == 1, "Should never write a non-namespaced value with VectorSessionFunctions");

            if (input.Callback == 0)
            {
                // We're doing a Metadata or InProgressDelete update

                Debug.Assert(key.GetNamespaceInPayload() == 0, "Should be operating on special namespace");

                if (key.LengthWithoutMetadata == 0)
                {
                    // Doing a Metadata update
                    Debug.Assert(value.LengthWithoutMetadata == VectorManager.ContextMetadata.Size, "Should be ContextMetadata");
                    Debug.Assert(input.CallbackContext != 0, "Should have data on VectorInput");

                    ref readonly var oldMetadata = ref MemoryMarshal.Cast<byte, VectorManager.ContextMetadata>(value.AsReadOnlySpan())[0];

                    SpanByte newMetadataValue;
                    unsafe
                    {
                        newMetadataValue = SpanByte.FromPinnedPointer((byte*)input.CallbackContext, VectorManager.ContextMetadata.Size);
                    }

                    ref readonly var newMetadata = ref MemoryMarshal.Cast<byte, VectorManager.ContextMetadata>(newMetadataValue.AsReadOnlySpan())[0];

                    if (newMetadata.Version < oldMetadata.Version)
                    {
                        rmwInfo.Action = RMWAction.CancelOperation;
                        return false;
                    }

                    return SpanByteFunctions<VectorInput, SpanByte, long>.DoSafeCopy(ref newMetadataValue, ref value, ref rmwInfo, ref recordInfo);
                }
                else
                {
                    // Doing an InProgressDelete update
                    Debug.Assert(input.CallbackContext != 0, "Should have data on VectorInput");
                    Debug.Assert(key.LengthWithoutMetadata == 1 && key.AsReadOnlySpan()[0] == 1, "Should be working on InProgressDeletes");

                    Span<byte> inProgressDeleteUpdateData;
                    bool adding;

                    unsafe
                    {
                        var len = BinaryPrimitives.ReadInt32LittleEndian(new Span<byte>(((byte*)input.CallbackContext + sizeof(long)), sizeof(int)));
                        adding = len > 0;
                        if (!adding)
                        {
                            len = -len;
                        }

                        inProgressDeleteUpdateData = new Span<byte>((byte*)input.CallbackContext, sizeof(ulong) + sizeof(int) + len);
                    }

                    VectorManager.UpdateInProgressDeletes(inProgressDeleteUpdateData, ref value, ref recordInfo, ref rmwInfo);
                    return true;
                }
            }
            else
            {
                Debug.Assert(input.WriteDesiredSize <= value.LengthWithoutMetadata, "Insufficient space for inplace update, this should never happen");

                unsafe
                {
                    // Callback takes: dataCallbackContext, dataPtr, dataLength
                    var callback = (delegate* unmanaged[Cdecl, SuppressGCTransition]<nint, nint, nuint, void>)input.Callback;
                    callback(input.CallbackContext, (nint)value.ToPointer(), (nuint)input.WriteDesiredSize);
                }

                return true;
            }
        }

        /// <inheritdoc />
        public bool NeedCopyUpdate(ref SpanByte key, ref VectorInput input, ref SpanByte oldValue, ref SpanByte output, ref RMWInfo rmwInfo)
        => input.WriteDesiredSize != 0;

        /// <inheritdoc />
        public bool CopyUpdater(ref SpanByte key, ref VectorInput input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByte output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            Debug.Assert(key.MetadataSize == 1, "Should never write a non-namespaced value with VectorSessionFunctions");

            if (input.Callback == 0)
            {
                // We're doing a Metadata or InProgressDelete update

                Debug.Assert(key.GetNamespaceInPayload() == 0, "Should be operating on special namespace");

                if (key.LengthWithoutMetadata == 0)
                {
                    // Doing a Metadata update
                    Debug.Assert(oldValue.LengthWithoutMetadata == VectorManager.ContextMetadata.Size, "Should be ContextMetadata");
                    Debug.Assert(newValue.LengthWithoutMetadata == VectorManager.ContextMetadata.Size, "Should be ContextMetadata");
                    Debug.Assert(input.CallbackContext != 0, "Should have data on VectorInput");

                    ref readonly var oldMetadata = ref MemoryMarshal.Cast<byte, VectorManager.ContextMetadata>(oldValue.AsReadOnlySpan())[0];

                    SpanByte newMetadataValue;
                    unsafe
                    {
                        newMetadataValue = SpanByte.FromPinnedPointer((byte*)input.CallbackContext, VectorManager.ContextMetadata.Size);
                    }

                    ref readonly var newMetadata = ref MemoryMarshal.Cast<byte, VectorManager.ContextMetadata>(newMetadataValue.AsReadOnlySpan())[0];

                    if (newMetadata.Version < oldMetadata.Version)
                    {
                        rmwInfo.Action = RMWAction.CancelOperation;
                        return false;
                    }

                    return SpanByteFunctions<VectorInput, SpanByte, long>.DoSafeCopy(ref newMetadataValue, ref newValue, ref rmwInfo, ref recordInfo);
                }
                else
                {
                    // Doing an InProgressDelete update
                    Debug.Assert(input.CallbackContext != 0, "Should have data on VectorInput");
                    Debug.Assert(key.LengthWithoutMetadata == 1 && key.AsReadOnlySpan()[0] == 1, "Should be working on InProgressDeletes");

                    Span<byte> inProgressDeleteUpdateData;
                    bool adding;

                    oldValue.CopyTo(ref newValue);

                    unsafe
                    {
                        var len = BinaryPrimitives.ReadInt32LittleEndian(new Span<byte>(((byte*)input.CallbackContext + sizeof(long)), sizeof(int)));
                        adding = len > 0;
                        if (!adding)
                        {
                            len = -len;
                        }

                        inProgressDeleteUpdateData = new Span<byte>((byte*)input.CallbackContext, sizeof(ulong) + sizeof(int) + len);
                    }

                    VectorManager.UpdateInProgressDeletes(inProgressDeleteUpdateData, ref newValue, ref recordInfo, ref rmwInfo);
                    return true;
                }
            }
            else
            {
                Debug.Assert(input.WriteDesiredSize <= newValue.LengthWithoutMetadata, "Insufficient space for copy update, this should never happen");
                Debug.Assert(input.WriteDesiredSize <= oldValue.LengthWithoutMetadata, "Insufficient space for copy update, this should never happen");

                oldValue.AsReadOnlySpan().CopyTo(newValue.AsSpan());

                unsafe
                {
                    // Callback takes: dataCallbackContext, dataPtr, dataLength
                    var callback = (delegate* unmanaged[Cdecl, SuppressGCTransition]<nint, nint, nuint, void>)input.Callback;
                    callback(input.CallbackContext, (nint)newValue.ToPointer(), (nuint)input.WriteDesiredSize);
                }

                return true;
            }
        }

        /// <inheritdoc />
        public bool PostCopyUpdater(ref SpanByte key, ref VectorInput input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByte output, ref RMWInfo rmwInfo)
        => true;
        /// <inheritdoc />
        public void RMWCompletionCallback(ref SpanByte key, ref VectorInput input, ref SpanByte output, long ctx, Status status, RecordMetadata recordMetadata) { }
        #endregion

        #region Utilities
        /// <inheritdoc />
        public void ConvertOutputToHeap(ref VectorInput input, ref SpanByte output) { }
        #endregion
    }
}