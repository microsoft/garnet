﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
            recordInfo.ClearHasETag();
            functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
            return true;
        }
        /// <inheritdoc />
        public bool ConcurrentDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
        {
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
                if (input.Callback != null)
                {
                    input.Callback(input.Index, input.CallbackContext, (nint)value.ToPointer(), (nuint)value.Length);
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
            // Only needed when updating ContextMetadata via RMW
            return key.LengthWithoutMetadata == 0 && key.GetNamespaceInPayload() == 0;
        }
        /// <inheritdoc />
        public bool InitialUpdater(ref SpanByte key, ref VectorInput input, ref SpanByte value, ref SpanByte output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            Debug.Assert(key.LengthWithoutMetadata == 0 && key.GetNamespaceInPayload() == 0, "Should only be updating ContextMetadata");

            SpanByte newMetadataValue;
            unsafe
            {
                newMetadataValue = SpanByte.FromPinnedPointer((byte*)input.CallbackContext, VectorManager.ContextMetadata.Size);
            }

            return SpanByteFunctions<VectorInput, SpanByte, long>.DoSafeCopy(ref newMetadataValue, ref value, ref rmwInfo, ref recordInfo);
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
            return SpanByteFunctions<VectorInput, SpanByte, long>.DoSafeCopy(ref src, ref dst, ref upsertInfo, ref recordInfo, 0);
        }
        #endregion

        #region RMW
        /// <inheritdoc />
        public int GetRMWInitialValueLength(ref VectorInput input)
        => sizeof(byte) + sizeof(int) + VectorManager.ContextMetadata.Size;
        /// <inheritdoc />
        public int GetRMWModifiedValueLength(ref SpanByte value, ref VectorInput input) => throw new NotImplementedException();
        /// <inheritdoc />

        public int GetUpsertValueLength(ref SpanByte value, ref VectorInput input)
        => sizeof(byte) + sizeof(int) + value.Length;
        /// <inheritdoc />
        public bool InPlaceUpdater(ref SpanByte key, ref VectorInput input, ref SpanByte value, ref SpanByte output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            Debug.Assert(key.GetNamespaceInPayload() == 0 && key.LengthWithoutMetadata == 0, "Should be special context key");
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

        /// <inheritdoc />
        public bool NeedCopyUpdate(ref SpanByte key, ref VectorInput input, ref SpanByte oldValue, ref SpanByte output, ref RMWInfo rmwInfo) => false;

        /// <inheritdoc />
        public bool CopyUpdater(ref SpanByte key, ref VectorInput input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByte output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => throw new NotImplementedException();

        /// <inheritdoc />
        public bool PostCopyUpdater(ref SpanByte key, ref VectorInput input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByte output, ref RMWInfo rmwInfo) => throw new NotImplementedException();
        /// <inheritdoc />
        public void RMWCompletionCallback(ref SpanByte key, ref VectorInput input, ref SpanByte output, long ctx, Status status, RecordMetadata recordMetadata) { }
        #endregion

        #region Utilities
        /// <inheritdoc />
        public void ConvertOutputToHeap(ref VectorInput input, ref SpanByte output) { }
        #endregion
    }
}