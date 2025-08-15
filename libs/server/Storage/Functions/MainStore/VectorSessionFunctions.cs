// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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
        public bool SingleDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) => throw new NotImplementedException();
        /// <inheritdoc />
        public void PostSingleDeleter(ref SpanByte key, ref DeleteInfo deleteInfo) => throw new NotImplementedException();
        /// <inheritdoc />
        public bool ConcurrentDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) => throw new NotImplementedException();
        #endregion

        #region Reads
        /// <inheritdoc />
        public bool SingleReader(ref SpanByte key, ref VectorInput input, ref SpanByte value, ref SpanByte dst, ref ReadInfo readInfo)
        {
            Debug.Assert(readInfo.RecordInfo.Hidden, "Should never read a non-hidden value with VectorSessionFunctions");
            Debug.Assert(dst.Length >= value.Length, "Should always have space for vector point reads");

            dst.Length = value.Length;
            value.AsReadOnlySpan(functionsState.etagState.etagSkippedStart).CopyTo(dst.AsSpan());

            return true;
        }
        /// <inheritdoc />
        public bool ConcurrentReader(ref SpanByte key, ref VectorInput input, ref SpanByte value, ref SpanByte dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            Debug.Assert(readInfo.RecordInfo.Hidden, "Should never read a non-hidden value with VectorSessionFunctions");
            Debug.Assert(dst.Length >= value.Length, "Should always have space for vector point reads");

            dst.Length = value.Length;
            value.AsReadOnlySpan(functionsState.etagState.etagSkippedStart).CopyTo(dst.AsSpan());

            return true;
        }
        /// <inheritdoc />
        public void ReadCompletionCallback(ref SpanByte key, ref VectorInput input, ref SpanByte output, long ctx, Status status, RecordMetadata recordMetadata)
        {
        }
        #endregion

        #region Initial Values
        /// <inheritdoc />
        public bool NeedInitialUpdate(ref SpanByte key, ref VectorInput input, ref SpanByte output, ref RMWInfo rmwInfo)
        => false;
        /// <inheritdoc />
        public bool InitialUpdater(ref SpanByte key, ref VectorInput input, ref SpanByte value, ref SpanByte output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => throw new NotImplementedException();
        /// <inheritdoc />
        public void PostInitialUpdater(ref SpanByte key, ref VectorInput input, ref SpanByte value, ref SpanByte output, ref RMWInfo rmwInfo) => throw new NotImplementedException();
        #endregion

        #region Writes
        /// <inheritdoc />
        public bool SingleWriter(ref SpanByte key, ref VectorInput input, ref SpanByte src, ref SpanByte dst, ref SpanByte output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
        {
            recordInfo.Hidden = true;
            return SpanByteFunctions<VectorInput, SpanByte, long>.DoSafeCopy(ref src, ref dst, ref upsertInfo, ref recordInfo, 0);
        }

        /// <inheritdoc />
        public void PostSingleWriter(ref SpanByte key, ref VectorInput input, ref SpanByte src, ref SpanByte dst, ref SpanByte output, ref UpsertInfo upsertInfo, WriteReason reason) { }
        /// <inheritdoc />
        public bool ConcurrentWriter(ref SpanByte key, ref VectorInput input, ref SpanByte src, ref SpanByte dst, ref SpanByte output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        {
            recordInfo.Hidden = true;
            return SpanByteFunctions<VectorInput, SpanByte, long>.DoSafeCopy(ref src, ref dst, ref upsertInfo, ref recordInfo, 0);
        }
        #endregion

        #region RMW
        /// <inheritdoc />
        public bool CopyUpdater(ref SpanByte key, ref VectorInput input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByte output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => throw new NotImplementedException();
        /// <inheritdoc />
        public int GetRMWInitialValueLength(ref VectorInput input) => throw new NotImplementedException();
        /// <inheritdoc />
        public int GetRMWModifiedValueLength(ref SpanByte value, ref VectorInput input) => throw new NotImplementedException();
        /// <inheritdoc />
        public int GetUpsertValueLength(ref SpanByte value, ref VectorInput input)
        => sizeof(int) + value.Length;
        /// <inheritdoc />
        public bool InPlaceUpdater(ref SpanByte key, ref VectorInput input, ref SpanByte value, ref SpanByte output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => throw new NotImplementedException();
        /// <inheritdoc />
        public bool NeedCopyUpdate(ref SpanByte key, ref VectorInput input, ref SpanByte oldValue, ref SpanByte output, ref RMWInfo rmwInfo) => throw new NotImplementedException();
        /// <inheritdoc />
        public bool PostCopyUpdater(ref SpanByte key, ref VectorInput input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByte output, ref RMWInfo rmwInfo) => throw new NotImplementedException();
        /// <inheritdoc />
        public void RMWCompletionCallback(ref SpanByte key, ref VectorInput input, ref SpanByte output, long ctx, Status status, RecordMetadata recordMetadata) => throw new NotImplementedException();
        #endregion

        #region Utilities
        /// <inheritdoc />
        public void ConvertOutputToHeap(ref VectorInput input, ref SpanByte output) => throw new NotImplementedException();
        #endregion
    }
}
