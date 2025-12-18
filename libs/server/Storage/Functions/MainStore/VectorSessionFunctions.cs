// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Functions for operating against the Main Store, but for data stored as part of a Vector Set operation - not a RESP command.
    /// </summary>
    public readonly struct VectorSessionFunctions : ISessionFunctions<VectorInput, PinnedSpanByte, long>
    {
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
        public readonly bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref VectorInput input, ref PinnedSpanByte output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
        => throw new NotImplementedException();

        /// <inheritdoc/>
        public readonly void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref VectorInput input, ref PinnedSpanByte output, long ctx, Status status, RecordMetadata recordMetadata)
        => throw new NotImplementedException();
        #endregion reads

        #region Upserts
        /// <inheritdoc/>
        public readonly bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, ReadOnlySpan<byte> srcValue, ref PinnedSpanByte output, ref UpsertInfo upsertInfo)
        => throw new NotImplementedException();

        /// <inheritdoc/>
        public readonly bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, IHeapObject srcValue, ref PinnedSpanByte output, ref UpsertInfo upsertInfo)
        => throw new NotImplementedException();

        /// <inheritdoc/>
        public readonly bool InitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, in TSourceLogRecord inputLogRecord, ref PinnedSpanByte output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        => throw new NotImplementedException();

        /// <inheritdoc/>
        public readonly void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, ReadOnlySpan<byte> srcValue, ref PinnedSpanByte output, ref UpsertInfo upsertInfo)
        => throw new NotImplementedException();

        /// <inheritdoc/>
        public readonly void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, IHeapObject srcValue, ref PinnedSpanByte output, ref UpsertInfo upsertInfo)
        => throw new NotImplementedException();

        /// <inheritdoc/>
        public readonly void PostInitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, in TSourceLogRecord inputLogRecord, ref PinnedSpanByte output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        => throw new NotImplementedException();

        /// <inheritdoc/>
        public readonly bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, ReadOnlySpan<byte> newValue, ref PinnedSpanByte output, ref UpsertInfo upsertInfo)
        => throw new NotImplementedException();

        /// <inheritdoc/>
        public readonly bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, IHeapObject newValue, ref PinnedSpanByte output, ref UpsertInfo upsertInfo)
        => throw new NotImplementedException();

        /// <inheritdoc/>
        public readonly bool InPlaceWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, in TSourceLogRecord inputLogRecord, ref PinnedSpanByte output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        => throw new NotImplementedException();
        #endregion Upserts

        #region RMWs
        #region Variable Length
        /// <summary>Length of resulting value object when performing RMW modification of value using given input</summary>
        public readonly RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref VectorInput input)
            where TSourceLogRecord : ISourceLogRecord
        => throw new NotImplementedException();

        /// <summary>Initial expected length of value object when populated by RMW using given input</summary>
        public readonly RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref VectorInput input)
        => throw new NotImplementedException();

        /// <summary>Length of value object, when populated by Upsert using given value and input</summary>
        public readonly RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref VectorInput input)
        => throw new NotImplementedException();

        /// <summary>Length of value object, when populated by Upsert using given value and input</summary>
        public readonly RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref VectorInput input)
        => throw new NotImplementedException();

        /// <summary>Length of value object, when populated by Upsert using given log record</summary>
        public readonly RecordFieldInfo GetUpsertFieldInfo<TSourceLogRecord>(ReadOnlySpan<byte> key, in TSourceLogRecord inputLogRecord, ref VectorInput input)
            where TSourceLogRecord : ISourceLogRecord
        => throw new NotImplementedException();
        #endregion Variable Length

        #region InitialUpdater
        /// <inheritdoc/>
        public readonly bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref VectorInput input, ref PinnedSpanByte output, ref RMWInfo rmwInfo)
        => throw new NotImplementedException();

        /// <inheritdoc/>
        public readonly bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, ref PinnedSpanByte output, ref RMWInfo rmwInfo)
        => throw new NotImplementedException();

        /// <inheritdoc/>
        public readonly void PostInitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, ref PinnedSpanByte output, ref RMWInfo rmwInfo)
        => throw new NotImplementedException();
        #endregion InitialUpdater

        #region CopyUpdater
        /// <inheritdoc/>
        public readonly bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref VectorInput input, ref PinnedSpanByte output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        => throw new NotImplementedException();

        /// <inheritdoc/>
        public readonly bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, ref PinnedSpanByte output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        => throw new NotImplementedException();

        /// <inheritdoc/>
        public readonly bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, ref PinnedSpanByte output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        => throw new NotImplementedException();
        #endregion CopyUpdater

        #region InPlaceUpdater
        /// <inheritdoc/>
        public readonly bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref VectorInput input, ref PinnedSpanByte output, ref RMWInfo rmwInfo)
        => throw new NotImplementedException();
        #endregion InPlaceUpdater

        /// <inheritdoc/>
        public readonly void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref VectorInput input, ref PinnedSpanByte output, long ctx, Status status, RecordMetadata recordMetadata)
        => throw new NotImplementedException();
        #endregion RMWs

        #region Deletes
        /// <inheritdoc/>
        public readonly bool InitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        => throw new NotImplementedException();

        /// <inheritdoc/>
        public readonly void PostInitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        => throw new NotImplementedException();

        /// <inheritdoc/>
        public readonly bool InPlaceDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        => throw new NotImplementedException();
        #endregion Deletes

        #region Utilities
        /// <inheritdoc/>
        public readonly void ConvertOutputToHeap(ref VectorInput input, ref PinnedSpanByte output)
        => throw new NotImplementedException();
        #endregion Utilities
    }
}