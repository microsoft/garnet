// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// This implementation of <see cref="ISessionFunctions{TInput, TOutput, TContext}"/> is used during compaction, iteration, and any other
    /// operations that require a session for ContinuePending but do operations directly on the <see cref="LogRecord"/> rather than calling
    /// <see cref="ISessionFunctions{TInput, TOutput, TContext}"/> methods for record operations (Delete methods simply return true to let
    /// Tsavorite proceed with the delete).
    /// </summary>
    /// <remarks>
    /// Because this is used for copy operations, the <see cref="GetUpsertFieldInfo{TSourceLogRecord}(ReadOnlySpan{byte}, in TSourceLogRecord, ref TInput)"/>,
    /// <see cref="InitialWriter{TSourceLogRecord}(ref LogRecord, in RecordSizeInfo, ref TInput, in TSourceLogRecord, ref TOutput, ref UpsertInfo)"/>, and
    /// <see cref="InPlaceWriter{TSourceLogRecord}(ref LogRecord, in RecordSizeInfo, ref TInput, in TSourceLogRecord, ref TOutput, ref UpsertInfo)"/>, and
    /// methods are implemented to allow for copy of log records via Upsert, but no other methods are implemented.
    /// </remarks>
    /// <typeparam name="TInput"></typeparam>
    /// <typeparam name="TOutput"></typeparam>
    /// <typeparam name="TContext"></typeparam>
    internal struct NoOpSessionFunctions<TInput, TOutput, TContext> : ISessionFunctions<TInput, TOutput, TContext>
    {
        public readonly bool InitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo) => true;

        public readonly void PostInitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo) { }

        public readonly bool InPlaceDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo) => true;

        public readonly bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
            => throw new NotImplementedException("InPlaceWriter(ReadOnlySpan<byte> value) is not supported in this ISessionFunctions implementation");

        public readonly bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
            => throw new NotImplementedException("InPlaceWriter(IHeapObject value) is not supported in this ISessionFunctions implementation");

        public readonly bool InPlaceWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            // This includes ETag and Expiration
            return dstLogRecord.TryCopyFrom(in inputLogRecord, in sizeInfo);
        }

        public readonly bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;

        public readonly bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;

        public readonly bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;
        public readonly void PostInitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) { }

        public readonly bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;

        public readonly bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;

        public readonly bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;

        public readonly void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata) { }

        public readonly void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata) { }

        public readonly RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TInput input)
            where TSourceLogRecord : ISourceLogRecord
             => throw new NotImplementedException("GetRMWModifiedFieldInfo is not supported in this ISessionFunctions implementation");
        public readonly RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref TInput input)
            => throw new NotImplementedException("GetRMWInitialFieldInfo is not supported in this ISessionFunctions implementation");
        public readonly RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref TInput input)
            => throw new NotImplementedException("GetUpsertFieldInfo(ReadOnlySpan<byte> value) is not supported in this ISessionFunctions implementation");
        public readonly RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref TInput input)
            => throw new NotImplementedException("IHeapObject value) is not supported in this ISessionFunctions implementation");
        public readonly RecordFieldInfo GetUpsertFieldInfo<TSourceLogRecord>(ReadOnlySpan<byte> key, in TSourceLogRecord inputLogRecord, ref TInput input)
            where TSourceLogRecord : ISourceLogRecord
            => new() { KeySize = key.Length, ValueSize = inputLogRecord.Info.ValueIsObject ? ObjectIdMap.ObjectIdSize : inputLogRecord.ValueSpan.Length, ValueIsObject = inputLogRecord.Info.ValueIsObject };

        /// <summary>
        /// No reads during compaction
        /// </summary>
        public readonly bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput dst, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;

        /// <summary>
        /// Write compacted live value to store
        /// </summary>
        public readonly bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo) => true;

        /// <summary>
        /// Write compacted live value to store
        /// </summary>
        public readonly bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo) => true;

        public readonly bool InitialWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            // This includes ETag and Expiration
            return dstLogRecord.TryCopyFrom(in inputLogRecord, in sizeInfo);
        }

        public readonly void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo) { }
        public readonly void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo) { }
        public readonly void PostInitialWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        { }

        public readonly void ConvertOutputToHeap(ref TInput input, ref TOutput output) { }
    }
}