// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Callback functions to Tsavorite
    /// </summary>
    public interface ISessionFunctions<TInput, TOutput, TContext> : IVariableLengthInput<TInput>
    {
        #region Reads
        /// <summary>
        /// Read the record by copying all or part of it to <paramref name="output"/>. 
        /// </summary>
        /// <param name="srcLogRecord">The log record being read</param>
        /// <param name="input">The user input for computing <paramref name="output"/> from the record value</param>
        /// <param name="output">Receives the output of the operation, if any</param>
        /// <param name="readInfo">Information about this read operation and its context</param>
        /// <returns>True if the value was available, else false (e.g. the value was expired)</returns>
        bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord;

        /// <summary>
        /// Read completion
        /// </summary>
        /// <param name="diskLogRecord">The log record that was read from disk</param>
        /// <param name="input">The user input that was used in the read operation</param>
        /// <param name="output">The result of the read operation; if this is a struct, then it will be a temporary and should be copied to <paramref name="ctx"/></param>
        /// <param name="ctx">The application context passed through the pending operation</param>
        /// <param name="status">The result of the pending operation</param>
        /// <param name="recordMetadata">Metadata for the record</param>
        void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata);
        #endregion reads

        #region Upserts
        /// <summary>
        /// Non-concurrent writer for Span value; called on an Upsert that does not find the key so does an insert or finds the key's record in the immutable region so does a read/copy/update (RCU).
        /// </summary>
        /// <param name="logRecord">The destination log record</param>
        /// <param name="sizeInfo">The size information for this record's fields</param>
        /// <param name="input">The user input to be used for computing <paramref name="output"/></param>
        /// <param name="srcValue">The input Span to be copied to the record value</param>
        /// <param name="output">The location where the result of the update may be placed</param>
        /// <param name="upsertInfo">Information about this update operation and its context</param>
        /// <returns>True if the write was performed, else false (e.g. cancellation)</returns>
        bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo);

        /// <summary>
        /// Non-concurrent writer for Object values; called on an Upsert that does not find the key so does an insert or finds the key's record in the immutable region so does a read/copy/update (RCU).
        /// </summary>
        /// <param name="logRecord">The destination log record</param>
        /// <param name="sizeInfo">The size information for this record's fields</param>
        /// <param name="input">The user input to be used for computing <paramref name="output"/></param>
        /// <param name="srcValue">The input Object to be copied to the record value</param>
        /// <param name="output">The location where the result of the update may be placed</param>
        /// <param name="upsertInfo">Information about this update operation and its context</param>
        /// <returns>True if the write was performed, else false (e.g. cancellation)</returns>
        bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo);

        /// <summary>
        /// Non-concurrent writer for Object values; called on an Upsert that does not find the key so does an insert or finds the key's record in the immutable region so does a read/copy/update (RCU).
        /// </summary>
        /// <param name="logRecord">The destination log record</param>
        /// <param name="sizeInfo">The size information for this record's fields</param>
        /// <param name="input">The user input to be used for computing <paramref name="output"/></param>
        /// <param name="inputLogRecord">The log record passed to Upsert, to be copied to the destination record</param>
        /// <param name="output">The location where the result of the update may be placed</param>
        /// <param name="upsertInfo">Information about this update operation and its context</param>
        /// <returns>True if the write was performed, else false (e.g. cancellation)</returns>
        bool InitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord;

        /// <summary>
        /// Called after InitialWriter when a record has been successfully inserted at the tail of the log.
        /// </summary>
        /// <param name="logRecord">The destination log record</param>
        /// <param name="sizeInfo">The size information for this record's fields</param>
        /// <param name="input">The user input that was used to compute <paramref name="output"/></param>
        /// <param name="srcValue">The input Span that was to be copied to the record value</param>
        /// <param name="output">The location where the result of the update may be placed</param>
        /// <param name="upsertInfo">Information about this update operation and its context</param>
        void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo);

        /// <summary>
        /// Called after InitialWriter when a record has been successfully inserted at the tail of the log.
        /// </summary>
        /// <param name="logRecord">The destination log record</param>
        /// <param name="sizeInfo">The size information for this record's fields</param>
        /// <param name="input">The user input that was used to compute <paramref name="output"/></param>
        /// <param name="srcValue">The input Object that was to be copied to the record value</param>
        /// <param name="output">The location where the result of the update may be placed</param>
        /// <param name="upsertInfo">Information about this update operation and its context</param>
        void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo);

        /// <summary>
        /// Called after InitialWriter when a record has been successfully inserted at the tail of the log.
        /// </summary>
        /// <param name="logRecord">The destination log record</param>
        /// <param name="sizeInfo">The size information for this record's fields</param>
        /// <param name="input">The user input that was used to compute <paramref name="output"/></param>
        /// <param name="inputLogRecord">The input LogRecord that was to be copied to the record value</param>
        /// <param name="output">The location where the result of the update may be placed</param>
        /// <param name="upsertInfo">Information about this update operation and its context</param>
        void PostInitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord;

        /// <summary>
        /// Concurrent writer; called on an Upsert that is in-place updating a record in the mutable range.
        /// </summary>
        /// <param name="logRecord">The destination log record</param>
        /// <param name="sizeInfo">The size information for this record's fields</param>
        /// <param name="input">The user input to be used for computing the destination record's value</param>
        /// <param name="newValue">The Span value passed to Upsert, to be copied to the destination record</param>
        /// <param name="output">The location where the result of the update may be placed</param>
        /// <param name="upsertInfo">Information about this update operation and its context</param>
        /// <returns>True if the value was written, else false</returns>
        /// <remarks>If the value is shrunk in-place, the caller must first zero the data that is no longer used, to ensure log-scan correctness.</remarks>
        bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> newValue, ref TOutput output, ref UpsertInfo upsertInfo);

        /// <summary>
        /// Concurrent writer; called on an Upsert that is in-place updating a record in the mutable range.
        /// </summary>
        /// <param name="logRecord">The destination log record</param>
        /// <param name="sizeInfo">The size information for this record's fields</param>
        /// <param name="input">The user input to be used for computing the destination record's value</param>
        /// <param name="newValue">The value passed to Upsert, to be copied to the destination record</param>
        /// <param name="output">The location where the result of the update may be placed</param>
        /// <param name="upsertInfo">Information about this update operation and its context</param>
        /// <returns>True if the value was written, else false</returns>
        /// <remarks>If the value is shrunk in-place, the caller must first zero the data that is no longer used, to ensure log-scan correctness.</remarks>
        bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, IHeapObject newValue, ref TOutput output, ref UpsertInfo upsertInfo);

        /// <summary>
        /// Concurrent writer; called on an Upsert that is in-place updating a record in the mutable range. The caller should be aware of ETag and Expiration in the source record.
        /// </summary>
        /// <param name="logRecord">The destination log record</param>
        /// <param name="sizeInfo">The size information for this record's fields</param>
        /// <param name="input">The user input to be used for computing the destination record's value</param>
        /// <param name="inputLogRecord">The log record passed to Upsert, to be copied to the destination record</param>
        /// <param name="output">The location where the result of the update may be placed</param>
        /// <param name="upsertInfo">Information about this update operation and its context</param>
        /// <returns>True if the value was written, else false</returns>
        /// <remarks>If the value is shrunk in-place, the caller must first zero the data that is no longer used, to ensure log-scan correctness.</remarks>
        bool InPlaceWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord;

        /// <summary>
        /// Called after the Upsert operation but before we unlock the record (if it was ephemerally locked).
        /// </summary>
        /// <remarks>
        /// This is always called after the operation whether it succeeds or not (including when it has gone pending), so must have information indicating whether
        /// the action is to be performed (such as by checking <see cref="UpsertInfo.UserData"/>
        /// </remarks>
        void PostUpsertOperation<TEpochAccessor>(ReadOnlySpan<byte> key, ref TInput input, ReadOnlySpan<byte> valueSpan, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor;

        /// <summary>
        /// Called after the Upsert operation but before we unlock the record (if it was ephemerally locked).
        /// </summary>
        /// <remarks>
        /// This is always called after the operation whether it succeeds or not, so must have information indicating whether
        /// the action is to be performed (such as by checking <see cref="UpsertInfo.UserData"/>
        /// </remarks>
        void PostUpsertOperation<TEpochAccessor>(ReadOnlySpan<byte> key, ref TInput input, IHeapObject valueObject, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor;

        #endregion Upserts

        #region RMWs
        #region InitialUpdater
        /// <summary>
        /// Whether we need to invoke initial-update for RMW
        /// </summary>
        /// <param name="key">The key for this record; this is the key passed to Upsert as we don't have a log record yet.</param>
        /// <param name="input">The user input to be used for computing the updated value</param>
        /// <param name="output">The location where the result of the <paramref name="input"/> operation is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo);

        /// <summary>
        /// Initial update for RMW (insert at the tail of the log).
        /// </summary>
        /// <param name="logRecord">The destination log record</param>
        /// <param name="sizeInfo">The size information for this record's fields</param>
        /// <param name="input">The user input to be used to create the destination record's value</param>
        /// <param name="output">The location where the output of the operation, if any, is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        /// <returns>True if the write was performed, else false (e.g. cancellation)</returns>
        bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo);

        /// <summary>
        /// Called after a record containing an initial update for RMW has been successfully inserted at the tail of the log.
        /// </summary>
        /// <param name="logRecord">The log record that was created</param>
        /// <param name="sizeInfo">The size information for this record's fields</param>
        /// <param name="input">The user input to be used to create the destination record's value</param>
        /// <param name="output">The location where the output of the operation, if any, is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        void PostInitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo);
        #endregion InitialUpdater

        #region CopyUpdater
        /// <summary>
        /// Whether we need to invoke copy-update for RMW
        /// </summary>
        /// <param name="srcLogRecord">The source record being copied from</param>
        /// <param name="input">The user input to be used for computing the updated value</param>
        /// <param name="output">The location where the output of the operation, if any, is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord;

        /// <summary>
        /// Copy-update for RMW (RCU (Read-Copy-Update) to the tail of the log)
        /// </summary>
        /// <param name="srcLogRecord">The source record being copied from</param>
        /// <param name="dstLogRecord">The destination log record being created</param>
        /// <param name="sizeInfo">The size information for this record's fields</param>
        /// <param name="input">The user input to be used to create the destination record's value</param>
        /// <param name="output">The location where the output of the operation, if any, is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        /// <returns>True if the write was performed, else false (e.g. cancellation)</returns>
        bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord;

        /// <summary>
        /// Called after a record containing an RCU (Read-Copy-Update) for RMW has been successfully inserted at the tail of the log.
        /// </summary>
        /// <param name="srcLogRecord">The source record being copied from</param>
        /// <param name="dstLogRecord">The destination log record being created</param>
        /// <param name="sizeInfo">The size information for this record's fields</param>
        /// <param name="input">The user input to be used to create the destination record's value</param>
        /// <param name="output">The location where the output of the operation, if any, is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        /// <returns>This is the only Post* method that returns non-void. The bool functions the same as CopyUpdater; this is because we do not want to modify
        /// objects in-memory until we know the "insert at tail" is successful. Therefore, we allow a false return as a signal to inspect <paramref name="rmwInfo.Action"/>
        /// and handle <see cref="RMWAction.ExpireAndStop"/>.</returns>
        bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord;
        #endregion CopyUpdater

        #region InPlaceUpdater
        /// <summary>
        /// In-place update for RMW
        /// </summary>
        /// <param name="logRecord">The log record that is being updated</param>
        /// <param name="sizeInfo">The size information for this record's fields</param>
        /// <param name="input">The user input to be used to create the destination record's value</param>
        /// <param name="output">The location where the output of the operation, if any, is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        /// <returns>True if the value was successfully updated, else false (e.g. the value was expired)</returns>
        /// <remarks>If the value is shrunk in-place, the caller must first zero the data that is no longer used, to ensure log-scan correctness.</remarks>
        bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo);
        #endregion InPlaceUpdater

        /// <summary>
        /// Called after the RMW operation, but before we unlock the record (if it was ephemerally locked).
        /// </summary>
        /// <remarks>
        /// This is always called after the operation whether it succeeds or not (including when it has gone pending), so must have information indicating whether
        /// the action is to be performed (such as by checking <see cref="RMWInfo.UserData"/>
        /// </remarks>
        void PostRMWOperation<TEpochAccessor>(ReadOnlySpan<byte> key, ref TInput input, ref RMWInfo rmwInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor;

        /// <summary>
        /// RMW completion
        /// </summary>
        /// <param name="diskLogRecord">The log record that was read from disk</param>
        /// <param name="input">The user input to be used to create the destination record's value</param>
        /// <param name="output">The location where the output of the operation, if any, is to be copied</param>
        /// <param name="ctx">The application context passed through the pending operation</param>
        /// <param name="status">The result of the pending operation</param>
        /// <param name="recordMetadata">The metadata of the modified or inserted record</param>
        void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata);
        #endregion RMWs

        #region Deletes
        /// <summary>
        /// Single deleter; called on a Delete that does not find the record in the mutable range and so inserts a new record.
        /// </summary>
        /// <param name="logRecord">The log record that is being created with a tombstone</param>
        /// <param name="deleteInfo">Information about this update operation and its context</param>
        /// <remarks>For Object Value types, Dispose() can be called here. If recordInfo.Invalid is true, this is called after the record was allocated and populated,
        /// but could not be appended at the end of the log.</remarks>
        /// <returns>True if the deleted record should be added, else false (e.g. cancellation)</returns>
        bool InitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo);

        /// <summary>
        /// Called after a record marking a Delete (with Tombstone set) has been successfully inserted at the tail of the log.
        /// </summary>
        /// <param name="logRecord">The log record that was created with a tombstone</param>
        /// <param name="deleteInfo">Information about this update operation and its context</param>
        /// <remarks>This does not have the address of the record that contains the value at 'key'; Delete does not retrieve records below HeadAddress, so
        ///     the last record we have in the 'key' chain may belong to 'key' or may be a collision.</remarks>
        void PostInitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo);

        /// <summary>
        /// Concurrent deleter; called on a Delete that finds the record in the mutable range.
        /// </summary>
        /// <param name="logRecord">The log record that is being deleted in-place</param>
        /// <param name="deleteInfo">Information about this update operation and its context</param>
        /// <remarks>For Object Value types, Dispose() can be called here. If logRecord.Info.Invalid is true, this is called after the record was allocated and populated,
        /// but could not be appended at the end of the log.</remarks>
        /// <returns>True if the value was successfully deleted, else false (e.g. the record was sealed)</returns>
        bool InPlaceDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo);

        /// <summary>
        /// Called after the Delete operation but before we unlock the record (if it was ephemerally locked).
        /// </summary>
        /// <remarks>
        /// This is always called after the operation whether it succeeds or not, so must have information indicating whether
        /// the action is to be performed (such as by checking <see cref="DeleteInfo.UserData"/>
        /// </remarks>
        void PostDeleteOperation<TEpochAccessor>(ReadOnlySpan<byte> key, ref DeleteInfo deleteInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor;
        #endregion Deletes

        #region Utilities
        /// <summary>
        /// Called by Tsavorite when the operation goes pending, so the app can signal to itself that any pinned
        /// buffer in the Output is no longer valid and a heap-based buffer must be created.
        /// </summary>
        /// <param name="output"></param>
        void ConvertOutputToHeap(ref TInput input, ref TOutput output);
        #endregion Utilities
    }

    /// <summary>
    /// Callback functions to Tsavorite (two-param version)
    /// </summary>
    public interface ISessionFunctions<TInputOutput> : ISessionFunctions<TInputOutput, Empty>
    {
    }

    /// <summary>
    /// Callback functions to Tsavorite (two-param version with context)
    /// </summary>
    public interface ISessionFunctions<TInputOutput, TContext> : ISessionFunctions<TInputOutput, TInputOutput, TContext>
    {
    }
}