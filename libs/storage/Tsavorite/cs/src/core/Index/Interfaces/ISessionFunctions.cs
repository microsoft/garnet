// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Callback functions to Tsavorite
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TInput"></typeparam>
    /// <typeparam name="TOutput"></typeparam>
    /// <typeparam name="TContext"></typeparam>
    public interface ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
    {
        #region Reads
        /// <summary>
        /// Non-concurrent reader. 
        /// </summary>
        /// <param name="logRecord">The log record being read</param>
        /// <param name="input">The user input for computing <paramref name="output"/> from the record value</param>
        /// <param name="output">Receives the output of the operation, if any</param>
        /// <param name="readInfo">Information about this read operation and its context</param>
        /// <returns>True if the value was available, else false (e.g. the value was expired)</returns>
        bool SingleReader(ref LogRecord logRecord, ref TInput input, ref TOutput output, ref ReadInfo readInfo);

        /// <summary>
        /// Concurrent reader
        /// </summary>
        /// <param name="logRecord">The log record being read</param>
        /// <param name="input">The user input for computing <paramref name="output"/> from the record value</param>
        /// <param name="output">Receives the output of the operation, if any</param>
        /// <param name="readInfo">Information about this read operation and its context</param>
        /// <param name="recordInfo">A reference to the RecordInfo for the record; used for variable-length record length modification</param>
        /// <returns>True if the value was available, else false (e.g. the value was expired)</returns>
        bool ConcurrentReader(ref LogRecord logRecord, ref TInput input, ref TOutput output, ref ReadInfo readInfo, ref RecordInfo recordInfo);

        /// <summary>
        /// Read completion
        /// </summary>
        /// <param name="logRecord">The log record being read</param>
        /// <param name="input">The user input that was used in the read operation</param>
        /// <param name="output">The result of the read operation; if this is a struct, then it will be a temporary and should be copied to <paramref name="ctx"/></param>
        /// <param name="ctx">The application context passed through the pending operation</param>
        /// <param name="status">The result of the pending operation</param>
        /// <param name="recordMetadata">Metadata for the record; may be used to obtain <see cref="RecordMetadata.RecordInfo"/>.PreviousAddress when doing iterative reads</param>
        void ReadCompletionCallback(ref LogRecord logRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata);
        #endregion reads

        #region Upserts
        /// <summary>
        /// Non-concurrent writer; called on an Upsert that does not find the key so does an insert or finds the key's record in the immutable region so does a read/copy/update (RCU).
        /// </summary>
        /// <param name="dstLogRecord">The destination log record</param>
        /// <param name="input">The user input to be used for computing <paramref name="output"/></param>
        /// <param name="srcValue">The input value to be copied to the record</param>
        /// <param name="output">The location where the result of the update may be placed</param>
        /// <param name="upsertInfo">Information about this update operation and its context</param>
        /// <param name="reason">The operation for which this write is being done</param>
        /// <param name="recordInfo">A reference to the RecordInfo for the record; used for variable-length record length modification</param>
        /// <returns>True if the write was performed, else false (e.g. cancellation)</returns>
        bool SingleWriter(ref LogRecord dstLogRecord, ref TInput input, TValue srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo);

        /// <summary>
        /// Called after SingleWriter when a record containing an upsert of a new key has been successfully inserted at the tail of the log.
        /// </summary>
        /// <param name="dstLogRecord">The destination log record</param>
        /// <param name="input">The user input that was used to compute <paramref name="output"/></param>
        /// <param name="srcValue">The previous value to be copied/updated</param>
        /// <param name="output">The location where the result of the update may be placed</param>
        /// <param name="upsertInfo">Information about this update operation and its context</param>
        /// <param name="reason">The operation for which this write is being done</param>
        void PostSingleWriter(ref LogRecord dstLogRecord, ref TInput input, TValue srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason);

        /// <summary>
        /// Concurrent writer; called on an Upsert that is in-place updating a record in the mutable range.
        /// </summary>
        /// <param name="dstLogRecord">The destination log record</param>
        /// <param name="input">The user input to be used for computing the destination record's value</param>
        /// <param name="newValue">The value passed to Upsert, to be copied to the destination record</param>
        /// <param name="output">The location where the result of the update may be placed</param>
        /// <param name="upsertInfo">Information about this update operation and its context</param>
        /// <param name="recordInfo">A reference to the RecordInfo for the record; used for variable-length record length modification</param>
        /// <returns>True if the value was written, else false</returns>
        /// <remarks>If the value is shrunk in-place, the caller must first zero the data that is no longer used, to ensure log-scan correctness.</remarks>
        bool ConcurrentWriter(ref LogRecord dstLogRecord, ref TInput input, TValue newValue, ref TOutput output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo);
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
        bool NeedInitialUpdate(ref TKey key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo);

        /// <summary>
        /// Initial update for RMW (insert at the tail of the log).
        /// </summary>
        /// <param name="dstLogRecord">The destination log record</param>
        /// <param name="input">The user input to be used to create the destination record's value</param>
        /// <param name="output">The location where the output of the operation, if any, is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        /// <param name="recordInfo">A reference to the RecordInfo for the record; used for variable-length record length modification</param>
        /// <returns>True if the write was performed, else false (e.g. cancellation)</returns>
        bool InitialUpdater(ref LogRecord dstLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo);

        /// <summary>
        /// Called after a record containing an initial update for RMW has been successfully inserted at the tail of the log.
        /// </summary>
        /// <param name="logRecord">The log record that was created</param>
        /// <param name="input">The user input to be used to create the destination record's value</param>
        /// <param name="output">The location where the output of the operation, if any, is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        void PostInitialUpdater(ref LogRecord logRecord, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo);
        #endregion InitialUpdater

        #region CopyUpdater
        /// <summary>
        /// Whether we need to invoke copy-update for RMW
        /// </summary>
        /// <param name="srcLogRecord">The source record being copied from</param>
        /// <param name="input">The user input to be used for computing the updated value</param>
        /// <param name="output">The location where the output of the operation, if any, is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : IReadOnlyLogRecord;

        /// <summary>
        /// Copy-update for RMW (RCU (Read-Copy-Update) to the tail of the log)
        /// </summary>
        /// <param name="srcLogRecord">The source record being copied from</param>
        /// <param name="dstLogRecord">The destination log record being created</param>
        /// <param name="input">The user input to be used to create the destination record's value</param>
        /// <param name="output">The location where the output of the operation, if any, is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        /// <returns>True if the write was performed, else false (e.g. cancellation)</returns>
        bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : IReadOnlyLogRecord;

        /// <summary>
        /// Called after a record containing an RCU (Read-Copy-Update) for RMW has been successfully inserted at the tail of the log.
        /// </summary>
        /// <param name="srcLogRecord">The source record being copied from</param>
        /// <param name="dstLogRecord">The destination log record being created</param>
        /// <param name="input">The user input to be used to create the destination record's value</param>
        /// <param name="output">The location where the output of the operation, if any, is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        /// <returns>This is the only Post* method that returns non-void. The bool functions the same as CopyUpdater; this is because we do not want to modify
        /// objects in-memory until we know the "insert at tail" is successful. Therefore, we allow a false return as a signal to inspect <paramref name="rmwInfo.Action"/>
        /// and handle <see cref="RMWAction.ExpireAndStop"/>.</returns>
        bool PostCopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : IReadOnlyLogRecord;
        #endregion CopyUpdater

        #region InPlaceUpdater
        /// <summary>
        /// In-place update for RMW
        /// </summary>
        /// <param name="logRecord">The log record that is being updated</param>
        /// <param name="input">The user input to be used to create the destination record's value</param>
        /// <param name="output">The location where the output of the operation, if any, is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        /// <param name="recordInfo">A reference to the RecordInfo for the record; used for variable-length record length modification</param>
        /// <returns>True if the value was successfully updated, else false (e.g. the value was expired)</returns>
        /// <remarks>If the value is shrunk in-place, the caller must first zero the data that is no longer used, to ensure log-scan correctness.</remarks>
        bool InPlaceUpdater(ref LogRecord logRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo);
        #endregion InPlaceUpdater

        #region Variable-length value size
        /// <summary>
        /// Length of resulting value object when performing RMW modification of value using given input
        /// </summary>
        int GetRMWModifiedValueLength(ref TValue value, ref TInput input);

        /// <summary>
        /// Initial expected length of value object when populated by RMW using given input
        /// </summary>
        int GetRMWInitialValueLength(ref TInput input);

        /// <summary>
        /// Length of resulting value object when performing Upsert of value using given input
        /// </summary>
        int GetUpsertValueLength(ref TValue value, ref TInput input);
        #endregion Variable-length value size

        /// <summary>
        /// RMW completion
        /// </summary>
        /// <param name="logRecord">The log record that was created</param>
        /// <param name="input">The user input to be used to create the destination record's value</param>
        /// <param name="output">The location where the output of the operation, if any, is to be copied</param>
        /// <param name="ctx">The application context passed through the pending operation</param>
        /// <param name="status">The result of the pending operation</param>
        /// <param name="recordMetadata">The metadata of the modified or inserted record</param>
        void RMWCompletionCallback(ref LogRecord logRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata);
        #endregion RMWs

        #region Deletes
        /// <summary>
        /// Single deleter; called on a Delete that does not find the record in the mutable range and so inserts a new record.
        /// </summary>
        /// <param name="logRecord">The log record that is being created with a tombstone</param>
        /// <param name="deleteInfo">Information about this update operation and its context</param>
        /// <param name="recordInfo">A reference to the RecordInfo for the record; used for variable-length record length modification</param>
        /// <remarks>For Object Value types, Dispose() can be called here. If recordInfo.Invalid is true, this is called after the record was allocated and populated, but could not be appended at the end of the log.</remarks>
        /// <returns>True if the deleted record should be added, else false (e.g. cancellation)</returns>
        bool SingleDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo);

        /// <summary>
        /// Called after a record marking a Delete (with Tombstone set) has been successfully inserted at the tail of the log.
        /// </summary>
        /// <param name="logRecord">The log record that was created with a tombstone</param>
        /// <param name="deleteInfo">Information about this update operation and its context</param>
        /// <remarks>This does not have the address of the record that contains the value at 'key'; Delete does not retrieve records below HeadAddress, so
        ///     the last record we have in the 'key' chain may belong to 'key' or may be a collision.</remarks>
        void PostSingleDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo);

        /// <summary>
        /// Concurrent deleter; called on a Delete that finds the record in the mutable range.
        /// </summary>
        /// <param name="logRecord">The log record that is being deleted in-place</param>
        /// <param name="deleteInfo">Information about this update operation and its context</param>
        /// <param name="recordInfo">A reference to the RecordInfo for the record; used for variable-length record length modification</param>
        /// <remarks>For Object Value types, Dispose() can be called here. If recordInfo.Invalid is true, this is called after the record was allocated and populated, but could not be appended at the end of the log.</remarks>
        /// <returns>True if the value was successfully deleted, else false (e.g. the record was sealed)</returns>
        bool ConcurrentDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo);
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
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public interface ISessionFunctions<TKey, TValue> : ISessionFunctions<TKey, TValue, TValue, TValue, Empty>
    {
    }

    /// <summary>
    /// Callback functions to Tsavorite (two-param version with context)
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TContext"></typeparam>
    public interface ISessionFunctions<TKey, TValue, TContext> : ISessionFunctions<TKey, TValue, TValue, TValue, TContext>
    {
    }
}