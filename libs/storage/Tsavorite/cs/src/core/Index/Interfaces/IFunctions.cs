// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Callback functions to Tsavorite
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public interface IFunctions<Key, Value, Input, Output, Context>
    {
        #region Reads
        /// <summary>
        /// Non-concurrent reader. 
        /// </summary>
        /// <param name="key">The key for the record to be read</param>
        /// <param name="input">The user input for computing <paramref name="dst"/> from <paramref name="value"/></param>
        /// <param name="value">The value for the record being read</param>
        /// <param name="dst">The location where <paramref name="value"/> is to be copied</param>
        /// <param name="readInfo">Information about this read operation and its context</param>
        /// <returns>True if the value was available, else false (e.g. the value was expired)</returns>
        bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo);

        /// <summary>
        /// Concurrent reader
        /// </summary>
        /// <param name="key">The key for the record to be read</param>
        /// <param name="input">The user input for computing <paramref name="dst"/> from <paramref name="value"/></param>
        /// <param name="value">The value for the record being read</param>
        /// <param name="dst">The location where <paramref name="value"/> is to be copied</param>
        /// <param name="readInfo">Information about this read operation and its context</param>
        /// <param name="recordInfo">A reference to the RecordInfo for the record; used for locking if <see cref="ConcurrencyControlMode.None"/>
        ///     is used, or for variable-length record length modification</param>
        /// <returns>True if the value was available, else false (e.g. the value was expired)</returns>
        bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo, ref RecordInfo recordInfo);

        /// <summary>
        /// Read completion
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input that was used in the read operation</param>
        /// <param name="output">The result of the read operation; if this is a struct, then it will be a temporary and should be copied to <paramref name="ctx"/></param>
        /// <param name="ctx">The application context passed through the pending operation</param>
        /// <param name="status">The result of the pending operation</param>
        /// <param name="recordMetadata">Metadata for the record; may be used to obtain <see cref="RecordMetadata.RecordInfo"/>.PreviousAddress when doing iterative reads</param>
        void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata);
        #endregion reads

        #region Upserts
        /// <summary>
        /// Non-concurrent writer; called on an Upsert that does not find the key so does an insert or finds the key's record in the immutable region so does a read/copy/update (RCU).
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing <paramref name="dst"/></param>
        /// <param name="src">The previous value to be copied/updated</param>
        /// <param name="dst">The destination to be updated; because this is an copy to a new location, there is no previous value there.</param>
        /// <param name="output">The location where the result of the update may be placed</param>
        /// <param name="upsertInfo">Information about this update operation and its context</param>
        /// <param name="reason">The operation for which this write is being done</param>
        /// <param name="recordInfo">A reference to the RecordInfo for the record; used for locking if <see cref="ConcurrencyControlMode.None"/>
        ///     is used, or for variable-length record length modification</param>
        /// <returns>True if the write was performed, else false (e.g. cancellation)</returns>
        bool SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo);

        /// <summary>
        /// Called after SingleWriter when a record containing an upsert of a new key has been successfully inserted at the tail of the log.
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input that was used to compute <paramref name="dst"/></param>
        /// <param name="src">The previous value to be copied/updated</param>
        /// <param name="dst">The destination to be updated; because this is an copy to a new location, there is no previous value there.</param>
        /// <param name="output">The location where the result of the update may be placed</param>
        /// <param name="upsertInfo">Information about this update operation and its context</param>
        /// <param name="reason">The operation for which this write is being done</param>
        void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason);

        /// <summary>
        /// Concurrent writer; called on an Upsert that finds the record in the mutable range.
        /// </summary>
        /// <param name="key">The key for the record to be written</param>
        /// <param name="input">The user input to be used for computing <paramref name="dst"/></param>
        /// <param name="src">The value to be copied to <paramref name="dst"/></param>
        /// <param name="dst">The location where <paramref name="src"/> is to be copied; because this method is called only for in-place updates, there is a previous value there.</param>
        /// <param name="output">The location where the result of the update may be placed</param>
        /// <param name="upsertInfo">Information about this update operation and its context</param>
        /// <param name="recordInfo">A reference to the RecordInfo for the record; used for locking if <see cref="ConcurrencyControlMode.None"/>
        ///     is used, or for variable-length record length modification</param>
        /// <returns>True if the value was written, else false</returns>
        /// <remarks>If the value is shrunk in-place, the caller must first zero the data that is no longer used, to ensure log-scan correctness.</remarks>
        bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo);
        #endregion Upserts

        #region RMWs
        #region InitialUpdater
        /// <summary>
        /// Whether we need to invoke initial-update for RMW
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing the updated value</param>
        /// <param name="output">The location where the result of the <paramref name="input"/> operation is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output, ref RMWInfo rmwInfo);

        /// <summary>
        /// Initial update for RMW (insert at the tail of the log).
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing the updated <paramref name="value"/></param>
        /// <param name="value">The destination to be updated; because this is an insert, there is no previous value there.</param>
        /// <param name="output">The location where the result of the <paramref name="input"/> operation on <paramref name="value"/> is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        /// <param name="recordInfo">A reference to the RecordInfo for the record; used for locking if <see cref="ConcurrencyControlMode.None"/>
        ///     is used, or for variable-length record length modification</param>
        /// <returns>True if the write was performed, else false (e.g. cancellation)</returns>
        bool InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo);

        /// <summary>
        /// Called after a record containing an initial update for RMW has been successfully inserted at the tail of the log.
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing the updated <paramref name="value"/></param>
        /// <param name="value">The destination to be updated; because this is an insert, there is no previous value there.</param>
        /// <param name="output">The location where the result of the <paramref name="input"/> operation on <paramref name="value"/> is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo);
        #endregion InitialUpdater

        #region CopyUpdater
        /// <summary>
        /// Whether we need to invoke copy-update for RMW
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing the updated value</param>
        /// <param name="oldValue">The existing value that would be copied.</param>
        /// <param name="output">The location where the result of the <paramref name="input"/> operation on <paramref name="oldValue"/> is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output, ref RMWInfo rmwInfo);

        /// <summary>
        /// Copy-update for RMW (RCU (Read-Copy-Update) to the tail of the log)
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing <paramref name="newValue"/> from <paramref name="oldValue"/></param>
        /// <param name="oldValue">The previous value to be copied/updated</param>
        /// <param name="newValue">The destination to be updated; because this is an copy to a new location, there is no previous value there.</param>
        /// <param name="output">The location where <paramref name="newValue"/> is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        /// <param name="recordInfo">A reference to the RecordInfo for the record; used for locking if <see cref="ConcurrencyControlMode.None"/>
        ///     is used, or for variable-length record length modification</param>
        /// <returns>True if the write was performed, else false (e.g. cancellation)</returns>
        bool CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo);

        /// <summary>
        /// Called after a record containing an RCU (Read-Copy-Update) for RMW has been successfully inserted at the tail of the log.
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing <paramref name="newValue"/> from <paramref name="oldValue"/></param>
        /// <param name="oldValue">The previous value to be copied/updated; may also be disposed here if appropriate</param>
        /// <param name="newValue">The destination to be updated; because this is an copy to a new location, there is no previous value there.</param>
        /// <param name="output">The location where <paramref name="newValue"/> is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        void PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo);
        #endregion CopyUpdater

        #region InPlaceUpdater
        /// <summary>
        /// In-place update for RMW
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing the updated <paramref name="value"/></param>
        /// <param name="value">The destination to be updated; because this is an in-place update, there is a previous value there.</param>
        /// <param name="output">The location where the result of the <paramref name="input"/> operation on <paramref name="value"/> is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        /// <param name="recordInfo">A reference to the RecordInfo for the record; used for locking if <see cref="ConcurrencyControlMode.None"/>
        ///     is used, or for variable-length record length modification</param>
        /// <returns>True if the value was successfully updated, else false (e.g. the value was expired)</returns>
        /// <remarks>If the value is shrunk in-place, the caller must first zero the data that is no longer used, to ensure log-scan correctness.</remarks>
        bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo);
        #endregion InPlaceUpdater

        #region Variable-length value size
        /// <summary>
        /// Length of resulting value object when performing RMW modification of value using given input
        /// </summary>
        int GetRMWModifiedValueLength(ref Value value, ref Input input);

        /// <summary>
        /// Initial expected length of value object when populated by RMW using given input
        /// </summary>
        int GetRMWInitialValueLength(ref Input input);
        #endregion Variable-length value size

        /// <summary>
        /// RMW completion
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input that was used to perform the modification</param>
        /// <param name="output">The result of the RMW operation; if this is a struct, then it will be a temporary and should be copied to <paramref name="ctx"/></param>
        /// <param name="ctx">The application context passed through the pending operation</param>
        /// <param name="status">The result of the pending operation</param>
        /// <param name="recordMetadata">The metadata of the modified or inserted record</param>
        void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata);
        #endregion RMWs

        #region Deletes
        /// <summary>
        /// Single deleter; called on a Delete that does not find the record in the mutable range and so inserts a new record.
        /// </summary>
        /// <param name="key">The key for the record to be deleted</param>
        /// <param name="value">The value for the record being deleted; because this method is called only for in-place updates, there is a previous value there. Usually this is ignored or assigned 'default'.</param>
        /// <param name="deleteInfo">Information about this update operation and its context</param>
        /// <param name="recordInfo">A reference to the RecordInfo for the record; used for locking if <see cref="ConcurrencyControlMode.None"/>
        ///     is used, or for variable-length record length modification</param>
        /// <remarks>For Object Value types, Dispose() can be called here. If recordInfo.Invalid is true, this is called after the record was allocated and populated, but could not be appended at the end of the log.</remarks>
        /// <returns>True if the deleted record should be added, else false (e.g. cancellation)</returns>
        bool SingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo);

        /// <summary>
        /// Called after a record marking a Delete (with Tombstone set) has been successfully inserted at the tail of the log.
        /// </summary>
        /// <param name="key">The key for the record that was deleted</param>
        /// <param name="deleteInfo">Information about this update operation and its context</param>
        /// <remarks>This does not have the address of the record that contains the value at 'key'; Delete does not retrieve records below HeadAddress, so
        ///     the last record we have in the 'key' chain may belong to 'key' or may be a collision.</remarks>
        void PostSingleDeleter(ref Key key, ref DeleteInfo deleteInfo);

        /// <summary>
        /// Concurrent deleter; called on a Delete that finds the record in the mutable range.
        /// </summary>
        /// <param name="key">The key for the record to be deleted</param>
        /// <param name="value">The value for the record being deleted; because this method is called only for in-place updates, there is a previous value there. Usually this is ignored or assigned 'default'.</param>
        /// <param name="deleteInfo">Information about this update operation and its context</param>
        /// <param name="recordInfo">A reference to the RecordInfo for the record; used for locking if <see cref="ConcurrencyControlMode.None"/>
        ///     is used, or for variable-length record length modification</param>
        /// <remarks>For Object Value types, Dispose() can be called here. If recordInfo.Invalid is true, this is called after the record was allocated and populated, but could not be appended at the end of the log.</remarks>
        /// <returns>True if the value was successfully deleted, else false (e.g. the record was sealed)</returns>
        bool ConcurrentDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo);
        #endregion Deletes

        #region Dispose
        /// <summary>
        /// Called after SingleWriter, if the CAS insertion of record into the store fails. Can be used to perform object disposal related actions.
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input that was used to compute <paramref name="dst"/></param>
        /// <param name="src">The previous value to be copied/updated</param>
        /// <param name="dst">The destination to be updated; because this is an copy to a new location, there is no previous value there.</param>
        /// <param name="output">The location where the result of the update may be placed</param>
        /// <param name="upsertInfo">Information about this update operation and its context</param>
        /// <param name="reason">The operation for which this write is being done</param>
        void DisposeSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason);

        /// <summary>
        /// Called after copy-update for RMW (RCU (Read-Copy-Update) to the tail of the log), if the CAS insertion of record into the store fails. Can be used to perform object disposal related actions.
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing <paramref name="newValue"/> from <paramref name="oldValue"/></param>
        /// <param name="oldValue">The previous value to be copied/updated</param>
        /// <param name="newValue">The destination to be updated; because this is an copy to a new location, there is no previous value there.</param>
        /// <param name="output">The location where <paramref name="newValue"/> is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        void DisposeCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo);

        /// <summary>
        /// Called after initial update for RMW (insert at the tail of the log), if the CAS insertion of record into the store fails. Can be used to perform object disposal related actions.
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing the updated <paramref name="value"/></param>
        /// <param name="value">The destination to be updated; because this is an insert, there is no previous value there.</param>
        /// <param name="output">The location where the result of the <paramref name="input"/> operation on <paramref name="value"/> is to be copied</param>
        /// <param name="rmwInfo">Information about this update operation and its context</param>
        void DisposeInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo);

        /// <summary>
        /// Called after a Delete that does not find the record in the mutable range and so inserts a new record, if the CAS insertion of record into the store fails. Can be used to perform object disposal related actions.
        /// </summary>
        /// <param name="key">The key for the record to be deleted</param>
        /// <param name="value">The value for the record being deleted; because this method is called only for in-place updates, there is a previous value there. Usually this is ignored or assigned 'default'.</param>
        /// <param name="deleteInfo">Information about this update operation and its context</param>
        /// <remarks>For Object Value types, Dispose() can be called here. If recordInfo.Invalid is true, this is called after the record was allocated and populated, but could not be appended at the end of the log.</remarks>
        void DisposeSingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo);

        /// <summary>
        /// Called after a record has been deserialized from the disk on a pending Read or RMW. Can be used to perform object disposal related actions.
        /// </summary>
        /// <param name="key">The key for the record</param>
        /// <param name="value">The value for the record</param>
        void DisposeDeserializedFromDisk(ref Key key, ref Value value);

        /// <summary>
        /// Called when a record is being revivified from the freelist, which will likely be for a different key. The previous Key must therefore be disposed; the Value probably already has been, at Delete() time.
        /// </summary>
        /// <param name="key">The key for the record</param>
        /// <param name="value">The value for the record</param>
        /// <param name="newKeySize">If > 0, this is a record from the freelist and we are disposing the key as well as value (it is -1 when revivifying a record in the hash chain or when doing a RETRY; for these the key does not change)</param>
        void DisposeForRevivification(ref Key key, ref Value value, int newKeySize);
        #endregion Dispose

        #region Checkpointing
        /// <summary>
        /// Checkpoint completion callback (called per client session)
        /// </summary>
        /// <param name="sessionID">ID of session reporting persistence</param>
        /// <param name="sessionName">Name of session reporting persistence</param>
        /// <param name="commitPoint">Commit point descriptor</param>
        void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint);
        #endregion Checkpointing
    }

    /// <summary>
    /// Callback functions to Tsavorite (two-param version)
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    public interface IFunctions<Key, Value> : IFunctions<Key, Value, Value, Value, Empty>
    {
    }

    /// <summary>
    /// Callback functions to Tsavorite (two-param version with context)
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public interface IFunctions<Key, Value, Context> : IFunctions<Key, Value, Value, Value, Context>
    {
    }
}