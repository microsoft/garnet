// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// What actions to take following the RMW ISessionFunctions method call, such as cancellation or record expiration.
    /// </summary>
    public enum UpsertAction : byte
    {
        /// <summary>
        /// Execute the default action for the method 'false' return.
        /// </summary>
        Default,

        /// <summary>
        /// Stop the operation immediately and return.
        /// </summary>
        CancelOperation
    }

    /// <summary>
    /// Information passed to <see cref="ISessionFunctions{TInput, TOutput, TContext}"/> record-update callbacks. 
    /// </summary>
    public struct UpsertInfo
    {
        /// <summary>
        /// The Tsavorite execution context version of the operation
        /// </summary>
        public long Version { get; internal set; }

        /// <summary>
        /// The logical address of the record being operated on
        /// </summary>
        public long Address { get; internal set; }

        /// <summary>
        /// Hash code of key being operated on
        /// </summary>
        public long KeyHash { get; internal set; }

        /// <summary>
        /// The ID of session context executing the operation
        /// </summary>
        public int SessionID { get; internal set; }

        /// <summary>
        /// What actions Tsavorite should perform on a false return from the ISessionFunctions method
        /// </summary>
        public UpsertAction Action { get; set; }

        /// <summary>
        /// User-defined byte of data associated with the operation
        /// </summary>
        public byte UserData { get; set; }

        /// <summary>
        /// Utility ctor
        /// </summary>
        public UpsertInfo(ref RMWInfo rmwInfo)
        {
            Version = rmwInfo.Version;
            SessionID = rmwInfo.SessionID;
            Address = rmwInfo.Address;
            KeyHash = rmwInfo.KeyHash;
            Action = UpsertAction.Default;
        }
    }

    /// <summary>
    /// What actions to take following the RMW ISessionFunctions method call, such as cancellation or record expiration.
    /// </summary>
    public enum RMWAction : byte
    {
        /// <summary>
        /// Execute the default action for the method 'false' return.
        /// </summary>
        Default,

        /// <summary>
        /// Expire the record, including continuing actions to reinsert a new record with initial state.
        /// </summary>
        ExpireAndResume,

        /// <summary>
        /// Expire the record, and do not attempt to insert a new record with initial state.
        /// </summary>
        ExpireAndStop,

        /// <summary>
        /// Stop the operation immediately with a "wrong type" error
        /// </summary>
        WrongType,

        /// <summary>
        /// Stop the operation immediately and return.
        /// </summary>
        CancelOperation
    }

    /// <summary>
    /// Information passed to <see cref="ISessionFunctions{TInput, TOutput, TContext}"/> record-update callbacks. 
    /// </summary>
    public struct RMWInfo
    {
        /// <summary>
        /// The Tsavorite execution context version of the operation
        /// </summary>
        public long Version { get; internal set; }

        /// <summary>
        /// The logical address of the record being operated on. For CopyUpdater, this is the source address,
        /// or <see cref="LogAddress.kInvalidAddress"/> if the source is the read cache.
        /// </summary>
        public long Address { get; internal set; }

        /// <summary>
        /// Hash code of key being operated on
        /// </summary>
        public long KeyHash { get; internal set; }

        /// <summary>
        /// The ID of session context executing the operation
        /// </summary>
        public int SessionID { get; internal set; }

        /// <summary>
        /// If set true by CopyUpdater, the source record for the RCU will not be elided from the tag chain even if this is otherwise possible.
        /// </summary>
        public bool PreserveCopyUpdaterSourceRecord { get; set; }

        /// <summary>
        /// If set true by RMW and there is a source ValueObject it will be cleared immediately (to manage object size tracking most effectively).
        /// </summary>
        public bool ClearSourceValueObject { get; set; }

        /// <summary>
        /// What actions Tsavorite should perform on a false return from the ISessionFunctions method
        /// </summary>
        public RMWAction Action { get; set; }

        /// <summary>
        /// User-defined byte of data associated with the operation
        /// </summary>
        public byte UserData { get; set; }
    }

    /// <summary>
    /// What actions to take following the RMW ISessionFunctions method call, such as cancellation or record expiration.
    /// </summary>
    public enum DeleteAction : byte
    {
        /// <summary>
        /// Execute the default action for the method 'false' return.
        /// </summary>
        Default,

        /// <summary>
        /// Stop the operation immediately and return.
        /// </summary>
        CancelOperation
    }
    /// <summary>
    /// Information passed to <see cref="ISessionFunctions{TInput, TOutput, TContext}"/> record-update callbacks. 
    /// </summary>
    public struct DeleteInfo
    {
        /// <summary>
        /// The Tsavorite execution context version of the operation
        /// </summary>
        public long Version { get; internal set; }

        /// <summary>
        /// The logical address of the record being operated on
        /// </summary>
        public long Address { get; internal set; }

        /// <summary>
        /// Hash code of key being operated on
        /// </summary>
        public long KeyHash { get; internal set; }

        /// <summary>
        /// The ID of session context executing the operation
        /// </summary>
        public int SessionID { get; internal set; }

        /// <summary>
        /// What actions Tsavorite should perform on a false return from the ISessionFunctions method
        /// </summary>
        public DeleteAction Action { get; set; }

        /// <summary>
        /// User-defined byte of data associated with the operation
        /// </summary>
        public byte UserData { get; set; }
    }

    /// <summary>
    /// What actions to take following the RMW ISessionFunctions method call, such as cancellation or record expiration.
    /// </summary>
    public enum ReadAction : byte
    {
        /// <summary>
        /// Execute the default action for the method 'false' return.
        /// </summary>
        Default,

        /// <summary>
        /// Expire the record. No subsequent actions are available for Read.
        /// </summary>
        Expire,

        /// <summary>
        /// Stop the operation immediately with a "wrong type" error
        /// </summary>
        WrongType,

        /// <summary>
        /// Stop the operation immediately and return.
        /// </summary>
        CancelOperation
    }

    /// <summary>
    /// Information passed to <see cref="ISessionFunctions{TInput, TOutput, TContext}"/> record-read callbacks. 
    /// </summary>
    public struct ReadInfo
    {
        /// <summary>
        /// The Tsavorite execution context version of the operation
        /// </summary>
        public long Version { get; internal set; }

        /// <summary>
        /// The logical address of the record being operated on
        /// </summary>
        public long Address { get; internal set; }

        /// <summary>
        /// Whether the call is from sync or async (pending) path
        /// </summary>
        public bool IsFromPending { get; internal set; }

        /// <summary>
        /// What actions Tsavorite should perform on a false return from the ISessionFunctions method
        /// </summary>
        public ReadAction Action { get; set; }
    }
}