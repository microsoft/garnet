// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Status result of operation on Tsavorite
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 1)]
    public struct RecordStatus
    {
        [FieldOffset(0)]
        internal readonly StatusCode statusCode;

        /// <summary>
        /// Whether a new record for a previously non-existent key was appended to the log.
        /// </summary>
        public bool Created => (statusCode & StatusCode.AdvancedMask) == StatusCode.CreatedRecord;

        /// <summary>
        /// Whether an existing record was updated in place.
        /// </summary>
        public bool InPlaceUpdated => (statusCode & StatusCode.AdvancedMask) == StatusCode.InPlaceUpdatedRecord;

        /// <summary>
        /// Whether an existing record key was copied, updated, and appended to the log.
        /// </summary>
        public bool CopyUpdated => (statusCode & StatusCode.AdvancedMask) == StatusCode.CopyUpdatedRecord;

        /// <summary>
        /// Whether an existing record key was copied and appended to the log.
        /// </summary>
        public bool Copied => (statusCode & StatusCode.AdvancedMask) == StatusCode.CopiedRecord;

        /// <summary>
        /// Whether an existing record key was copied, updated, and added to the readcache.
        /// </summary>
        public bool CopiedToReadCache => (statusCode & StatusCode.AdvancedMask) == StatusCode.CopiedRecordToReadCache;
    }

    /// <summary>
    /// Status result of operation on Tsavorite
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 1)]
    public struct Status
    {
        [FieldOffset(0)]
        internal readonly StatusCode statusCode;

        /// <summary>
        /// Status specific to the record
        /// </summary>
        [FieldOffset(0)]
        public RecordStatus Record;

        /// <summary>
        /// Create status from given status code
        /// </summary>
        /// <param name="statusCode"></param>
        internal Status(StatusCode statusCode) : this() => this.statusCode = statusCode;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status(OperationStatus operationStatus) : this()
        {
            var basicOperationStatus = OperationStatusUtils.BasicOpCode(operationStatus);
            Debug.Assert(basicOperationStatus <= OperationStatus.MAX_MAP_TO_COMPLETED_STATUSCODE);
            statusCode = (StatusCode)basicOperationStatus | (StatusCode)((int)operationStatus >> OperationStatusUtils.OpStatusToStatusCodeShift);
        }

        /// <summary>
        /// Create a <see cref="Found"/> Status value.
        /// </summary>
        public static Status CreateFound() => new(StatusCode.Found);

        /// <summary>
        /// Create a <see cref="IsPending"/> Status value. Use the Is* properties to query.
        /// </summary>
        public static Status CreatePending() => new(StatusCode.Pending);

        /// <summary>
        /// Whether a Read or RMW found the key
        /// </summary>
        public bool Found => (Record.statusCode & StatusCode.BasicMask) == StatusCode.Found;

        /// <summary>
        /// Whether a Read or RMW did not find the key
        /// </summary>
        public bool NotFound => (statusCode & StatusCode.BasicMask) == StatusCode.NotFound;

        /// <summary>
        /// Whether the operation went pending
        /// </summary>
        public bool IsPending => statusCode == StatusCode.Pending;

        /// <summary>
        /// Whether the operation went pending
        /// </summary>
        public bool IsCompleted => !IsPending;

        /// <summary>
        /// Whether the operation is in an error state
        /// </summary>
        public bool IsFaulted => statusCode == StatusCode.Error;

        /// <summary>
        /// Whether the operation was canceled
        /// </summary>
        public bool IsCanceled => statusCode == StatusCode.Canceled;

        /// <summary>
        /// Whether the operation found an expired record
        /// </summary>
        public bool Expired => (statusCode & StatusCode.Expired) == StatusCode.Expired;

        /// <summary>
        /// Whether the operation completed successfully, i.e., it is not pending and did not error out
        /// </summary>
        public bool IsCompletedSuccessfully
        {
            get
            {
                var basicCode = statusCode & StatusCode.BasicMask;
                return basicCode != StatusCode.Pending && basicCode != StatusCode.Error;
            }
        }

        /// <summary>
        /// Get the underlying status code value
        /// </summary>
        public byte Value => (byte)statusCode;

        /// <inheritdoc />
        /// <remarks>"Found" is zero, so does not appear in the output by default; this handles that explicitly</remarks>
        public override string ToString() => (Found ? "Found, " : string.Empty) + statusCode.ToString();
    }
}