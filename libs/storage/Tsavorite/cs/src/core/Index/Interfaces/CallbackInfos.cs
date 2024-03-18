// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    /// <summary>
    /// What actions to take following the RMW IFunctions method call, such as cancellation or record expiration.
    /// </summary>
    public enum UpsertAction
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
    /// Information passed to <see cref="IFunctions{Key, Value, Input, Output, Context}"/> record-update callbacks. 
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
        /// The header of the record.
        /// </summary>
        public RecordInfo RecordInfo { get; private set; }

        internal void SetRecordInfo(ref RecordInfo recordInfo) => RecordInfo = recordInfo;

        /// <summary>
        /// The length of data in the value that is in use. Incoming, it is set by Tsavorite.
        /// If an application wants to allow data to shrink and then grow again within the same record, it must set this to the correct length on output. 
        /// </summary>
        public int UsedValueLength { get; set; }

        /// <summary>
        /// The allocated length of the record value.
        /// </summary>
        public int FullValueLength { get; internal set; }

        /// <summary>
        /// What actions Tsavorite should perform on a false return from the IFunctions method
        /// </summary>
        public UpsertAction Action { get; set; }

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

        /// <summary>
        /// Retrieve the extra value length from the record, if present, and then clear it to ensure consistent log scan during in-place update.
        /// </summary>
        /// <param name="recordValue">Reference to the record value</param>
        /// <param name="usedValueLength">The currently-used length of the record value</param>
        /// <param name="recordInfo">The record header</param>
        /// <typeparam name="TValue">The type of the value</typeparam>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly unsafe void ClearExtraValueLength<TValue>(ref RecordInfo recordInfo, ref TValue recordValue, int usedValueLength)
        {
            Debug.Assert(usedValueLength == UsedValueLength, $"UpsertInfo: usedValueLength ({usedValueLength}) != this.UsedValueLength ({UsedValueLength})");
            StaticClearExtraValueLength(ref recordInfo, ref recordValue, usedValueLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void StaticClearExtraValueLength<TValue>(ref RecordInfo recordInfo, ref TValue recordValue, int usedValueLength)
        {
            if (!recordInfo.Filler)
                return;

            var valueAddress = (long)Unsafe.AsPointer(ref recordValue);
            int* extraLengthPtr = (int*)(valueAddress + RoundUp(usedValueLength, sizeof(int)));

            *extraLengthPtr = 0;
            recordInfo.Filler = false;
        }

        /// <summary>
        /// Set the extra value length, if any, into the record past the used value length.
        /// </summary>
        /// <param name="recordInfo">The record header</param>
        /// <param name="recordValue">Reference to the record value</param>
        /// <param name="usedValueLength">The currently-used length of the record value</param>
        /// <typeparam name="TValue">The type of the value</typeparam>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void SetUsedValueLength<TValue>(ref RecordInfo recordInfo, ref TValue recordValue, int usedValueLength)
        {
            StaticSetUsedValueLength(ref recordInfo, ref recordValue, usedValueLength, FullValueLength);
            UsedValueLength = usedValueLength;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void StaticSetUsedValueLength<TValue>(ref RecordInfo recordInfo, ref TValue recordValue, int usedValueLength, int fullValueLength)
        {
            // Note: This is only called for variable-length types, and for those we have ensured the location of recordValue is pinned.
            long valueAddress = (long)Unsafe.AsPointer(ref recordValue);
            Debug.Assert(!recordInfo.Filler, "Filler should have been cleared by ClearExtraValueLength()");

            usedValueLength = RoundUp(usedValueLength, sizeof(int));
            int extraValueLength = fullValueLength - usedValueLength;
            if (extraValueLength >= sizeof(int))
            {
                int* extraValueLengthPtr = (int*)(valueAddress + usedValueLength);
                Debug.Assert(*extraValueLengthPtr == 0 || *extraValueLengthPtr == extraValueLength, "existing ExtraValueLength should be 0 or the same value");
                *extraValueLengthPtr = extraValueLength;
                recordInfo.Filler = true;
            }
        }
    }

    /// <summary>
    /// What actions to take following the RMW IFunctions method call, such as cancellation or record expiration.
    /// </summary>
    public enum RMWAction
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
        /// Stop the operation immediately and return.
        /// </summary>
        CancelOperation
    }

    /// <summary>
    /// Information passed to <see cref="IFunctions{Key, Value, Input, Output, Context}"/> record-update callbacks. 
    /// </summary>
    public struct RMWInfo
    {
        /// <summary>
        /// The Tsavorite execution context version of the operation
        /// </summary>
        public long Version { get; internal set; }

        /// <summary>
        /// The logical address of the record being operated on. For CopyUpdater, this is the source address,
        /// or <see cref="Constants.kInvalidAddress"/> if the source is the read cache.
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
        /// The header of the record.
        /// </summary>
        public RecordInfo RecordInfo { get; private set; }

        internal void SetRecordInfo(ref RecordInfo recordInfo) => RecordInfo = recordInfo;
        internal void ClearRecordInfo() => RecordInfo = default;

        /// <summary>
        /// The length of data in the value that is in use. Incoming, it is set by Tsavorite.
        /// If an application wants to allow data to shrink and then grow again within the same record, it must set this to the correct length on output. 
        /// </summary>
        public int UsedValueLength { get; set; }

        /// <summary>
        /// The allocated length of the record value.
        /// </summary>
        public int FullValueLength { get; internal set; }

        /// <summary>
        /// If set true by CopyUpdater, the source record for the RCU will not be elided from the tag chain even if this is otherwise possible.
        /// </summary>
        public bool PreserveCopyUpdaterSourceRecord { get; set; }

        /// <summary>
        /// What actions Tsavorite should perform on a false return from the IFunctions method
        /// </summary>
        public RMWAction Action { get; set; }

        /// <summary>
        /// Retrieve the extra value length from the record, if present, and then clear it to ensure consistent log scan during in-place update.
        /// </summary>
        /// <param name="recordInfo">Reference to the record header</param>
        /// <param name="recordValue">Reference to the record value</param>
        /// <param name="usedValueLength">The currently-used length of the record value</param>
        /// <typeparam name="TValue">The type of the value</typeparam>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ClearExtraValueLength<TValue>(ref RecordInfo recordInfo, ref TValue recordValue, int usedValueLength)
        {
            Debug.Assert(usedValueLength == UsedValueLength, $"RMWInfo: usedValueLength ({usedValueLength}) != this.UsedValueLength ({UsedValueLength})");
            UpsertInfo.StaticClearExtraValueLength(ref recordInfo, ref recordValue, usedValueLength);
        }

        /// <summary>
        /// Set the extra value length, if any, into the record past the used value length.
        /// </summary>
        /// <param name="recordInfo">Reference to the record header</param>
        /// <param name="recordValue">Reference to the record value</param>
        /// <param name="usedValueLength">The currently-used length of the record value</param>
        /// <typeparam name="TValue">The type of the value</typeparam>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void SetUsedValueLength<TValue>(ref RecordInfo recordInfo, ref TValue recordValue, int usedValueLength)
        {
            UpsertInfo.StaticSetUsedValueLength(ref recordInfo, ref recordValue, usedValueLength, FullValueLength);
            UsedValueLength = usedValueLength;
        }
    }

    /// <summary>
    /// What actions to take following the RMW IFunctions method call, such as cancellation or record expiration.
    /// </summary>
    public enum DeleteAction
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
    /// Information passed to <see cref="IFunctions{Key, Value, Input, Output, Context}"/> record-update callbacks. 
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
        /// The header of the record.
        /// </summary>
        public RecordInfo RecordInfo { get; private set; }

        internal void SetRecordInfo(ref RecordInfo recordInfo) => RecordInfo = recordInfo;

        /// <summary>
        /// The length of data in the value that is in use. Incoming, it is set by Tsavorite to the result.
        /// If an application wants to allow data to shrink and then grow again within the same record, it must set this to the correct length on output. 
        /// </summary>
        public int UsedValueLength { get; set; }

        /// <summary>
        /// The allocated length of the record value.
        /// </summary>
        public int FullValueLength { get; internal set; }

        /// <summary>
        /// What actions Tsavorite should perform on a false return from the IFunctions method
        /// </summary>
        public DeleteAction Action { get; set; }
    }

    /// <summary>
    /// What actions to take following the RMW IFunctions method call, such as cancellation or record expiration.
    /// </summary>
    public enum ReadAction
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
        /// Stop the operation immediately and return.
        /// </summary>
        CancelOperation
    }

    /// <summary>
    /// Information passed to <see cref="IFunctions{Key, Value, Input, Output, Context}"/> record-read callbacks. 
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
        /// The header of the record.
        /// </summary>
        public RecordInfo RecordInfo { get; private set; }

        internal void SetRecordInfo(ref RecordInfo recordInfo) => RecordInfo = recordInfo;

        /// <summary>
        /// What actions Tsavorite should perform on a false return from the IFunctions method
        /// </summary>
        public ReadAction Action { get; set; }
    }
}