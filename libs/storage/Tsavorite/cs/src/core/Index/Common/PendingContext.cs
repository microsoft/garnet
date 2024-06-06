// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<Key, Value> : TsavoriteBase
    {
        internal struct PendingContext<Input, Output, Context>
        {
            // User provided information
            internal OperationType type;
            internal IHeapContainer<Key> key;
            internal IHeapContainer<Value> value;
            internal IHeapContainer<Input> input;
            internal Output output;
            internal Context userContext;
            internal long keyHash;

            // Some additional information about the previous attempt
            internal long id;
            internal long logicalAddress;
            internal long InitialLatestLogicalAddress;

            // operationFlags values
            internal ushort operationFlags;
            internal const ushort kNoOpFlags = 0;
            internal const ushort kNoKey = 0x0001;
            internal const ushort kIsAsync = 0x0002;

            internal ReadCopyOptions readCopyOptions;   // Two byte enums
            internal WriteReason writeReason;   // for ConditionalCopyToTail; one byte enum

            internal RecordInfo recordInfo;
            internal long minAddress;

            // For flushing head pages on tail allocation.
            internal CompletionEvent flushEvent;

            // For RMW if an allocation caused the source record for a copy to go from readonly to below HeadAddress, or for any operation with CAS failure.
            internal long retryNewLogicalAddress;

            internal ScanCursorState<Key, Value> scanCursorState;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal PendingContext(long keyHash) => this.keyHash = keyHash;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal PendingContext(ReadCopyOptions sessionReadCopyOptions, ref ReadOptions readOptions, bool isAsync = false, bool noKey = false)
            {
                // The async flag is often set when the PendingContext is created, so preserve that.
                operationFlags = (ushort)((noKey ? kNoKey : kNoOpFlags) | (isAsync ? kIsAsync : kNoOpFlags));
                readCopyOptions = ReadCopyOptions.Merge(sessionReadCopyOptions, readOptions.CopyOptions);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal PendingContext(ReadCopyOptions readCopyOptions, bool isAsync = false, bool noKey = false)
            {
                // The async flag is often set when the PendingContext is created, so preserve that.
                operationFlags = (ushort)((noKey ? kNoKey : kNoOpFlags) | (isAsync ? kIsAsync : kNoOpFlags));
                this.readCopyOptions = readCopyOptions;
            }

            internal bool NoKey
            {
                readonly get => (operationFlags & kNoKey) != 0;
                set => operationFlags = value ? (ushort)(operationFlags | kNoKey) : (ushort)(operationFlags & ~kNoKey);
            }

            internal readonly bool HasMinAddress => minAddress != Constants.kInvalidAddress;

            internal bool IsAsync
            {
                readonly get => (operationFlags & kIsAsync) != 0;
                set => operationFlags = value ? (ushort)(operationFlags | kIsAsync) : (ushort)(operationFlags & ~kIsAsync);
            }

            // RecordInfo is not used as such during the pending phase, so we reuse the space here.
            internal long InitialEntryAddress
            {
                readonly get => recordInfo.PreviousAddress;
                set => recordInfo.PreviousAddress = value;
            }

            public void Dispose()
            {
                key?.Dispose();
                key = default;
                value?.Dispose();
                value = default;
                input?.Dispose();
                input = default;
            }
        }
    }
}