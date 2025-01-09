// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TValue>
        where TAllocator : IAllocator<TValue, TStoreFunctions>
    {
        internal struct PendingContext<TInput, TOutput, TContext>
        {
            // User provided information
            internal OperationType type;
            internal IHeapContainer<SpanByte> key;
            internal IHeapContainer<TValue> value;
            internal IHeapContainer<TInput> input;
            internal TOutput output;
            internal TContext userContext;
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
            internal const ushort kIsReadAtAddress = 0x0004;

            internal ReadCopyOptions readCopyOptions;   // Two byte enums
            internal WriteReason writeReason;   // for ConditionalCopyToTail; one byte enum

            internal long minAddress;

            // For flushing head pages on tail allocation.
            internal CompletionEvent flushEvent;

            // For RMW if an allocation caused the source record for a copy to go from readonly to below HeadAddress, or for any operation with CAS failure.
            internal long retryNewLogicalAddress;

            // Address of the initial entry in the hash chain upon start of Internal(RUMD).
            internal long InitialEntryAddress;

            internal ScanCursorState<TValue> scanCursorState;

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

            internal bool IsReadAtAddress
            {
                readonly get => (operationFlags & kIsReadAtAddress) != 0;
                set => operationFlags = value ? (ushort)(operationFlags | kIsReadAtAddress) : (ushort)(operationFlags & ~kIsReadAtAddress);
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