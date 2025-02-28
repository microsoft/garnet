// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TValue>
        where TAllocator : IAllocator<TValue, TStoreFunctions>
    {
        internal struct PendingContext<TInput, TOutput, TContext> : ISourceLogRecord<TValue>
        {
            // User provided information
            internal OperationType type;
            internal RecordInfo recordInfo;
            internal IHeapContainer<SpanByte> key;
            internal IHeapContainer<TValue> value;
            internal IHeapContainer<TInput> input;
            internal TOutput output;
            internal TContext userContext;
            internal long keyHash;
            internal long eTag;
            internal long expiration;

            // Some additional information about the previous attempt
            internal long id;
            internal long logicalAddress;
            internal long InitialLatestLogicalAddress;

            // operationFlags values
            internal ushort operationFlags;
            internal const ushort kNoOpFlags = 0;
            internal const ushort kIsNoKey = 0x0001;
            internal const ushort kIsAsync = 0x0002;
            internal const ushort kIsReadAtAddress = 0x0004;
            internal const ushort kIsObjectRecord = 0x0008;

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
                operationFlags = (ushort)((noKey ? kIsNoKey : kNoOpFlags) | (isAsync ? kIsAsync : kNoOpFlags));
                readCopyOptions = ReadCopyOptions.Merge(sessionReadCopyOptions, readOptions.CopyOptions);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal PendingContext(ReadCopyOptions readCopyOptions, bool isAsync = false, bool noKey = false)
            {
                // The async flag is often set when the PendingContext is created, so preserve that.
                operationFlags = (ushort)((noKey ? kIsNoKey : kNoOpFlags) | (isAsync ? kIsAsync : kNoOpFlags));
                this.readCopyOptions = readCopyOptions;
            }

            internal readonly bool IsNoKey => (operationFlags & kIsNoKey) != 0;
            internal void SetIsNoKey() => operationFlags |= kIsNoKey;

            internal readonly bool HasMinAddress => minAddress != Constants.kInvalidAddress;

            internal readonly bool IsAsync => (operationFlags & kIsAsync) != 0;

            internal readonly bool IsReadAtAddress => (operationFlags & kIsReadAtAddress) != 0;
            internal void SetIsReadAtAddress() => operationFlags |= kIsReadAtAddress;

            // Getter is in ISourceLogRecord region
            internal void SetIsObjectRecord() => operationFlags |= kIsObjectRecord;

            public void Dispose()
            {
                key?.Dispose();
                key = default;
                value?.Dispose();
                value = default;
                input?.Dispose();
                input = default;
            }

            #region ISourceLogRecord
            /// <inheritdoc/>
            public readonly bool IsObjectRecord => (operationFlags & kIsObjectRecord) != 0;
            /// <inheritdoc/>
            public readonly ref RecordInfo InfoRef => throw new TsavoriteException("Cannot call InfoRef on PendingContext"); // Cannot return ref to 'this'
            /// <inheritdoc/>
            public readonly RecordInfo Info => recordInfo;

            /// <inheritdoc/>
            public readonly bool IsSet => !recordInfo.IsNull;

            /// <inheritdoc/>
            public readonly SpanByte Key => key.Get();

            /// <inheritdoc/>
            public readonly unsafe SpanByte ValueSpan => IsObjectRecord ? throw new TsavoriteException("Cannot use ValueSpan on an Object record") : Unsafe.As<TValue, SpanByte>(ref value.Get());

            /// <inheritdoc/>
            public readonly TValue ValueObject => value.Get();

            /// <inheritdoc/>
            public readonly TValue GetReadOnlyValue() => value.Get();

            /// <inheritdoc/>
            public readonly long ETag => eTag;

            /// <inheritdoc/>
            public readonly long Expiration => expiration;

            /// <inheritdoc/>
            public readonly void ClearValueObject(Action<TValue> disposer) { }  // Not relevant for PendingContext

            /// <inheritdoc/>
            public readonly LogRecord<TValue> AsLogRecord() => throw new TsavoriteException("PendingContext cannot be converted to AsLogRecord");

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public readonly RecordFieldInfo GetRecordFieldInfo() => new()
                {
                    KeyTotalSize = Key.TotalSize,
                    ValueTotalSize = IsObjectRecord && !Info.ValueIsInline ? ObjectIdMap.ObjectIdSize : ValueSpan.TotalSize,
                    HasETag = Info.HasETag,
                    HasExpiration = Info.HasExpiration
                };
            #endregion // ISourceLogRecord
        }
    }
}