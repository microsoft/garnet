// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        internal unsafe struct PendingContext<TInput, TOutput, TContext> : ISourceLogRecord
        {
            // User provided information
            internal OperationType type;
            internal RecordInfo recordInfo;
            internal SpanByteHeapContainer key;
            internal SpanByteHeapContainer valueSpan;
            internal IHeapObject valueObject;
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
            internal const ushort kIsObjectRecord = 0x0008; // TODO replace with RecordInfo.ValueIsObject

            internal ReadCopyOptions readCopyOptions;   // Two byte enums
            internal WriteReason writeReason;   // for ConditionalCopyToTail; one byte enum

            internal long minAddress;
            internal long maxAddress;

            // For flushing head pages on tail allocation.
            internal CompletionEvent flushEvent;

            // For RMW if an allocation caused the source record for a copy to go from readonly to below HeadAddress, or for any operation with CAS failure.
            internal long retryNewLogicalAddress;

            // Address of the initial entry in the hash chain upon start of Internal(RUMD).
            internal long InitialEntryAddress;

            internal ScanCursorState scanCursorState;

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
                valueSpan?.Dispose();
                valueSpan = default;
                input?.Dispose();
                input = default;
            }

            #region ISourceLogRecord
            /// <inheritdoc/>
            public readonly bool ValueIsObject => (operationFlags & kIsObjectRecord) != 0;
            /// <inheritdoc/>
            public readonly ref RecordInfo InfoRef => throw new TsavoriteException("Cannot call InfoRef on PendingContext"); // Cannot return ref to 'this'
            /// <inheritdoc/>
            public readonly RecordInfo Info => recordInfo;

            /// <inheritdoc/>
            public readonly bool IsSet => !recordInfo.IsNull;

            /// <inheritdoc/>
            public readonly ReadOnlySpan<byte> Key => key.Get().ReadOnlySpan;

            /// <inheritdoc/>
            public readonly bool IsPinnedKey => true;

            /// <inheritdoc/>
            public byte* PinnedKeyPointer => key.Get().ToPointer();

            /// <inheritdoc/>
            public readonly unsafe Span<byte> ValueSpan => valueObject is null ? valueSpan.Get().Span : throw new TsavoriteException("Cannot use ValueSpan on an Object value");

            /// <inheritdoc/>
            public readonly IHeapObject ValueObject => valueObject;

            /// <inheritdoc/>
            public bool IsPinnedValue => true;

            /// <inheritdoc/>
            public byte* PinnedValuePointer => valueSpan.Get().ToPointer();

            /// <inheritdoc/>
            public readonly long ETag => recordInfo.HasETag ? eTag : LogRecord.NoETag;

            /// <inheritdoc/>
            public readonly long Expiration => recordInfo.HasExpiration ? expiration : 0;

            /// <inheritdoc/>
            public readonly void ClearValueObject(Action<IHeapObject> disposer) { }  // Not relevant for PendingContext

            /// <inheritdoc/>
            public readonly LogRecord AsLogRecord() => throw new TsavoriteException("PendingContext cannot be converted to AsLogRecord");

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public readonly RecordFieldInfo GetRecordFieldInfo() => new()
                {
                    KeyDataSize = Key.Length,
                    ValueDataSize = ValueIsObject ? ObjectIdMap.ObjectIdSize : ValueSpan.Length,
                    ValueIsObject = ValueIsObject,
                    HasETag = Info.HasETag,
                    HasExpiration = Info.HasExpiration
                };
            #endregion // ISourceLogRecord
        }
    }
}