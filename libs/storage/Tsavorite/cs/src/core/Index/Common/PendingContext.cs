// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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

            /// <summary>
            /// DiskLogRecord carries either the input to RUMD operations or a log record image. It is used for:
            /// <list type="bullet">
            ///     <item>Pending RUMD operations; in this case it contains only the key for all operations, and values for Upsert.
            ///         Optionals (ETag and Expiration) are presumed to be carried in <see cref="input"/></item>
            ///     <item>For pending ConditionalCopy operations, where it is one of:
            ///         <list type="bullet">
            ///             <item>A <see cref="DiskLogRecord"/> created by serializing from an in-memory <see cref="LogRecord"/></item>
            ///             <item>A <see cref="DiskLogRecord"/> retrieved from the disk, for operations such as Compact</item>
            ///         </list>
            ///     </item>
            /// </list>
            /// </summary>
            internal DiskLogRecord diskLogRecord;

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
            internal const ushort kIsNoKey = 0x0001;
            internal const ushort kIsAsync = 0x0002;
            internal const ushort kIsReadAtAddress = 0x0004;
            internal const ushort kIsObjectRecord = 0x0008; // TODO replace with RecordInfo.ValueIsObject

            internal ReadCopyOptions readCopyOptions;   // Two byte enums

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
                diskLogRecord.Dispose();
                diskLogRecord = default;
                input?.Dispose();
                input = default;
            }

            #region Serialized Record Creation
            /// <summary>
            /// Serialize for RUMD operations
            /// </summary>
            /// <param name="key">Record key</param>
            /// <param name="input">Input to the operation</param>
            /// <param name="valueSpan">Record value as a Span, if Upsert</param>
            /// <param name="valueObject">Record value as an object, if Upsert</param>
            /// <param name="output">Output from the operation</param>
            /// <param name="userContext">User context for the operation</param>
            /// <param name="sessionFunctions">Session functions wrapper for the operation</param>
            /// <param name="bufferPool">Allocator for backing storage</param>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Serialize<TSessionFunctionsWrapper>(ReadOnlySpan<byte> key, ref TInput input, ReadOnlySpan<byte> valueSpan, IHeapObject valueObject, ref TOutput output, TContext userContext,
                    TSessionFunctionsWrapper sessionFunctions, SectorAlignedBufferPool bufferPool)
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            {
                if (diskLogRecord.IsSet)
                    return;
                diskLogRecord.SerializeForPendingRUMD(key, valueSpan, valueObject, bufferPool);
                CopyIOC(ref input, output, userContext, sessionFunctions);
            }

            /// <summary>
            /// Serialize a <see cref="LogRecord"/> or <see cref="DiskLogRecord"/> and Input, Output, and userContext into the local <see cref="DiskLogRecord"/> for Pending operations
            /// </summary>
            /// <param name="srcLogRecord">The log record. This may be either in-memory or from disk IO</param>
            /// <param name="input">Input to the operation</param>
            /// <param name="output">Output from the operation</param>
            /// <param name="userContext">User context for the operation</param>
            /// <param name="sessionFunctions">Session functions wrapper for the operation</param>
            /// <param name="bufferPool">Allocator for backing storage</param>
            /// <param name="valueSerializer">Serializer for value object (if any); if null, the object is to be held as an object (e.g. for Pending IO operations)
            ///     rather than serialized to a byte stream (e.g. for out-of-process operations)</param>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Serialize<TSessionFunctionsWrapper, TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, TContext userContext, TSessionFunctionsWrapper sessionFunctions,
                    SectorAlignedBufferPool bufferPool, IObjectSerializer<IHeapObject> valueSerializer)
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
                where TSourceLogRecord : ISourceLogRecord
            {
                Serialize(ref srcLogRecord, sessionFunctions, bufferPool, valueSerializer);
                CopyIOC(ref input, output, userContext, sessionFunctions);
            }

            /// <summary>
            /// Serialize a <see cref="LogRecord"/> or <see cref="DiskLogRecord"/> into the local <see cref="DiskLogRecord"/> for Pending operations
            /// </summary>
            /// <param name="srcLogRecord">The log record to be copied into the <see cref="PendingContext{TInput, TOutput, TContext}"/>. This may be either in-memory or from disk IO</param>
            /// <param name="sessionFunctions">Session functions wrapper for the operation</param>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Serialize<TSessionFunctionsWrapper, TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, TSessionFunctionsWrapper sessionFunctions,
                    SectorAlignedBufferPool bufferPool, IObjectSerializer<IHeapObject> valueSerializer)
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
                where TSourceLogRecord : ISourceLogRecord
            {
                Debug.Assert(!diskLogRecord.IsSet, "Should not try to reset PendingContext.diskLogRecord");
                if (srcLogRecord.AsLogRecord(out var logRecord))
                {
                    diskLogRecord.Serialize(in logRecord, bufferPool, valueSerializer);
                }
                else
                {
                    // If the inputDiskLogRecord owns its memory, transfer it to the local diskLogRecord; otherwise we need to deep copy.
                    _ = srcLogRecord.AsDiskLogRecord(out var inputDiskLogRecord);
                    if (inputDiskLogRecord.OwnsMemory)
                        diskLogRecord.Transfer(ref inputDiskLogRecord);
                    else
                        diskLogRecord.CloneFrom(ref inputDiskLogRecord, bufferPool, preferDeserializedObject: true);
                }
            }

            private void CopyIOC<TSessionFunctionsWrapper>(ref TInput input, TOutput output, TContext userContext, TSessionFunctionsWrapper sessionFunctions) 
                    where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            {
                if (this.input == default)
                    this.input = new StandardHeapContainer<TInput>(ref input);
                this.output = output;
                sessionFunctions.ConvertOutputToHeap(ref input, ref this.output);
                this.userContext = userContext;
            }

            #endregion // Serialized Record Creation

            #region ISourceLogRecord
            /// <inheritdoc/>
            public readonly ref RecordInfo InfoRef => ref diskLogRecord.InfoRef;
            /// <inheritdoc/>
            public readonly RecordInfo Info => diskLogRecord.Info;

            /// <inheritdoc/>
            public readonly bool IsSet => diskLogRecord.IsSet;

            /// <inheritdoc/>
            public readonly ReadOnlySpan<byte> Key => diskLogRecord.Key;

            /// <inheritdoc/>
            public readonly bool IsPinnedKey => diskLogRecord.IsPinnedKey;

            /// <inheritdoc/>
            public byte* PinnedKeyPointer => diskLogRecord.PinnedKeyPointer;

            /// <inheritdoc/>
            public readonly unsafe Span<byte> ValueSpan => diskLogRecord.ValueSpan;

            /// <inheritdoc/>
            public readonly IHeapObject ValueObject => diskLogRecord.ValueObject;

            /// <inheritdoc/>
            public ReadOnlySpan<byte> RecordSpan => diskLogRecord.RecordSpan;

            /// <inheritdoc/>
            public bool IsPinnedValue => diskLogRecord.IsPinnedValue;

            /// <inheritdoc/>
            public byte* PinnedValuePointer => diskLogRecord.PinnedValuePointer;

            /// <inheritdoc/>
            public readonly long ETag => diskLogRecord.ETag;

            /// <inheritdoc/>
            public readonly long Expiration => diskLogRecord.Expiration;

            /// <inheritdoc/>
            public readonly void ClearValueObject(Action<IHeapObject> disposer) { }  // Not relevant for PendingContext

            /// <inheritdoc/>
            public readonly bool AsLogRecord(out LogRecord logRecord)
            {
                logRecord = default;
                return false;
            }

            /// <inheritdoc/>
            public readonly bool AsDiskLogRecord(out DiskLogRecord diskLogRecord)
            {
                diskLogRecord = this.diskLogRecord;
                return true;
            }

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public readonly RecordFieldInfo GetRecordFieldInfo() => new()
                {
                    KeyDataSize = Key.Length,
                    ValueDataSize = Info.ValueIsObject ? ObjectIdMap.ObjectIdSize : ValueSpan.Length,
                    ValueIsObject = Info.ValueIsObject,
                    HasETag = Info.HasETag,
                    HasExpiration = Info.HasExpiration
                };
            #endregion // ISourceLogRecord
        }
    }
}