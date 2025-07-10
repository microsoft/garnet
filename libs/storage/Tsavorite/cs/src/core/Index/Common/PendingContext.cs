// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    using static LogAddress;

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
            internal long initialLatestLogicalAddress;

            // operationFlags values
            internal ushort operationFlags;
            internal const ushort kNoOpFlags = 0;
            internal const ushort kIsNoKey = 0x0001;
            internal const ushort kIsReadAtAddress = 0x0002;

            internal ReadCopyOptions readCopyOptions;   // Two byte enums

            internal long minAddress;
            internal long maxAddress;

            // For flushing head pages on tail allocation.
            internal CompletionEvent flushEvent;

            // For RMW if an allocation caused the source record for a copy to go from readonly to below HeadAddress, or for any operation with CAS failure.
            internal long retryNewLogicalAddress;

            // Address of the initial entry in the hash chain upon start of Internal(RUMD).
            internal long initialEntryAddress;

            internal ScanCursorState scanCursorState;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal PendingContext(long keyHash) => this.keyHash = keyHash;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal PendingContext(ReadCopyOptions sessionReadCopyOptions, ref ReadOptions readOptions)
            {
                // The async flag is often set when the PendingContext is created, so preserve that.
                operationFlags = kNoOpFlags;
                readCopyOptions = ReadCopyOptions.Merge(sessionReadCopyOptions, readOptions.CopyOptions);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal PendingContext(ReadCopyOptions readCopyOptions)
            {
                // The async flag is often set when the PendingContext is created, so preserve that.
                operationFlags = kNoOpFlags;
                this.readCopyOptions = readCopyOptions;
            }

            internal readonly bool IsNoKey => (operationFlags & kIsNoKey) != 0;
            internal void SetIsNoKey() => operationFlags |= kIsNoKey;

            internal readonly bool HasMinAddress => minAddress != kInvalidAddress;

            internal readonly bool IsReadAtAddress => (operationFlags & kIsReadAtAddress) != 0;
            internal void SetIsReadAtAddress() => operationFlags |= kIsReadAtAddress;

            public void Dispose()
            {
                diskLogRecord.Dispose();
                diskLogRecord = default;
                input?.Dispose();
                input = default;
            }

            #region Serialized Record Creation
            /// <summary>
            /// Serialize for Read and RMW operations; no Value is passed
            /// </summary>
            /// <param name="key">Record key</param>
            /// <param name="input">Input to the operation</param>
            /// <param name="output">Output from the operation</param>
            /// <param name="userContext">User context for the operation</param>
            /// <param name="sessionFunctions">Session functions wrapper for the operation</param>
            /// <param name="bufferPool">Allocator for backing storage</param>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void SerializeForReadOrRMW<TSessionFunctionsWrapper>(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, TContext userContext,
                    TSessionFunctionsWrapper sessionFunctions, SectorAlignedBufferPool bufferPool)
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            {
                if (diskLogRecord.IsSet)
                    return;
                diskLogRecord.SerializeForPendingReadOrRMW(key, bufferPool);
                CopyIOC(ref input, output, userContext, sessionFunctions);
            }

            /// <summary>
            /// Serialize a <see cref="LogRecord"/> or <see cref="DiskLogRecord"/> into the local <see cref="DiskLogRecord"/> for Pending operations
            /// </summary>
            /// <param name="srcLogRecord">The log record to be copied into the <see cref="PendingContext{TInput, TOutput, TContext}"/>. This may be either in-memory or from disk IO</param>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Serialize<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, SectorAlignedBufferPool bufferPool)
                where TSourceLogRecord : ISourceLogRecord
            {
                Debug.Assert(!diskLogRecord.IsSet, "Should not try to reset PendingContext.diskLogRecord");
                if (srcLogRecord.IsMemoryLogRecord)
                {
                    ref var inMemoryLogRecord = ref srcLogRecord.AsMemoryLogRecordRef();
                    diskLogRecord.Serialize(in inMemoryLogRecord, bufferPool);
                    return;
                }

                // If the inputDiskLogRecord owns its record's memory buffer, transfer it to the local diskLogRecord; otherwise we need to deep copy.
                if (!srcLogRecord.IsDiskLogRecord)
                    throw new TsavoriteException("Unknown TSourceLogRecord type");
                ref var inputDiskLogRecord = ref srcLogRecord.AsDiskLogRecordRef();
                if (inputDiskLogRecord.OwnsRecordBuffer)
                    diskLogRecord.TransferFrom(ref inputDiskLogRecord);
                else
                    diskLogRecord.CloneFrom(ref inputDiskLogRecord, bufferPool, preferDeserializedObject: true);
            }

            private void CopyIOC<TSessionFunctionsWrapper>(ref TInput input, TOutput output, TContext userContext, TSessionFunctionsWrapper sessionFunctions) 
                    where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            {
                if (this.input == default)
                {
                    if (typeof(TInput) == typeof(PinnedSpanByte))
                        this.input = new SpanByteHeapContainer(Unsafe.As<TInput, PinnedSpanByte>(ref input), sessionFunctions.Store.hlogBase.bufferPool) as IHeapContainer<TInput>;
                    else
                        this.input = new StandardHeapContainer<TInput>(ref input);
                }
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
            public ReadOnlySpan<byte> AsReadOnlySpan() => diskLogRecord.AsReadOnlySpan();

            /// <inheritdoc/>
            public bool IsPinnedValue => diskLogRecord.IsPinnedValue;

            /// <inheritdoc/>
            public byte* PinnedValuePointer => diskLogRecord.PinnedValuePointer;

            /// <inheritdoc/>
            public readonly long ETag => diskLogRecord.ETag;

            /// <inheritdoc/>
            public readonly long Expiration => diskLogRecord.Expiration;

            /// <inheritdoc/>
            public readonly bool IsMemoryLogRecord => false;

            /// <inheritdoc/>
            public unsafe ref LogRecord AsMemoryLogRecordRef() => throw new InvalidOperationException("Cannot cast a PendingContext to a memory LogRecord.");

            /// <inheritdoc/>
            public readonly bool IsDiskLogRecord => true;

            /// <inheritdoc/>
            public unsafe ref DiskLogRecord AsDiskLogRecordRef() => ref Unsafe.AsRef(in diskLogRecord);

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