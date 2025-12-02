// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static LogAddress;

    public partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        internal unsafe struct PendingContext<TInput, TOutput, TContext>// : ISourceLogRecord
        {
            // User provided information
            internal OperationType type;

            /// <summary>
            /// DiskLogRecord carries a log record image. It is used for:
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

            /// <summary>The Key that was sent to this operation if it was RUMD.</summary>
            internal SpanByteHeapContainer request_key;
            /// <summary>The hash of <see cref="request_key"/> if it is present.</summary>
            internal long keyHash;

            /// <summary>The Input that was sent to this operation if it was RUMD.</summary>
            internal IHeapContainer<TInput> input;
            /// <summary>The Output to be returned from this operation if it was RUM.</summary>
            internal TOutput output;
            /// <summary>The user Context that was sent to this operation if it was RUMD.</summary>
            internal TContext userContext;

            /// <summary>The id of this operation in the <see cref="TsavoriteKV{TStoreFunctions, TAllocator}.TsavoriteExecutionContext{TInput, TOutput, TContext}.ioPendingRequests"/> queue.</summary>
            internal long id;

            /// <summary>The logical address of the found record, if any; used to create <see cref="RecordMetadata"/>.</summary>
            internal long logicalAddress;

            /// <summary>The record's ETag, if any; used to create <see cref="RecordMetadata"/> output in RUMD.</summary>
            internal long eTag;

            /// <summary>The initial highest logical address of the search; used to limit search ranges when the pending operation completes (e.g. to see if a duplicate was inserted).</summary>
            internal long initialLatestLogicalAddress;

            // operationFlags values
            internal ushort operationFlags;
#pragma warning disable IDE1006 // Naming Styles
            internal const ushort kNoOpFlags = 0;
            internal const ushort kIsNoKey = 0x0001;
            internal const ushort kIsReadAtAddress = 0x0002;
#pragma warning restore IDE1006 // Naming Styles

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
                request_key?.Dispose();
                request_key = default;
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
            internal void CopyInputsForReadOrRMW<TSessionFunctionsWrapper>(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, TContext userContext,
                    TSessionFunctionsWrapper sessionFunctions, SectorAlignedBufferPool bufferPool)
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            {
                request_key?.Dispose();
                request_key = new(key, bufferPool);

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

            /// <summary>
            /// Does an in-memory transfer of a record into the pending context. The transfer operates based on the implementation of the <typeparamref name="TSourceLogRecord"/>:
            /// <list type="bullet">
            ///     <item>If it is a <see cref="LogRecord"/>, it comes from the log or possibly an iterator; we copy the inline record data into a <see cref="SectorAlignedMemory"/>
            ///         and then reassign its ObjectIds to the <paramref name="transientObjectIdMap"/>.</item>
            ///     <item>Otherwise it is a <see cref="DiskLogRecord"/> and we can transfer its <see cref="SectorAlignedMemory"/> into the local <see cref="DiskLogRecord"/>.
            ///         It will already have a <paramref name="transientObjectIdMap"/> in its contained <see cref="LogRecord"/>.</item>
            /// </list>
            /// </summary>
            /// <param name="srcLogRecord">The log record to be copied into the <see cref="PendingContext{TInput, TOutput, TContext}"/>. This may be either in-memory or from disk IO</param>
            /// <param name="bufferPool"></param>
            /// <param name="transientObjectIdMap"></param>
            /// <param name="objectDisposer"></param>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void TransferFrom<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, SectorAlignedBufferPool bufferPool, ObjectIdMap transientObjectIdMap, Action<IHeapObject> objectDisposer)
                where TSourceLogRecord : ISourceLogRecord
            {
                Debug.Assert(!diskLogRecord.IsSet, "Should not try to reset PendingContext.diskLogRecord");
                if (srcLogRecord.IsMemoryLogRecord)
                {
                    ref var memoryLogRecord = ref srcLogRecord.AsMemoryLogRecordRef();
                    diskLogRecord = DiskLogRecord.CopyFrom(in memoryLogRecord, bufferPool, transientObjectIdMap, objectDisposer);
                    return;
                }

                // If the inputDiskLogRecord owns its memory, transfer it to the local diskLogRecord; otherwise we need to deep copy.
                Debug.Assert(srcLogRecord.IsDiskLogRecord, $"Unknown SrcLogRecord implementation: {srcLogRecord}");
                ref var inputDiskLogRecord = ref srcLogRecord.AsDiskLogRecordRef();
                diskLogRecord = DiskLogRecord.TransferFrom(ref inputDiskLogRecord);
            }

            internal void TransferFrom(ref DiskLogRecord inputDiskLogRecord)
            {
                Debug.Assert(!diskLogRecord.IsSet, "Should not try to reset PendingContext.diskLogRecord");
                diskLogRecord = DiskLogRecord.TransferFrom(ref inputDiskLogRecord);
            }

            #endregion // Serialized Record Creation

            #region Shortcuts to contained DiskLogRecord
            /// <inheritdoc/>
            public readonly RecordInfo Info
            {
                get
                {
                    Debug.Assert(IsSet, "PendingContext.diskLogRecord must be set for 'Info'");
                    return diskLogRecord.Info;
                }
            }

            /// <inheritdoc/>
            public readonly bool IsSet => diskLogRecord.IsSet;

            /// <inheritdoc/>
            public readonly ReadOnlySpan<byte> Key
            {
                get
                {
                    Debug.Assert(IsSet, "PendingContext.diskLogRecord must be set for 'Key'");
                    return diskLogRecord.Key;
                }
            }
            #endregion Shortcuts to contained DiskLogRecord
        }
    }
}