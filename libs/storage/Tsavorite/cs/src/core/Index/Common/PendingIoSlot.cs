// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// The pending-only payload of an in-flight pending IO operation. Carried inline on the rented
        /// <see cref="PendingIoContext{TInput, TOutput, TContext}"/> (never on the in-memory caller's stack),
        /// so the hot path's <see cref="PendingContext{TInput, TOutput, TContext}"/> can stay small.
        /// </summary>
        /// <remarks>
        /// Fields here are touched only after the in-memory call has decided to go pending
        /// (RECORD_ON_DISK / CONDITIONAL_INSERT / CONDITIONAL_SCAN_PUSH). The pending-going
        /// helpers (CreatePendingReadContext / CreatePendingRMWContext / PrepareIOForConditionalOperation
        /// / PrepareIOForConditionalScan) rent a <see cref="PendingIoContext{TInput, TOutput, TContext}"/>
        /// from the per-session pool and populate <c>op.slot</c> in place. On re-pend (a ContinuePending*
        /// call returning RECORD_ON_DISK), the slot's heap-owning fields (<see cref="requestKey"/>,
        /// <see cref="input"/>, <see cref="diskLogRecord"/>) are moved by struct copy to the fresh op
        /// and the old op's slot is cleared so disposal happens exactly once.
        /// </remarks>
        internal struct PendingIoSlot<TInput, TOutput, TContext>
        {
            /// <summary>The operation type (READ / RMW / CONDITIONAL_INSERT / CONDITIONAL_SCAN_PUSH). Used
            /// to dispatch the appropriate ContinuePending* in <see cref="TsavoriteKV{TStoreFunctions, TAllocator}.InternalCompletePendingRequestFromContext"/>.</summary>
            internal OperationType type;

            internal readonly bool IsConditionalOp => type is OperationType.CONDITIONAL_INSERT or OperationType.CONDITIONAL_SCAN_PUSH;

            /// <summary>
            /// DiskLogRecord carries a log record image. It is used for pending ConditionalCopy operations, where it is one of:
            /// <list type="bullet">
            ///     <item>A <see cref="DiskLogRecord"/> created by serializing from an in-memory <see cref="LogRecord"/></item>
            ///     <item>A <see cref="DiskLogRecord"/> retrieved from the disk, for operations such as Compact</item>
            /// </list>
            /// </summary>
            internal DiskLogRecord diskLogRecord;

            /// <summary>The Key that was sent to this operation if it was RUMD.</summary>
            internal ConditionallyHoistedKey requestKey;

            /// <summary>The hash of <see cref="requestKey"/> if it is present.</summary>
            internal long keyHash;

            /// <summary>The Input that was sent to this operation if it was RUMD.</summary>
            internal IHeapContainer<TInput> input;

            /// <summary>The Output to be returned from this operation if it was RUM.</summary>
            internal TOutput output;

            /// <summary>The user Context that was sent to this operation if it was RUMD.</summary>
            internal TContext userContext;

            /// <summary>Lower-bound address for conditional ops (CONDITIONAL_INSERT / CONDITIONAL_SCAN_PUSH).</summary>
            internal long minAddress;

            /// <summary>Upper-bound address for conditional ops (CONDITIONAL_INSERT / CONDITIONAL_SCAN_PUSH).</summary>
            internal long maxAddress;

            /// <summary>The scan-cursor state for CONDITIONAL_SCAN_PUSH.</summary>
            internal ScanCursorState scanCursorState;

            internal readonly bool HasMinAddress => minAddress != LogAddress.kInvalidAddress;

            public void Dispose()
            {
                if (diskLogRecord.IsSet)
                    diskLogRecord.Dispose();
                diskLogRecord = default;
                requestKey = default;
                input?.Dispose();
                input = default;
            }

            #region Serialized Record Creation

            /// <summary>
            /// Serialize for Read and RMW operations; no Value is passed.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void CopyInputsForReadOrRMW<TKey, TSessionFunctionsWrapper>(TKey key, ref TInput input, ref TOutput output, TContext userContext,
                    TSessionFunctionsWrapper sessionFunctions, SectorAlignedBufferPool bufferPool)
                where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            {
                CopyKey(key, bufferPool, sessionFunctions);

                if (this.input == default)
                {
                    // Rent a heap-container wrapper from the per-session pool so the disk-pending
                    // hot path is allocation-free in steady state. The wrapper returns itself to
                    // the pool when its Dispose() is invoked by Dispose(); on a cold session
                    // (pool empty) we allocate once and the wrapper joins the pool on first disposal.
                    var pool = sessionFunctions.Ctx.heapContainerPool;
                    if (typeof(TInput) == typeof(PinnedSpanByte))
                    {
                        // Under this typeof-folded branch, TInput is statically PinnedSpanByte at JIT
                        // specialization, so the Stack<IHeapContainer<TInput>> is the same CLR type as
                        // Stack<IHeapContainer<PinnedSpanByte>>; reinterpret the reference without a
                        // CLR type check.
                        var spanByteInput = Unsafe.As<TInput, PinnedSpanByte>(ref input);
                        var spanBytePool = Unsafe.As<Stack<IHeapContainer<PinnedSpanByte>>>(pool);
                        if (spanBytePool.TryPop(out var rented))
                        {
                            Unsafe.As<SpanByteHeapContainer>(rented).Initialize(spanByteInput, sessionFunctions.Store.hlogBase.bufferPool, spanBytePool);
                            this.input = Unsafe.As<IHeapContainer<PinnedSpanByte>, IHeapContainer<TInput>>(ref rented);
                        }
                        else
                        {
                            this.input = new SpanByteHeapContainer(spanByteInput, sessionFunctions.Store.hlogBase.bufferPool, spanBytePool) as IHeapContainer<TInput>;
                        }
                    }
                    else
                    {
                        if (pool.TryPop(out var rented))
                        {
                            Unsafe.As<StandardHeapContainer<TInput>>(rented).Initialize(ref input, pool);
                            this.input = rented;
                        }
                        else
                        {
                            this.input = new StandardHeapContainer<TInput>(ref input, pool);
                        }
                    }
                }
                this.output = output;
                sessionFunctions.ConvertOutputToHeap(ref input, ref this.output);
                this.userContext = userContext;
            }

            /// <summary>Copy the passed key into our <see cref="requestKey"/>.</summary>
            internal void CopyKey<TKey, TSessionFunctionsWrapper>(TKey key, SectorAlignedBufferPool bufferPool, TSessionFunctionsWrapper sessionFunctions)
                where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
                where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            {
                if (requestKey.IsEmpty)
                    requestKey = ConditionallyHoistedKey.Create(key, bufferPool);
                else
                    Debug.Assert(sessionFunctions.Store.StoreFunctions.KeysEqual(requestKey, key), "slot.requestKey should not change keys");
            }

            /// <summary>
            /// Does an in-memory transfer of a record into the pending slot. The transfer operates based on the implementation of the <typeparamref name="TSourceLogRecord"/>:
            /// <list type="bullet">
            ///     <item>If it is a <see cref="LogRecord"/>, it comes from the log or possibly an iterator; we copy the inline record data into a <see cref="SectorAlignedMemory"/>
            ///         and then reassign its ObjectIds to the <paramref name="transientObjectIdMap"/>.</item>
            ///     <item>Otherwise it is a <see cref="DiskLogRecord"/> and we can transfer its <see cref="SectorAlignedMemory"/> into the local <see cref="DiskLogRecord"/>.
            ///         It will already have a <paramref name="transientObjectIdMap"/> in its contained <see cref="LogRecord"/>.</item>
            /// </list>
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void CopyFrom<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, SectorAlignedBufferPool bufferPool, ObjectIdMap transientObjectIdMap)
                where TSourceLogRecord : ISourceLogRecord
            {
                Debug.Assert(!diskLogRecord.IsSet, "Should not try to reset slot.diskLogRecord");
                if (srcLogRecord.IsMemoryLogRecord)
                {
                    ref var memoryLogRecord = ref srcLogRecord.AsMemoryLogRecordRef();
                    diskLogRecord = DiskLogRecord.CopyFrom(in memoryLogRecord, bufferPool, transientObjectIdMap);
                    return;
                }

                Debug.Assert(srcLogRecord.IsDiskLogRecord, $"Unknown SrcLogRecord implementation: {srcLogRecord}");
                ref var inputDiskLogRecord = ref srcLogRecord.AsDiskLogRecordRef();
                // If the inputDiskLogRecord owns its memory this will efficiently transfer it to the local diskLogRecord; otherwise it will deep copy.
                diskLogRecord = DiskLogRecord.TransferFrom(ref inputDiskLogRecord, bufferPool);
            }

            internal void TransferFrom(ref DiskLogRecord inputDiskLogRecord, SectorAlignedBufferPool bufferPool)
            {
                Debug.Assert(!diskLogRecord.IsSet, "Should not try to reset slot.diskLogRecord");
                diskLogRecord = DiskLogRecord.TransferFrom(ref inputDiskLogRecord, bufferPool);
            }

            #endregion // Serialized Record Creation

            #region Shortcuts to contained DiskLogRecord
            public readonly DiskLogRecord DiskLogRecord
            {
                get
                {
                    Debug.Assert(diskLogRecord.IsSet, "slot.diskLogRecord must be set for 'DiskLogRecord'");
                    return diskLogRecord;
                }
            }

            /// <inheritdoc/>
            public readonly RecordInfo Info
            {
                get
                {
                    Debug.Assert(diskLogRecord.IsSet, "slot.diskLogRecord must be set for 'Info'");
                    return diskLogRecord.Info;
                }
            }

            /// <inheritdoc/>
            public readonly ReadOnlySpan<byte> Key
            {
                get
                {
                    Debug.Assert(diskLogRecord.IsSet, "slot.diskLogRecord must be set for 'Key'");
                    return diskLogRecord.Key;
                }
            }
            #endregion Shortcuts to contained DiskLogRecord
        }
    }
}
