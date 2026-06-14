// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive

    public partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// The in-memory hot-path state for a Read/RMW/Upsert/Delete operation. Carried by value on the
        /// caller's stack for the duration of the in-memory call, and never embedded directly on a pending
        /// IO op object — the pending-only fields (key/input/output/userContext/diskLogRecord/etc.) live on
        /// <see cref="PendingIoSlot{TInput, TOutput, TContext}"/> inside the rented
        /// <see cref="PendingIoContext{TInput, TOutput, TContext}"/>.
        /// </summary>
        /// <remarks>
        /// When an operation decides to go pending, the appropriate pending-going helper
        /// (<see cref="TsavoriteKV{TStoreFunctions, TAllocator}.CreatePendingReadContext"/>,
        /// <see cref="TsavoriteKV{TStoreFunctions, TAllocator}.CreatePendingRMWContext"/>,
        /// or <see cref="TsavoriteKV{TStoreFunctions, TAllocator}.PrepareIOForConditionalOperation"/>)
        /// rents a <see cref="PendingIoContext{TInput, TOutput, TContext}"/> from the per-session pool,
        /// populates <c>op.slot</c> in place, and stashes the op reference on <see cref="pendingOp"/>.
        /// <c>HandleOperationStatus</c> then finalizes
        /// the IO issue on <see cref="OperationStatus.RECORD_ON_DISK"/>.
        /// </remarks>
        internal struct PendingContext<TInput, TOutput, TContext>
        {
            /// <summary>The logical address of the found record, if any; used to create <see cref="RecordMetadata"/>.</summary>
            internal long logicalAddress;

            /// <summary>The logical address of the original record. Used by:
            /// <list type="bullet">
            /// <item>ConditionalScanPush — retains the address of the record we will push to the caller
            ///     if it is not found later in the log; <see cref="logicalAddress"/> must be the one we
            ///     pass to request.</item>
            /// <item>TryCopyToTail's PostCopyToTail trigger — when the source record is from disk
            ///     (HasMainLogSrc=false), this field carries the source logical address from the
            ///     compaction / CopyReadsToTail / ContinuePending caller.</item>
            /// </list></summary>
            internal long originalAddress;

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

            /// <summary>Initial IO record size for disk reads; <see cref="KVSettings.UseDefaultInitialIORecordSize"/> means inherit from session or store level.
            /// Note: default(PendingContext) leaves this as 0, which is also treated as "use default" by <see cref="TsavoriteKV{TStoreFunctions, TAllocator}.ResolveInitialIORecordSize"/>.</summary>
            internal int initialIORecordSize;

            // For flushing head pages on tail allocation.
            internal CompletionEvent flushEvent;

            // For RMW if an allocation caused the source record for a copy to go from readonly to below HeadAddress, or for any operation with CAS failure.
            internal long retryNewLogicalAddress;

            // Address of the initial entry in the hash chain upon start of Internal(RUMD).
            internal long initialEntryAddress;

            /// <summary>
            /// The pending IO op rented + populated by the pending-going helper. Non-null only when we are
            /// going pending; <c>HandleOperationStatus</c>
            /// reads this on <see cref="OperationStatus.RECORD_ON_DISK"/> to finalize the IO issue. The
            /// op's slot (<see cref="PendingIoContext{TInput, TOutput, TContext}.slot"/>) carries the
            /// pending-only payload (key/input/output/diskLogRecord/etc.).
            /// </summary>
            internal PendingIoContext<TInput, TOutput, TContext> pendingOp;

            /// <inheritdoc/>
            public override readonly string ToString()
            {
                return $"LA={logicalAddress}, InitLLA={initialLatestLogicalAddress}, ReadCopyOpt={readCopyOptions}, InitIORecSz={initialIORecordSize}, Flags=0x{operationFlags:x}, HasPendingOp={pendingOp is not null}";
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal PendingContext(ReadCopyOptions sessionReadCopyOptions, ref ReadOptions readOptions)
            {
                // operationFlags defaults to 0 (== kNoOpFlags); skip the redundant write.
                readCopyOptions = ReadCopyOptions.Merge(sessionReadCopyOptions, readOptions.CopyOptions);
                initialIORecordSize = readOptions.InitialIORecordSize;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal PendingContext(ReadCopyOptions readCopyOptions)
            {
                // operationFlags defaults to 0 (== kNoOpFlags) and initialIORecordSize defaults to 0 (treated as
                // "use default" by ResolveInitialIORecordSize); skip the redundant writes.
                this.readCopyOptions = readCopyOptions;
            }

            internal readonly bool IsNoKey => (operationFlags & kIsNoKey) != 0;
            internal void SetIsNoKey() => operationFlags |= kIsNoKey;

            internal readonly bool IsReadAtAddress => (operationFlags & kIsReadAtAddress) != 0;
            internal void SetIsReadAtAddress() => operationFlags |= kIsReadAtAddress;
        }
    }
}
