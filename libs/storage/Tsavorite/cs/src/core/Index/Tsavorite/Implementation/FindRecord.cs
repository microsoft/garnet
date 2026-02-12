// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    using static LogAddress;

    public unsafe partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SkipLocalsInit]
        private bool TryFindRecordInMemory<TInput, TOutput, TContext>(ReadOnlySpan<byte> key, ReadOnlySpan<byte> namespaceBytes, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx,
                                                                   ref PendingContext<TInput, TOutput, TContext> pendingContext)
        {
            // Add 1 to the pendingContext minAddresses because we don't want an inclusive search; we're looking to see if it was added *after*.
            if (UseReadCache)
            {
                var minRC = IsReadCache(pendingContext.initialEntryAddress) ? pendingContext.initialEntryAddress + 1 : kInvalidAddress;

                if (FindInReadCache(key, namespaceBytes, ref stackCtx, minAddress: minRC))
                    return true;
            }

            var minLog = pendingContext.initialLatestLogicalAddress < hlogBase.HeadAddress ? hlogBase.HeadAddress : pendingContext.initialLatestLogicalAddress + 1;

            return TraceBackForKeyMatch(key, namespaceBytes, ref stackCtx.recSrc, minAddress: minLog);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        // Return true if the record is found in the log, else false and an indication of whether we need to do IO to continue the search
        internal bool TryFindRecordInMainLogForConditionalOperation<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ReadOnlySpan<byte> key, ReadOnlySpan<byte> namespaceBytes, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, long currentAddress, long minAddress, long maxAddress, out OperationStatus internalStatus, out bool needIO)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (!FindTag(ref stackCtx.hei))
            {
                internalStatus = OperationStatus.NOTFOUND;
                return needIO = false;
            }

            internalStatus = OperationStatus.SUCCESS;
            if (!stackCtx.hei.IsReadCache)
            {
                // If the address in the HashBucketEntry is the current address, there'll be no record above it, so return false (not found).
                // If there are no valid records in the HashBucketEntry (minAddress is inclusive), return false (not found).
                if (stackCtx.hei.Address == currentAddress || stackCtx.hei.Address < minAddress || stackCtx.hei.Address < hlogBase.BeginAddress)
                {
                    stackCtx.SetRecordSourceToHashEntry(hlogBase);
                    return needIO = false;
                }
                if (stackCtx.hei.Address < hlogBase.HeadAddress)
                {
                    stackCtx.SetRecordSourceToHashEntry(hlogBase);
                    needIO = true;
                    return false;
                }
            }

            if (RevivificationManager.UseFreeRecordPool)
            {
                // The EphemeralSLock here is necessary only for the tag chain to avoid record elision/revivification during traceback.
                if (!TryEphemeralSLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out internalStatus))
                    return needIO = false;
            }
            else
                stackCtx.SetRecordSourceToHashEntry(hlogBase);

            try
            {
                if (UseReadCache)
                    SkipReadCache(ref stackCtx, out _); // Where this is called, we have no dependency on source addresses so we don't care if it Refreshed

                // We don't have a pendingContext here, so pass the minAddress directly.
                needIO = false;
                if (TryFindRecordInMainLogForPendingOperation(key, namespaceBytes, ref stackCtx, minAddress < hlogBase.HeadAddress ? hlogBase.HeadAddress : minAddress, maxAddress, out internalStatus))
                    return true;

                needIO = stackCtx.recSrc.LogicalAddress >= minAddress && stackCtx.recSrc.LogicalAddress < hlogBase.HeadAddress && stackCtx.recSrc.LogicalAddress >= hlogBase.BeginAddress;
                return false;
            }
            finally
            {
                EphemeralSUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx);
            }
        }

        // We want to return non-Invalid records or Invalid records that are Sealed, because Sealed is part of our "need RETRY" handling.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsValidTracebackRecord(RecordInfo recordInfo) => !recordInfo.Invalid || recordInfo.IsSealed;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(ReadOnlySpan<byte> key, ReadOnlySpan<byte> namespaceBytes, ref RecordSource<TStoreFunctions, TAllocator> recSrc, long minAddress)
        {
            Debug.Assert(!recSrc.HasInMemorySrc, "Should not have found record before this call");
            if (recSrc.LogicalAddress >= minAddress)
            {
                _ = recSrc.SetPhysicalAddress();
                var logRecord = recSrc.CreateLogRecord();
                if (IsValidTracebackRecord(logRecord.Info) && storeFunctions.KeysEqual(key, namespaceBytes, logRecord.Key, logRecord.Namespace))
                {
                    recSrc.SetHasMainLogSrc();
                    return true;
                }

                if (TraceBackForKeyMatch(key, namespaceBytes, logRecord.Info.PreviousAddress, minAddress, out recSrc.LogicalAddress, out recSrc.PhysicalAddress))
                {
                    recSrc.SetHasMainLogSrc();
                    return true;
                }
            }
            return false;
        }

        // Overload with maxAddress to avoid the extra if condition - TODO: check that this duplication saves on IL/perf
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(ReadOnlySpan<byte> key, ReadOnlySpan<byte> namespaceBytes, ref RecordSource<TStoreFunctions, TAllocator> recSrc, long minAddress, long maxAddress)
        {
            Debug.Assert(!recSrc.HasInMemorySrc, "Should not have found record before this call");
            if (recSrc.LogicalAddress >= minAddress)
            {
                _ = recSrc.SetPhysicalAddress();
                var logRecord = recSrc.CreateLogRecord();
                if (IsValidTracebackRecord(logRecord.Info) && recSrc.LogicalAddress < maxAddress && storeFunctions.KeysEqual(key, namespaceBytes, logRecord.Key, logRecord.Namespace))
                {
                    recSrc.SetHasMainLogSrc();
                    return true;
                }

                if (TraceBackForKeyMatch(key, namespaceBytes, logRecord.Info.PreviousAddress, minAddress, maxAddress, out recSrc.LogicalAddress, out recSrc.PhysicalAddress))
                {
                    recSrc.SetHasMainLogSrc();
                    return true;
                }
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(ReadOnlySpan<byte> key, ReadOnlySpan<byte> namespaceBytes, long fromLogicalAddress, long minAddress, out long foundLogicalAddress, out long foundPhysicalAddress)
        {
            // This overload is called when the record at the "current" logical address does not match 'key'; fromLogicalAddress is its .PreviousAddress.
            foundLogicalAddress = fromLogicalAddress;
            while (foundLogicalAddress >= minAddress)
            {
                var logRecord = hlog.CreateLogRecord(foundLogicalAddress);
                foundPhysicalAddress = logRecord.physicalAddress;

                if (IsValidTracebackRecord(logRecord.Info) && storeFunctions.KeysEqual(key, namespaceBytes, logRecord.Key, logRecord.Namespace))
                    return true;

                foundLogicalAddress = logRecord.Info.PreviousAddress;
            }
            foundPhysicalAddress = kInvalidAddress;
            return false;
        }

        // Overload with maxAddress to avoid the extra if condition - TODO: check that this duplication saves on IL/perf
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(ReadOnlySpan<byte> key, ReadOnlySpan<byte> namespaceBytes, long fromLogicalAddress, long minAddress, long maxAddress, out long foundLogicalAddress, out long foundPhysicalAddress)
        {
            // This overload is called when the record at the "current" logical address does not match 'key'; fromLogicalAddress is its .PreviousAddress.
            foundLogicalAddress = fromLogicalAddress;
            while (foundLogicalAddress >= minAddress)
            {
                var logRecord = hlog.CreateLogRecord(foundLogicalAddress);
                foundPhysicalAddress = logRecord.physicalAddress;

                if (IsValidTracebackRecord(logRecord.Info) && foundLogicalAddress < maxAddress && storeFunctions.KeysEqual(key, namespaceBytes, logRecord.Key, logRecord.Namespace))
                    return true;

                foundLogicalAddress = logRecord.Info.PreviousAddress;
            }
            foundPhysicalAddress = kInvalidAddress;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordForUpdate(ReadOnlySpan<byte> key, ReadOnlySpan<byte> namespaceBytes, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, long minAddress, out OperationStatus internalStatus)
        {
            // This routine returns true if we should proceed with the InternalXxx operation (whether the record was found or not),
            // else false (including false if we need a RETRY). If it returns true with recSrc.HasInMemorySrc, caller must set srcRecordInfo.

            // We are not here from Read() so have not processed readcache; search that as well as the in-memory log.
            // minAddress is either HeadAddress or ReadOnlyAddress for the main log.
            if ((UseReadCache && FindInReadCache(key, namespaceBytes, ref stackCtx, minAddress: kInvalidAddress))
                || TraceBackForKeyMatch(key, namespaceBytes, ref stackCtx.recSrc, minAddress: minAddress))
            {
                if (stackCtx.recSrc.GetInfo().IsClosed)
                {
                    internalStatus = OperationStatus.RETRY_LATER;
                    return false;
                }
            }
            internalStatus = OperationStatus.SUCCESS;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SkipLocalsInit]
        private bool TryFindRecordForPendingOperation<TInput, TOutput, TContext>(ReadOnlySpan<byte> key, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, out OperationStatus internalStatus,
                                                      ref PendingContext<TInput, TOutput, TContext> pendingContext)
        {
            // This routine returns true if we find the key, else false.
            internalStatus = OperationStatus.SUCCESS;

            // TODO: Verify most of this erases as expected
            Span<byte> namespacesBytesMut = stackalloc byte[8];
            scoped var namespaceBytes = LogRecord.DefaultNamespace;
            if (InputExtraOptions.TryGetNamespace(ref pendingContext.input.Get(), ref namespacesBytesMut))
            {
                namespaceBytes = namespacesBytesMut;
            }

            if (!TryFindRecordInMemory(key, namespaceBytes, ref stackCtx, ref pendingContext))
                return false;
            if (stackCtx.recSrc.GetInfo().IsClosed)
                internalStatus = OperationStatus.RETRY_LATER;

            // We do not lock here; this is just to see if the key is found
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordInMainLogForPendingOperation(ReadOnlySpan<byte> key, ReadOnlySpan<byte> namespaceBytes, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, long minAddress, long maxAddress, out OperationStatus internalStatus)
        {
            // This overload is called when we do not have a PendingContext to get minAddress from, and we've skipped the readcache if present.

            // This routine returns true if we find the key, else false.
            internalStatus = OperationStatus.SUCCESS;

            if (!TraceBackForKeyMatch(key, namespaceBytes, ref stackCtx.recSrc, minAddress, maxAddress))
                return false;
            if (stackCtx.recSrc.GetInfo().IsClosed)
                internalStatus = OperationStatus.RETRY_LATER;

            // We do not lock here; this is just to see if the key is found
            return true;
        }
    }
}