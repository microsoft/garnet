// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    public partial class VectorManager
    {
        private readonly ConcurrentDictionary<byte[], int> activeImportFinalizations;
#if NET9_0_OR_GREATER
        private readonly ConcurrentDictionary<byte[], int>.AlternateLookup<ReadOnlySpan<byte>> activeImportFinalizationsLookup;
#endif

        /// <summary>
        /// Creates an empty Vector Set.
        /// </summary>
        internal GarnetStatus CreateEmptyVectorSet(
            StorageSession storageSession,
            ReadOnlySpan<byte> key,
            uint dims,
            uint reduceDims,
            VectorQuantType quantizer,
            uint buildExplorationFactor,
            uint numLinks,
            VectorDistanceMetricType distanceMetric,
            bool hasQuantState,
            ReadOnlySpan<byte> quantState,
            uint startPointId,
            out VectorManagerResult result,
            out ReadOnlySpan<byte> errorMsg
        )
        {
            Span<byte> indexSpan = stackalloc byte[IndexSizeBytes];

            SessionParseState reusableParseState = new();
            var dimsArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref dims, 1)));
            var reduceDimsArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref reduceDims, 1)));
            PinnedSpanByte valueTypeArg = default;
            PinnedSpanByte valuesArg = default;
            PinnedSpanByte elementArg = default;
            var quantizerArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<VectorQuantType, byte>(MemoryMarshal.CreateSpan(ref quantizer, 1)));
            var buildExplorationFactorArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref buildExplorationFactor, 1)));
            PinnedSpanByte attributesArg = default;
            var numLinksArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref numLinks, 1)));
            var distanceMetricArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<VectorDistanceMetricType, byte>(MemoryMarshal.CreateSpan(ref distanceMetric, 1)));

            reusableParseState.InitializeWithArguments([dimsArg, reduceDimsArg, valueTypeArg, valuesArg, elementArg, quantizerArg, buildExplorationFactorArg, attributesArg, numLinksArg, distanceMetricArg]);

            var input = new StringInput(RespCommand.VADD, ref reusableParseState);

            using (ReadOrCreateVectorIndex(storageSession, key, ref input, indexSpan, startPointId, out var indexRes, out _, demandCreate: true))
            {
                if (indexRes == GarnetStatus.WRONGTYPE)
                {
                    result = VectorManagerResult.Duplicate;
                    errorMsg = "ERR key already exists"u8;
                    return GarnetStatus.OK;
                }
                else if (indexRes != GarnetStatus.OK)
                {
                    result = VectorManagerResult.BadParams;
                    errorMsg = "ERR vector set create failed"u8;
                    return GarnetStatus.OK;
                }

                ReadIndex(indexSpan, out var context, out _, out _, out _, out _, out _, out _, out _, out var indexPtr);

                if (hasQuantState && !Service.SetQuantState(context, indexPtr, quantState))
                {
                    _ = storageSession.DELETE(PinnedSpanByte.FromPinnedSpan(key), ref storageSession.unifiedBasicContext);

                    errorMsg = "ERR vector set quantizer state initialization failed"u8;
                    result = VectorManagerResult.BadParams;
                    return GarnetStatus.OK;
                }

                ReplicateVectorSetCreate(key, dims, reduceDims, quantizer, buildExplorationFactor, numLinks, distanceMetric, hasQuantState, quantState, startPointId);
            }

            result = VectorManagerResult.OK;
            errorMsg = ""u8;
            return GarnetStatus.OK;
        }

        /// <summary>
        /// Import a single term into a previously XVCREATE'd Vector Set.
        /// </summary>
        internal bool ImportTerm(ReadOnlySpan<byte> key, ReadOnlySpan<byte> indexConfig, uint termType, ReadOnlySpan<byte> id, ReadOnlySpan<byte> value)
        {
            if (IsImportCompleted(indexConfig) || IsImportFailed(indexConfig))
            {
                return false;
            }

            ReadIndex(indexConfig, out var context, out _, out _, out _, out _, out _, out _, out _, out var indexPtr);

            if (!Service.ImportTerm(context, indexPtr, termType, id, value))
            {
                return false;
            }

            if (!IsImportPending(indexConfig))
            {
                SetFlags(key, VectorSetFlags.ImportPending, ref ActiveThreadSession.stringBasicContext, SetImportStateArg);
            }

            ReplicateVectorSetImport(key, termType, id, value);
            return true;

        }

        /// <summary>
        /// During replication replay, we need to be able to wait for in progress finalization (if any) to complete.
        /// 
        /// This does that.
        /// </summary>
        internal void WaitForImportFinalization(ReadOnlySpan<byte> key)
        {
#if !NET9_0_OR_GREATER
            byte[] keyCopy = null;
#endif

            while (
                !quantizationOrImportChannel.Reader.Completion.IsCompleted &&
#if NET9_0_OR_GREATER
                activeImportFinalizationsLookup.ContainsKey(key)
#else
                activeImportFinalizations.ContainsKey(keyCopy ??= key.ToArray())
#endif
            )
            {
                _ = Thread.Yield();
            }
        }

        /// <summary>
        /// Joins or retries finalization while the caller retains the index lifetime lock.
        /// Partition progress is handle-local; completion and terminal failure are persisted in the index stub.
        /// </summary>
        internal NativeDiskANNMethods.DiskANNImportResult FinishImport(ReadOnlySpan<byte> key, ReadOnlySpan<byte> indexConfig)
        {
            if (IsImportFailed(indexConfig))
            {
                return NativeDiskANNMethods.DiskANNImportResult.FinishFailed;
            }

            if (IsImportCompleted(indexConfig))
            {
                return NativeDiskANNMethods.DiskANNImportResult.Success;
            }

            // If there's already an active finalization, don't do anything else
            var keyCopy = key.ToArray();
            if (!activeImportFinalizations.TryAdd(keyCopy, 0))
            {
                return NativeDiskANNMethods.DiskANNImportResult.Success;
            }

            ReadIndex(indexConfig, out var context, out _, out _, out _, out _, out _, out _, out _, out var indexPtr);

            if (!IsImportPending(indexConfig))
            {
                if (!Service.CanImport(context, indexPtr))
                {
                    return NativeDiskANNMethods.DiskANNImportResult.TaskFailed;
                }

                SetFlags(key, VectorSetFlags.ImportPending, ref ActiveThreadSession.stringBasicContext, SetImportStateArg);
            }

            var countdown = new CountdownHolder(quantizationAndImportTasks.Length);

            // Queue up finalization tasks
            for (var taskIx = 0; taskIx < quantizationAndImportTasks.Length; taskIx++)
            {
                if (!quantizationOrImportChannel.Writer.TryWrite(new(keyCopy, QuantizationOrImportStep.FinalizeImport, taskIx, countdown)))
                {
                    _ = activeImportFinalizations.TryRemove(keyCopy, out _);

                    return NativeDiskANNMethods.DiskANNImportResult.TaskFailed;
                }
            }

            ReplicateVectorSetFinishImport(key);

            return NativeDiskANNMethods.DiskANNImportResult.Success;
        }

        private bool TryProcessImportPartitionFinalize(ReadOnlySpan<byte> key, ulong context, nint indexPtr, int taskIndex, int taskCount)
        {
            try
            {
                var result = Service.FinishImport(context, indexPtr, taskIndex, taskCount);

                return result == NativeDiskANNMethods.DiskANNImportResult.Success;
            }
            catch (Exception exception)
            {
                logger?.LogError(exception, "Import finalization partition {taskIndex}/{taskCount} failed for context {context}", taskIndex, taskCount, context);
            }

            return false;
        }


        private static bool IsImportPending(ReadOnlySpan<byte> indexConfig)
        {
            ReadIndex(indexConfig, out _, out _, out _, out _, out _, out _, out _, out var flags, out _);
            return (flags & VectorSetFlags.ImportPending) != 0;
        }

        private static bool IsImportCompleted(ReadOnlySpan<byte> indexConfig)
        {
            ReadIndex(indexConfig, out _, out _, out _, out _, out _, out _, out _, out var flags, out _);
            return (flags & VectorSetFlags.ImportCompleted) != 0;
        }

        private static bool IsImportFailed(ReadOnlySpan<byte> indexConfig)
        {
            ReadIndex(indexConfig, out _, out _, out _, out _, out _, out _, out _, out var flags, out _);
            return (flags & VectorSetFlags.ImportFailed) != 0;
        }
    }
}