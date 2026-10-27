// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Garnet.server
{
    public partial class VectorManager
    {
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
        /// Attempts to finish importing a Vector Set.
        /// </summary>
        internal bool TryFinishImport(scoped ReadOnlySpan<byte> key, scoped ReadOnlySpan<byte> indexConfig, out ReadOnlySpan<byte> errorMsg)
        {
            if (IsImportFailed(indexConfig))
            {
                errorMsg = "ERR Vector set import finish or validation failed previously"u8;
                return false;
            }

            if (IsImportCompleted(indexConfig))
            {
                errorMsg = default;
                return true;
            }

            ReadIndex(indexConfig, out var context, out _, out _, out _, out _, out _, out _, out _, out var indexPtr);

            if (!IsImportPending(indexConfig))
            {
                if (!Service.CanImport(context, indexPtr))
                {
                    errorMsg = "ERR Vector Set cannot be imported into"u8;
                    return false;
                }

                SetFlags(key, VectorSetFlags.ImportPending, ref ActiveThreadSession.stringBasicContext, SetImportStateArg);
            }

            if (!Service.FinishImport(context, indexPtr))
            {
                SetFlags(key, VectorSetFlags.ImportPending | VectorSetFlags.ImportFailed, ref ActiveThreadSession.stringBasicContext, SetImportStateArg);
                errorMsg = "ERR Vector set import finish failed"u8;
                return false;
            }

            SetFlags(key, VectorSetFlags.ImportCompleted, ref ActiveThreadSession.stringBasicContext, SetImportStateArg);

            ReplicateVectorSetFinishImport(key);

            errorMsg = default;
            return true;
        }

        /// <summary>
        /// Attempts to verify a partition of an imported Vector Set.
        /// 
        /// Failure marks the Vector Set import as failed.
        /// </summary>
        internal bool TryVerifyImport(scoped ReadOnlySpan<byte> key, scoped ReadOnlySpan<byte> indexConfig, int taskIndex, int taskCount, out ReadOnlySpan<byte> errorMsg)
        {
            ReadIndex(indexConfig, out var context, out _, out _, out _, out _, out _, out _, out _, out var indexPtr);

            // Validation on a not-importing Vector Set succeeds by definition
            if (!IsImportPending(indexConfig))
            {
                errorMsg = default;
                return true;
            }

            if (!Service.VerifyImport(context, indexPtr, taskIndex, taskCount))
            {
                SetFlags(key, VectorSetFlags.ImportPending | VectorSetFlags.ImportFailed, ref ActiveThreadSession.stringBasicContext, SetImportStateArg);
                errorMsg = "ERR Vector set import validation failed"u8;
                return false;
            }

            errorMsg = default;
            return true;
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