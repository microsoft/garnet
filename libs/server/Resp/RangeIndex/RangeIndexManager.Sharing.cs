// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Methods for safe shared access to RangeIndex operations.
    ///
    /// Epochs are acquired for data operations (RI.SET, RI.GET, RI.DEL field)
    /// to prevent concurrent deletion of the underlying BfTree.
    /// Epoch bump is used to safely perform index lifecycle operations (DEL key, eviction).
    /// </summary>
    public sealed partial class RangeIndexManager
    {
        /// <summary>
        /// RAII holder for a shared lock on a RangeIndex key.
        /// Disposing releases the shared lock.
        /// </summary>
        internal readonly struct LightEpochSuspender : IDisposable
        {
            private readonly LightEpoch rangeIndexEpoch;

            internal LightEpochSuspender(LightEpoch rangeIndexEpoch)
            {
                Debug.Assert(rangeIndexEpoch != null);
                this.rangeIndexEpoch = rangeIndexEpoch;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                rangeIndexEpoch.Suspend();
            }
        }

        /// <summary>
        /// Read the RangeIndex stub under epoch protection (shared lock).
        /// Used by RI.SET, RI.GET, RI.DEL (field-level operations).
        /// Returns disposable epoch holder; caller can safely operate on 
        /// the BfTree while the epoch is held.
        /// </summary>
        internal LightEpochSuspender ReadRangeIndex(
            StorageSession session,
            PinnedSpanByte key,
            ref StringInput input,
            scoped Span<byte> indexSpan,
            out GarnetStatus status)
        {
            rangeIndexEpoch.Resume();

            Debug.Assert(indexSpan.Length >= IndexSizeBytes, "Insufficient space for index");

            var output = StringOutput.FromPinnedSpan(indexSpan);
            GarnetStatus readRes;

            readRes = session.Read_MainStore(key.ReadOnlySpan, ref input, ref output, ref session.stringBasicContext);

            if (readRes != GarnetStatus.OK)
            {
                status = readRes;
                return new(rangeIndexEpoch);
            }

            // Validate that the output is a valid range index stub.
            // The Reader copies the value into output; check the actual written length
            // and ProcessInstanceId to reject non-range-index keys.
            var outputSpan = output.SpanByteAndMemory.IsSpanByte
                ? output.SpanByteAndMemory.SpanByte.ReadOnlySpan
                : output.SpanByteAndMemory.MemorySpan;

            if (outputSpan.Length < IndexSizeBytes)
            {
                status = GarnetStatus.NOTFOUND;
                return new(rangeIndexEpoch);
            }

            status = GarnetStatus.OK;
            return new(rangeIndexEpoch);
        }

        /// <summary>
        /// Safely delete range index, after all threads concurrently using it via
        /// ReadRangeIndex have completed their operations.
        /// </summary>
        internal void SafeDeleteRangeIndex(nint treeHandle)
        {
            rangeIndexEpoch.Resume();
            try
            {
                rangeIndexEpoch.BumpCurrentEpoch(() => UnregisterIndex(treeHandle));
            }
            finally
            {
                rangeIndexEpoch.Suspend();
            }
        }
    }
}