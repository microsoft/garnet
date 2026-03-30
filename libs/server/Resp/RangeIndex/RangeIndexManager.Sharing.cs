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
        /// Acquire epoch protection (shared) for range index operations.
        /// Caller must pair with <see cref="ReleaseShared"/> in a finally block.
        /// </summary>
        internal void AcquireShared() => rangeIndexEpoch.Resume();

        /// <summary>
        /// Release epoch protection (shared) for range index operations.
        /// Must be called in a finally block after <see cref="AcquireShared"/>.
        /// </summary>
        internal void ReleaseShared() => rangeIndexEpoch.Suspend();

        /// <summary>
        /// Read the RangeIndex stub from the main store.
        /// Must be called under epoch protection (between <see cref="AcquireShared"/> and <see cref="ReleaseShared"/>).
        /// </summary>
        internal GarnetStatus ReadRangeIndex(
            StorageSession session,
            PinnedSpanByte key,
            ref StringInput input,
            scoped Span<byte> indexSpan)
        {
            Debug.Assert(indexSpan.Length >= IndexSizeBytes, "Insufficient space for index");

            var output = StringOutput.FromPinnedSpan(indexSpan);

            var readRes = session.Read_MainStore(key.ReadOnlySpan, ref input, ref output, ref session.stringBasicContext);

            if (readRes != GarnetStatus.OK)
                return readRes;

            // Validate that the output is a valid range index stub.
            // The Reader copies the value into output; check the actual written length
            // and ProcessInstanceId to reject non-range-index keys.
            var outputSpan = output.SpanByteAndMemory.IsSpanByte
                ? output.SpanByteAndMemory.SpanByte.ReadOnlySpan
                : output.SpanByteAndMemory.MemorySpan;

            if (outputSpan.Length < IndexSizeBytes)
                return GarnetStatus.NOTFOUND;

            return GarnetStatus.OK;
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