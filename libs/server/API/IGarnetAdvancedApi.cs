// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Advanced API calls, not to be used by normal clients
    /// </summary>
    /// <remarks>
    /// The <c>_WithPending</c> methods are the non-waiting counterparts of the per-command methods on
    /// <see cref="IGarnetReadApi"/>: they report that an operation went to the device instead of waiting on
    /// it, leaving the caller to decide how to wait. That is what lets a RESP command handler suspend its
    /// session rather than hold the network completion thread, so these are what a handler calls. The cost
    /// is that the caller owns the pending protocol -- it must drain, through a <c>CompletePendingAsync</c>
    /// or an equivalent, before it reads an output.
    /// </remarks>
    public interface IGarnetAdvancedApi
    {
        /// <summary>
        /// GET with support for pending multiple ongoing operations, scatter gather IO for outputs
        /// </summary>
        /// <remarks>
        /// The key is read in place, so the memory behind it must stay unchanged until every read this call
        /// leaves pending has completed. Use <see cref="GET_WithDetachedPending"/> when it may not.
        /// </remarks>
        GarnetStatus GET_WithPending(PinnedSpanByte key, ref StringInput input, ref StringOutput output, long ctx, out bool pending);

        /// <summary>
        /// GET whose pending read may complete after the caller returns and the memory behind
        /// <paramref name="key"/> has been reused, because the key is copied into the pending operation.
        /// </summary>
        /// <remarks>
        /// A read that completes in memory costs the same as <see cref="GET_WithPending"/>; only a read that goes
        /// to the device pays for the copy.
        /// </remarks>
        GarnetStatus GET_WithDetachedPending(PinnedSpanByte key, ref StringInput input, ref StringOutput output, long ctx, out bool pending);

        /// <summary>
        /// Complete pending read operations on main store
        /// </summary>
        /// <param name="outputArr"></param>
        /// <param name="wait"></param>
        bool GET_CompletePending((GarnetStatus Status, StringOutput Output)[] outputArr, bool wait = false);

        /// <summary>
        /// Complete pending read operations on main store
        /// </summary>
        /// <param name="completedOutputs"></param>
        /// <param name="wait"></param>
        /// <returns></returns>
        bool GET_CompletePending(out CompletedOutputIterator<StringInput, StringOutput, long> completedOutputs, bool wait = false);

        /// <summary>
        /// Completes pending read operations on the main store without blocking the calling thread, so a
        /// read that went to disk does not hold the thread that issued it.
        /// </summary>
        /// <returns>The completed outputs, which the caller must dispose.</returns>
        ValueTask<CompletedOutputIterator<StringInput, StringOutput, long>> GET_CompletePendingAsync();

        /// <summary>
        /// RMW operation on main store
        /// </summary>
        GarnetStatus RMW_MainStore(PinnedSpanByte key, ref StringInput input, ref StringOutput output);

        /// <summary>
        /// Read operation on main store
        /// </summary>
        GarnetStatus Read_MainStore(PinnedSpanByte key, ref StringInput input, ref StringOutput output);

        /// <summary>
        /// RMW operation on the main store that reports rather than waits when the record is not in memory.
        /// A pending update is applied when the record arrives, in the completion drained through
        /// <see cref="GET_CompletePendingAsync"/>, and must not be issued again.
        /// </summary>
        /// <param name="key">Key to update.</param>
        /// <param name="input">Input describing the update.</param>
        /// <param name="output">Output of the update, whose memory must outlive a pending update.</param>
        /// <returns>The store's status, which the caller maps to the outcome of its command.</returns>
        Status RMW_MainStore_WithPending(PinnedSpanByte key, ref StringInput input, ref StringOutput output);

        /// <summary>
        /// Read operation on the main store that reports rather than waits when the record is not in memory.
        /// </summary>
        /// <param name="key">Key to read.</param>
        /// <param name="input">Input describing what to read.</param>
        /// <param name="output">Output of the read, whose memory must outlive a pending read.</param>
        /// <returns>The store's status, which the caller maps to the outcome of its command.</returns>
        Status Read_MainStore_WithPending(PinnedSpanByte key, ref StringInput input, ref StringOutput output);

        /// <summary>
        /// RMW operation on object store
        /// </summary>
        GarnetStatus RMW_ObjectStore(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Read operation on object store
        /// </summary>
        GarnetStatus Read_ObjectStore(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// RMW operation on unified store
        /// </summary>
        GarnetStatus RMW_UnifiedStore(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output);

        /// <summary>
        /// Read operation on unified store
        /// </summary>
        GarnetStatus Read_UnifiedStore(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output);

        /// <summary>
        /// Read operation on the unified store that reports rather than waits when the record is not in
        /// memory, so the caller can decide how to wait for the device.
        /// </summary>
        /// <param name="key">Key to read.</param>
        /// <param name="input">Input describing what to read.</param>
        /// <param name="output">Output for a read served from memory.</param>
        /// <param name="pending">True if the read went to the device, leaving the status meaningless.</param>
        GarnetStatus Read_UnifiedStore_WithPending(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output, out bool pending);

        /// <summary>
        /// RMW operation on the unified store that reports rather than waits when the record is not in
        /// memory. The update has not been applied when this reports pending: it is applied when the record
        /// arrives, in the completion of the pending operation.
        /// </summary>
        /// <param name="key">Key to update.</param>
        /// <param name="input">Input describing the update.</param>
        /// <param name="output">Output for an update served from memory.</param>
        /// <param name="pending">True if the update went to the device, leaving the status meaningless.</param>
        GarnetStatus RMW_UnifiedStore_WithPending(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output, out bool pending);

        /// <summary>
        /// Completes pending unified-store operations on this session.
        /// </summary>
        /// <param name="completedOutputs">Outputs of the operations that completed.</param>
        /// <param name="wait">Whether to wait for every outstanding operation.</param>
        /// <returns>True if every outstanding operation completed.</returns>
        bool Unified_CompletePending(out CompletedOutputIterator<UnifiedInput, UnifiedOutput, long> completedOutputs, bool wait = false);

        /// <summary>
        /// Completes pending unified-store operations without blocking the calling thread, so an operation
        /// that went to disk does not hold the thread that issued it.
        /// </summary>
        /// <returns>The completed outputs, which the caller must dispose.</returns>
        ValueTask<CompletedOutputIterator<UnifiedInput, UnifiedOutput, long>> Unified_CompletePendingAsync();

        /// <summary>
        /// Reads the object store, reporting rather than waiting when the record is not in memory.
        /// </summary>
        /// <param name="key">Key to read.</param>
        /// <param name="input">Input describing the read.</param>
        /// <param name="output">Output for a read served from memory.</param>
        /// <param name="pending">True if the read went to the device, leaving the status meaningless.</param>
        GarnetStatus Read_ObjectStore_WithPending(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, out bool pending);

        /// <summary>
        /// Updates the object store, reporting rather than waiting when the record is not in memory. The update
        /// has not been applied when this reports pending: it is applied when the record arrives, in the
        /// completion of the pending operation.
        /// </summary>
        /// <param name="key">Key to update.</param>
        /// <param name="input">Input describing the update.</param>
        /// <param name="output">Output for an update served from memory.</param>
        /// <param name="pending">True if the update went to the device, leaving the status meaningless.</param>
        GarnetStatus RMW_ObjectStore_WithPending(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, out bool pending);

        /// <summary>
        /// Completes pending object-store operations on this session.
        /// </summary>
        /// <param name="completedOutputs">Outputs of the operations that completed.</param>
        /// <param name="wait">Whether to wait for every outstanding operation.</param>
        /// <returns>True if every outstanding operation completed.</returns>
        bool Object_CompletePending(out CompletedOutputIterator<ObjectInput, ObjectOutput, long> completedOutputs, bool wait = false);

        /// <summary>
        /// Completes pending object-store operations without blocking the calling thread, so an operation
        /// that went to disk does not hold the thread that issued it.
        /// </summary>
        /// <returns>The completed outputs, which the caller must dispose.</returns>
        ValueTask<CompletedOutputIterator<ObjectInput, ObjectOutput, long>> Object_CompletePendingAsync();

        /// <summary>
        /// Read batch of keys on main store.
        /// </summary>
        void ReadWithPrefetch<TBatch>(ref TBatch batch, long context = default)
            where TBatch : IReadArgBatch<FixedSpanByteKey, StringInput, StringOutput>, allows ref struct
            ;
    }
}