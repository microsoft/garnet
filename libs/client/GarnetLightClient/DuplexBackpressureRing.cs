// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Garnet.client
{
    /// <summary>
    /// Transmit one chunk of a request payload over the wire. <paramref name="context"/> is the ring-owned
    /// flush-completion token that MUST be handed back to
    /// <see cref="LightPayloadAsyncFlushResult{TRequest}.CompleteChunk"/> once the
    /// asynchronous send completes. This is the ring's only transport dependency; the chunking and the
    /// flush-completion accounting are owned by the ring.
    /// </summary>
    internal delegate void ProcessPayload(byte[] buffer, int offset, int length, object context);

    /// <summary>
    /// A duplex, back-pressured ring for out-of-line (chunked) request/response traffic over a single
    /// connection. It owns ONE combined address allocator (<see cref="PageOffset"/>) that advances two
    /// logically distinct lanes in a single atomic step:
    /// <list type="bullet">
    /// <item><description>
    /// The <b>request lane</b> — a circular page buffer of fixed-size, self-referential descriptors. Its
    /// bytes are flushed to the network and the descriptor slot is freed on flush (send), tracked by
    /// <c>FlushedUntilAddress</c>. Back-pressure here is local and autonomous (it only waits for the
    /// producer's own flush to catch up).
    /// </description></item>
    /// <item><description>
    /// The <b>completion lane</b> — a separate array of <typeparamref name="TCompletion"/> slots keyed by
    /// the completion ticket handed out alongside the request address. A completion must outlive its
    /// flush (it is needed until the reply arrives), so this lane is freed on reply, tracked by
    /// <c>repliedUntil</c>. Back-pressure here is remote (it waits for the peer to answer).
    /// </description></item>
    /// </list>
    /// Because a single <c>Interlocked.Add</c> advances the request address (page/offset) and the
    /// completion ticket (taskId) together, the pairing is structurally guaranteed: for any two claims
    /// A and B, <c>requestA &lt; requestB ⟺ completionA &lt; completionB</c>. No external coordination
    /// can break that ordering, which is what lets the reply reader match replies to completions by a
    /// plain monotonic counter.
    /// <para>
    /// This type is network-transport agnostic apart from an injected <see cref="ProcessPayload"/>
    /// (supplied at construction). The <see cref="LightNetworkWriter"/>
    /// is a thin shell that owns the socket/handler and buffer pool and delegates to this ring.
    /// </para>
    /// <para>
    /// The response (receiver) side is intentionally left minimal for now: the completion lane provides
    /// storage, publication and accounting hooks (<see cref="CompletionTail"/>,
    /// <see cref="TryReadCompletion"/>, <see cref="AdvanceReplied"/>), but the reply reader that drains it
    /// is implemented separately. Until a reader advances <c>repliedUntil</c>, response-expecting claims
    /// are bounded by the completion-lane capacity; fire-and-forget claims never touch the lane.
    /// </para>
    /// </summary>
    internal sealed class DuplexBackpressureRing<TRequest, TCompletion> : IDisposable, IFlushCompletionSink
        where TRequest : struct, IPayload
    {
        /// <summary>
        /// Size of the fixed descriptor written per out-of-line request.
        /// </summary>
        const int PayloadDescriptorSize = sizeof(long);
        const long PageWrapDistance = 1L << (PageOffset.kPageBits - 1);

        public readonly LightEpoch epoch;

        readonly LightPage[] values;
        readonly ILogger logger;
        readonly int BufferSize, LogPageSizeBits, PageSizeMask;
        internal readonly int PageSize;
        readonly long WrapDistance;
        readonly int sendBufferSize;

        PageOffset TailPageOffset;
        long FlushedUntilAddress, ReadOnlyAddress;

        int _ongoingAggressiveShiftReadOnly;

        // Injected transport primitive and the failure callback, supplied at construction.
        readonly ProcessPayload processPayload;
        readonly Action<Exception> onFlushError;

        bool disposed;

        // Request lane freed on flush (send); producers block on this when the ring's own send is behind.
        CompletionEvent requestFreed;

        // Completion lane freed on reply (receiver); producers block on this when too many replies are outstanding.
        CompletionEvent completionFreed;

        // Request side-table: each physical descriptor maps to one request payload. Publication happens-before
        // is established by the self-key written into page memory under the epoch barrier; ownership transfer at
        // flush/teardown is arbitrated by atomically zeroing that key.
        readonly TRequest[] requestSlots;

        // Completion lane. Slots are indexed by (ticket & completionMask); a companion marker array carries the
        // release fence and lets a barrier-less reader detect a fully-published completion.
        readonly TCompletion[] completions;
        readonly long[] completionPublished;
        readonly int completionCapacity, completionMask;
        long repliedUntil;

        /// <summary>
        /// Create a duplex back-pressured ring over a single connection.
        /// <para>
        /// The ring owns one combined address allocator whose atomic advance covers both the request lane
        /// (a circular buffer of fixed-size descriptors, freed on flush) and the completion lane (reply
        /// tickets, freed on reply), so a producer's (address, ticket) pair is always mutually ordered. The
        /// caller injects the transport: <paramref name="processPayload"/> transmits one chunk, and the
        /// static network flush-completion callback (<see cref="LightPayloadAsyncFlushResult{TRequest}.CompleteChunk"/>)
        /// hands the flush token back to the ring via its <see cref="IFlushCompletionSink"/>.
        /// </para>
        /// </summary>
        /// <param name="sendPageSize">Size in bytes of each descriptor page; rounds down to a power of two and
        /// bounds how many requests can be queued before page reuse waits for an earlier flush.</param>
        /// <param name="bufferSize">Number of circular pages backing the request lane. Must not exceed
        /// <see cref="PageOffset.kPageMask"/>.</param>
        /// <param name="completionCapacity">Maximum number of outstanding response-expecting requests; rounds
        /// up to a power of two and bounds the completion lane before producers back-pressure on replies.</param>
        /// <param name="sendBufferSize">Size of a single network send buffer; caps the per-send chunk length.</param>
        /// <param name="processPayload">Transport callback used to flush one request chunk to the network.</param>
        /// <param name="onFlushError">Invoked when a flush send fails, or with a payload-validation failure
        /// whose reason has already been logged (exception may be null in that case).</param>
        /// <param name="epoch">Shared epoch protecting the page allocator and flush machinery.</param>
        /// <param name="logger">Logger instance.</param>
        public DuplexBackpressureRing(
            int sendPageSize,
            int bufferSize,
            int completionCapacity,
            int sendBufferSize,
            ProcessPayload processPayload,
            Action<Exception> onFlushError,
            LightEpoch epoch,
            ILogger logger = null)
        {
            this.BufferSize = bufferSize;
            if (BufferSize > PageOffset.kPageMask) throw new ArgumentOutOfRangeException(nameof(bufferSize));

            ArgumentNullException.ThrowIfNull(processPayload);
            ArgumentNullException.ThrowIfNull(onFlushError);

            requestFreed.Initialize();
            completionFreed.Initialize();

            this.epoch = epoch;
            this.PageSize = sendPageSize;
            this.sendBufferSize = sendBufferSize;
            this.processPayload = processPayload;
            this.onFlushError = onFlushError;
            this.logger = logger;

            var payloadSlotCount = BufferSize * sendPageSize / PayloadDescriptorSize;
            this.requestSlots = new TRequest[payloadSlotCount];

            this.LogPageSizeBits = Utility.NumBitsPreviousPowerOf2(sendPageSize);
            this.WrapDistance = PageWrapDistance << LogPageSizeBits;
            PageSizeMask = sendPageSize - 1;

            values = new LightPage[BufferSize];
            for (var i = 0; i < BufferSize; i++)
                values[i] = new LightPage(this.PageSize);

            this.completionCapacity = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(1, completionCapacity));
            this.completionMask = this.completionCapacity - 1;
            this.completions = new TCompletion[this.completionCapacity];
            this.completionPublished = new long[this.completionCapacity];
        }

        /// <inheritdoc />
        public unsafe void Dispose()
        {
            Volatile.Write(ref disposed, true);

            // Reclaim any request buffers still outstanding, arbitrating with an in-flight flush by
            // atomically zeroing each descriptor key before taking the slot.
            for (var page = 0; page < BufferSize; page++)
            {
                var basePtr = values[page].pointer;
                for (var offset = 0; offset < PageSize; offset += PayloadDescriptorSize)
                {
                    var address = ((long)page << LogPageSizeBits) | (uint)offset;
                    if (TryClaimDescriptor(address, (long*)(basePtr + offset), out _, out var payload))
                        payload.Dispose();
                }
            }

            requestFreed.Dispose();
            completionFreed.Dispose();
        }

        /// <summary>
        /// Register (store and publish) a request payload at the descriptor address, making it eligible for
        /// flushing. Publication writes the descriptor's self-key into page memory; the producing thread must
        /// hold the epoch, so the flusher (which validates key == address only after the epoch barrier) never
        /// observes a partially-written slot.
        /// </summary>
        /// <param name="address"></param>
        /// <param name="payload"></param>
        internal unsafe void RegisterRequest(long address, TRequest payload)
        {
            Debug.Assert(epoch.ThisInstanceProtected());
            ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed), this);
            var slot = GetPayloadSlot(address);
            requestSlots[slot] = payload;
            // Publish request to the consumer
            var ptr = (long*)GetPhysicalAddress(address);
            *ptr = address;

            // If Dispose swept this slot before publication, its reclaim pass has already run and will not
            // revisit the slot. Re-check and reclaim here via the same atomic claim so the just-published
            // payload cannot leak.
            if (Volatile.Read(ref disposed) && TryClaimDescriptor(address, ptr, out _, out var reclaimed))
                reclaimed.Dispose();
        }

        /// <summary>
        /// Register (store and publish) a completion for the given ticket. The release-fenced marker write
        /// happens after the completion store, so the barrier-less reply reader that observes the marker also
        /// observes a fully-written completion.
        /// </summary>
        /// <param name="ticket"></param>
        /// <param name="completion"></param>
        internal void RegisterCompletion(int ticket, TCompletion completion)
        {
            completions[ticket & completionMask] = completion;
            Volatile.Write(ref completionPublished[ticket & completionMask], (long)ticket + 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int GetPayloadSlot(long address)
        {
            var pageIndex = (int)((address >> LogPageSizeBits) & (BufferSize - 1));
            var offset = (int)(address & PageSizeMask);
            return ((pageIndex * PageSize) + offset) / PayloadDescriptorSize;
        }

        /// <summary>
        /// Get tail address
        /// </summary>
        public long GetTailAddress()
        {
            var local = TailPageOffset;
            if (local.Offset >= PageSize)
            {
                local.Page = (local.Page + 1) & (int)PageOffset.kPageMask;
                local.Offset = 0;
            }
            return (((long)local.Page) << LogPageSizeBits) | (uint)local.Offset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long TryAllocateInternal(int size, out int taskId, bool expectsResponse)
        {
            PageOffset localTailPageOffset = default;
            localTailPageOffset.PageAndOffset = TailPageOffset.PageAndOffset;

            // Necessary to check because threads keep retrying and we do not
            // want to overflow offset more than once per thread
            if (localTailPageOffset.Offset > PageSize)
            {
                taskId = 0;
                if (NeedToWait(localTailPageOffset.Page + 1))
                    return -1; // RETRY_LATER
                return -2; // RETRY_NOW
            }

            // A single atomic advances the request address (page/offset) and, for response-expecting
            // claims, the completion ticket (taskId) — this is what guarantees the send/completion pairing.
            localTailPageOffset.PageAndOffset = expectsResponse
                ? Interlocked.Add(ref TailPageOffset.PageAndOffset, size + (1L << PageOffset.kTaskOffset))
                : Interlocked.Add(ref TailPageOffset.PageAndOffset, size);

            taskId = localTailPageOffset.PrevTaskId;
            var page = localTailPageOffset.Page;
            var offset = localTailPageOffset.Offset - size;

            #region HANDLE PAGE OVERFLOW
            if (localTailPageOffset.Offset > PageSize)
            {
                var pageIndex = (localTailPageOffset.Page + 1) & (int)PageOffset.kPageMask;

                // Non-responsible overflow threads back off
                if (offset > PageSize)
                {
                    if (NeedToWait(pageIndex))
                        return -1; // RETRY_LATER
                    return -2; // RETRY_NOW
                }

                if (offset < PageSize)
                {
                    Debug.Assert(values[page % BufferSize].lastOffset == 0);
                    values[page % BufferSize].lastOffset = offset;
                }

                // Responsible overflow thread tries to shift address
                DoAggressiveShiftReadOnly();

                if (NeedToWait(pageIndex))
                {
                    // Reset to end of page so that next attempt can retry, restoring the taskId so the
                    // completion ticket is not consumed twice across the retry.
                    localTailPageOffset.TaskId = taskId;
                    localTailPageOffset.Offset = PageSize;
                    Interlocked.Exchange(ref TailPageOffset.PageAndOffset, localTailPageOffset.PageAndOffset);
                    return -1; // RETRY_LATER
                }

                localTailPageOffset.Page = pageIndex;
                localTailPageOffset.Offset = size;
                TailPageOffset = localTailPageOffset;
                page++;
                offset = 0;
            }
            #endregion

            return (((long)page) << LogPageSizeBits) | ((long)offset);

            // Request-lane back-pressure: page reuse waits for the ring's own flush to catch up.
            bool NeedToWait(int page)
            {
                var limit = (BufferSize + (int)(FlushedUntilAddress >> LogPageSizeBits)) & (int)PageOffset.kPageMask;
                return page >= limit && (page - limit < PageWrapDistance);
            }
        }

        /// <summary>
        /// Claim a request address and (for response-expecting claims) a completion ticket. On success
        /// returns a non-negative address; on back-pressure returns -1 and sets <paramref name="waitEvent"/>
        /// to the lane the caller should await (request lane freed by flush, completion lane freed by reply).
        /// The RETRY_NOW spin (epoch drain) is handled internally.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (int taskId, long address) TryAllocate(int size, bool expectsResponse, out CompletionEvent waitEvent)
        {
            const int kFlushSpinCount = 10;
            var spins = 0;
            while (true)
            {
                Debug.Assert(epoch.ThisInstanceProtected());

                // Completion-lane back-pressure is checked first so a full completion lane never forces us
                // to consume (and then unwind) a completion ticket.
                if (expectsResponse && !CompletionHasRoom())
                {
                    waitEvent = completionFreed;
                    return (0, -1);
                }

                waitEvent = requestFreed;
                var logicalAddress = TryAllocateInternal(size, out var taskId, expectsResponse);

                if (logicalAddress >= 0)
                    return (taskId, logicalAddress);
                if (logicalAddress == -1)
                {
                    if (spins++ < kFlushSpinCount)
                    {
                        Thread.Yield();
                        continue;
                    }
                    return (taskId, logicalAddress);
                }
                epoch.ProtectAndDrain();
                Thread.Yield();
            }
        }

        /// <summary>
        /// Physical address of a descriptor slot in page memory.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long GetPhysicalAddress(long logicalAddress)
        {
            var offset = (int)(logicalAddress & ((1L << LogPageSizeBits) - 1));
            var pageIndex = (int)((logicalAddress >> LogPageSizeBits) & (BufferSize - 1));
            return values[pageIndex].pointer + offset;
        }

        /// <summary>
        /// Atomically claim the descriptor published at <paramref name="address"/> by zeroing its self-key,
        /// arbitrating single ownership across the three teardown/flush paths that race for it: the flusher
        /// (<see cref="AsyncFlushPayloads"/>), <see cref="Dispose"/>, and the <see cref="RegisterRequest"/>
        /// post-publish recheck. Exactly one caller observes a non-zero key and takes the payload; the losers
        /// see key == 0 and skip. On success, clears the slot, hands back its payload, and outputs the claimed
        /// self-key so callers can validate key == address. Returns false when the slot was empty or already
        /// claimed by another path.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        unsafe bool TryClaimDescriptor(long address, long* keyPtr, out long key, out TRequest payload)
        {
            key = Interlocked.Exchange(ref *keyPtr, 0L);
            if (key == 0)
            {
                payload = default;
                return false;
            }

            var slot = GetPayloadSlot(address);
            payload = requestSlots[slot];
            requestSlots[slot] = default;
            return true;
        }

        #region Completion lane

        /// <summary>Number of completion tickets issued so far (task-space, wraps at 2^kTaskBits).</summary>
        internal int CompletionTail => TailPageOffset.TaskId;

        bool CompletionHasRoom()
        {
            var outstanding = (TailPageOffset.TaskId - (int)(Volatile.Read(ref repliedUntil) & PageOffset.kTaskMask)) & (int)PageOffset.kTaskMask;
            return outstanding < completionCapacity;
        }

        /// <summary>
        /// Reader-side: try to read a published completion for the given ticket. Returns false if the
        /// producer has not finished publishing it yet.
        /// </summary>
        internal bool TryReadCompletion(int ticket, out TCompletion completion)
        {
            if (Volatile.Read(ref completionPublished[ticket & completionMask]) == (long)ticket + 1)
            {
                completion = completions[ticket & completionMask];
                return true;
            }
            completion = default;
            return false;
        }

        /// <summary>
        /// Reader-side: advance the reply watermark, freeing completion slots for reuse and waking any
        /// producer blocked on completion-lane back-pressure.
        /// </summary>
        internal void AdvanceReplied(int consumedCount)
        {
            if (consumedCount <= 0) return;
            Volatile.Write(ref repliedUntil, Volatile.Read(ref repliedUntil) + consumedCount);
            completionFreed.Set();
        }

        #endregion

        void OnPagesMarkedReadOnly(long oldReadOnlyAddress, long newReadOnlyAddress)
            => AsyncFlushPayloads(oldReadOnlyAddress, newReadOnlyAddress);

        /// <summary>
        /// Flush an address range of request descriptors to the network. Only the request buffer is sent;
        /// the completion (if any) lives in the completion lane and is untouched here.
        /// </summary>
        /// <param name="fromAddress"></param>
        /// <param name="untilAddress"></param>
        unsafe void AsyncFlushPayloads(long fromAddress, long untilAddress)
        {
            var startPage = fromAddress >> LogPageSizeBits;
            var endPage = untilAddress >> LogPageSizeBits;
            var count = new CountWrapper
            {
                count = 1,
                untilAddress = untilAddress
            };
            var flushFailed = false;

            var flushPage = startPage;
            while (true)
            {
                long startOffset = 0, endOffset = 1L << LogPageSizeBits;
                if (flushPage == startPage) startOffset = GetOffsetInPage(fromAddress);
                if (flushPage == endPage) endOffset = GetOffsetInPage(untilAddress);

                var realEndOffset = endOffset;
                ref var page = ref values[flushPage % BufferSize];
                if (page.lastOffset > 0 && endOffset > page.lastOffset)
                {
                    realEndOffset = page.lastOffset;
                    page.lastOffset = 0;
                }

                if ((startOffset & (PayloadDescriptorSize - 1)) != 0 || (realEndOffset & (PayloadDescriptorSize - 1)) != 0)
                {
                    FailOnPayloadFlush(ref flushFailed, $"Out-of-line flush range {flushPage}:{startOffset}-{realEndOffset} is not aligned to {PayloadDescriptorSize}-byte records.");
                    realEndOffset -= (realEndOffset - startOffset) & (PayloadDescriptorSize - 1);
                }

                for (var offset = startOffset; offset < realEndOffset; offset += PayloadDescriptorSize)
                {
                    var address = (flushPage << LogPageSizeBits) | (uint)offset;
                    var ptr = page.pointer + offset;

                    // Claim the descriptor by atomically zeroing its self-key, arbitrating with teardown.
                    if (!TryClaimDescriptor(address, (long*)ptr, out var key, out var payload))
                        continue; // Taken by Dispose.

                    if (key != address)
                    {
                        FailOnPayloadFlush(ref flushFailed, $"Out-of-line payload key {key} does not match its log address {address}.");
                        payload.Dispose();
                        continue;
                    }

                    if (flushFailed)
                    {
                        payload.Dispose();
                        continue;
                    }

                    ProcessPayloadChunks(ref payload, count, ref flushFailed);
                }

                if (flushPage == endPage) break;
                flushPage = (flushPage + 1) & PageOffset.kPageMask;
            }

            CompleteFlush(count);

            void ProcessPayloadChunks(ref TRequest payload, CountWrapper count, ref bool flushFailed)
            {
                var chunkSize = Math.Max(1, sendBufferSize);
                var chunkCount = ((payload.Length - 1) / chunkSize) + 1;
                var result = new LightPayloadAsyncFlushResult<TRequest>
                {
                    count = count,
                    payload = payload,
                    remainingChunks = chunkCount,
                    sink = this
                };
                _ = Interlocked.Increment(ref count.count);

                var dispatchedChunks = 0;
                try
                {
                    for (var offset = 0; offset < payload.Length; offset += chunkSize)
                    {
                        var length = Math.Min(chunkSize, payload.Length - offset);
                        processPayload(payload.Buffer, offset, length, result);
                        dispatchedChunks++;
                    }
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "Exception sending an out-of-line payload");
                    flushFailed = true;
                    onFlushError(ex);

                    // Undispatched chunks will never get a network completion, so account for them here via
                    // the same completion routine the network callback uses.
                    for (var i = dispatchedChunks; i < chunkCount; i++)
                        LightPayloadAsyncFlushResult<TRequest>.CompleteChunk(result);
                }
            }

            void FailOnPayloadFlush(ref bool flushFailed, string message)
            {
                if (flushFailed)
                    return;

                flushFailed = true;
                logger?.LogError("{Message}", message);
                onFlushError(new InvalidOperationException(message));
            }
        }

        /// <inheritdoc />
        public void CompleteFlush(CountWrapper count)
        {
            try
            {
                if (Interlocked.Decrement(ref count.count) == 0)
                {
                    long endAddress = count.untilAddress;
                    Utility.MonotonicUpdate(ref FlushedUntilAddress, endAddress, WrapDistance, out _);
                    // The request lane is now free up to endAddress; wake producers waiting on request back-pressure.
                    requestFreed.Set();
                    AggressiveShiftReadOnlyRunner(true);
                }
            }
            catch when (disposed) { }
        }

        long GetOffsetInPage(long address) => address & PageSizeMask;

        public void DoAggressiveShiftReadOnly()
        {
            if (_ongoingAggressiveShiftReadOnly == 0 && Interlocked.CompareExchange(ref _ongoingAggressiveShiftReadOnly, 1, 0) == 0)
                AggressiveShiftReadOnlyRunner(false);
        }

        void EpochProtectAggressiveShiftReadOnlyRunner()
        {
            try
            {
                epoch.Resume();
                AggressiveShiftReadOnlyRunner(false);
            }
            finally
            {
                epoch.Suspend();
            }
        }

        bool ToShift()
        {
            var tailAddress = GetTailAddress();
            return tailAddress > ReadOnlyAddress || (ReadOnlyAddress - tailAddress > WrapDistance);
        }

        void AggressiveShiftReadOnlyRunner(bool recurse)
        {
            do
            {
                if (ToShift())
                {
                    if (recurse)
                    {
                        Task.Run(EpochProtectAggressiveShiftReadOnlyRunner);
                        return;
                    }
                    else
                    {
                        if (AggressiveFlushShiftReadOnlyBump()) return;
                    }
                }
                _ongoingAggressiveShiftReadOnly = 0;
            } while (ToShift() && _ongoingAggressiveShiftReadOnly == 0 && Interlocked.CompareExchange(ref _ongoingAggressiveShiftReadOnly, 1, 0) == 0);
        }

        bool AggressiveFlushShiftReadOnlyBump()
        {
            var newReadOnlyAddress = GetTailAddress();
            if (Utility.MonotonicUpdate(ref ReadOnlyAddress, newReadOnlyAddress, WrapDistance, out long oldReadOnlyAddress))
            {
                epoch.BumpCurrentEpoch(() => OnPagesMarkedReadOnly(oldReadOnlyAddress, newReadOnlyAddress));
                return true;
            }
            return false;
        }
    }
}
