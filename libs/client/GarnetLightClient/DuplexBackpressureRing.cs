// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Garnet.client
{
    /// <summary>
    /// A request-lane payload stored in a <see cref="DuplexBackpressureRing{TRequest, TCompletion}"/>.
    /// The ring transfers ownership of the underlying buffer to the sender (flusher), which disposes
    /// it once the bytes have been handed to the network.
    /// </summary>
    internal interface IRequest : IDisposable
    {
        /// <summary>
        /// Backing buffer holding the serialized request bytes.
        /// </summary>
        byte[] Buffer { get; }

        /// <summary>
        /// Number of valid bytes in <see cref="Buffer"/>.
        /// </summary>
        int Length { get; }
    }

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
    /// <see cref="TryReadCompletion"/>, <see cref="AdvanceCompletion"/>), but the reply reader that drains it
    /// is implemented separately. Until a reader advances <c>repliedUntil</c>, response-expecting claims
    /// are bounded by the completion-lane capacity; fire-and-forget claims never touch the lane.
    /// </para>
    /// </summary>
    internal sealed class DuplexBackpressureRing<TRequest, TCompletion> : IDisposable, IFlushCompletionSink
        where TRequest : struct, IRequest
    {
        /// <summary>
        /// A single fixed-size page in the <see cref="LightNetworkWriter"/> circular buffer.
        /// The page stores fixed-size out-of-line payload descriptors (self-referential log
        /// addresses), not payload bytes; the payload bytes live in separately rented buffers.
        /// </summary>
        unsafe struct RingPage
        {
            public readonly byte[] value;
            public readonly long pointer;
            public FullPageStatus PageStatusIndicator;
            public long lastOffset;

            public RingPage(int pageSize)
            {
                value = GC.AllocateArray<byte>(pageSize, true);
                pointer = (long)Unsafe.AsPointer(ref value[0]);
                PageStatusIndicator = default;
                lastOffset = 0;
            }
        }

        /// <summary>
        /// Size of the fixed descriptor written per out-of-line request.
        /// </summary>
        const int RingDescriptorSize = sizeof(long);
        const long PageWrapDistance = 1L << (PageOffset.kPageBits - 1);

        public readonly LightEpoch epoch;

        readonly RingPage[] bufferPages;
        readonly ILogger logger;
        readonly int ringPageCount, pageSizeBits, pageSizeMask;
        internal readonly int ringPageSizeBytes;
        readonly long wrapDistance;
        readonly int maxChunkSizeBytes;

        PageOffset TailPageOffset;
        long flushedUntilAddress, readOnlyAddress;

        int ongoingAggressiveShiftReadOnly;

        // Injected transport primitive and the failure callback, supplied at construction.
        readonly ProcessPayload operateOnPayload;
        readonly Action<Exception> onFlushError;

        bool disposed;

        // Request lane freed on flush (send); producers block on this when the ring's own send is behind.
        CompletionEvent requestFreed;

        // Completion lane freed on reply (receiver); producers block on this when too many replies are outstanding.
        CompletionEvent completionFreed;

        // Request side-table: each physical descriptor maps to one request payload. Publication happens-before
        // is established by the self-key written into page memory under the epoch barrier; ownership transfer at
        // flush/teardown is arbitrated by atomically zeroing that key.
        readonly TRequest[] requests;

        // Completion lane. Each slot co-locates the release-fenced guard word with its completion so the reply
        // reader's matched read (guard, then completion) hits one cache line instead of two. Indexed by
        // (ticket & completionMask).
        readonly CompletionSlot[] completionLane;
        readonly int completionCapacity, completionMask;
        long completionUntil;

        // Cache-line-isolated completion slot. Padded to 64 bytes so two producers publishing adjacent head
        // tickets do not share a line. NOTE: the completion lane is single-consumer FIFO, so the reader trails
        // the producers by the pipeline depth and never shares a line with them; the padding therefore only
        // guards concurrent producer-vs-producer writes at the head. Drop the Size to pack the lane if that
        // contention is negligible — the single-line delivery read is preserved either way.
        [StructLayout(LayoutKind.Sequential, Size = 64)]
        struct CompletionSlot
        {
            // 0 = empty, ticket+1 = published, -(ticket+1) = claimed. The only field mutated with Volatile/Interlocked.
            public long published;
            public TCompletion completion;
        }

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
        /// <param name="ringPageSizeBytes">Size in bytes of each descriptor page; rounds down to a power of two and
        /// bounds how many requests can be queued before page reuse waits for an earlier flush.</param>
        /// <param name="ringPageCount">Number of circular pages backing the request lane. Must not exceed
        /// <see cref="PageOffset.kPageMask"/>.</param>
        /// <param name="completionCapacity">Maximum number of outstanding response-expecting requests; rounds
        /// up to a power of two and bounds the completion lane before producers back-pressure on replies.</param>
        /// <param name="maxChunkSizeBytes">Size of a single network send buffer; caps the per-send chunk length.</param>
        /// <param name="processPayload">Transport callback used to flush one request chunk to the network.</param>
        /// <param name="onFlushError">Invoked when a flush send fails, or with a payload-validation failure
        /// whose reason has already been logged (exception may be null in that case).</param>
        /// <param name="epoch">Shared epoch protecting the page allocator and flush machinery.</param>
        /// <param name="logger">Logger instance.</param>
        public DuplexBackpressureRing(
            int ringPageSizeBytes,
            int ringPageCount,
            int completionCapacity,
            int maxChunkSizeBytes,
            ProcessPayload processPayload,
            Action<Exception> onFlushError,
            LightEpoch epoch,
            ILogger logger = null)
        {
            this.ringPageCount = ringPageCount;
            if (this.ringPageCount > PageOffset.kPageMask) throw new ArgumentOutOfRangeException(nameof(ringPageCount));

            ArgumentNullException.ThrowIfNull(processPayload);
            ArgumentNullException.ThrowIfNull(onFlushError);

            requestFreed.Initialize();
            completionFreed.Initialize();

            this.epoch = epoch;
            this.ringPageSizeBytes = ringPageSizeBytes;
            this.maxChunkSizeBytes = maxChunkSizeBytes;
            this.operateOnPayload = processPayload;
            this.onFlushError = onFlushError;
            this.logger = logger;

            var ringSlotCount = this.ringPageCount * ringPageSizeBytes / RingDescriptorSize;
            this.requests = new TRequest[ringSlotCount];

            this.pageSizeBits = Utility.NumBitsPreviousPowerOf2(ringPageSizeBytes);
            this.wrapDistance = PageWrapDistance << pageSizeBits;
            pageSizeMask = ringPageSizeBytes - 1;

            bufferPages = new RingPage[this.ringPageCount];
            for (var i = 0; i < this.ringPageCount; i++)
                bufferPages[i] = new RingPage(this.ringPageSizeBytes);

            this.completionCapacity = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(1, completionCapacity));
            this.completionMask = this.completionCapacity - 1;
            this.completionLane = new CompletionSlot[this.completionCapacity];
        }

        /// <inheritdoc />
        public unsafe void Dispose()
        {
            Volatile.Write(ref disposed, true);

            // Reclaim any request buffers still outstanding, arbitrating with an in-flight flush by
            // atomically zeroing each descriptor key before taking the slot.
            for (var page = 0; page < ringPageCount; page++)
            {
                var basePtr = bufferPages[page].pointer;
                for (var offset = 0; offset < ringPageSizeBytes; offset += RingDescriptorSize)
                {
                    var address = ((long)page << pageSizeBits) | (uint)offset;
                    if (TryClaimRequestDescriptor(address, (long*)(basePtr + offset), out _, out var payload))
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
            var slot = ComputeSlot(address);
            requests[slot] = payload;
            // Publish request to the consumer
            var ptr = (long*)GetPhysicalAddress(address);
            *ptr = address;

            // If Dispose swept this slot before publication, its reclaim pass has already run and will not
            // revisit the slot. Re-check and reclaim here via the same atomic claim so the just-published
            // payload cannot leak.
            if (Volatile.Read(ref disposed) && TryClaimRequestDescriptor(address, ptr, out _, out var reclaimed))
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
            var slot = ticket & completionMask;
            completionLane[slot].completion = completion;
            Volatile.Write(ref completionLane[slot].published, (long)ticket + 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int ComputeSlot(long address)
        {
            var pageIndex = (int)((address >> pageSizeBits) & (ringPageCount - 1));
            var offset = (int)(address & pageSizeMask);
            return ((pageIndex * ringPageSizeBytes) + offset) / RingDescriptorSize;
        }

        /// <summary>
        /// Get tail address
        /// </summary>
        public long GetTailAddress()
        {
            var local = TailPageOffset;
            if (local.Offset >= ringPageSizeBytes)
            {
                local.Page = (local.Page + 1) & (int)PageOffset.kPageMask;
                local.Offset = 0;
            }
            return (((long)local.Page) << pageSizeBits) | (uint)local.Offset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long TryAllocateInternal(int size, out int taskId, bool expectsResponse)
        {
            PageOffset localTailPageOffset = default;
            localTailPageOffset.PageAndOffset = TailPageOffset.PageAndOffset;

            // Necessary to check because threads keep retrying and we do not
            // want to overflow offset more than once per thread
            if (localTailPageOffset.Offset > ringPageSizeBytes)
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
            if (localTailPageOffset.Offset > ringPageSizeBytes)
            {
                var pageIndex = (localTailPageOffset.Page + 1) & (int)PageOffset.kPageMask;

                // Non-responsible overflow threads back off
                if (offset > ringPageSizeBytes)
                {
                    if (NeedToWait(pageIndex))
                        return -1; // RETRY_LATER
                    return -2; // RETRY_NOW
                }

                if (offset < ringPageSizeBytes)
                {
                    Debug.Assert(bufferPages[page % ringPageCount].lastOffset == 0);
                    bufferPages[page % ringPageCount].lastOffset = offset;
                }

                // Responsible overflow thread tries to shift address
                DoAggressiveShiftReadOnly();

                if (NeedToWait(pageIndex))
                {
                    // Reset to end of page so that next attempt can retry, restoring the taskId so the
                    // completion ticket is not consumed twice across the retry.
                    localTailPageOffset.TaskId = taskId;
                    localTailPageOffset.Offset = ringPageSizeBytes;
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

            return (((long)page) << pageSizeBits) | ((long)offset);

            // Request-lane back-pressure: page reuse waits for the ring's own flush to catch up.
            bool NeedToWait(int page)
            {
                var limit = (ringPageCount + (int)(flushedUntilAddress >> pageSizeBits)) & (int)PageOffset.kPageMask;
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
            var offset = (int)(logicalAddress & ((1L << pageSizeBits) - 1));
            var pageIndex = (int)((logicalAddress >> pageSizeBits) & (ringPageCount - 1));
            return bufferPages[pageIndex].pointer + offset;
        }

        /// <summary>
        /// Atomically claim the descriptor published at <paramref name="address"/> by zeroing its self-key,
        /// arbitrating single ownership across the three teardown/flush paths that race for it: the flusher
        /// (<see cref="AsyncFlushRequests"/>), <see cref="Dispose"/>, and the <see cref="RegisterRequest"/>
        /// post-publish recheck. Exactly one caller observes a non-zero key and takes the payload; the losers
        /// see key == 0 and skip. On success, clears the slot, hands back its payload, and outputs the claimed
        /// self-key so callers can validate key == address. Returns false when the slot was empty or already
        /// claimed by another path.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        unsafe bool TryClaimRequestDescriptor(long address, long* keyPtr, out long key, out TRequest payload)
        {
            key = Interlocked.Exchange(ref *keyPtr, 0L);
            if (key == 0)
            {
                payload = default;
                return false;
            }

            var slot = ComputeSlot(address);
            payload = requests[slot];
            requests[slot] = default;
            return true;
        }

        /// <summary>
        /// Atomically claim a published completion for single delivery. Arbitrates between the receive-side
        /// teardown drain and a producer whose request failed to publish after its completion was registered:
        /// both may target the same ticket concurrently. CAS-es the publication marker from its live value to a
        /// negative sentinel, so exactly one caller observes the live marker and takes the completion; any later
        /// <see cref="TryReadCompletion"/> then reports not-published. This runs only on the disposal slow path,
        /// so the extra interlocked op is off the request/reply hot path. Returns false if the completion was
        /// not published or was already claimed.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryClaimCompletionTicket(int ticket, out TCompletion completion)
        {
            var slot = ticket & completionMask;
            var expected = (long)ticket + 1;
            if (Volatile.Read(ref completionLane[slot].published) == expected &&
                Interlocked.CompareExchange(ref completionLane[slot].published, -expected, expected) == expected)
            {
                completion = completionLane[slot].completion;
                return true;
            }
            completion = default;
            return false;
        }

        #region Completion lane

        /// <summary>Number of completion tickets issued so far (task-space, wraps at 2^kTaskBits).</summary>
        internal int CompletionTail => TailPageOffset.TaskId;

        bool CompletionHasRoom()
        {
            var outstanding = (TailPageOffset.TaskId - (int)(Volatile.Read(ref completionUntil) & PageOffset.kTaskMask)) & (int)PageOffset.kTaskMask;
            return outstanding < completionCapacity;
        }

        /// <summary>
        /// Reader-side: try to read a published completion for the given ticket. Returns false if the
        /// producer has not finished publishing it yet.
        /// </summary>
        internal bool TryReadCompletion(int ticket, out TCompletion completion)
        {
            var slot = ticket & completionMask;
            if (Volatile.Read(ref completionLane[slot].published) == (long)ticket + 1)
            {
                completion = completionLane[slot].completion;
                return true;
            }
            completion = default;
            return false;
        }

        /// <summary>
        /// Reader-side: advance the reply watermark, freeing completion slots for reuse and waking any
        /// producer blocked on completion-lane back-pressure.
        /// </summary>
        internal void AdvanceCompletion(int consumedCount)
        {
            if (consumedCount <= 0) return;
            Volatile.Write(ref completionUntil, Volatile.Read(ref completionUntil) + consumedCount);
            completionFreed.Set();
        }

        #endregion

        void OnPagesMarkedReadOnly(long oldReadOnlyAddress, long newReadOnlyAddress)
            => AsyncFlushRequests(oldReadOnlyAddress, newReadOnlyAddress);

        /// <summary>
        /// Flush an address range of request descriptors to the network. Only the request buffer is sent;
        /// the completion (if any) lives in the completion lane and is untouched here.
        /// </summary>
        /// <param name="fromAddress"></param>
        /// <param name="untilAddress"></param>
        unsafe void AsyncFlushRequests(long fromAddress, long untilAddress)
        {
            var startPage = fromAddress >> pageSizeBits;
            var endPage = untilAddress >> pageSizeBits;
            var count = new CountWrapper
            {
                count = 1,
                untilAddress = untilAddress
            };
            var flushFailed = false;

            var flushPage = startPage;
            while (true)
            {
                long startOffset = 0, endOffset = 1L << pageSizeBits;
                if (flushPage == startPage) startOffset = GetOffsetInPage(fromAddress);
                if (flushPage == endPage) endOffset = GetOffsetInPage(untilAddress);

                var realEndOffset = endOffset;
                ref var page = ref bufferPages[flushPage % ringPageCount];
                if (page.lastOffset > 0 && endOffset > page.lastOffset)
                {
                    realEndOffset = page.lastOffset;
                    page.lastOffset = 0;
                }

                if ((startOffset & (RingDescriptorSize - 1)) != 0 || (realEndOffset & (RingDescriptorSize - 1)) != 0)
                {
                    FailOnPayloadFlush(ref flushFailed, $"Out-of-line flush range {flushPage}:{startOffset}-{realEndOffset} is not aligned to {RingDescriptorSize}-byte records.");
                    realEndOffset -= (realEndOffset - startOffset) & (RingDescriptorSize - 1);
                }

                for (var offset = startOffset; offset < realEndOffset; offset += RingDescriptorSize)
                {
                    var address = (flushPage << pageSizeBits) | (uint)offset;
                    var ptr = page.pointer + offset;

                    // Claim the descriptor by atomically zeroing its self-key, arbitrating with teardown.
                    if (!TryClaimRequestDescriptor(address, (long*)ptr, out var key, out var payload))
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

                    ProcessRequestChunks(ref payload, count, ref flushFailed);
                }

                if (flushPage == endPage) break;
                flushPage = (flushPage + 1) & PageOffset.kPageMask;
            }

            CompleteFlush(count);

            void ProcessRequestChunks(ref TRequest payload, CountWrapper count, ref bool flushFailed)
            {
                var chunkSize = Math.Max(1, maxChunkSizeBytes);
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
                        operateOnPayload(payload.Buffer, offset, length, result);
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
                    var endAddress = count.untilAddress;
                    _ = Utility.MonotonicUpdate(ref flushedUntilAddress, endAddress, wrapDistance, out _);
                    // The request lane is now free up to endAddress; wake producers waiting on request back-pressure.
                    requestFreed.Set();
                    AggressiveShiftReadOnlyRunner(true);
                }
            }
            catch when (disposed) { }
        }

        long GetOffsetInPage(long address) => address & pageSizeMask;

        public void DoAggressiveShiftReadOnly()
        {
            if (ongoingAggressiveShiftReadOnly == 0 && Interlocked.CompareExchange(ref ongoingAggressiveShiftReadOnly, 1, 0) == 0)
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
            return tailAddress > readOnlyAddress || (readOnlyAddress - tailAddress > wrapDistance);
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
                ongoingAggressiveShiftReadOnly = 0;
            } while (ToShift() && ongoingAggressiveShiftReadOnly == 0 && Interlocked.CompareExchange(ref ongoingAggressiveShiftReadOnly, 1, 0) == 0);
        }

        bool AggressiveFlushShiftReadOnlyBump()
        {
            var newReadOnlyAddress = GetTailAddress();
            if (Utility.MonotonicUpdate(ref readOnlyAddress, newReadOnlyAddress, wrapDistance, out long oldReadOnlyAddress))
            {
                epoch.BumpCurrentEpoch(() => OnPagesMarkedReadOnly(oldReadOnlyAddress, newReadOnlyAddress));
                return true;
            }
            return false;
        }
    }
}