// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.client
{
    /// <summary>
    /// A request-lane request stored in a duplex backpressure ring.
    /// The ring transfers ownership of the underlying buffer to the sender (flusher), which disposes
    /// it once the bytes have been handed to the network.
    /// </summary>
    internal interface IRequestContext : IDisposable
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
    /// Transport operations invoked by a duplex ring. Struct implementations are stored directly by the ring,
    /// allowing constrained calls without delegate allocation, boxing, or interface dispatch at the call site.
    /// </summary>
    internal interface ITransportContext
    {
        /// <summary>Transmit one request chunk and preserve <paramref name="context"/> for send completion.</summary>
        void Send(byte[] buffer, int offset, int length, object context);

        /// <summary>
        /// Whether this transport can coalesce a run of single-chunk records into one scatter/gather send. When
        /// false (e.g. TLS, which has no gather primitive) the ring sends each record individually and never
        /// calls <see cref="SendGather"/>.
        /// </summary>
        bool SupportsGather { get; }

        /// <summary>
        /// Transmit a coalesced run of single-chunk request payloads (<paramref name="batch"/>) in one gather
        /// send. On success the transport must invoke each record's completion exactly once via
        /// <see cref="DuplexOperationAsyncFlushResult{TRequestContext}.CompleteChunk"/> (passing the batch as the
        /// completion context fans out to every record) and release the batch. Only called when
        /// <see cref="SupportsGather"/> is true.
        /// </summary>
        void SendGather(GatherSendBatch batch);

        /// <summary>Handle a request-flush failure.</summary>
        void OnFlushError(Exception exception);
    }

    /// <summary>
    /// A duplex, back-pressured ring for inline and out-of-line request/response traffic over a single
    /// connection. It owns ONE combined address allocator (<see cref="PageOffset"/>) that advances two
    /// logically distinct lanes in a single atomic step:
    /// <list type="bullet">
    /// <item><description>
    /// The <b>request lane</b> — a circular page buffer of request descriptors and inline payloads. Its
    /// records are flushed to the network and freed on flush (send), tracked by
    /// <c>FlushedUntilAddress</c>. Back-pressure here is local and autonomous (it only waits for the
    /// producer's own flush to catch up).
    /// </description></item>
    /// <item><description>
    /// The <b>completion lane</b> — a separate array of <typeparamref name="TCompletionContext"/> slots keyed by
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
    /// This type is network-transport agnostic apart from its struct-specialized
    /// <typeparamref name="TTransport"/>.
    /// </para>
    /// <para>
    /// The response (receiver) side is intentionally left minimal for now: the completion lane provides
    /// storage, publication and accounting hooks (<see cref="CompletionTail"/>,
    /// <see cref="TryReadCompletion"/>, <see cref="AdvanceCompletion"/>), but the reply reader that drains it
    /// is implemented separately. Until a reader advances <c>repliedUntil</c>, response-expecting claims
    /// are bounded by the completion-lane capacity; fire-and-forget claims never touch the lane.
    /// </para>
    /// </summary>
    internal sealed class DuplexOperationChannel<TRequestContext, TCompletionContext, TTransport> : IDisposable
        where TRequestContext : struct, IRequestContext
        where TTransport : struct, ITransportContext
    {
        /// <summary>Largest command payload that can be written inline into a single page.</summary>
        internal int MaxInlinePayloadSize => store.MaxInlinePayloadSize;

        /// <summary>
        /// Ring bytes a command of <paramref name="payloadLength"/> advances the tail by, and whether it is
        /// written inline (its whole payload packed contiguously into one page) or out-of-line (its payload
        /// held in a separately rented buffer while the tail advances by a fixed nominal amount).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetRecordSize(int payloadLength, out bool isInline)
            => store.GetRecordSize(payloadLength, out isInline);

        public readonly LightEpoch epoch;

        readonly DuplexRingRecordStore<TRequestContext, TCompletionContext, DuplexOperationAsyncFlushResult<TRequestContext>> store;
        readonly DuplexAdmissionController controller;
        readonly TTransport transport;
        readonly ILogger logger;
        readonly int maxSendChunkSizeBytes;
        readonly FlushResultAllocationMode flushResultAllocationMode;

        // Pool of fixed-capacity gather-send holders, created only when the transport can coalesce sends.
        readonly SimpleObjectPool<GatherSendBatch> gatherPool;

        bool disposed;

        /// <summary>Number of completion tickets issued so far (task-space, wraps at 2^kTaskBits).</summary>
        internal int CompletionTail => controller.CompletionTail;

        internal int AllocatedFlushContextCount
            => flushResultAllocationMode == FlushResultAllocationMode.Buffered
                ? store.AllocatedFlushContextCount
                : controller.ActivePerOperationFlushResultCount;

        /// <summary>
        /// Create a duplex back-pressured ring over a single connection.
        /// <para>
        /// The ring owns one combined address allocator whose atomic advance covers both the request lane
        /// (a circular buffer of fixed-size descriptors, freed on flush) and the completion lane (reply
        /// tickets, freed on reply), so a producer's (address, ticket) pair is always mutually ordered. The
        /// caller injects the struct-specialized <paramref name="transport"/>, and the
        /// static network flush-completion callback (<see cref="DuplexOperationAsyncFlushResult{TRequest}.CompleteChunk"/>)
        /// hands the flush token directly to the admission controller.
        /// </para>
        /// </summary>
        /// <param name="ringPageSizeBytes">Size in bytes of each descriptor page; rounds down to a power of two and
        /// bounds how many requests can be queued before page reuse waits for an earlier flush.</param>
        /// <param name="ringPageCount">Number of circular pages backing the request lane. Must not exceed
        /// <see cref="LightPageAndOffset.kPageMask"/>.</param>
        /// <param name="maxOutstandingRequests">Maximum number of minimum-allocation requests awaiting local send completion. Zero derives the current header-granularity capacity.</param>
        /// <param name="completionCapacity">Maximum number of outstanding response-expecting requests; rounds
        /// up to a power of two and bounds the completion lane before producers back-pressure on replies.</param>
        /// <param name="maxSendChunkSizeBytes">Maximum number of request bytes dispatched in one transport send.</param>
        /// <param name="transport">Struct-specialized transport used to send chunks and handle flush failures.</param>
        /// <param name="epoch">Shared epoch protecting the page allocator and flush machinery.</param>
        /// <param name="flushResultAllocationMode">Controls whether flush completion contexts are retained per request slot or allocated per operation.</param>
        /// <param name="logger">Logger instance.</param>
        public DuplexOperationChannel(
            int ringPageSizeBytes,
            int ringPageCount,
            int maxOutstandingRequests,
            int completionCapacity,
            int maxSendChunkSizeBytes,
            TTransport transport,
            LightEpoch epoch,
            FlushResultAllocationMode flushResultAllocationMode = FlushResultAllocationMode.Buffered,
            ILogger logger = null)
        {
            if (ringPageCount is <= 0 or > (int)LightPageAndOffset.kPageMask) throw new ArgumentOutOfRangeException(nameof(ringPageCount));
            if (!Enum.IsDefined(flushResultAllocationMode)) throw new ArgumentOutOfRangeException(nameof(flushResultAllocationMode));

            this.epoch = epoch;
            this.maxSendChunkSizeBytes = maxSendChunkSizeBytes;
            this.transport = transport;
            this.logger = logger;
            this.flushResultAllocationMode = flushResultAllocationMode;

            store = new DuplexRingRecordStore<TRequestContext, TCompletionContext, DuplexOperationAsyncFlushResult<TRequestContext>>(
                ringPageSizeBytes,
                ringPageCount,
                maxOutstandingRequests,
                completionCapacity);
            controller = new DuplexAdmissionController(
                store.Shape,
                store.CompletionCapacity,
                store.MaxOutstandingRequests,
                epoch,
                OnPagesMarkedReadOnly,
                store);

            // A single flush can claim at most one record per outstanding request, so a gather holder sized to
            // the schedule capacity never overflows. Only create the pool when the transport can coalesce sends.
            if (transport.SupportsGather)
            {
                var slotCount = store.ScheduleCapacity;
                gatherPool = new SimpleObjectPool<GatherSendBatch>(() => new GatherSendBatch(slotCount, gatherPool));
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Volatile.Write(ref disposed, true);
            controller.Dispose();
            store.CloseAndReclaimRequests();
        }

        #region Utilities

        /// <summary>
        /// Get tail address
        /// </summary>
        public long GetTailAddress()
            => controller.GetTailAddress();

        /// <summary>
        /// Attempts to reserve the paired request address and optional completion ticket for one operation.
        /// On backpressure, identifies the request or completion capacity event that the caller should await.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryScheduleOperation(
            int requestSize,
            bool expectsCompletion,
            out DuplexOperationReservation reservation,
            out CompletionEvent waitEvent)
            => controller.TryScheduleOperation(requestSize, expectsCompletion, out reservation, out waitEvent);

        /// <summary>
        /// Advances published requests toward the read-only frontier so epoch-safe request flushing can run.
        /// </summary>
        public void DrainRequests()
            => controller.DrainRequests();

        internal ValueTask WaitForIdleAsync(CancellationToken token)
            => controller.WaitForIdleAsync(token);

        #endregion

        #region RequestContext Impl

        /// <summary>
        /// Reserve an inline record at <paramref name="address"/> and return a pointer to write its payload
        /// bytes directly into page memory, skipping the pooled buffer entirely. This only returns the write
        /// destination; it publishes nothing. The caller writes the payload and then calls
        /// <see cref="PublishInlineRecord"/> to make the record visible to the flusher. Because publication
        /// happens after the payload is written (release-fenced inside <see cref="DuplexRingRecordStore{TR,TC,TF}.RegisterSchedule"/>),
        /// a claimer that observes the schedule entry also observes the fully-written payload.
        /// </summary>
        /// <param name="address">Ring address returned by <see cref="TryScheduleOperation"/> for an
        /// inline-sized allocation (see <see cref="GetRecordSize"/>).</param>
        /// <returns>Pointer to the first payload byte in page memory.</returns>
        internal unsafe byte* RegisterInlineRecord(long address)
        {
            Debug.Assert(epoch.ThisInstanceProtected());
            ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed), this);
            return store.GetInlineWritePointer(address);
        }

        /// <summary>
        /// Publish an inline record whose payload has already been written into page memory, making it eligible
        /// for flushing. The dense request-id schedule entry is published last (release-fenced), so the flusher
        /// never observes the entry before the payload bytes.
        /// </summary>
        /// <param name="requestId">Dense request id from the operation's reservation.</param>
        /// <param name="address">Ring address the payload was written to.</param>
        /// <param name="payloadLength">Number of payload bytes written.</param>
        internal void PublishInlineRecord(int requestId, long address, int payloadLength)
        {
            Debug.Assert(epoch.ThisInstanceProtected());
            ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed), this);
            store.RegisterSchedule(requestId, address, RequestKind.Inline, payloadLength, default);
        }

        /// <summary>
        /// Register (store and publish) an out-of-line request, making it eligible for flushing. The request's
        /// payload lives in its rented buffer; the dense request-id schedule entry records the context and is
        /// published last (release-fenced), so the flusher observes a fully-initialized entry.
        /// </summary>
        /// <param name="requestId">Dense request id from the operation's reservation.</param>
        /// <param name="address">Ring address reserved for throttling this request.</param>
        /// <param name="request">Request context owning the out-of-line payload buffer.</param>
        internal void RegisterOfflineRecord(int requestId, long address, TRequestContext request)
        {
            Debug.Assert(epoch.ThisInstanceProtected());
            ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed), this);
            store.RegisterSchedule(requestId, address, RequestKind.OutOfLine, request.Length, request);
        }

        #endregion

        #region CompletionContext Impl

        /// <summary>
        /// Register (store and publish) a completion for the given ticket. The release-fenced marker write
        /// happens after the completion store, so the barrier-less reply reader that observes the marker also
        /// observes a fully-written completion.
        /// </summary>
        /// <param name="ticket"></param>
        /// <param name="completion"></param>
        internal void RegisterCompletion(int ticket, TCompletionContext completion)
            => store.RegisterCompletion(ticket, completion);

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
        internal bool TryClaimCompletionTicket(int ticket, out TCompletionContext completion)
            => store.TryClaimCompletion(ticket, out completion);

        /// <summary>
        /// Reader-side: try to read a published completion for the given ticket. Returns false if the
        /// producer has not finished publishing it yet.
        /// </summary>
        internal bool TryReadCompletion(int ticket, out TCompletionContext completion)
            => store.TryReadCompletion(ticket, out completion);

        /// <summary>
        /// Reader-side: advance the reply watermark, freeing completion slots for reuse and waking any
        /// producer blocked on completion-lane back-pressure.
        /// </summary>
        internal void AdvanceCompletion(int consumedCount)
            => controller.AdvanceCompletion(consumedCount);

        #endregion

        void OnPagesMarkedReadOnly(int oldRequestId, int newRequestId)
            => AsyncFlushRequestContext(oldRequestId, newRequestId);

        /// <summary>
        /// Flush a dense request-id range of scheduled requests to the network. The flusher walks requests in
        /// request-id order (not by ring address), claiming each schedule entry exactly once. Inline payloads
        /// are read directly from page memory at each entry's own address; out-of-line payloads are read from
        /// their rented buffers. Only request bytes are sent; the completion (if any) lives in the completion
        /// lane and is untouched here. The ring byte end of the range, which gates ring-page reuse once the
        /// range is flushed, is taken from the last entry's footprint: request ids are assigned in ring byte
        /// order, so the last entry's end is the range's end.
        /// </summary>
        void AsyncFlushRequestContext(int fromRequestId, int untilRequestId)
        {
            var count = (untilRequestId - fromRequestId) & (int)LightPageAndOffset.kRequestMask;
            var lastConsumedBytes = store.GetScheduleFootprint((untilRequestId - 1) & (int)LightPageAndOffset.kRequestMask, out var lastAddress);
            controller.BeginFlushRange(lastAddress, lastConsumedBytes);
            var flushFailed = false;

            // Coalesce a contiguous run of single-chunk record payloads into one scatter/gather send. Each
            // record still registers and completes its own flush part, so slot-freeing and flushedUntil
            // accounting are unchanged; only the socket send is batched. Holders are pooled (allocation-free in
            // steady state); a fresh one is checked out per dispatched send so an in-flight send's completion
            // can read it while the next batch accumulates into a different holder. Gather is only attempted
            // when the transport supports it (e.g. not TLS); otherwise every record streams individually.
            var canGather = transport.SupportsGather;
            GatherSendBatch batch = null;

            var released = 0;
            for (var i = 0; i < count; i++)
            {
                // Once disposed there is no live connection to send on, and Dispose independently reclaims
                // every unclaimed schedule entry. Bail before claiming the next request so remaining ones fall
                // to Dispose; requests we already claimed were sent or disposed above.
                if (Volatile.Read(ref disposed))
                    break;

                var requestId = (fromRequestId + i) & (int)LightPageAndOffset.kRequestMask;
                if (!store.TryClaimSchedule(requestId, out var schedule))
                    continue; // Already reclaimed by teardown.
                released++;

                var request = schedule.request;
                var flushContext = store.GetFlushResult(requestId);

                if (flushFailed)
                {
                    if (schedule.kind == RequestKind.OutOfLine)
                        request.Dispose();
                    continue;
                }

                if (schedule.kind == RequestKind.Inline)
                {
                    // Header-free inline payload lives contiguously in page memory at the scheduled address.
                    if (schedule.payloadLength > 0)
                    {
                        var page = store.Shape.GetUnwrappedPageIndex(schedule.address);
                        var pageOffset = (int)store.Shape.GetOffsetInPage(schedule.address);
                        ProcessRequestContext(store.GetPageBuffer(page), pageOffset, schedule.payloadLength, request, requestId, flushContext, ref flushFailed);
                    }
                }
                else
                    ProcessRequestContext(request.Buffer, 0, request.Length, request, requestId, flushContext, ref flushFailed);
            }

            FlushBatch(); // Dispatch any remaining coalesced single-chunk records.
            controller.ReleaseScheduleReservations(released); // Free the schedule slots the flush consumed.
            controller.CompleteFlushPart(); // Release the scanner-owned range sentinel.

            void ProcessRequestContext(byte[] buffer, int baseOffset, int length, TRequestContext request, int requestId, DuplexOperationAsyncFlushResult<TRequestContext> flushContext, ref bool flushFailed)
            {
                // An empty payload has nothing to send, and the chunk-count protocol below assumes length >= 1:
                // bumping count without dispatching a chunk would leave the flush count permanently unbalanced
                // and stall flushedUntil.
                if (length == 0)
                {
                    request.Dispose();
                    return;
                }

                var chunkSize = Math.Max(1, maxSendChunkSizeBytes);
                var chunkCount = ((length - 1) / chunkSize) + 1;
                var result = flushContext;
                var perOperationAllocation = false;
                if (result == null)
                {
                    result = new DuplexOperationAsyncFlushResult<TRequestContext>();
                    if (flushResultAllocationMode == FlushResultAllocationMode.Buffered)
                        store.SetFlushResult(requestId, result);
                    else
                    {
                        controller.RegisterPerOperationFlushResult();
                        perOperationAllocation = true;
                    }
                }
                result.Initialize(request, chunkCount, controller, perOperationAllocation);
                controller.RegisterFlushPart();

                if (chunkCount == 1 && canGather)
                {
                    // Defer into the coalescing batch; the single gather send dispatched at batch flush
                    // completes this record's one chunk.
                    batch ??= gatherPool.Checkout();
                    batch.Append(buffer, baseOffset, length, result);
                    return;
                }

                // A multi-chunk record cannot join the gather (its chunks need distinct completions), and when
                // gather is unsupported every record streams here. Flush the records accumulated before this one
                // to preserve wire order, then stream its chunks directly.
                FlushBatch();

                var dispatchedChunks = 0;
                try
                {
                    for (var offset = 0; offset < length; offset += chunkSize)
                    {
                        var chunkLength = Math.Min(chunkSize, length - offset);
                        transport.Send(buffer, baseOffset + offset, chunkLength, result);
                        dispatchedChunks++;
                    }
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "Exception sending a request");
                    flushFailed = true;
                    transport.OnFlushError(ex);

                    // Undispatched chunks will never get a network completion, so account for them here via
                    // the same completion routine the network callback uses.
                    for (var i = dispatchedChunks; i < chunkCount; i++)
                        DuplexOperationAsyncFlushResult<TRequestContext>.CompleteChunk(result);
                }
            }

            void FlushBatch()
            {
                if (batch == null)
                    return;

                // Hand ownership of the holder to the dispatched send; clear the field so the next batch checks
                // out a fresh holder instead of mutating one an in-flight gather completion still reads.
                var current = batch;
                batch = null;

                if (current.RecordCount == 1)
                {
                    // A lone record does not benefit from a scatter/gather send; use the plain per-record path
                    // (its own send completion finalizes the record), then return the holder to the pool.
                    var segment = current[0];
                    var context = current.ContextAt(0);
                    try
                    {
                        transport.Send(segment.Array, segment.Offset, segment.Count, context);
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "Exception sending a coalesced single request");
                        flushFailed = true;
                        transport.OnFlushError(ex);
                        DuplexOperationAsyncFlushResult<TRequestContext>.CompleteChunk(context);
                    }
                    current.Release();
                    return;
                }

                try
                {
                    // On success the send completion fans out per-record completions and releases the holder.
                    transport.SendGather(current);
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "Exception sending a coalesced request batch");
                    flushFailed = true;
                    transport.OnFlushError(ex);

                    // No network completion will arrive for an un-dispatched batch, so complete each record's
                    // single chunk here via the same routine the network callback uses, then release the holder.
                    for (var i = 0; i < current.RecordCount; i++)
                        DuplexOperationAsyncFlushResult<TRequestContext>.CompleteChunk(current.ContextAt(i));
                    current.Release();
                }
            }
        }
    }
}