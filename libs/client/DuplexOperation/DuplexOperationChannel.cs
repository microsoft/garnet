// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
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

        /// <summary>Handle a request-flush failure.</summary>
        void OnFlushError(Exception exception);
    }

    /// <summary>
    /// A duplex, back-pressured ring for inline and out-of-line request/response traffic over a single
    /// connection. It owns ONE combined allocator cursor (<see cref="LightPageAndOffset"/>) that advances two
    /// logically distinct lanes in a single compare-and-swap:
    /// <list type="bullet">
    /// <item><description>
    /// The <b>request lane</b> — a dense request-id-indexed schedule plus a circular inline payload buffer.
    /// Requests are flushed to the network in request-id order and freed on flush (send), tracked by the
    /// flushed inline address and the schedule reservations. Back-pressure here is local and autonomous (it
    /// only waits for the producer's own flush to catch up).
    /// </description></item>
    /// <item><description>
    /// The <b>completion lane</b> — a separate array of <typeparamref name="TCompletionContext"/> slots keyed by
    /// the completion ticket handed out alongside the request id. A completion must outlive its
    /// flush (it is needed until the reply arrives), so this lane is freed on reply, tracked by
    /// <c>repliedUntil</c>. Back-pressure here is remote (it waits for the peer to answer).
    /// </description></item>
    /// </list>
    /// Because a single compare-and-swap advances the request id and the completion ticket (taskId) together,
    /// the pairing is structurally guaranteed: for any two claims A and B,
    /// <c>requestA &lt; requestB ⟺ completionA &lt; completionB</c>. No external coordination
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
        /// <summary>Largest command payload that can be written inline.</summary>
        internal int MaxInlinePayloadSize => store.MaxInlinePayloadSize;

        /// <summary>
        /// Inline-buffer bytes a command of <paramref name="payloadLength"/> reserves, and whether it is
        /// written inline (its whole payload packed contiguously into the inline buffer) or out-of-line (its
        /// payload held in a separately rented buffer, reserving no inline bytes).
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
        /// The ring owns one combined allocator cursor whose compare-and-swap advance covers both the request
        /// lane (request ids and inline bytes, freed on flush) and the completion lane (reply tickets, freed on
        /// reply), so a producer's (request id, ticket) pair is always mutually ordered. The
        /// caller injects the struct-specialized <paramref name="transport"/>, and the
        /// static network flush-completion callback (<see cref="DuplexOperationAsyncFlushResult{TRequest}.CompleteChunk"/>)
        /// hands the flush token directly to the admission controller.
        /// </para>
        /// </summary>
        /// <param name="ringPageSizeBytes">Largest inline payload, in bytes; a power of two. Larger payloads are sent
        /// out-of-line from a rented buffer.</param>
        /// <param name="ringPageCount">Power-of-two multiplier of <paramref name="ringPageSizeBytes"/> giving the
        /// capacity of the circular inline buffer.</param>
        /// <param name="maxOutstandingRequests">Maximum number of requests awaiting local send completion, regardless of size. Zero derives one request per header-sized unit of the inline buffer.</param>
        /// <param name="completionCapacity">Maximum number of outstanding response-expecting requests; rounds
        /// up to a power of two and bounds the completion lane before producers back-pressure on replies.</param>
        /// <param name="maxSendChunkSizeBytes">Maximum number of request bytes dispatched in one transport send.</param>
        /// <param name="transport">Struct-specialized transport used to send chunks and handle flush failures.</param>
        /// <param name="epoch">Shared epoch protecting the allocator cursor and flush machinery.</param>
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
                store.InlineBufferCapacity,
                store.CompletionCapacity,
                store.MaxOutstandingRequests,
                epoch,
                OnPagesMarkedReadOnly,
                OnFlushError);
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
        /// bytes directly into the inline buffer, skipping the pooled buffer entirely. This only returns the write
        /// destination; it publishes nothing. The caller writes the payload and then calls
        /// <see cref="PublishInlineRecord"/> to make the record visible to the flusher. Because publication
        /// happens after the payload is written (release-fenced inside <see cref="DuplexRingRecordStore{TR,TC,TF}.RegisterSchedule"/>),
        /// a claimer that observes the schedule entry also observes the fully-written payload.
        /// </summary>
        /// <param name="address">Inline address returned by <see cref="TryScheduleOperation"/> for an
        /// inline-sized allocation (see <see cref="GetRecordSize"/>).</param>
        /// <returns>Pointer to the first payload byte in the inline buffer.</returns>
        internal unsafe byte* RegisterInlineRecord(long address)
        {
            Debug.Assert(epoch.ThisInstanceProtected());
            ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed), this);
            return store.GetInlineWritePointer(address);
        }

        /// <summary>
        /// Publish an inline record whose payload has already been written into the inline buffer, making it eligible
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
        /// <param name="address">Inline address from the reservation; unused by out-of-line records.</param>
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

        void OnFlushError(Exception exception)
        {
            logger?.LogError(exception, "Exception continuing a request flush");
            transport.OnFlushError(exception);
        }

        /// <summary>
        /// Flush a dense request-id range of scheduled requests to the network. The flusher walks requests in
        /// request-id order, claiming each schedule entry exactly once. Inline payloads are reserved back to back in
        /// request-id order, so each run of consecutive inline records occupies one contiguous span of the inline
        /// buffer and is sent as a single coalesced payload; a run ends at an out-of-line record or where a record
        /// was placed at the start of the next lap. Out-of-line payloads are sent from their rented buffers. Only
        /// request bytes are sent; the completion (if any) lives in the completion lane and is untouched here.
        /// Every id in the range holds one schedule reservation, released here whether or not its producer
        /// published it (a producer that failed after reserving publishes nothing).
        /// </summary>
        void AsyncFlushRequestContext(int fromRequestId, int untilRequestId)
        {
            var count = (untilRequestId - fromRequestId) & (int)LightPageAndOffset.kRequestMask;
            controller.BeginFlushRange();
            var flushFailed = false;

            // Pending run of contiguous inline payloads, identified by its first request id.
            var runOffset = 0;
            var runLength = 0;
            var runRequestId = 0;

            var released = 0;
            for (var i = 0; i < count; i++)
            {
                // Once disposed there is no live connection to send on, and Dispose independently reclaims
                // every unclaimed schedule entry. Bail before claiming the next request so remaining ones fall
                // to Dispose; requests we already claimed were sent or disposed above.
                if (Volatile.Read(ref disposed))
                    break;

                released++;
                var requestId = (fromRequestId + i) & (int)LightPageAndOffset.kRequestMask;
                if (!store.TryClaimSchedule(requestId, out var schedule))
                    continue; // Reclaimed by teardown, or never published by a producer that failed.

                if (flushFailed)
                {
                    if (schedule.kind == RequestKind.OutOfLine)
                        schedule.request.Dispose();
                    continue;
                }

                if (schedule.kind == RequestKind.Inline)
                {
                    if (schedule.payloadLength == 0)
                        continue;

                    var offset = store.GetInlineBufferOffset(schedule.address);
                    if (runLength > 0 && offset != runOffset + runLength)
                        FlushRun(ref flushFailed);
                    if (runLength == 0)
                    {
                        runOffset = offset;
                        runRequestId = requestId;
                    }
                    runLength += schedule.payloadLength;
                }
                else
                {
                    // Preserve wire order: everything reserved before this record goes out first.
                    FlushRun(ref flushFailed);
                    if (flushFailed)
                        schedule.request.Dispose();
                    else
                        SendPayload(schedule.request.Buffer, 0, schedule.request.Length, schedule.request, requestId, ref flushFailed);
                }
            }

            FlushRun(ref flushFailed);
            controller.ReleaseActiveRequests(released); // Free the request reservations the flush consumed.
            controller.CompleteFlushPart(); // Release the scanner-owned range sentinel.

            void FlushRun(ref bool flushFailed)
            {
                if (runLength == 0)
                    return;

                var length = runLength;
                runLength = 0;
                if (!flushFailed)
                    SendPayload(store.InlineBuffer, runOffset, length, default, runRequestId, ref flushFailed);
            }

            void SendPayload(byte[] buffer, int baseOffset, int length, TRequestContext request, int requestId, ref bool flushFailed)
            {
                // An empty payload has nothing to send, and the chunk-count protocol below assumes length >= 1:
                // bumping count without dispatching a chunk would leave the flush count permanently unbalanced.
                if (length == 0)
                {
                    request.Dispose();
                    return;
                }

                var chunkSize = Math.Max(1, maxSendChunkSizeBytes);
                var chunkCount = ((length - 1) / chunkSize) + 1;
                var result = store.GetFlushResult(requestId);
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
        }
    }
}