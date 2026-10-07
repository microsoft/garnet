// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
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
        /// Ring bytes a command of <paramref name="payloadLength"/> reserves, and whether it is written inline
        /// (its whole payload packed into one page after an 8-byte header) or out-of-line (an 8-byte descriptor
        /// pointing at a separately rented payload buffer).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetRecordSize(int payloadLength, out bool isInline)
            => store.GetRecordSize(payloadLength, out isInline);

        public readonly LightEpoch epoch;

        readonly DuplexRingRecordStore<TRequestContext, TCompletionContext, DuplexOperationAsyncFlushResult<TRequestContext>> store;
        readonly DuplexAdmissionController controller;
        readonly TTransport transport;
        readonly ILogger logger;
        readonly int maxChunkSizeBytes;

        bool disposed;

        /// <summary>Number of completion tickets issued so far (task-space, wraps at 2^kTaskBits).</summary>
        internal int CompletionTail => controller.CompletionTail;

        internal int AllocatedFlushContextCount => store.AllocatedFlushContextCount;

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
        /// <see cref="PageOffset.kPageMask"/>.</param>
        /// <param name="completionCapacity">Maximum number of outstanding response-expecting requests; rounds
        /// up to a power of two and bounds the completion lane before producers back-pressure on replies.</param>
        /// <param name="maxChunkSizeBytes">Size of a single network send buffer; caps the per-send chunk length.</param>
        /// <param name="transport">Struct-specialized transport used to send chunks and handle flush failures.</param>
        /// <param name="epoch">Shared epoch protecting the page allocator and flush machinery.</param>
        /// <param name="logger">Logger instance.</param>
        public DuplexOperationChannel(
            int ringPageSizeBytes,
            int ringPageCount,
            int completionCapacity,
            int maxChunkSizeBytes,
            TTransport transport,
            LightEpoch epoch,
            ILogger logger = null)
        {
            if (ringPageCount > PageOffset.kPageMask) throw new ArgumentOutOfRangeException(nameof(ringPageCount));

            this.epoch = epoch;
            this.maxChunkSizeBytes = maxChunkSizeBytes;
            this.transport = transport;
            this.logger = logger;

            store = new DuplexRingRecordStore<TRequestContext, TCompletionContext, DuplexOperationAsyncFlushResult<TRequestContext>>(
                ringPageSizeBytes,
                ringPageCount,
                completionCapacity);
            controller = new DuplexAdmissionController(
                store.Shape,
                store.CompletionCapacity,
                epoch,
                store.SetPageLastOffset,
                OnPagesMarkedReadOnly);

            // A descriptor's low 56 bits must hold the full log address, so the page-index bits plus the
            // in-page offset bits must fit below the tag byte. This bounds how the descriptor codec can
            // coexist with the address layout for any configured page size.
            Debug.Assert(PageOffset.kPageBits + store.Shape.PageSizeBits <= DuplexRingRecordFormat.TagShift,
                "Descriptor address space must leave the most-significant byte free for the payload kind tag.");
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

        #endregion

        #region RequestContext Impl

        /// <summary>
        /// Reserve an inline record at <paramref name="address"/> and return a pointer to write its payload
        /// bytes directly into page memory, skipping the pooled buffer entirely. The producing thread must
        /// hold the epoch. The descriptor header (kind + payload length) is written <b>before</b> the caller
        /// fills the payload, so any concurrent teardown walk recovers this record's size and strides over the
        /// payload region instead of misreading it; the memory barrier orders that header ahead of the payload
        /// writes. Inline records own no side-table entry — their bytes live in the page and are freed on flush
        /// like any other record — so no post-publish reclaim is needed.
        /// <para>
        /// Flush-safety of this early header publish rests entirely on the epoch: the flusher
        /// (<see cref="AsyncFlushRequestContext"/>) runs only as the drain-list action queued by
        /// <see cref="LightEpoch.BumpCurrentEpoch(System.Action)"/> when read-only shifts (see
        /// <c>AggressiveFlushShiftReadOnlyBump</c>), so it cannot read this record until every producer that
        /// held the epoch at shift time has drained. Unlike out-of-line records — whose payload lives in a pool
        /// buffer written in full before the descriptor is published — inline has no data-ordering backstop, so
        /// <b>the caller MUST hold the epoch continuously from the tail-advancing <see cref="TryScheduleOperation"/>
        /// through the completed payload write</b> (no <c>Suspend</c>, no <c>await</c> in between). Releasing
        /// the epoch mid-write would let a concurrent flush observe this header with an unwritten payload.
        /// </para>
        /// </summary>
        /// <param name="address">Descriptor address returned by <see cref="TryScheduleOperation"/> for an
        /// inline-sized allocation (see <see cref="GetRecordSize"/>).</param>
        /// <param name="payloadLength">Number of payload bytes the caller will write.</param>
        /// <returns>Pointer to the first payload byte (immediately after the 8-byte header).</returns>
        internal unsafe byte* RegisterInlineRecord(long address, int payloadLength)
        {
            Debug.Assert(epoch.ThisInstanceProtected());
            ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed), this);
            return store.RegisterInlineRecord(address, payloadLength, default);
        }

        /// <summary>
        /// Register (store and publish) a request request at the descriptor address, making it eligible for
        /// flushing. Publication writes the descriptor's self-key into page memory; the producing thread must
        /// hold the epoch, so the flusher (which validates key == address only after the epoch barrier) never
        /// observes a partially-written slot.
        /// <para>
        /// The word is tagged <see cref="RequestKind.OutOfLine"/> with the log address in its low 56
        /// bits; since that tag is <c>0x00</c> the published word is byte-identical to the raw address and can
        /// never collide with the uninitialized empty sentinel.
        /// </para>
        /// </summary>
        /// <param name="address"></param>
        /// <param name="request"></param>
        internal void RegisterOfflineRecord(long address, TRequestContext request)
        {
            Debug.Assert(epoch.ThisInstanceProtected());
            ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed), this);
            store.RegisterRequest(address, request);
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

        void OnPagesMarkedReadOnly(long oldReadOnlyAddress, long newReadOnlyAddress)
            => AsyncFlushRequestContext(oldReadOnlyAddress, newReadOnlyAddress);

        /// <summary>
        /// Flush an address range of request descriptors to the network. Only the request buffer is sent;
        /// the completion (if any) lives in the completion lane and is untouched here.
        /// </summary>
        /// <param name="fromAddress"></param>
        /// <param name="untilAddress"></param>
        void AsyncFlushRequestContext(long fromAddress, long untilAddress)
        {
            var startPage = store.Shape.GetUnwrappedPageIndex(fromAddress);
            var endPage = store.Shape.GetUnwrappedPageIndex(untilAddress);
            var count = new CountWrapper
            {
                count = 1,
                untilAddress = untilAddress
            };
            var flushFailed = false;
            var disposedBail = false;

            var flushPage = startPage;
            while (true)
            {
                long startOffset = 0, endOffset = store.Shape.PageSizeBytes;
                if (flushPage == startPage) startOffset = store.Shape.GetOffsetInPage(fromAddress);
                if (flushPage == endPage) endOffset = store.Shape.GetOffsetInPage(untilAddress);

                var realEndOffset = store.ConsumePageEndOffset(flushPage, endOffset);

                if ((startOffset & (DuplexRingRecordFormat.HeaderSize - 1)) != 0 ||
                    (realEndOffset & (DuplexRingRecordFormat.HeaderSize - 1)) != 0)
                {
                    FailOnRequestFlush(ref flushFailed, $"Out-of-line flush range {flushPage}:{startOffset}-{realEndOffset} is not aligned to {DuplexRingRecordFormat.HeaderSize}-byte records.");
                    realEndOffset -= (realEndOffset - startOffset) & (DuplexRingRecordFormat.HeaderSize - 1);
                }

                for (var offset = startOffset; offset < realEndOffset;)
                {
                    // Once disposed there is no live connection to send on, and Dispose independently walks
                    // every page (from offset 0, with the same variable stride) to reclaim any record we have
                    // not yet claimed. Bail before claiming the next record so those fall to Dispose; records
                    // we already claimed in earlier iterations were sent or disposed above. Records past the
                    // taken bit are skipped by Dispose, so we must not claim one here and then abandon it.
                    if (Volatile.Read(ref disposed))
                    {
                        disposedBail = true;
                        break;
                    }
                    var address = store.Shape.PackAddress(flushPage, offset);

                    // Claim the record, recovering its stride whether or not we win the claim.
                    var won = store.TryClaimRequest(address, out var kind, out var recordSize, out var payloadLength, out var key, out var request, out var flushContext);
                    if (!won)
                    {
                        offset += recordSize; // Empty slot or taken by teardown.
                        continue;
                    }

                    if (kind == RequestKind.OutOfLine && key != address)
                    {
                        FailOnRequestFlush(ref flushFailed, $"Out-of-line request key {key} does not match its log address {address}.");
                        request.Dispose();
                        offset += recordSize;
                        continue;
                    }

                    if (flushFailed)
                    {
                        request.Dispose();
                        offset += recordSize;
                        continue;
                    }

                    if (kind == RequestKind.Inline)
                    {
                        // Header-only inline records carry no payload; skip the send path entirely.
                        if (payloadLength > 0)
                            ProcessRequestContext(store.GetPageBuffer(flushPage), (int)(offset + DuplexRingRecordFormat.HeaderSize), payloadLength, request, address, flushContext, count, ref flushFailed);
                    }
                    else if (kind == RequestKind.OutOfLine)
                        ProcessRequestContext(request.Buffer, 0, request.Length, request, address, flushContext, count, ref flushFailed);
                    else
                    {
                        // A won claim is always Inline or OutOfLine; any other kind here means an uninitialized
                        // or corrupted descriptor slipped past the claim, so fail the flush instead of sending it.
                        FailOnRequestFlush(ref flushFailed, $"Claimed record at {address} has unexpected kind {kind}.");
                        request.Dispose();
                    }

                    offset += recordSize;
                }

                if (disposedBail || flushPage == endPage) break;
                flushPage = (flushPage + 1) & PageOffset.kPageMask;
            }

            controller.CompleteFlush(count);

            void ProcessRequestContext(byte[] buffer, int baseOffset, int length, TRequestContext request, long address, DuplexOperationAsyncFlushResult<TRequestContext> flushContext, CountWrapper count, ref bool flushFailed)
            {
                // An empty payload has nothing to send, and the chunk-count protocol below assumes length >= 1:
                // bumping count without dispatching a chunk would leave the flush count permanently unbalanced
                // and stall flushedUntil.
                if (length == 0)
                {
                    request.Dispose();
                    return;
                }

                var chunkSize = Math.Max(1, maxChunkSizeBytes);
                var chunkCount = ((length - 1) / chunkSize) + 1;
                var result = flushContext;
                if (result == null)
                {
                    result = new DuplexOperationAsyncFlushResult<TRequestContext>();
                    store.SetFlushContext(address, result);
                }
                result.Initialize(count, request, chunkCount, controller);
                _ = Interlocked.Increment(ref count.count);

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

            void FailOnRequestFlush(ref bool flushFailed, string message)
            {
                if (flushFailed)
                    return;

                flushFailed = true;
                logger?.LogError("{Message}", message);
                transport.OnFlushError(new InvalidOperationException(message));
            }
        }
    }
}