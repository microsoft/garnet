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
    /// Payload framing of a request-lane descriptor, carried in the most-significant byte (bits [56..63])
    /// of the 8-byte descriptor word. The low 56 bits carry the per-kind metadata: the request's log
    /// address for <see cref="OutOfLine"/> records, or the in-page payload length for <see cref="Inline"/>
    /// records.
    /// </summary>
    internal enum RequestKind : byte
    {
        /// <summary>
        /// The descriptor references request bytes held in a separately rented pool buffer (via the request
        /// side-table). The low 56 bits hold the descriptor's log address; this is byte-identical to a raw
        /// address word, so an out-of-line descriptor is indistinguishable on the wire from the pre-tag
        /// format.
        /// </summary>
        OutOfLine = 0x00,

        /// <summary>
        /// The descriptor is immediately followed, in the same page, by the request bytes. The low 56 bits
        /// hold the payload length; no pool buffer or side-table entry is used.
        /// </summary>
        Inline = 0x01,

        /// <summary>
        /// Empty/claimed slot. Matches the <c>0xFF</c> page fill and the <c>-1</c> reset value used when a
        /// descriptor is claimed, so an unpublished or already-claimed slot always decodes to this kind.
        /// </summary>
        Uninitialized = 0xFF,
    }

    /// <summary>
    /// A request-lane request stored in a <see cref="DuplexBackpressureRing{TRequest, TCompletion}"/>.
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
    /// Transmit one chunk of a request over the wire. <paramref name="context"/> is the ring-owned
    /// flush-completion token that MUST be handed back to
    /// <see cref="LightRequestAsyncFlushResult{TRequest}.CompleteChunk"/> once the
    /// asynchronous send completes. This is the ring's only transport dependency; the chunking and the
    /// flush-completion accounting are owned by the ring.
    /// </summary>
    internal delegate void ProcessRequest(byte[] buffer, int offset, int length, object context);

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
    /// This type is network-transport agnostic apart from an injected <see cref="ProcessRequest"/>
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
    internal sealed class DuplexBackpressureRing<TRequest, TCompletion> : IDisposable, IFlushCompletionSink, IOutOfLinePayloadBudget
        where TRequest : struct, IRequest
    {
        /// <summary>
        /// A single fixed-size page in the <see cref="LightNetworkWriter"/> circular buffer.
        /// The page stores out-of-line request descriptors and complete inline request records.
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
                // Descriptor slots start empty. The empty sentinel is -1 (not 0) so that a descriptor at
                // address 0 (the first allocation, and every wrap back to page 0 / offset 0) is stored
                // verbatim and remains distinguishable from an unpublished/claimed slot.
                value.AsSpan().Fill(0xFF);
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

        // Descriptor-word codec. The most-significant byte (bits [56..63]) carries the payload kind tag
        // (<see cref="RequestPayloadKind"/>); the low 56 bits carry the per-kind metadata (the log address
        // for out-of-line records, the in-page payload length for inline records). The tag rides the same
        // atomic word as the metadata, so tagging adds neither an extra atomic nor extra space.
        const int TagShift = 56;
        const long PayloadMetaMask = (1L << TagShift) - 1;

        // Bytes an inline record reserves ahead of its payload for the descriptor word.
        const int InlineHeaderSize = 8;

        // Empty/claimed sentinel: MSByte = Uninitialized (0xFF), low bits all set. Equals the 0xFF page fill
        // and the value written when a slot is claimed, so an unpublished or claimed slot decodes to
        // Uninitialized (never a valid OutOfLine or Inline record).
        const long UninitializedDescriptor = -1L;

        /// <summary>Pack a payload kind and its 56-bit metadata into a descriptor word.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static long EncodeDescriptor(RequestKind kind, long meta)
            => ((long)(byte)kind << TagShift) | (meta & PayloadMetaMask);

        /// <summary>Extract the 56-bit metadata (log address or payload length) from a descriptor word.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static long DecodeMeta(long word) => word & PayloadMetaMask;

        // Claiming a record sets this bit (the sign bit == the top bit of the tag byte) while preserving the
        // low 56 bits, so a claimed record still yields its record size to any concurrent walker. The
        // Uninitialized tag (0xFF) also has this bit set, so an empty slot is always tested first.
        const long TakenBit = 1L << 63;

        // Records are laid out on an 8-byte grid so descriptor words stay naturally aligned.
        const int RecordAlignment = 8;

        /// <summary>Bytes an inline record occupies in a page: the 8-byte header plus its payload, rounded up
        /// to the record grid.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int InlineRecordSize(int payloadLength)
            => (InlineHeaderSize + payloadLength + (RecordAlignment - 1)) & ~(RecordAlignment - 1);

        /// <summary>Largest command payload that can be written inline into a single page.</summary>
        internal int MaxInlinePayloadSize => ringPageSizeBytes - InlineHeaderSize;

        /// <summary>True when a command of <paramref name="payloadLength"/> bytes fits inline in one page.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool CanInline(int payloadLength) => (uint)payloadLength <= (uint)MaxInlinePayloadSize;

        public readonly LightEpoch epoch;

        readonly RingPage[] bufferPages;
        readonly ILogger logger;
        readonly int ringPageCount, pageSizeBits, pageSizeMask;
        internal readonly int ringPageSizeBytes;
        readonly long wrapDistance;
        readonly int maxChunkSizeBytes;

        PageOffset tailPageOffset;
        long flushedUntilAddress, readOnlyAddress;
        int ongoingAggressiveShiftReadOnly;

        // Injected transport primitive and the failure callback, supplied at construction.
        readonly ProcessRequest operateOnRequest;
        readonly Action<Exception> onFlushError;

        bool disposed;

        // Request lane freed on flush (send); producers block on this when the ring's own send is behind.
        CompletionEvent requestFreed;

        // Completion lane freed on reply (receiver); producers block on this when too many replies are outstanding.
        CompletionEvent completionFreed;

        // Out-of-line payload bytes are reserved before pool rent and before either ring lane is allocated.
        // The reservation is released only when the payload's final owner disposes it.
        readonly long maxOutOfLineBytesBudget;
        long outOfLinePayloadBytes;
        long peakOutOfLinePayloadBytes;
        CompletionEvent outOfLinePayloadBudgetFreed;

        /// <summary>Number of completion tickets issued so far (task-space, wraps at 2^kTaskBits).</summary>
        internal int CompletionTail => tailPageOffset.TaskId;

        // Request side-table: each physical descriptor maps to one request. Publication happens-before
        // is established by the self-key written into page memory under the epoch barrier; ownership transfer at
        // flush/teardown is arbitrated by atomically zeroing that key.
        readonly TRequest[] requests;

        // Completion lane. Each slot co-locates the release-fenced guard word with its completion so the reply
        // reader's matched read (guard, then completion) hits one cache line instead of two. Indexed by
        // (ticket & completionMask).
        readonly CompletionSlot[] completionLane;
        readonly int completionCapacity, completionMask;
        long completionUntil;
        int completionReservations;

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
        /// caller injects the transport: <paramref name="operateOnRequest"/> transmits one chunk, and the
        /// static network flush-completion callback (<see cref="LightRequestAsyncFlushResult{TRequest}.CompleteChunk"/>)
        /// hands the flush token back to the ring via its <see cref="IFlushCompletionSink"/>.
        /// </para>
        /// </summary>
        /// <param name="ringPageSizeBytes">Size in bytes of each descriptor page; rounds down to a power of two and
        /// bounds how many requests can be queued before page reuse waits for an earlier flush.</param>
        /// <param name="ringPageCount">Number of circular pages backing the request lane. Must not exceed
        /// <see cref="PageOffset.kPageMask"/>.</param>
        /// <param name="completionCapacity">Maximum number of outstanding response-expecting requests; rounds
        /// up to a power of two and bounds the completion lane before producers back-pressure on replies.</param>
        /// <param name="maxOutOfLineBytesBudget">Maximum checked-out bytes across out-of-line request payloads.
        /// Zero means unlimited.</param>
        /// <param name="maxChunkSizeBytes">Size of a single network send buffer; caps the per-send chunk length.</param>
        /// <param name="operateOnRequest">Transport callback used to flush one request chunk to the network.</param>
        /// <param name="onFlushError">Invoked when a flush send fails, or with a request-validation failure
        /// whose reason has already been logged (exception may be null in that case).</param>
        /// <param name="epoch">Shared epoch protecting the page allocator and flush machinery.</param>
        /// <param name="logger">Logger instance.</param>
        public DuplexBackpressureRing(
            int ringPageSizeBytes,
            int ringPageCount,
            int completionCapacity,
            long maxOutOfLineBytesBudget,
            int maxChunkSizeBytes,
            ProcessRequest operateOnRequest,
            Action<Exception> onFlushError,
            LightEpoch epoch,
            ILogger logger = null)
        {
            this.ringPageCount = ringPageCount;
            if (this.ringPageCount > PageOffset.kPageMask) throw new ArgumentOutOfRangeException(nameof(ringPageCount));
            if (maxOutOfLineBytesBudget < 0) throw new ArgumentOutOfRangeException(nameof(maxOutOfLineBytesBudget));

            ArgumentNullException.ThrowIfNull(operateOnRequest);
            ArgumentNullException.ThrowIfNull(onFlushError);

            requestFreed.Initialize();
            completionFreed.Initialize();
            outOfLinePayloadBudgetFreed.Initialize();

            this.epoch = epoch;
            this.ringPageSizeBytes = ringPageSizeBytes;
            this.maxChunkSizeBytes = maxChunkSizeBytes;
            this.maxOutOfLineBytesBudget = maxOutOfLineBytesBudget;
            this.operateOnRequest = operateOnRequest;
            this.onFlushError = onFlushError;
            this.logger = logger;

            var ringSlotCount = this.ringPageCount * ringPageSizeBytes / RingDescriptorSize;
            this.requests = new TRequest[ringSlotCount];

            this.pageSizeBits = Utility.NumBitsPreviousPowerOf2(ringPageSizeBytes);
            this.wrapDistance = PageWrapDistance << pageSizeBits;
            pageSizeMask = ringPageSizeBytes - 1;

            // A descriptor's low 56 bits must hold the full log address, so the page-index bits plus the
            // in-page offset bits must fit below the tag byte. This bounds how the descriptor codec can
            // coexist with the address layout for any configured page size.
            Debug.Assert(PageOffset.kPageBits + pageSizeBits <= TagShift,
                "Descriptor address space must leave the most-significant byte free for the payload kind tag.");

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

            // Reclaim any out-of-line request buffers still outstanding by walking each page with the same
            // variable-stride claim the flusher uses, so inline payload bytes are skipped (by their recorded
            // size) instead of being misread as descriptors. Inline records own no pooled buffer, so claiming
            // one only prevents a concurrent flush from sending it. Records already flushed carry the taken
            // bit and are skipped while still yielding their stride.
            for (var page = 0; page < ringPageCount; page++)
            {
                var basePtr = bufferPages[page].pointer;
                for (var offset = 0; offset < ringPageSizeBytes;)
                {
                    var address = ((long)page << pageSizeBits) | (uint)offset;
                    if (TryClaimRequest(address, (long*)(basePtr + offset), out var kind, out var recordSize, out _, out _, out var request) &&
                        kind == RequestKind.OutOfLine)
                        request.Dispose();

                    // A reused (wrapped) page can hold stale bytes past the current generation's records: a
                    // smaller record overwriting a larger one leaves the older record's payload tail, which may
                    // decode as a bogus inline header whose stride is non-positive or overruns the page. Such
                    // garbage only ever follows the last live record on the page (live records are contiguous
                    // from offset 0), so once the stride would leave the page there is nothing left to reclaim
                    // here — stop rather than dereference past the page bounds.
                    if (recordSize <= 0 || offset + recordSize > ringPageSizeBytes)
                        break;
                    offset += recordSize;
                }
            }

            requestFreed.Dispose();
            completionFreed.Dispose();
            outOfLinePayloadBudgetFreed.Dispose();
        }

        #region Utilities

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
            var local = tailPageOffset;
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
            localTailPageOffset.PageAndOffset = tailPageOffset.PageAndOffset;

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
                ? Interlocked.Add(ref tailPageOffset.PageAndOffset, size + (1L << PageOffset.kTaskOffset))
                : Interlocked.Add(ref tailPageOffset.PageAndOffset, size);

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
                    Interlocked.Exchange(ref tailPageOffset.PageAndOffset, localTailPageOffset.PageAndOffset);
                    return -1; // RETRY_LATER
                }

                localTailPageOffset.Page = pageIndex;
                localTailPageOffset.Offset = size;
                tailPageOffset = localTailPageOffset;
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
        public (int taskId, long address) TryAllocate(int size, bool expectsCompletion, out CompletionEvent waitEvent)
        {
            const int flushSpinCount = 10;
            var spins = 0;
            var completionReserved = false;
            while (true)
            {
                Debug.Assert(epoch.ThisInstanceProtected());

                // Reserve completion capacity before issuing the paired ticket. Capture the event first so a
                // concurrent release either wakes this snapshot or makes the reservation succeed.
                if (expectsCompletion && !completionReserved)
                {
                    waitEvent = completionFreed;
                    if (!TryReserveCompletion())
                        return (0, -1);
                    completionReserved = true;
                }

                waitEvent = requestFreed;
                var logicalAddress = TryAllocateInternal(size, out var taskId, expectsCompletion);

                if (logicalAddress >= 0)
                    return (taskId, logicalAddress);
                if (logicalAddress == -1)
                {
                    if (spins++ < flushSpinCount)
                    {
                        Thread.Yield();
                        continue;
                    }
                    if (completionReserved)
                        ReleaseCompletions(1);
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

        #endregion

        #region Request Implementation

        /// <summary>
        /// Maximum checked-out bytes permitted for out-of-line payloads. Zero means unlimited.
        /// </summary>
        internal long MaxOutOfLineBytesBudget => maxOutOfLineBytesBudget;

        /// <summary>
        /// Currently reserved out-of-line payload bytes.
        /// </summary>
        internal long OutOfLinePayloadBytes => Interlocked.Read(ref outOfLinePayloadBytes);

        /// <summary>
        /// Highest observed out-of-line payload reservation.
        /// </summary>
        internal long PeakOutOfLinePayloadBytes => Interlocked.Read(ref peakOutOfLinePayloadBytes);

        /// <summary>
        /// Try to reserve bytes before renting an out-of-line payload buffer.
        /// </summary>
        internal bool TryReserveOutOfLinePayloadBytes(int bytes, out CompletionEvent waitEvent)
        {
            Debug.Assert(bytes > 0);
            ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed), this);
            waitEvent = outOfLinePayloadBudgetFreed;

            if (maxOutOfLineBytesBudget == 0)
                return true;
            if (bytes > maxOutOfLineBytesBudget)
                return false;

            while (true)
            {
                var current = Volatile.Read(ref outOfLinePayloadBytes);
                if (current > maxOutOfLineBytesBudget - bytes)
                    return false;

                var next = current + bytes;
                if (Interlocked.CompareExchange(ref outOfLinePayloadBytes, next, current) != current)
                    continue;

                UpdatePeakOutOfLinePayloadBytes(next);
                return true;
            }
        }

        /// <inheritdoc />
        public void ReleaseOutOfLinePayloadBytes(int bytes)
        {
            if (maxOutOfLineBytesBudget == 0)
                return;

            Debug.Assert(bytes > 0);
            var remaining = Interlocked.Add(ref outOfLinePayloadBytes, -bytes);
            Debug.Assert(remaining >= 0);
            if (!Volatile.Read(ref disposed))
                outOfLinePayloadBudgetFreed.Set();
        }

        void UpdatePeakOutOfLinePayloadBytes(long value)
        {
            var peak = Volatile.Read(ref peakOutOfLinePayloadBytes);
            while (value > peak)
            {
                var observed = Interlocked.CompareExchange(ref peakOutOfLinePayloadBytes, value, peak);
                if (observed == peak)
                    return;
                peak = observed;
            }
        }

        /// <summary>
        /// Peek a record's framing and, when it is live, atomically claim it by setting the taken bit while
        /// preserving its metadata, arbitrating single ownership across the flusher
        /// (<see cref="AsyncFlushRequests"/>), <see cref="Dispose"/>, and the <see cref="RegisterRequest"/>
        /// post-publish recheck. Always outputs <paramref name="recordSize"/> — the stride to the next record —
        /// even when the slot is empty or already claimed, so a concurrent walker never loses its place. For an
        /// out-of-line record it also hands back the side-table request and its decoded log address in
        /// <paramref name="key"/> so callers can validate key == address; inline records carry no side-table
        /// entry (their payload lives in the page). Returns true only for the caller that transitions the
        /// record from live to claimed.
        /// <para>
        /// Because the claim only sets the taken bit (never erasing the length), an inline record still yields
        /// its size after being claimed. An empty/Uninitialized slot (the <c>0xFF</c> page fill, or an
        /// allocated-but-unpublished 8-byte out-of-line descriptor) strides by one descriptor; an inline
        /// reservation always writes its header before its payload, so an empty header never hides inline
        /// payload bytes.
        /// </para>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        unsafe bool TryClaimRequest(long address, long* recPtr, out RequestKind kind, out int recordSize, out int payloadLength, out long key, out TRequest request)
        {
            request = default;
            payloadLength = 0;
            var word = Volatile.Read(ref *recPtr);
            var tag = (byte)((ulong)word >> TagShift);

            if (tag == (byte)RequestKind.Uninitialized)
            {
                kind = RequestKind.Uninitialized;
                recordSize = RingDescriptorSize;
                key = default;
                return false;
            }

            key = DecodeMeta(word);
            if ((tag & (byte)RequestKind.Inline) != 0)
            {
                kind = RequestKind.Inline;
                payloadLength = (int)key;
                recordSize = InlineRecordSize(payloadLength);
            }
            else
            {
                kind = RequestKind.OutOfLine;
                recordSize = RingDescriptorSize;
            }

            // Already claimed by another walker; the record size was still recovered above.
            if ((word & TakenBit) != 0)
                return false;

            // Win the record by flipping it to claimed while preserving its metadata.
            if (Interlocked.CompareExchange(ref *recPtr, word | TakenBit, word) != word)
                return false;

            if (kind == RequestKind.OutOfLine)
            {
                var slot = ComputeSlot(address);
                request = requests[slot];
                requests[slot] = default;
            }
            return true;
        }

        /// <summary>
        /// Register (store and publish) a request request at the descriptor address, making it eligible for
        /// flushing. Publication writes the descriptor's self-key into page memory; the producing thread must
        /// hold the epoch, so the flusher (which validates key == address only after the epoch barrier) never
        /// observes a partially-written slot.
        /// <para>
        /// The word is tagged <see cref="RequestKind.OutOfLine"/> with the log address in its low 56
        /// bits; since that tag is <c>0x00</c> the published word is byte-identical to the raw address and can
        /// never collide with the <see cref="UninitializedDescriptor"/> empty sentinel.
        /// </para>
        /// </summary>
        /// <param name="address"></param>
        /// <param name="request"></param>
        internal unsafe void RegisterRequest(long address, TRequest request)
        {
            Debug.Assert(epoch.ThisInstanceProtected());
            ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed), this);
            var slot = ComputeSlot(address);
            requests[slot] = request;
            // Publish request to the consumer. Tagged OutOfLine, so the low 56 bits hold the log address and
            // the word is byte-identical to a raw address (OutOfLine == 0x00).
            var ptr = (long*)GetPhysicalAddress(address);
            *ptr = EncodeDescriptor(RequestKind.OutOfLine, address);

            // If Dispose swept this slot before publication, its reclaim pass has already run and will not
            // revisit the slot. Re-check and reclaim here via the same atomic claim so the just-published
            // request cannot leak.
            if (Volatile.Read(ref disposed) &&
                TryClaimRequest(address, ptr, out var kind, out _, out _, out _, out var reclaimed) &&
                kind == RequestKind.OutOfLine)
                reclaimed.Dispose();
        }

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
        /// (<see cref="AsyncFlushRequests"/>) runs only as the drain-list action queued by
        /// <see cref="LightEpoch.BumpCurrentEpoch(System.Action)"/> when read-only shifts (see
        /// <c>AggressiveFlushShiftReadOnlyBump</c>), so it cannot read this record until every producer that
        /// held the epoch at shift time has drained. Unlike out-of-line records — whose payload lives in a pool
        /// buffer written in full before the descriptor is published — inline has no data-ordering backstop, so
        /// <b>the caller MUST hold the epoch continuously from the tail-advancing <see cref="TryAllocate"/>
        /// through the completed payload write</b> (no <c>Suspend</c>, no <c>await</c> in between). Releasing
        /// the epoch mid-write would let a concurrent flush observe this header with an unwritten payload.
        /// </para>
        /// </summary>
        /// <param name="address">Descriptor address returned by <see cref="TryAllocate"/> for an
        /// <see cref="InlineRecordSize"/>-sized allocation.</param>
        /// <param name="payloadLength">Number of payload bytes the caller will write.</param>
        /// <returns>Pointer to the first payload byte (immediately after the 8-byte header).</returns>
        internal unsafe byte* ReserveInlineRecord(long address, int payloadLength)
        {
            Debug.Assert(epoch.ThisInstanceProtected());
            ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed), this);
            var basePtr = GetPhysicalAddress(address);
            Volatile.Write(ref *(long*)basePtr, EncodeDescriptor(RequestKind.Inline, payloadLength));
            Interlocked.MemoryBarrier();
            return (byte*)(basePtr + InlineHeaderSize);
        }

        #endregion

        #region Completion Implementation

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

        bool TryReserveCompletion()
        {
            if (Interlocked.Increment(ref completionReservations) <= completionCapacity)
                return true;

            _ = Interlocked.Decrement(ref completionReservations);
            return false;
        }

        void ReleaseCompletions(int count)
        {
            _ = Interlocked.Add(ref completionReservations, -count);
            completionFreed.Set();
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
            ReleaseCompletions(consumedCount);
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
            var disposedBail = false;

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
                    FailOnRequestFlush(ref flushFailed, $"Out-of-line flush range {flushPage}:{startOffset}-{realEndOffset} is not aligned to {RingDescriptorSize}-byte records.");
                    realEndOffset -= (realEndOffset - startOffset) & (RingDescriptorSize - 1);
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

                    var address = (flushPage << pageSizeBits) | (uint)offset;
                    var ptr = page.pointer + offset;

                    // Claim the record, recovering its stride whether or not we win the claim.
                    var won = TryClaimRequest(address, (long*)ptr, out var kind, out var recordSize, out var payloadLength, out var key, out var request);
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

                    if (kind == RequestKind.OutOfLine)
                        ProcessRequestChunks(request.Buffer, 0, request.Length, request, count, ref flushFailed);
                    else if (payloadLength > 0)
                        ProcessRequestChunks(page.value, (int)(offset + InlineHeaderSize), payloadLength, default, count, ref flushFailed);

                    offset += recordSize;
                }

                if (disposedBail || flushPage == endPage) break;
                flushPage = (flushPage + 1) & PageOffset.kPageMask;
            }

            CompleteFlush(count);

            void ProcessRequestChunks(byte[] buffer, int baseOffset, int length, TRequest request, CountWrapper count, ref bool flushFailed)
            {
                var chunkSize = Math.Max(1, maxChunkSizeBytes);
                var chunkCount = ((length - 1) / chunkSize) + 1;
                var result = new LightRequestAsyncFlushResult<TRequest>
                {
                    count = count,
                    request = request,
                    remainingChunks = chunkCount,
                    sink = this
                };
                _ = Interlocked.Increment(ref count.count);

                var dispatchedChunks = 0;
                try
                {
                    for (var offset = 0; offset < length; offset += chunkSize)
                    {
                        var chunkLength = Math.Min(chunkSize, length - offset);
                        operateOnRequest(buffer, baseOffset + offset, chunkLength, result);
                        dispatchedChunks++;
                    }
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "Exception sending a request");
                    flushFailed = true;
                    onFlushError(ex);

                    // Undispatched chunks will never get a network completion, so account for them here via
                    // the same completion routine the network callback uses.
                    for (var i = dispatchedChunks; i < chunkCount; i++)
                        LightRequestAsyncFlushResult<TRequest>.CompleteChunk(result);
                }
            }

            void FailOnRequestFlush(ref bool flushFailed, string message)
            {
                if (flushFailed)
                    return;

                flushFailed = true;
                logger?.LogError("{Message}", message);
                onFlushError(new InvalidOperationException(message));
            }

            long GetOffsetInPage(long address) => address & pageSizeMask;
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

        public void DoAggressiveShiftReadOnly()
        {
            if (ongoingAggressiveShiftReadOnly == 0 && Interlocked.CompareExchange(ref ongoingAggressiveShiftReadOnly, 1, 0) == 0)
                AggressiveShiftReadOnlyRunner(false);
        }

        void AggressiveShiftReadOnlyRunner(bool recurse)
        {
            do
            {
                if (ToShift())
                {
                    if (recurse)
                    {
                        _ = Task.Run(EpochProtectAggressiveShiftReadOnlyRunner);
                        return;
                    }
                    else
                    {
                        if (AggressiveFlushShiftReadOnlyBump()) return;
                    }
                }
                ongoingAggressiveShiftReadOnly = 0;
            } while (ToShift() && ongoingAggressiveShiftReadOnly == 0 && Interlocked.CompareExchange(ref ongoingAggressiveShiftReadOnly, 1, 0) == 0);

            bool ToShift()
            {
                var tailAddress = GetTailAddress();
                return tailAddress > readOnlyAddress || (readOnlyAddress - tailAddress > wrapDistance);
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
        }
    }
}