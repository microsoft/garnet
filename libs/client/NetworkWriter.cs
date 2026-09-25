// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Net.Security;
using System.Net.Sockets;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace Garnet.client
{
    struct Payload : IDisposable
    {
        internal PoolEntry Entry;
        internal int Length;

        internal byte[] Buffer => Entry.entry;

        internal Payload(PoolEntry entry, int length)
        {
            this.Entry = entry;
            this.Length = length;
        }

        public void Dispose() => Entry?.Dispose();
    }

    [StructLayout(LayoutKind.Explicit)]
    struct FullPageStatus
    {
        [FieldOffset(0)]
        public long LastFlushedUntilAddress;
        [FieldOffset(8)]
        public long LastClosedUntilAddress;
    }

    unsafe struct Page
    {
        public readonly byte[] value;
        public readonly long pointer;
        public FullPageStatus PageStatusIndicator;
        public long lastOffset;

        public Page(int PageSize)
        {
            value = GC.AllocateArray<byte>(PageSize, true);
            pointer = (long)Unsafe.AsPointer(ref value[0]);
            PageStatusIndicator = default;
            lastOffset = 0;
        }
    }

    /// <summary>
    /// Concurrent network writer
    /// </summary>
    internal sealed class NetworkWriter : IDisposable
    {
        /// <summary>
        /// Size of descriptor used for chunked send.
        /// </summary>
        const int PayloadDescriptorSize = sizeof(long);
        const long PageWrapDistance = 1L << (PageOffset.kPageBits - 1);

        public readonly LightEpoch epoch;

        // Circular buffer definition
        readonly Page[] values;
        readonly ILogger logger;
        readonly int BufferSize = 2, LogPageSizeBits, PageSizeMask;
        internal readonly int PageSize;
        readonly long WrapDistance;

        PageOffset TailPageOffset;
        long FlushedUntilAddress, ReadOnlyAddress;
        int _ongoingAggressiveShiftReadOnly;

        readonly INetworkSender networkSender;

        /// <summary>
        /// Whether log is disposed
        /// </summary>
        bool disposed = false;

        /// <summary>
        /// The "event" to be waited on for flush completion by the initiator of an operation
        /// </summary>
        CompletionEvent FlushEvent;

        readonly NetworkBufferSettings networkBufferSettings;
        readonly LimitedFixedBufferPool networkPool;
        readonly GarnetClientTcpNetworkHandler networkHandler;
        readonly bool useOutOfLineExecution;

        // Each physical descriptor maps to one slot; the entry reference transfers ownership atomically.
        readonly Payload[] pendingNetworkSendPayloads;

        /// <summary>
        /// Constructor
        /// </summary>
        public NetworkWriter(GarnetClient serverHook, Socket socket, int messageBufferSize, SslClientAuthenticationOptions sslOptions, out GarnetClientTcpNetworkHandler networkHandler, int sendPageSize, int networkSendThrottleMax, LightEpoch epoch, PoolOwnerType ownerType, bool useOutOfLineExecution, ILogger logger = null)
        {
            this.networkBufferSettings = new NetworkBufferSettings(messageBufferSize, messageBufferSize);
            this.networkPool = networkBufferSettings.CreateBufferPool(ownerType: ownerType, logger: logger);

            if (BufferSize > PageOffset.kPageMask) throw new Exception();
            this.networkHandler = networkHandler = new GarnetClientTcpNetworkHandler(serverHook, AsyncFlushPageCallback, socket, networkBufferSettings, networkPool, sslOptions != null, serverHook, networkSendThrottleMax: networkSendThrottleMax, logger: logger);
            networkSender = networkHandler.GetNetworkSender();

            FlushEvent.Initialize();
            this.epoch = epoch;
            this.PageSize = sendPageSize;
            this.logger = logger;
            this.useOutOfLineExecution = useOutOfLineExecution;
            if (useOutOfLineExecution)
            {
                var payloadSlotCount = BufferSize * sendPageSize / PayloadDescriptorSize;
                this.pendingNetworkSendPayloads = new Payload[payloadSlotCount];
            }
            this.LogPageSizeBits = Utility.NumBitsPreviousPowerOf2(sendPageSize);
            this.WrapDistance = PageWrapDistance << LogPageSizeBits;

            PageSizeMask = sendPageSize - 1;
            values = new Page[BufferSize];

            for (int i = 0; i < BufferSize; i++)
            {
                values[i] = new Page(this.PageSize);
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Volatile.Write(ref disposed, true);
            if (pendingNetworkSendPayloads != null)
            {
                for (var i = 0; i < pendingNetworkSendPayloads.Length; i++)
                {
                    var entry = Interlocked.Exchange(ref pendingNetworkSendPayloads[i].Entry, null);
                    entry?.Dispose();
                }
            }
            FlushEvent.Dispose();
            networkHandler.Dispose();
            networkPool?.Dispose();
        }

        internal Payload RentPayloadBuffer(int length)
        {
            var allocationSize = length;
            if (length <= networkPool.MaxAllocationSize)
            {
                var minimumSize = Math.Max(length, networkPool.MinAllocationSize);
                var roundedSize = BitOperations.RoundUpToPowerOf2((uint)minimumSize);
                if (roundedSize <= (uint)networkPool.MaxAllocationSize)
                    allocationSize = (int)roundedSize;
            }

            var entry = networkPool.Get(allocationSize, PoolEntryBufferType.OutOfLinePayload);
            ObjectDisposedException.ThrowIf(entry is null, this);
            return new(entry, length);
        }

        internal void EnqueuePayload(long address, Payload payload)
        {
            Debug.Assert(epoch.ThisInstanceProtected());
            ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed), this);

            var slot = GetPayloadSlot(address);
            // Ring reuse waits for flush to clear this slot and advance FlushedUntilAddress, so the new address maps to an empty slot.
            if (Interlocked.CompareExchange(ref pendingNetworkSendPayloads[slot].Entry, payload.Entry, null) != null)
                throw new InvalidOperationException($"An out-of-line payload is already registered at address {address}.");

            Volatile.Write(ref pendingNetworkSendPayloads[slot].Length, payload.Length);

            if (Volatile.Read(ref disposed))
            {
                var entry = Interlocked.Exchange(ref pendingNetworkSendPayloads[slot].Entry, null);
                entry?.Dispose();
            }
        }

        internal bool DequeuePayload(long address, out Payload payload)
        {
            var slot = GetPayloadSlot(address);
            var entry = Interlocked.Exchange(ref pendingNetworkSendPayloads[slot].Entry, null);
            if (entry == null)
            {
                payload = default;
                return false;
            }

            payload = new(entry, Volatile.Read(ref pendingNetworkSendPayloads[slot].Length));
            Volatile.Write(ref pendingNetworkSendPayloads[slot].Length, 0);
            return true;
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
        /// <returns></returns>
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

        /// <summary>
        /// Get next unassigned task ID
        /// </summary>
        /// <returns></returns>
        public int GetNextTaskId() => TailPageOffset.TaskId;

        /// <summary>
        /// Try allocate, no thread spinning allowed
        /// </summary>
        /// <param name="size">Number of bytes to allocate</param>
        /// <param name="taskId"></param>
        /// <param name="skipTaskIdIncrement"></param>
        /// <returns>The allocated logical address, or negative in case of inability to allocate</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long TryAllocate(int size, out int taskId, bool skipTaskIdIncrement = false)
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

            // Determine insertion index.
            localTailPageOffset.PageAndOffset = skipTaskIdIncrement ? Interlocked.Add(ref TailPageOffset.PageAndOffset, size) : Interlocked.Add(ref TailPageOffset.PageAndOffset, size + (1L << PageOffset.kTaskOffset));

            taskId = localTailPageOffset.PrevTaskId;
            int page = localTailPageOffset.Page;
            int offset = localTailPageOffset.Offset - size;
            // Console.WriteLine($"temp: {taskId}");
            #region HANDLE PAGE OVERFLOW
            if (localTailPageOffset.Offset > PageSize)
            {
                int pageIndex = (localTailPageOffset.Page + 1) & (int)PageOffset.kPageMask;

                // Non-responsible overflow threads back off
                if (offset > PageSize)
                {
                    if (NeedToWait(pageIndex))
                        return -1; // RETRY_LATER
                    return -2; // RETRY_NOW
                }
                // Console.WriteLine($"Reset task is {localTailPageOffset.TaskId}");

                //Console.WriteLine($"Setting lastOffset for page {page} to {offset}");

                if (offset < PageSize)
                {
                    Debug.Assert(values[page % BufferSize].lastOffset == 0);
                    values[page % BufferSize].lastOffset = offset;
                }

                // Responsible overflow thread tries to shift address
                DoAggressiveShiftReadOnly();

                if (NeedToWait(pageIndex))
                {
                    // Reset to end of page so that next attempt can retry
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
        }

        /// <summary>
        /// Async wrapper for TryAllocate
        /// </summary>
        /// <param name="size">Number of slots to allocate</param>
        /// <param name="flushEvent"></param>
        /// <param name="skipTaskIdIncrement"></param>
        /// <returns>The allocated logical address</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (int, long) TryAllocate(int size, out CompletionEvent flushEvent, bool skipTaskIdIncrement = false)
        {
            const int kFlushSpinCount = 10;
            var spins = 0;
            while (true)
            {
                Debug.Assert(epoch.ThisInstanceProtected());
                flushEvent = this.FlushEvent;
                var logicalAddress = this.TryAllocate(size, out int taskId, skipTaskIdIncrement: skipTaskIdIncrement);
                // Console.WriteLine($"Allocated {logicalAddress}-{logicalAddress + size}");

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
        /// Get physical address
        /// </summary>
        /// <param name="logicalAddress"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPhysicalAddress(long logicalAddress)
        {
            // Offset within page
            int offset = (int)(logicalAddress & ((1L << LogPageSizeBits) - 1));

            // Index of page within the circular buffer
            int pageIndex = (int)((logicalAddress >> LogPageSizeBits) & (BufferSize - 1));
            return values[pageIndex].pointer + offset;
        }

        bool NeedToWait(int page)
        {
            int limit = (BufferSize + (int)(FlushedUntilAddress >> LogPageSizeBits)) & (int)PageOffset.kPageMask;
            return page >= limit && (page - limit < PageWrapDistance);
        }

        void OnPagesMarkedReadOnly(long oldReadOnlyAddress, long newReadOnlyAddress)
        {
            if (useOutOfLineExecution)
            {
                AsyncFlushPayloads(oldReadOnlyAddress, newReadOnlyAddress);
            }
            else
            {
                AsyncFlushPages(oldReadOnlyAddress, newReadOnlyAddress);
            }
        }

        /// <summary>
        /// Flush page range to disk
        /// Called when all threads have agreed that a page range is sealed.
        /// </summary>
        /// <param name="fromAddress"></param>
        /// <param name="untilAddress"></param>
        public void AsyncFlushPages(long fromAddress, long untilAddress)
        {
            Debug.Assert(!useOutOfLineExecution, "Inline page flushing cannot be used in out-of-line mode.");
            long startPage = fromAddress >> LogPageSizeBits;
            long endPage = untilAddress >> LogPageSizeBits;

            var count = new CountWrapper
            {
                count = (int)(endPage - startPage + 1),
                untilAddress = untilAddress
            };
            // Handle wrap-around of count
            if (count.count < 0) count.count = (int)PageOffset.kPageMask + 1 + count.count;

            /* Request asynchronous writes to the device. If waitForPendingFlushComplete
             * is set, then a CountDownEvent is set in the callback handle.
             */
            long flushPage = startPage;
            while (true)
            {
                long startOffset = 0, endOffset = 1L << LogPageSizeBits;
                if (flushPage == startPage) startOffset = GetOffsetInPage(fromAddress);
                if (flushPage == endPage) endOffset = GetOffsetInPage(untilAddress);

                var asyncResult = new PageAsyncFlushResult
                {
                    page = flushPage,
                    count = count,
                    fromOffset = startOffset,
                    untilOffset = endOffset
                };

                long realEndOffset = endOffset;
                if (values[flushPage % BufferSize].lastOffset > 0 && endOffset > values[flushPage % BufferSize].lastOffset)
                {
                    realEndOffset = values[flushPage % BufferSize].lastOffset;
                    //Console.WriteLine($"Setting lastOffset for page {flushPage} to 0, real: {realEndOffset}");
                    values[flushPage % BufferSize].lastOffset = 0;
                }

                // Console.WriteLine($"Flushing page {flushPage}; offset {startOffset}-{endOffset}; lastOffset: {values[flushPage % BufferSize].lastOffset}; realEndOffset: {realEndOffset})");

                if (startOffset >= realEndOffset)
                {
                    AsyncFlushPageCallback(asyncResult);
                }
                else
                {
                    //Console.WriteLine($"Sending {flushPage}: {startOffset} -- {(int)(realEndOffset - startOffset)}");
                    try
                    {
                        networkSender.SendResponse(values[flushPage % BufferSize].value, (int)startOffset, (int)(realEndOffset - startOffset), asyncResult);
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "Exception calling networkSender.SendResponse in AsyncFlushPages");
                        networkHandler.Dispose();
                        // Still signal completion for this page so FlushEvent waiters (e.g. GarnetClient
                        // callers blocked in InternalExecuteNoResponse) are not left hanging indefinitely.
                        AsyncFlushPageCallback(asyncResult);
                    }
                }
                if (flushPage == endPage) break;
                flushPage = (flushPage + 1) & PageOffset.kPageMask;
            }
        }

        /// <summary>
        /// Flush an address range containing out-of-line payload keys
        /// </summary>
        /// <param name="fromAddress">Start address</param>
        /// <param name="untilAddress">End address</param>
        public unsafe void AsyncFlushPayloads(long fromAddress, long untilAddress)
        {
            Debug.Assert(useOutOfLineExecution, "Out-of-line page flushing requires out-of-line mode.");
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
                    var key = *(long*)(page.pointer + offset);

                    if (key != address)
                    {
                        FailOnPayloadFlush(ref flushFailed, $"Out-of-line payload key {key} does not match its log address {address}.");
                        if (DequeuePayload(address, out var mismatchedPayload))
                            mismatchedPayload.Dispose();
                        continue;
                    }

                    if (!DequeuePayload(key, out var payload))
                    {
                        FailOnPayloadFlush(ref flushFailed, $"No out-of-line payload is registered for address {address}.");
                        continue;
                    }

                    if (flushFailed)
                    {
                        payload.Dispose();
                        continue;
                    }

                    SendPayload(ref payload, count, ref flushFailed);
                }

                if (flushPage == endPage) break;
                flushPage = (flushPage + 1) & PageOffset.kPageMask;
            }

            CompleteFlush(count);

            void SendPayload(ref Payload payload, CountWrapper count, ref bool flushFailed)
            {
                var chunkSize = Math.Max(1, networkBufferSettings.sendBufferSize);
                var chunkCount = ((payload.Length - 1) / chunkSize) + 1;
                var result = new PayloadAsyncFlushResult
                {
                    count = count,
                    payload = payload,
                    remainingChunks = chunkCount
                };
                _ = Interlocked.Increment(ref count.count);

                var dispatchedChunks = 0;
                try
                {
                    for (var offset = 0; offset < payload.Length; offset += chunkSize)
                    {
                        var length = Math.Min(chunkSize, payload.Length - offset);
                        networkSender.SendResponse(payload.Buffer, offset, length, result);
                        dispatchedChunks++;
                    }
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "Exception sending an out-of-line payload");
                    flushFailed = true;
                    networkHandler.Dispose();

                    for (var i = dispatchedChunks; i < chunkCount; i++)
                        AsyncFlushPageCallback(result);
                }
            }

            void FailOnPayloadFlush(ref bool flushFailed, string message)
            {
                if (flushFailed)
                    return;

                flushFailed = true;
                logger?.LogError("{Message}", message);
                networkHandler.Dispose();
            }
        }

        /// <summary>
        /// Completion callback for network flush
        /// </summary>
        /// <param name="context"></param>
        private void AsyncFlushPageCallback(object context)
        {
            switch (context)
            {
                case PageAsyncFlushResult pageResult:
                    CompleteFlush(pageResult.count);
                    break;

                case PayloadAsyncFlushResult payloadResult:
                    if (Interlocked.Decrement(ref payloadResult.remainingChunks) == 0)
                    {
                        payloadResult.payload.Dispose();
                        CompleteFlush(payloadResult.count);
                    }
                    break;

                default:
                    Debug.Fail($"Unexpected network flush context type {context?.GetType().FullName ?? "null"}.");
                    break;
            }
        }

        void CompleteFlush(CountWrapper count)
        {
            try
            {
                if (Interlocked.Decrement(ref count.count) == 0)
                {
                    long endAddress = count.untilAddress;
                    if (Utility.MonotonicUpdate(ref FlushedUntilAddress, endAddress, WrapDistance, out _))
                    {
                        FlushEvent.Set();
                    }
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
            long tailAddress = GetTailAddress();
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
            long newReadOnlyAddress = GetTailAddress();
            if (Utility.MonotonicUpdate(ref ReadOnlyAddress, newReadOnlyAddress, WrapDistance, out long oldReadOnlyAddress))
            {
                epoch.BumpCurrentEpoch(() => OnPagesMarkedReadOnly(oldReadOnlyAddress, newReadOnlyAddress));
                return true;
            }
            return false;
        }
    }
}