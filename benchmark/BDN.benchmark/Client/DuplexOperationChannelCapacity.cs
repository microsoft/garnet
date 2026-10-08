// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Garnet.client;
using Garnet.common;

namespace BDN.benchmark.Client
{
    /// <summary>
    /// Measures full request-ring bursts while network send completions are deferred. When the request lane
    /// reaches capacity, the benchmark manually completes one send and pumps the ring before continuing,
    /// matching the production backpressure lifecycle without using a socket.
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class DuplexOperationChannelCapacity
    {
        const int MaxSendChunkSizeBytes = 4096;
        const int RequestPoolMinAllocationSize = 64;
        const int InlinePayloadLength = 32;
        const int InlineRecordSize = 40;
        const int OutOfLinePayloadLength = 4096;
        const int OutOfLineRecordSize = sizeof(long);

        readonly struct BenchmarkCompletion
        {
            internal readonly int Sequence;

            internal BenchmarkCompletion(int sequence)
            {
                Sequence = sequence;
            }
        }

        readonly struct DeferredTransport : ITransportContext
        {
            readonly DeferredTransportState state;

            internal DeferredTransport(DeferredTransportState state)
            {
                this.state = state;
            }

            public void Send(byte[] buffer, int offset, int length, object context)
                => state.ProcessRequest(buffer, offset, length, context);

            public void OnFlushError(Exception exception)
                => state.RecordFlushError(exception);
        }

        sealed class DeferredTransportState
        {
            readonly object[] pendingFlushes;

            long checksum;
            int pendingCount;
            int completedCount;
            int capacityErrors;
            int flushErrors;
            int classificationErrors;
            int completionErrors;
            string firstFlushError;

            internal long Checksum => Volatile.Read(ref checksum);
            internal int Errors => Volatile.Read(ref capacityErrors) +
            Volatile.Read(ref flushErrors) +
            Volatile.Read(ref classificationErrors) +
            Volatile.Read(ref completionErrors);

            internal DeferredTransportState(int requestCapacity)
            {
                pendingFlushes = new object[requestCapacity];
            }

            internal void ProcessRequest(byte[] buffer, int offset, int length, object context)
            {
                long next = 17;
                var end = offset + length;
                for (var i = offset; i < end; i++)
                    next = unchecked((next * 31) + buffer[i]);
                Interlocked.Add(ref checksum, next);

                var index = Interlocked.Increment(ref pendingCount) - 1;
                if ((uint)index >= (uint)pendingFlushes.Length)
                {
                    Interlocked.Increment(ref capacityErrors);
                    DuplexOperationAsyncFlushResult<LightRequestContext>.CompleteChunk(context);
                    return;
                }

                Volatile.Write(ref pendingFlushes[index], context);
            }

            internal bool TryCompleteOnePendingFlush()
            {
                var index = Volatile.Read(ref completedCount);
                if (index >= Volatile.Read(ref pendingCount))
                    return false;

                var context = Volatile.Read(ref pendingFlushes[index]);
                if (context == null)
                    return false;

                Volatile.Write(ref pendingFlushes[index], null);
                Volatile.Write(ref completedCount, index + 1);
                DuplexOperationAsyncFlushResult<LightRequestContext>.CompleteChunk(context);
                return true;
            }

            internal void ResetPendingFlushes(int expectedCount)
            {
                if (Volatile.Read(ref pendingCount) != expectedCount ||
                    Volatile.Read(ref completedCount) != expectedCount)
                    Interlocked.Increment(ref capacityErrors);

                Volatile.Write(ref pendingCount, 0);
                Volatile.Write(ref completedCount, 0);
            }

            internal void RecordFlushError(Exception exception)
            {
                _ = Interlocked.CompareExchange(ref firstFlushError, exception?.Message ?? "Unknown flush error", null);
                Interlocked.Increment(ref flushErrors);
            }

            internal void RecordClassificationError() => Interlocked.Increment(ref classificationErrors);

            internal void RecordCompletionError() => Interlocked.Increment(ref completionErrors);

            internal string GetErrorSummary()
                => $"capacity={capacityErrors}, flush={flushErrors}, classification={classificationErrors}, completion={completionErrors}, firstFlushError={firstFlushError}";
        }

        LightEpoch epoch;
        LimitedFixedBufferPool requestPool;
        DeferredTransportState transportState;
        DuplexOperationChannel<LightRequestContext, BenchmarkCompletion, DeferredTransport> channel;
        LightRequestContext[] prewarmedRequests;
        int[] completionTickets;
        byte[] inlineRequest;
        byte[] outOfLineRequest;
        int inlineRequestsPerBurst;
        int outOfLineRequestsPerBurst;
        int nextCompletion;

        /// <summary>
        /// Number of circular request pages in the measured ring.
        /// </summary>
        [Params(2, 4, 8)]
        public int RingPageCount { get; set; }

        /// <summary>
        /// Page sizes selected from the fixed-two-page sweep for the page-count comparison.
        /// </summary>
        [Params(1024, 2048)]
        public int RingPageSize { get; set; }

        /// <summary>
        /// Creates the configured channel and prewarms enough out-of-line buffers for one full-ring burst.
        /// </summary>
        [GlobalSetup]
        public void GlobalSetup()
        {
            inlineRequestsPerBurst = RingPageCount * (RingPageSize / InlineRecordSize);
            outOfLineRequestsPerBurst = RingPageCount * RingPageSize / OutOfLineRecordSize;

            epoch = new LightEpoch();
            requestPool = new LimitedFixedBufferPool(
                minAllocationSize: RequestPoolMinAllocationSize,
                maxEntriesPerLevel: outOfLineRequestsPerBurst,
                numLevels: 7,
                ownerType: PoolOwnerType.GarnetClient);
            transportState = new DeferredTransportState(outOfLineRequestsPerBurst);
            channel = new DuplexOperationChannel<LightRequestContext, BenchmarkCompletion, DeferredTransport>(
                RingPageSize,
                RingPageCount,
                outOfLineRequestsPerBurst,
                outOfLineRequestsPerBurst,
                MaxSendChunkSizeBytes,
                new DeferredTransport(transportState),
                epoch);

            inlineRequest = CreateRequest(InlinePayloadLength);
            outOfLineRequest = CreateRequest(OutOfLinePayloadLength);
            completionTickets = new int[outOfLineRequestsPerBurst];
            PrewarmRequestPool();
        }

        /// <summary>
        /// Disposes benchmark resources and reports lifecycle errors without throwing from the measured path.
        /// </summary>
        [GlobalCleanup]
        public void GlobalCleanup()
        {
            if (transportState.Errors != 0)
                Console.Error.WriteLine($"DuplexOperationChannel capacity benchmark errors: {transportState.GetErrorSummary()}.");

            channel.Dispose();
            requestPool.Dispose();
            epoch.Dispose();
        }

        /// <summary>
        /// Publishes one physical ring's worth of inline records under deferred-send backpressure.
        /// </summary>
        [Benchmark]
        [BenchmarkCategory("Capacity", "Inline")]
        public long InlineCapacityBurst()
        {
            var recordSize = channel.GetRecordSize(inlineRequest.Length, out var inline);
            if (!inline)
                transportState.RecordClassificationError();

            return ExecuteBurst(inlineRequestsPerBurst, recordSize, inlineRequest, inline: true);
        }

        /// <summary>
        /// Publishes one physical ring's worth of out-of-line descriptors under deferred-send backpressure.
        /// </summary>
        [Benchmark]
        [BenchmarkCategory("Capacity", "OutOfLine")]
        public long OutOfLineCapacityBurst()
        {
            var recordSize = channel.GetRecordSize(outOfLineRequest.Length, out var inline);
            if (inline)
                transportState.RecordClassificationError();

            return ExecuteBurst(outOfLineRequestsPerBurst, recordSize, outOfLineRequest, inline: false);
        }

        long ExecuteBurst(int requestCount, int recordSize, byte[] source, bool inline)
        {
            var scheduledCount = 0;
            var completedCount = 0;
            epoch.Resume();
            try
            {
                for (; scheduledCount < requestCount; scheduledCount++)
                {
                    DuplexOperationReservation reservation;
                    while (!channel.TryScheduleOperation(
                        recordSize,
                        expectsCompletion: true,
                        out reservation,
                        out _))
                    {
                        epoch.ProtectAndDrain();
                        channel.DrainRequests();
                        epoch.Suspend();

                        if (transportState.TryCompleteOnePendingFlush())
                            completedCount++;
                        else
                            Thread.Yield();

                        epoch.Resume();
                    }

                    completionTickets[scheduledCount] = reservation.completionTicket;
                    channel.RegisterCompletion(
                        reservation.completionTicket,
                        new BenchmarkCompletion(Interlocked.Increment(ref nextCompletion)));

                    if (inline)
                    {
                        var destination = channel.RegisterInlineRecord(reservation.requestAddress, source.Length);
                        source.AsSpan().CopyTo(new Span<byte>(destination, source.Length));
                    }
                    else
                    {
                        var request = LightRequestContext.RentRequestBuffer(requestPool, source.Length);
                        source.AsSpan().CopyTo(request.Buffer);
                        channel.RegisterOfflineRecord(reservation.requestAddress, request);
                    }

                }

                epoch.ProtectAndDrain();
                channel.DrainRequests();
            }
            finally
            {
                epoch.Suspend();
            }

            var spinner = new SpinWait();
            while (completedCount < scheduledCount)
            {
                epoch.Resume();
                try
                {
                    epoch.ProtectAndDrain();
                    channel.DrainRequests();
                }
                finally
                {
                    epoch.Suspend();
                }

                if (transportState.TryCompleteOnePendingFlush())
                    completedCount++;
                else
                    spinner.SpinOnce();
            }

            transportState.ResetPendingFlushes(scheduledCount);

            long result = transportState.Checksum;
            for (var i = 0; i < scheduledCount; i++)
            {
                if (!channel.TryReadCompletion(completionTickets[i], out var completion))
                {
                    transportState.RecordCompletionError();
                    continue;
                }

                result += completion.Sequence;
            }
            channel.AdvanceCompletion(scheduledCount);
            return result;
        }

        static byte[] CreateRequest(int length)
        {
            var request = new byte[length];
            for (var i = 0; i < request.Length; i++)
                request[i] = (byte)((i * 17) + 3);
            return request;
        }

        void PrewarmRequestPool()
        {
            prewarmedRequests = new LightRequestContext[outOfLineRequestsPerBurst];
            for (var i = 0; i < prewarmedRequests.Length; i++)
                prewarmedRequests[i] = LightRequestContext.RentRequestBuffer(requestPool, OutOfLinePayloadLength);
            for (var i = 0; i < prewarmedRequests.Length; i++)
                prewarmedRequests[i].Dispose();
        }
    }
}