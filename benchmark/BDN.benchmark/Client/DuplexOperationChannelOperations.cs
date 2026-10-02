// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Garnet.client;
using Garnet.common;

namespace BDN.benchmark.Client
{
    /// <summary>
    /// Exercises the complete single-operation lifecycle of the duplex channel without a socket.
    /// The transport records the request bytes and defers send completion until the benchmark
    /// explicitly drains it, after which the completion lane is consumed like ProcessReplies.
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class DuplexOperationChannelOperations
    {
        const int RingPageSize = 64;
        const int RingPageCount = 8;
        const int CompletionCapacity = 32;
        const int MaxChunkSize = 256;
        const int InlinePayloadLength = 32;
        const int OutOfLinePayloadLength = 64;

        readonly struct BenchmarkCompletion
        {
            internal readonly int Sequence;

            internal BenchmarkCompletion(int sequence)
            {
                Sequence = sequence;
            }
        }

        readonly struct NoOpTransport : ITransportContext
        {
            readonly TransportState state;

            internal NoOpTransport(TransportState state)
            {
                this.state = state;
            }

            public void Send(byte[] buffer, int offset, int length, object context)
                => state.ProcessRequest(buffer, offset, length, context);

            public void OnFlushError(Exception exception)
                => state.RecordFlushError(exception);
        }

        sealed class TransportState
        {
            object pendingFlush;
            long checksum;
            int missingFlushCompletions;
            int overlappingFlushes;
            int flushErrors;
            int modeErrors;
            string firstFlushError;

            internal long Checksum => Volatile.Read(ref checksum);
            internal int Errors => Volatile.Read(ref missingFlushCompletions) +
                Volatile.Read(ref overlappingFlushes) +
                Volatile.Read(ref flushErrors) +
                Volatile.Read(ref modeErrors);

            internal void ProcessRequest(byte[] buffer, int offset, int length, object context)
            {
                long next = 17;
                var end = offset + length;
                for (var i = offset; i < end; i++)
                    next = unchecked((next * 31) + buffer[i]);
                Volatile.Write(ref checksum, next);

                if (Interlocked.CompareExchange(ref pendingFlush, context, null) != null)
                    Interlocked.Increment(ref overlappingFlushes);
            }

            internal void CompletePendingFlush()
            {
                var context = Interlocked.Exchange(ref pendingFlush, null);
                if (context == null)
                {
                    Interlocked.Increment(ref missingFlushCompletions);
                    return;
                }

                DuplexOperationAsyncFlushResult<LightRequestContext>.CompleteChunk(context);
            }

            internal void RecordFlushError(Exception exception)
            {
                _ = Interlocked.CompareExchange(ref firstFlushError, exception?.Message ?? "Unknown flush error", null);
                Interlocked.Increment(ref flushErrors);
            }

            internal void RecordModeError() => Interlocked.Increment(ref modeErrors);

            internal string GetErrorSummary()
                => $"missing={missingFlushCompletions}, overlapping={overlappingFlushes}, flush={flushErrors}, mode={modeErrors}, firstFlushError={firstFlushError}";
        }

        LightEpoch epoch;
        LimitedFixedBufferPool requestPool;
        TransportState transportState;
        DuplexOperationChannel<LightRequestContext, BenchmarkCompletion, NoOpTransport> channel;
        byte[] inlineRequest;
        byte[] outOfLineRequest;
        int nextCompletion;

        /// <summary>
        /// Creates a small channel whose inline threshold cleanly separates the two benchmark payloads.
        /// </summary>
        [GlobalSetup]
        public void GlobalSetup()
        {
            epoch = new LightEpoch();
            requestPool = new LimitedFixedBufferPool(
                minAllocationSize: RingPageSize,
                maxEntriesPerLevel: 4,
                numLevels: 4,
                ownerType: PoolOwnerType.GarnetClient);
            transportState = new TransportState();
            channel = new DuplexOperationChannel<LightRequestContext, BenchmarkCompletion, NoOpTransport>(
                RingPageSize,
                RingPageCount,
                CompletionCapacity,
                MaxChunkSize,
                new NoOpTransport(transportState),
                epoch);

            inlineRequest = CreateRequest(InlinePayloadLength);
            outOfLineRequest = CreateRequest(OutOfLinePayloadLength);
            PrewarmRequestPool();

            _ = channel.GetRecordSize(inlineRequest.Length, out var inline);
            if (!inline)
                transportState.RecordModeError();
            _ = channel.GetRecordSize(outOfLineRequest.Length, out inline);
            if (inline)
                transportState.RecordModeError();
        }

        /// <summary>
        /// Disposes the channel and its supporting resources. Errors are reported without throwing from
        /// the measured path.
        /// </summary>
        [GlobalCleanup]
        public void GlobalCleanup()
        {
            if (transportState.Errors != 0)
                Console.Error.WriteLine($"DuplexOperationChannel benchmark errors: {transportState.GetErrorSummary()}.");

            channel.Dispose();
            requestPool.Dispose();
            epoch.Dispose();
        }

        /// <summary>
        /// Schedules, writes, drains, and completes one inline request.
        /// </summary>
        [Benchmark]
        [BenchmarkCategory("Inline")]
        public long InlineOperation()
        {
            var recordSize = channel.GetRecordSize(inlineRequest.Length, out var inline);
            if (!inline)
                transportState.RecordModeError();

            return ExecuteOperation(recordSize, inlineRequest, inline: true, default);
        }

        /// <summary>
        /// Rents, writes, schedules, drains, and completes one out-of-line request.
        /// </summary>
        [Benchmark]
        [BenchmarkCategory("OutOfLine")]
        public long OutOfLineOperation()
        {
            var recordSize = channel.GetRecordSize(outOfLineRequest.Length, out var inline);
            if (inline)
                transportState.RecordModeError();

            var request = LightRequestContext.RentRequestBuffer(requestPool, outOfLineRequest.Length);
            outOfLineRequest.AsSpan().CopyTo(request.Buffer);
            return ExecuteOperation(recordSize, outOfLineRequest, inline: false, request);
        }

        long ExecuteOperation(int recordSize, byte[] source, bool inline, LightRequestContext request)
        {
            var requestRegistered = false;
            var epochProtected = false;
            DuplexOperationReservation reservation;

            try
            {
                epoch.Resume();
                epochProtected = true;

                while (!channel.TryScheduleOperation(
                    recordSize,
                    expectsCompletion: true,
                    out reservation,
                    out _))
                {
                    epoch.ProtectAndDrain();
                    channel.DrainRequests();
                    epoch.Suspend();
                    epochProtected = false;
                    transportState.CompletePendingFlush();
                    epoch.Resume();
                    epochProtected = true;
                }

                var completion = new BenchmarkCompletion(Interlocked.Increment(ref nextCompletion));
                channel.RegisterCompletion(reservation.completionTicket, completion);

                if (inline)
                {
                    var destination = channel.RegisterInlineRecord(reservation.requestAddress, source.Length);
                    source.AsSpan().CopyTo(new Span<byte>(destination, source.Length));
                }
                else
                {
                    channel.RegisterOfflineRecord(reservation.requestAddress, request);
                    requestRegistered = true;
                }

                epoch.ProtectAndDrain();
                channel.DrainRequests();
            }
            finally
            {
                if (epochProtected)
                    epoch.Suspend();
                if (!inline && !requestRegistered)
                    request.Dispose();
            }

            transportState.CompletePendingFlush();

            if (!channel.TryReadCompletion(reservation.completionTicket, out var registeredCompletion))
            {
                transportState.RecordModeError();
                registeredCompletion = default;
            }
            channel.AdvanceCompletion(1);

            return transportState.Checksum + registeredCompletion.Sequence;
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
            var requests = new LightRequestContext[4];
            for (var i = 0; i < requests.Length; i++)
                requests[i] = LightRequestContext.RentRequestBuffer(requestPool, OutOfLinePayloadLength);
            for (var i = 0; i < requests.Length; i++)
                requests[i].Dispose();
        }
    }
}