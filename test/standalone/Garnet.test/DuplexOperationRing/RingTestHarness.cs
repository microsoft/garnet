// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// A request-lane payload used to drive
    /// <see cref="DuplexOperationChannel{TRequest, TCompletion, TTransport}"/> in
    /// isolation. It carries a plain byte buffer plus an identity used to verify that the ring transfers
    /// ownership to the sender exactly once: every non-default instance records each <see cref="Dispose"/> in a
    /// shared counter keyed by <see cref="id"/>, so both leaks (zero disposes) and double-disposes (two) are
    /// detectable. The <c>default</c> instance (used by inline records, which own no buffer) is a no-op.
    /// </summary>
    internal readonly struct TestRequest : IRequestContext
    {
        readonly byte[] buffer;
        readonly int length;
        readonly long id;
        readonly ConcurrentDictionary<long, int> disposeCounts;

        internal TestRequest(byte[] buffer, int length, long id, ConcurrentDictionary<long, int> disposeCounts)
        {
            this.buffer = buffer;
            this.length = length;
            this.id = id;
            this.disposeCounts = disposeCounts;
        }

        /// <inheritdoc />
        public byte[] Buffer => buffer;

        /// <inheritdoc />
        public int Length => length;

        /// <inheritdoc />
        public void Dispose()
        {
            if (disposeCounts != null)
                disposeCounts.AddOrUpdate(id, 1, (_, c) => c + 1);
        }
    }

    /// <summary>
    /// Deterministic variable-size payload codec. Each payload embeds its own id, length and a checksum plus an
    /// id-derived body pattern, so a reassembled buffer can be validated for loss, duplication, truncation and
    /// cross-record contamination regardless of the order in which the ring flushed it.
    /// </summary>
    internal static class RingPayload
    {
        internal const int HeaderSize = 16; // id(8) + length(4) + crc(4)

        /// <summary>
        /// Create a self-describing payload formatted as [id:8][length:4][checksum:4][id-derived body],
        /// allowing the reassembled bytes to be identified and validated without retaining the original buffer.
        /// </summary>
        internal static byte[] Create(long id, int length)
        {
            if (length < HeaderSize) length = HeaderSize;
            var b = new byte[length];
            BinaryPrimitives.WriteInt64LittleEndian(b, id);
            BinaryPrimitives.WriteInt32LittleEndian(b.AsSpan(8), length);
            var seed = unchecked((byte)((id * 31) + 7));
            for (var i = HeaderSize; i < length; i++)
                b[i] = unchecked((byte)(seed + i));
            BinaryPrimitives.WriteInt32LittleEndian(b.AsSpan(12), Checksum(b.AsSpan(HeaderSize)));
            return b;
        }

        /// <summary>Validate a reassembled payload; returns the decoded id and whether it is intact.</summary>
        internal static (long id, bool ok) Verify(byte[] b)
        {
            if (b.Length < HeaderSize)
                return (-1, false);
            var id = BinaryPrimitives.ReadInt64LittleEndian(b);
            var len = BinaryPrimitives.ReadInt32LittleEndian(b.AsSpan(8));
            if (len != b.Length)
                return (id, false);
            var crc = BinaryPrimitives.ReadInt32LittleEndian(b.AsSpan(12));
            var seed = unchecked((byte)((id * 31) + 7));
            for (var i = HeaderSize; i < len; i++)
            {
                if (b[i] != unchecked((byte)(seed + i)))
                    return (id, false);
            }
            return (id, crc == Checksum(b.AsSpan(HeaderSize)));
        }

        static int Checksum(ReadOnlySpan<byte> s)
        {
            unchecked
            {
                var h = 17;
                foreach (var x in s)
                    h = (h * 31) + x;
                return h;
            }
        }
    }

    /// <summary>
    /// Test harness that drives a
    /// <see cref="DuplexOperationChannel{TRequest, TCompletion, TTransport}"/> without a socket.
    /// It supplies a fake transport that reassembles each request's chunks (keyed by the ring's per-request
    /// flush-result context) into a completed payload, tracks buffer disposals for leak/double-dispose
    /// detection, and can inject send failures. Producers run the same allocate/register/drain dance the real
    /// client uses, choosing inline vs out-of-line automatically via
    /// <see cref="DuplexOperationChannel{TRequest, TCompletion, TTransport}.GetRecordSize"/>.
    /// </summary>
    internal sealed class RingTestHarness : IDisposable
    {
        internal readonly struct RingTransport : ITransportContext
        {
            readonly RingTestHarness owner;

            internal RingTransport(RingTestHarness owner)
            {
                this.owner = owner;
            }

            public void Send(byte[] buffer, int offset, int length, object context)
                => owner.Send(buffer, offset, length, context);

            public void OnFlushError(Exception exception)
                => owner.OnFlushError(exception);
        }

        internal readonly LightEpoch epoch;
        internal readonly DuplexOperationChannel<TestRequest, int, RingTransport> Ring;

        readonly ConcurrentDictionary<object, List<byte>> reassembly = new();
        readonly ConcurrentBag<byte[]> completed = new();
        int completedCount;

        readonly ConcurrentDictionary<long, int> disposeCounts = new();
        readonly ConcurrentDictionary<long, byte> createdOutOfLineIds = new();
        long nextRequestId;

        readonly ConcurrentQueue<Exception> flushErrors = new();
        readonly ConcurrentQueue<object> deferredCompletions = new();
        volatile Func<int, bool> failPredicate;
        volatile bool deferCompletions;
        int sendCallIndex;

        internal RingTestHarness(int pageSize, int pageCount, int completionCapacity, int maxChunkSize)
        {
            epoch = new LightEpoch();
            Ring = new DuplexOperationChannel<TestRequest, int, RingTransport>(
                pageSize, pageCount, completionCapacity, maxChunkSize, new RingTransport(this), epoch);
        }

        internal int CompletedCount => Volatile.Read(ref completedCount);
        internal IReadOnlyCollection<byte[]> Completed => completed;
        internal IReadOnlyCollection<Exception> FlushErrors => flushErrors;
        internal int MaxInlinePayloadSize => Ring.MaxInlinePayloadSize;
        internal int DeferredCompletionCount => deferredCompletions.Count;

        /// <summary>Number of completion tickets the ring has issued so far (response-expecting claims).</summary>
        internal int CompletionTail => Ring.CompletionTail;

        /// <summary>Reader-side: advance the reply watermark, freeing completion slots for reuse.</summary>
        internal void AdvanceCompletions(int count) => Ring.AdvanceCompletion(count);

        /// <summary>Dispose-tracking dictionary for tests that construct out-of-line requests directly.</summary>
        internal ConcurrentDictionary<long, int> DisposeCounts => disposeCounts;

        /// <summary>Register an id as an out-of-line request the harness expects to be disposed exactly once.</summary>
        internal void TrackOutOfLine(long id) => createdOutOfLineIds[id] = 0;

        /// <summary>Inject a send failure whenever the predicate (called with a monotonic send-call index) is true.</summary>
        internal void SetFailurePredicate(Func<int, bool> predicate) => failPredicate = predicate;

        internal void DeferCompletions() => deferCompletions = true;

        internal bool CompleteOneDeferredChunk()
        {
            if (!deferredCompletions.TryDequeue(out var context))
                return false;
            CompleteSend(context);
            return true;
        }

        internal long TailAddress => Ring.GetTailAddress();

        void OnFlushError(Exception ex) => flushErrors.Enqueue(ex ?? new InvalidOperationException("flush error with null exception"));

        void Send(byte[] buffer, int offset, int length, object context)
        {
            var idx = Interlocked.Increment(ref sendCallIndex) - 1;
            var predicate = failPredicate;
            if (predicate != null && predicate(idx))
                throw new System.IO.IOException($"injected send failure at call {idx}");

            var acc = reassembly.GetOrAdd(context, _ => new List<byte>());
            // A request's chunks are dispatched sequentially on one flush thread, so there is no cross-chunk
            // contention within a context; the lock only guards accumulator growth against defensive reads.
            lock (acc)
            {
                for (var i = 0; i < length; i++)
                    acc.Add(buffer[offset + i]);
            }

            if (deferCompletions)
                deferredCompletions.Enqueue(context);
            else
                CompleteSend(context);
        }

        void CompleteSend(object context)
        {
            var result = (DuplexOperationAsyncFlushResult<TestRequest>)context;
            DuplexOperationAsyncFlushResult<TestRequest>.CompleteChunk(context);

            // CompleteChunk decrements remainingChunks; the final chunk of a request drives it to zero. Because
            // a context's chunks are sequential, exactly the last-chunk sender observes zero and finalizes.
            if (Volatile.Read(ref result.remainingChunks) == 0)
            {
                if (reassembly.TryRemove(context, out var acc))
                {
                    byte[] payload;
                    lock (acc)
                        payload = acc.ToArray();
                    completed.Add(payload);
                    Interlocked.Increment(ref completedCount);
                }
            }
        }

        /// <summary>
        /// Enqueue one payload, replicating the client's ring dance: hold the epoch across allocate, register
        /// (inline reserve or out-of-line publish) and drain; wait on the request lane under back-pressure.
        /// </summary>
        internal async Task EnqueueAsync(byte[] payload, bool expectCompletion, CancellationToken token)
        {
            var totalLength = payload.Length;
            var size = Ring.GetRecordSize(totalLength, out var inline);
            TestRequest outOfLineRequest = default;
            var outOfLineRequestCreated = false;
            var outOfLineRequestRegistered = false;

            if (!inline)
            {
                var id = Interlocked.Increment(ref nextRequestId);
                createdOutOfLineIds[id] = 0;
                outOfLineRequest = new TestRequest((byte[])payload.Clone(), totalLength, id, disposeCounts);
                outOfLineRequestCreated = true;
            }

            epoch.Resume();
            try
            {
                long address;
                int taskId;
                while (true)
                {
                    token.ThrowIfCancellationRequested();
                    if (Ring.TryScheduleOperation(
                        size,
                        expectCompletion,
                        out var reservation,
                        out var flushEvent))
                    {
                        taskId = reservation.completionTicket;
                        address = reservation.requestAddress;
                        break;
                    }

                    try
                    {
                        epoch.Suspend();
                        await flushEvent.WaitAsync(token).ConfigureAwait(false);
                    }
                    finally
                    {
                        epoch.Resume();
                    }
                }

                if (expectCompletion)
                    Ring.RegisterCompletion(taskId, taskId);

                if (inline)
                {
                    unsafe
                    {
                        var curr = Ring.RegisterInlineRecord(address, totalLength);
                        new ReadOnlySpan<byte>(payload).CopyTo(new Span<byte>(curr, totalLength));
                    }
                }
                else
                {
                    Ring.RegisterOfflineRecord(address, outOfLineRequest);
                    outOfLineRequestRegistered = true;
                }

                epoch.ProtectAndDrain();
                Ring.DrainRequests();
            }
            finally
            {
                epoch.Suspend();
                // If publication threw (teardown), the ring never took ownership — dispose it here so
                // the exactly-once accounting and budget reservation are both released.
                if (outOfLineRequestCreated && !outOfLineRequestRegistered)
                    outOfLineRequest.Dispose();
            }
        }

        /// <summary>Nudge the ring to flush any residual records that no producer's shift has yet driven out.</summary>
        internal void Pump()
        {
            epoch.Resume();
            try
            {
                epoch.ProtectAndDrain();
                Ring.DrainRequests();
            }
            finally
            {
                epoch.Suspend();
            }
        }

        /// <summary>Pump and poll until <paramref name="expected"/> payloads have flushed, or time out.</summary>
        internal async Task DrainUntilAsync(int expected, TimeSpan timeout, CancellationToken token)
        {
            var sw = Stopwatch.StartNew();
            while (Volatile.Read(ref completedCount) < expected)
            {
                token.ThrowIfCancellationRequested();
                Pump();
                if (sw.Elapsed > timeout)
                    throw new TimeoutException($"Drain timed out: {CompletedCount}/{expected} flushed after {sw.Elapsed}.");
                await Task.Delay(2, token).ConfigureAwait(false);
            }
        }

        /// <summary>Pump and poll until <paramref name="condition"/> holds, or throw on timeout (hang detector).</summary>
        internal async Task PumpUntilAsync(Func<bool> condition, TimeSpan timeout, CancellationToken token)
        {
            var sw = Stopwatch.StartNew();
            while (!condition())
            {
                token.ThrowIfCancellationRequested();
                Pump();
                if (sw.Elapsed > timeout)
                    throw new TimeoutException($"Condition not met within {timeout} ({CompletedCount} flushed, {FlushErrors.Count} flush errors).");
                await Task.Delay(2, token).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Assert every out-of-line request buffer the harness created was disposed exactly once (no leak, no
        /// double-dispose).
        /// </summary>
        internal void AssertNoBufferLeaks()
        {
            foreach (var id in createdOutOfLineIds.Keys)
            {
                var found = disposeCounts.TryGetValue(id, out var count);
                ClassicAssert.IsTrue(found, $"Out-of-line request {id} was never disposed (buffer leak).");
                ClassicAssert.AreEqual(1, count, $"Out-of-line request {id} was disposed {count} times (expected exactly one).");
            }
        }

        /// <summary>Verify each completed payload is intact and that the received id set equals the expected set.</summary>
        internal void AssertReceived(IEnumerable<long> expectedIds)
        {
            var received = new HashSet<long>();
            foreach (var payload in completed)
            {
                var (id, ok) = RingPayload.Verify(payload);
                ClassicAssert.IsTrue(ok, $"Reassembled payload for id {id} failed integrity validation.");
                ClassicAssert.IsTrue(received.Add(id), $"Payload id {id} was received more than once (duplicate delivery).");
            }

            var expected = new HashSet<long>(expectedIds);
            ClassicAssert.IsTrue(received.SetEquals(expected),
                $"Received id set differs from expected. Missing: [{string.Join(",", Difference(expected, received))}]; Unexpected: [{string.Join(",", Difference(received, expected))}].");
        }

        static IEnumerable<long> Difference(HashSet<long> a, HashSet<long> b)
        {
            foreach (var x in a)
            {
                if (!b.Contains(x))
                    yield return x;
            }
        }

        public void Dispose()
        {
            Ring.Dispose();
            epoch.Dispose();
        }
    }
}