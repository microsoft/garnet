// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Deserializes one chunked AOF object value while its chunks are still arriving, so the serialized bytes are never
    /// retained as a second whole copy of the object.
    /// </summary>
    /// <remarks>
    /// <see cref="GarnetObjectSerializer"/> deserialization is synchronous — a <see cref="BinaryReader"/> driving a
    /// constructor — so it cannot be parked on an <c>await</c> and must run on its own thread while the replay thread feeds
    /// it. The two are joined by a bounded queue of pooled buffers: the replay thread copies each arriving segment in
    /// (blocking only while the queue is full) and the worker drains it through a <see cref="Stream"/>. The queue bounds how
    /// much serialized data is held at once, and the copy means the worker never sees a pointer into the replay entry buffer
    /// or the log's own memory, both of which are reclaimed as soon as the record is processed.
    /// <para>
    /// The replay thread blocks only when the queue is full, which the worker always resolves by draining; it never waits on
    /// data that has yet to arrive. Admission is therefore the only place a stall could become a deadlock, and it is
    /// non-blocking (see <see cref="StreamingObjectValueDeserializerLimiter"/>).
    /// </para>
    /// </remarks>
    internal sealed class StreamingObjectValueDeserializer : IDisposable
    {
        /// <summary>Buffers queued between the replay thread and the worker. Bounds retained serialized bytes at
        /// <see cref="QueueCapacity"/> * <see cref="BufferSize"/>.</summary>
        const int QueueCapacity = 4;

        /// <summary>Size of each queued buffer; matches <see cref="PooledChunkList.DefaultBufferSize"/> so it stays below
        /// the large-object-heap threshold and within the pooled size classes.</summary>
        const int BufferSize = PooledChunkList.DefaultBufferSize;

        readonly BlockingCollection<(byte[] buffer, int length)> queue = new(QueueCapacity);
        readonly ArrayPool<byte> pool = ArrayPool<byte>.Shared;
        readonly StreamingObjectValueDeserializerLimiter limiter;
        readonly Task<IGarnetObject> worker;

        /// <summary>Set before <see cref="BlockingCollection{T}.CompleteAdding"/> when the producer aborts, so the worker's
        /// next read throws instead of seeing a clean end of stream.</summary>
        volatile Exception producerFault;

        int currentFilled;
        byte[] current;
        bool completed;

        internal StreamingObjectValueDeserializer(GarnetObjectSerializer serializer, StreamingObjectValueDeserializerLimiter limiter)
        {
            this.limiter = limiter;
            // LongRunning so this runs on a dedicated thread: the worker spends its life blocked in a read, and parking a
            // thread-pool thread there would consume the pool for the duration of the value.
            worker = Task.Factory.StartNew(
                () => serializer.Deserialize(new QueueStream(this)),
                CancellationToken.None,
                TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
                TaskScheduler.Default);
        }

        /// <summary>Feed the next run of value bytes, spilling into additional queued buffers as needed.</summary>
        internal void Append(ReadOnlySpan<byte> data)
        {
            while (!data.IsEmpty)
            {
                current ??= pool.Rent(BufferSize);
                var toCopy = Math.Min(data.Length, BufferSize - currentFilled);
                data.Slice(0, toCopy).CopyTo(current.AsSpan(currentFilled));
                currentFilled += toCopy;
                data = data.Slice(toCopy);
                if (currentFilled == BufferSize)
                    FlushCurrent();
            }
        }

        void FlushCurrent()
        {
            if (current is null || currentFilled == 0)
                return;
            // Blocks while the queue is full; the worker is draining, so this always makes progress.
            queue.Add((current, currentFilled));
            current = null;
            currentFilled = 0;
        }

        /// <summary>Count of object values deserialized while their chunks arrived, and of values accumulated as bytes
        /// instead. Test-only diagnostics.</summary>
        internal static long TotalStreamed;
        internal static long TotalAccumulated;

        /// <summary>Reset the test-only counters.</summary>
        internal static void ResetCounters()
        {
            Volatile.Write(ref TotalStreamed, 0);
            Volatile.Write(ref TotalAccumulated, 0);
        }

        /// <summary>Signal that the value is complete and return the deserialized object, propagating any worker failure.</summary>
        internal IGarnetObject Complete()
        {
            FlushCurrent();
            completed = true;
            queue.CompleteAdding();
            _ = Interlocked.Increment(ref TotalStreamed);
#pragma warning disable VSTHRD002 // AOF replay is synchronous throughout; the worker exists only to give the synchronous deserializer its own stack, and the replay thread must have the object before it can dispatch the record.
            return worker.GetAwaiter().GetResult();
#pragma warning restore VSTHRD002
        }

        /// <summary>Abort the value: fault the worker rather than letting it observe a clean end of stream, so a truncated
        /// or abandoned record can never be mistaken for a complete one.</summary>
        internal void Fault(Exception exception)
        {
            if (completed)
                return;
            producerFault = exception ?? new GarnetException("Chunked object value was abandoned before completion");
            completed = true;
            queue.CompleteAdding();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            Fault(null);
            try
            {
                // Observe the worker so an aborted deserialization does not surface as an unobserved task exception, and so
                // the thread is known to be gone before the queued buffers are returned.
#pragma warning disable VSTHRD002 // Synchronous by design; see Complete().
                _ = worker.GetAwaiter().GetResult();
#pragma warning restore VSTHRD002
            }
            catch
            {
                // Expected when the value was aborted.
            }

            if (current is not null)
            {
                pool.Return(current);
                current = null;
            }
            while (queue.TryTake(out var item))
                pool.Return(item.buffer);
            queue.Dispose();
            limiter?.Release();
        }

        /// <summary>The worker's view of the queue: a read-only forward stream over the queued buffers.</summary>
        sealed class QueueStream : Stream
        {
            readonly StreamingObjectValueDeserializer owner;
            byte[] currentBuffer;
            int currentLength;
            int currentOffset;

            internal QueueStream(StreamingObjectValueDeserializer owner) => this.owner = owner;

            public override int Read(byte[] array, int offset, int count) => Read(array.AsSpan(offset, count));

            public override int Read(Span<byte> destination)
            {
                if (destination.IsEmpty)
                    return 0;
                if (!EnsureCurrent())
                    return 0;

                var toCopy = Math.Min(destination.Length, currentLength - currentOffset);
                currentBuffer.AsSpan(currentOffset, toCopy).CopyTo(destination);
                currentOffset += toCopy;
                return toCopy;
            }

            public override int ReadByte()
            {
                if (!EnsureCurrent())
                    return -1;
                return currentBuffer[currentOffset++];
            }

            // Take the next queued buffer when the current one is exhausted. Returns false only at a clean end of stream;
            // a producer abort throws instead, so a partial value is never mistaken for a complete one.
            bool EnsureCurrent()
            {
                while (currentBuffer is null || currentOffset == currentLength)
                {
                    if (currentBuffer is not null)
                    {
                        owner.pool.Return(currentBuffer);
                        currentBuffer = null;
                    }

                    if (!owner.queue.TryTake(out var item, Timeout.Infinite))
                    {
                        var fault = owner.producerFault;
                        if (fault is not null)
                            throw fault;
                        return false;
                    }

                    var pending = owner.producerFault;
                    if (pending is not null)
                    {
                        owner.pool.Return(item.buffer);
                        throw pending;
                    }

                    currentBuffer = item.buffer;
                    currentLength = item.length;
                    currentOffset = 0;
                }
                return true;
            }

            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => false;
            public override void Flush() { }
            public override long Length => throw new NotSupportedException();
            public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] array, int offset, int count) => throw new NotSupportedException();

            protected override void Dispose(bool disposing)
            {
                if (currentBuffer is not null)
                {
                    owner.pool.Return(currentBuffer);
                    currentBuffer = null;
                }
                base.Dispose(disposing);
            }
        }
    }

    /// <summary>
    /// Caps how many chunked object values may be deserialized concurrently, across every sublog.
    /// </summary>
    /// <remarks>
    /// Each streamed value holds a dedicated worker thread for its lifetime, so the count must be bounded. Admission is
    /// strictly non-blocking: a replay thread that waited for a slot could no longer reach the chunks that would release
    /// one, since the in-flight values are fed by that same thread. Callers fall back to accumulating the bytes instead.
    /// </remarks>
    internal sealed class StreamingObjectValueDeserializerLimiter
    {
        readonly int maxConcurrent;
        int active;

        internal StreamingObjectValueDeserializerLimiter(int maxConcurrent) => this.maxConcurrent = maxConcurrent;

        /// <summary>Try to claim a slot without waiting. Returns false when all slots are in use.</summary>
        internal bool TryAcquire()
        {
            while (true)
            {
                var current = Volatile.Read(ref active);
                if (current >= maxConcurrent)
                    return false;
                if (Interlocked.CompareExchange(ref active, current + 1, current) == current)
                    return true;
            }
        }

        /// <summary>Release a slot claimed by <see cref="TryAcquire"/>.</summary>
        internal void Release() => Interlocked.Decrement(ref active);
    }
}