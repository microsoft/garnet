// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;

namespace Tsavorite.core.Allocator.ObjectSerialization
{
    /// <summary>
    /// Serializes an <see cref="IHeapObject"/> into a bounded circular buffer, draining the buffer to an
    /// <see cref="IChunkedObjectSerializerConsumer"/> as it fills. This lets an arbitrarily large object be written as a
    /// sequence of chunks without materializing its entire serialized form at once (only up to <c>bufferSize</c> bytes are
    /// held at a time). The generic <see cref="ChunkedObjectSerializer{TContext, TInput}"/> adds the record's key and input,
    /// which are forwarded to the consumer.
    /// </summary>
    /// <remarks>
    /// The buffer is a true ring (no compaction): <see cref="head"/> is the next position to fill and <see cref="tail"/> the
    /// next to consume, so available bytes are exposed to the consumer as up to two spans (the run from tail to the end, then
    /// the wrapped run from the start). Buffer-fill drains use <c>isComplete: false</c>; a final drain after serialization
    /// completes uses <c>isComplete: true</c> (its data length may be zero).
    /// <para>
    /// Two drive modes share the ring: the object mode (<see cref="Serialize"/>, used by the AOF write path) streams one
    /// <see cref="IObjectSerializer{T}"/>-serialized object; the manual mode (<see cref="BeginSerialize"/> /
    /// <see cref="WriteBytes"/> / <see cref="GetStream"/> / <see cref="EndSerialize"/>, used by the network migration /
    /// replication path) frames several raw byte components — record header, key, value, optionals — into one chunk stream,
    /// content-agnostically. Both drain through the value-only <c>IChunkedObjectSerializerConsumer.Consume</c>.
    /// </para>
    /// </remarks>
    /// <typeparam name="TContext">Caller state threaded through <c>Drain</c> to the consumer (e.g. the write-side chunk state).</typeparam>
    public unsafe class ChunkedObjectSerializer<TContext>
    {
        IObjectSerializer<IHeapObject> serializer;
        IHeapObject valueObject;

        /// <summary>The consumer that turns drained bytes into chunk records.</summary>
        protected IChunkedObjectSerializerConsumer consumer;

        /// <summary>Caller state passed through to the consumer on every drain; set for the duration of <see cref="Serialize"/>.</summary>
        protected TContext context;

        /// <summary>The ring's backing store on the manual (network) path, owned for the instance's lifetime. Null when the ring
        /// is pooled.</summary>
        readonly byte[] managedBuffer;
        /// <summary>The ring's backing store on the pooled path, rented for the duration of one write and returned by
        /// <see cref="ClearWriteTarget"/>. Null between writes and on the managed path.</summary>
        SectorAlignedMemory pooledBuffer;
        /// <summary>Length of the ring. A pooled buffer may be larger than this (the pool rounds up to a size class); the ring
        /// uses exactly this many bytes either way.</summary>
        readonly int bufferLength;

        /// <summary>The circular buffer holding serialized value bytes not yet consumed.</summary>
        Span<byte> Ring => managedBuffer is not null ? managedBuffer.AsSpan(0, bufferLength) : new Span<byte>(pooledBuffer.aligned_pointer, bufferLength);
        /// <summary>Next position to fill (write into).</summary>
        int head;
        /// <summary>Next position to consume (drain from).</summary>
        int tail;
        /// <summary>Number of valid (unconsumed) bytes currently in the ring; disambiguates head==tail empty vs full.</summary>
        int count;
        /// <summary>True until the first drain of the current serialization; passed to the consumer as <c>isStart</c>. Set by
        /// <see cref="BeginSerialize"/> / <see cref="Serialize"/>.</summary>
        bool firstDrainPending;
        /// <summary>The write-only stream over the ring, created once and reused across serializations (it holds no state of its
        /// own beyond the owning serializer).</summary>
        ChunkStreamWriter streamWriter;

        /// <summary>
        /// Create a chunk writer for the network (migration / replication) path, which frames raw byte components (record
        /// header, key, value, optionals) — and optionally streams an object value via <see cref="GetStream"/> — into one
        /// chunk stream using <see cref="BeginSerialize"/> / <see cref="WriteBytes"/> / <see cref="EndSerialize"/>. There is
        /// no fixed value object; <see cref="Serialize"/> is not used on this instance.
        /// </summary>
        public ChunkedObjectSerializer(IChunkedObjectSerializerConsumer consumer, int bufferSize)
            : this(bufferSize)
        {
            this.consumer = consumer;
        }

        /// <summary>
        /// Create a reusable chunk writer whose consumer, object serializer, and value object are bound per write by
        /// <see cref="SetObjectWriteTarget"/> and released by <see cref="ClearWriteTarget"/>, over a managed ring owned for
        /// this instance's lifetime.
        /// </summary>
        protected ChunkedObjectSerializer(int bufferSize)
            : this(bufferSize, poolRing: false)
        {
        }

        /// <summary>
        /// Create a reusable chunk writer as above, choosing how the ring is backed.
        /// </summary>
        /// <remarks>
        /// The choice follows the instance's lifetime, which in turn follows which of the two drive modes it uses:
        /// <list type="table">
        ///   <listheader>
        ///     <term></term>
        ///     <description><c>poolRing: true</c> (AOF write path) / <c>poolRing: false</c> (network path)</description>
        ///   </listheader>
        ///   <item>
        ///     <term>Mode</term>
        ///     <description>object, via <see cref="SetObjectWriteTarget"/> / manual, via <see cref="BeginSerialize"/></description>
        ///   </item>
        ///   <item>
        ///     <term>Lifetime</term>
        ///     <description>thread-static cache, indefinite / bounded by one migration or snapshot send</description>
        ///   </item>
        ///   <item>
        ///     <term>Duty cycle</term>
        ///     <description>idle between writes / in near-continuous use</description>
        ///   </item>
        ///   <item>
        ///     <term>Ring size</term>
        ///     <description>a pool-friendly constant / caller-chosen and arbitrary</description>
        ///   </item>
        ///   <item>
        ///     <term>Pool available</term>
        ///     <description>yes, the log's buffer pool / no, there is no log</description>
        ///   </item>
        /// </list>
        /// An indefinitely cached instance would otherwise pin its ring to one thread for the process's life even while idle,
        /// so it rents per write; an instance that lives for one operation and uses the ring throughout would gain nothing
        /// from renting and its size need not be a pool size class.
        /// <para>
        /// Structurally the pool arrives <em>per write</em> through <see cref="SetObjectWriteTarget"/>, reachable only from
        /// the object mode, so the manual mode has nowhere to receive one and must own its ring. <c>poolRing: true</c> is
        /// therefore valid only for object-mode instances; <see cref="BeginSerialize"/> asserts this.
        /// </para>
        /// </remarks>
        /// <param name="bufferSize">The ring length (the max value bytes held at once).</param>
        /// <param name="poolRing">When true the ring is rented per write from the pool passed to
        /// <see cref="SetObjectWriteTarget"/>, so a cached instance holds no large buffer between writes and the memory is
        /// reused across threads via the pool's depot. When false this instance owns a managed ring for its lifetime.</param>
        protected ChunkedObjectSerializer(int bufferSize, bool poolRing)
        {
            bufferLength = bufferSize;
            if (!poolRing)
                managedBuffer = new byte[bufferSize];
        }

        /// <summary>Bind this (reused) serializer to one streamed write and reset the ring. Paired with
        /// <see cref="ClearWriteTarget"/>, which drops the references again and returns a pooled ring.</summary>
        /// <param name="bufferPool">Pool to rent the ring from; required when this instance was created with
        /// <c>poolRing: true</c> and ignored otherwise. It is passed per write rather than held, because a cached instance is
        /// keyed only by thread and input type and so may be reused across logs with different pools.</param>
        protected void SetObjectWriteTarget(IChunkedObjectSerializerConsumer consumer, IObjectSerializer<IHeapObject> serializer, IHeapObject valueObject, SectorAlignedBufferPool bufferPool)
        {
            this.consumer = consumer;
            this.serializer = serializer;
            this.valueObject = valueObject;

            if (managedBuffer is null)
            {
                // clearOnReturn is false because the ring is always written before it is read (only the `count` bytes written
                // since the last drain are ever exposed), so a previous rental's bytes cannot be observed; clearing would zero
                // the whole buffer on every write.
                pooledBuffer?.Return();
                pooledBuffer = bufferPool.Get(bufferLength, clearOnReturn: false);
            }

            // Reset the ring explicitly rather than relying on the previous write's FlushFinal, so a write that failed partway
            // through cannot leave stale bytes for the next one.
            head = tail = count = 0;
        }

        /// <summary>Drop the references taken by <see cref="SetObjectWriteTarget"/> so a cached serializer does not root the
        /// value object, consumer, or caller context between writes, and return a pooled ring to its pool.</summary>
        protected virtual void ClearWriteTarget()
        {
            consumer = null;
            serializer = null;
            valueObject = null;
            context = default;

            if (pooledBuffer is not null)
            {
                pooledBuffer.Return();
                pooledBuffer = null;
            }
        }

        /// <summary>Set the context for a manual chunked write (see the network-path constructor); follow with
        /// <see cref="WriteBytes"/> / <see cref="GetStream"/> and finish with <see cref="EndSerialize"/>.</summary>
        public void BeginSerialize(TContext context)
        {
            // The manual mode has no per-write binding call, so there is nowhere to hand it a pool: it must own its ring.
            // Only the object mode (SetObjectWriteTarget, reached via SetRecord) can rent one.
            Debug.Assert(managedBuffer is not null, "A pooled ring cannot be used for a manual chunked write; construct with poolRing: false.");
            this.context = context;
            firstDrainPending = true;
        }

        /// <summary>Append raw bytes to the chunk stream, draining to the consumer as the ring fills.</summary>
        public void WriteBytes(ReadOnlySpan<byte> bytes) => Write(bytes);

        /// <summary>A write-only <see cref="Stream"/> over the chunk ring, e.g. to run an <see cref="IObjectSerializer{T}"/>
        /// directly into it; bytes written drain to the consumer as the ring fills. The same instance is returned on every call
        /// (it is stateless beyond this serializer), so disposing it is a no-op and it remains usable.</summary>
        public Stream GetStream() => streamWriter ??= new ChunkStreamWriter(this);

        /// <summary>Flush the ring's remaining bytes as the final chunk(s) (<c>isComplete: true</c>), completing a manual write.</summary>
        public void EndSerialize() => FlushFinal();

        /// <summary>
        /// Serialize the value object, draining the ring to the consumer as it fills, then perform a final drain with
        /// <c>isComplete: true</c> (which also carries the key/input tail on the generic subclass). <paramref name="context"/>
        /// is passed through to the consumer on every drain.
        /// </summary>
        public void Serialize(TContext context)
        {
            this.context = context;
            firstDrainPending = true;
            var stream = GetStream();
            serializer.BeginSerialize(stream);
            serializer.Serialize(valueObject);
            serializer.EndSerialize();
            FlushFinal();
        }

        /// <summary>
        /// Drain the ring's available bytes (as two spans, <paramref name="first"/> then <paramref name="second"/>) to the
        /// consumer. The base form carries only the value; the generic subclass overrides this to also carry the key and input.
        /// </summary>
        /// <param name="first">The contiguous run of available bytes (from tail to head or the buffer end).</param>
        /// <param name="second">The wrapped run of available bytes; empty when the ring is not wrapped.</param>
        /// <param name="isStart">True on the first drain of the value.</param>
        /// <param name="isComplete">True on the final drain of the value.</param>
        /// <returns>The number of bytes consumed (0..<c>first.Length + second.Length</c>).</returns>
        protected virtual int Drain(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, bool isStart, bool isComplete)
            => consumer.Consume(first, second, isStart, isComplete, context);

        // Append bytes into the ring, draining (isComplete: false) whenever it fills.
        void Write(ReadOnlySpan<byte> src)
        {
            while (src.Length > 0)
            {
                if (count == bufferLength)
                {
                    DrainOnce(isComplete: false);
                    // If the consumer could not free any space, we cannot make progress.
                    if (count == bufferLength)
                        throw new TsavoriteException("Chunk consumer did not consume any bytes on a full buffer");
                }

                // Fill contiguously from head to the buffer end (or as much as fits/remains), then wrap on the next iteration.
                var free = bufferLength - count;
                var toEnd = bufferLength - head;
                var toCopy = Math.Min(src.Length, Math.Min(free, toEnd));
                src.Slice(0, toCopy).CopyTo(Ring.Slice(head));
                head += toCopy;
                if (head == bufferLength)
                    head = 0;
                count += toCopy;
                src = src.Slice(toCopy);
            }
        }

        // Present the ring's unconsumed bytes as [tail..end] then [0..head] (the second span is empty when not wrapped) and
        // drain them, advancing tail by however many the consumer took.
        void DrainOnce(bool isComplete)
        {
            var ring = Ring;
            var firstLen = Math.Min(count, bufferLength - tail);
            ReadOnlySpan<byte> first = ring.Slice(tail, firstLen);
            var secondLen = count - firstLen;
            ReadOnlySpan<byte> second = secondLen > 0 ? ring.Slice(0, secondLen) : default;

            var isStart = firstDrainPending;
            firstDrainPending = false;
            var consumed = Drain(first, second, isStart, isComplete);
            if (consumed < 0 || consumed > count)
                throw new TsavoriteException($"Chunk consumer returned invalid consumed count {consumed} for {count} bytes");

            tail += consumed;
            if (tail >= bufferLength)
                tail -= bufferLength;
            count -= consumed;
        }

        // Drain whatever remains as the final chunk (isComplete: true). Loops in case the consumer takes it in pieces.
        void FlushFinal()
        {
            do
            {
                var before = count;
                DrainOnce(isComplete: true);
                // The final drain must run once even when the ring is empty (to deliver isComplete and, on the generic
                // subclass, the key/input tail), but a non-empty ring the consumer did not shrink means no progress:
                // fail rather than spin forever (mirrors the full-buffer guard in Write).
                if (count > 0 && count == before)
                    throw new TsavoriteException("Chunk consumer did not consume any bytes on the final flush");
            } while (count > 0);

            // Reset the (now-empty) ring to position 0 so the next serialization drains contiguously (a record that fits the ring
            // arrives as a single un-wrapped span); this lets a consumer recognize a whole record from one drain.
            head = tail = 0;
        }

        /// <summary>A write-only <see cref="Stream"/> that funnels serialized bytes into the owning serializer's ring buffer.</summary>
        sealed class ChunkStreamWriter : Stream
        {
            readonly ChunkedObjectSerializer<TContext> owner;

            internal ChunkStreamWriter(ChunkedObjectSerializer<TContext> owner) => this.owner = owner;

            public override void Write(byte[] array, int offset, int length) => owner.Write(new ReadOnlySpan<byte>(array, offset, length));
            public override void Write(ReadOnlySpan<byte> span) => owner.Write(span);
            public override void WriteByte(byte value) { unsafe { owner.Write(new ReadOnlySpan<byte>(&value, 1)); } }

            public override bool CanWrite => true;
            public override bool CanRead => false;
            public override bool CanSeek => false;
            public override void Flush() { }
            public override long Length => throw new NotSupportedException();
            public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
            public override int Read(byte[] array, int offset, int length) => throw new NotSupportedException();
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();

            // This stream owns no resources and holds no per-serialization state, and the owning serializer reuses one instance
            // across writes, so disposal is a no-op and the stream stays usable afterwards (callers may wrap it in `using`).
            protected override void Dispose(bool disposing) { }
        }
    }

    /// <summary>
    /// A <see cref="ChunkedObjectSerializer{TContext}"/> that also carries the record's key and input, passed to the consumer
    /// on each drain so it can write the key (first chunk) and input (final chunk) alongside the value data.
    /// </summary>
    /// <typeparam name="TContext">Caller state threaded through to the consumer.</typeparam>
    /// <typeparam name="TInput">The record input type.</typeparam>
    public sealed unsafe class ChunkedObjectSerializer<TContext, TInput> : ChunkedObjectSerializer<TContext>
        where TInput : IStoreInput
    {
        // TODO: Consider changing this from ConditionallyHoistedKey to IKey. One concern is that if this is called with a
        // LogRecord.Key, then the underlying LogRecord might be evicted when we pulse epochAccessor. That could be dealt with by
        // having ConditionallyHoistedKey "adopt" the underlying byte[] or OverflowByteArray rather than copying it as it
        // currently does.
        /// <summary>The record's key, passed to the consumer on each drain.</summary>
        ConditionallyHoistedKey key;

        /// <summary>The record's input, passed to the consumer on each drain.</summary>
        /// <remarks>SAFETY: safe as long as we do not exit the scope of any pinned or fixed memory.</remarks>
        TInput input;

        /// <summary>Create a reusable serializer; bind each record with <see cref="SetRecord"/> and release it with
        /// <see cref="Clear"/>.</summary>
        /// <param name="bufferSize">Size of the circular buffer (the max value bytes held at once), allocated once here.</param>
        public ChunkedObjectSerializer(int bufferSize)
            : base(bufferSize)
        {
        }

        /// <summary>Create a reusable serializer as above, choosing how the ring is backed.</summary>
        /// <param name="bufferSize">Size of the circular buffer (the max value bytes held at once).</param>
        /// <param name="poolRing">When true the ring is rented per write from the pool passed to <see cref="SetRecord"/>.</param>
        public ChunkedObjectSerializer(int bufferSize, bool poolRing)
            : base(bufferSize, poolRing)
        {
        }

        /// <summary>Bind this serializer to one record's key, input, consumer, object serializer, and value object.</summary>
        /// <param name="bufferPool">Pool to rent the ring from when this instance pools its ring; ignored otherwise.</param>
        public void SetRecord(in ConditionallyHoistedKey key, ref TInput input, IChunkedObjectSerializerConsumer consumer, IObjectSerializer<IHeapObject> serializer, IHeapObject valueObject, SectorAlignedBufferPool bufferPool = null)
        {
            this.key = key;
            this.input = input;
            SetObjectWriteTarget(consumer, serializer, valueObject, bufferPool);
        }

        /// <summary>Release the bound record so a cached serializer does not root the key's hoisted memory, the input's pointers,
        /// or the value object between writes.</summary>
        public void Clear()
        {
            key = default;
            input = default;
            ClearWriteTarget();
        }

        /// <inheritdoc/>
        protected override int Drain(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, bool isStart, bool isComplete)
            => consumer.Consume(first, second, isStart, isComplete, key, ref input, context);
    }
}