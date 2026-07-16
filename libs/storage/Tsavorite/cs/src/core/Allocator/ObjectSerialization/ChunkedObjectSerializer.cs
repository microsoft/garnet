// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
    /// </remarks>
    /// <typeparam name="TContext">Caller state threaded through <c>Drain</c> to the consumer (e.g. the write-side chunk state).</typeparam>
    public class ChunkedObjectSerializer<TContext>
    {
        readonly IObjectSerializer<IHeapObject> serializer;
        readonly IHeapObject valueObject;

        /// <summary>The consumer that turns drained bytes into chunk records.</summary>
        protected readonly IChunkedObjectSerializerConsumer consumer;

        /// <summary>Caller state passed through to the consumer on every drain; set for the duration of <see cref="Serialize"/>.</summary>
        protected TContext context;

        /// <summary>The circular buffer holding serialized value bytes not yet consumed.</summary>
        readonly byte[] buffer;
        /// <summary>Next position to fill (write into).</summary>
        int head;
        /// <summary>Next position to consume (drain from).</summary>
        int tail;
        /// <summary>Number of valid (unconsumed) bytes currently in the ring; disambiguates head==tail empty vs full.</summary>
        int count;

        protected ChunkedObjectSerializer(IChunkedObjectSerializerConsumer consumer, IObjectSerializer<IHeapObject> serializer, IHeapObject valueObject, int bufferSize)
        {
            this.consumer = consumer;
            this.serializer = serializer;
            this.valueObject = valueObject;
            this.buffer = new byte[bufferSize];
        }

        /// <summary>
        /// Serialize the value object, draining the ring to the consumer as it fills, then perform a final drain with
        /// <c>isComplete: true</c> (which also carries the key/input tail on the generic subclass). <paramref name="context"/>
        /// is passed through to the consumer on every drain.
        /// </summary>
        public void Serialize(TContext context)
        {
            this.context = context;
            using var stream = new ChunkStreamWriter(this);
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
        /// <param name="isComplete">True on the final drain of the value.</param>
        /// <returns>The number of bytes consumed (0..<c>first.Length + second.Length</c>).</returns>
        protected virtual int Drain(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, bool isComplete)
            => consumer.Consume(first, second, isComplete, context);

        // Append bytes into the ring, draining (isComplete: false) whenever it fills.
        void Write(ReadOnlySpan<byte> src)
        {
            while (src.Length > 0)
            {
                if (count == buffer.Length)
                {
                    DrainOnce(isComplete: false);
                    // If the consumer could not free any space, we cannot make progress.
                    if (count == buffer.Length)
                        throw new TsavoriteException("Chunk consumer did not consume any bytes on a full buffer");
                }

                // Fill contiguously from head to the buffer end (or as much as fits/remains), then wrap on the next iteration.
                var free = buffer.Length - count;
                var toEnd = buffer.Length - head;
                var toCopy = Math.Min(src.Length, Math.Min(free, toEnd));
                src.Slice(0, toCopy).CopyTo(buffer.AsSpan(head));
                head += toCopy;
                if (head == buffer.Length)
                    head = 0;
                count += toCopy;
                src = src.Slice(toCopy);
            }
        }

        // Present the ring's unconsumed bytes as [tail..end] then [0..head] (the second span is empty when not wrapped) and
        // drain them, advancing tail by however many the consumer took.
        void DrainOnce(bool isComplete)
        {
            var firstLen = Math.Min(count, buffer.Length - tail);
            var first = new ReadOnlySpan<byte>(buffer, tail, firstLen);
            var secondLen = count - firstLen;
            var second = secondLen > 0 ? new ReadOnlySpan<byte>(buffer, 0, secondLen) : default;

            var consumed = Drain(first, second, isComplete);
            if (consumed < 0 || consumed > count)
                throw new TsavoriteException($"Chunk consumer returned invalid consumed count {consumed} for {count} bytes");

            tail += consumed;
            if (tail >= buffer.Length)
                tail -= buffer.Length;
            count -= consumed;
        }

        // Drain whatever remains as the final chunk (isComplete: true). Loops in case the consumer takes it in pieces.
        void FlushFinal()
        {
            do
            {
                DrainOnce(isComplete: true);
            } while (count > 0);
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

        /// <summary>The input's serialized length, captured at construction so it is available independent of <see cref="input"/>.</summary>
        readonly int inputSerializedLength;

        public ChunkedObjectSerializer(in ConditionallyHoistedKey key, ref TInput input, IChunkedObjectSerializerConsumer consumer, IObjectSerializer<IHeapObject> serializer, IHeapObject valueObject, int bufferSize)
            : base(consumer, serializer, valueObject, bufferSize)
        {
            this.key = key;
            this.input = input;
            this.inputSerializedLength = input.SerializedLength;
        }

        /// <summary>The input's serialized length, captured at construction.</summary>
        public int InputSerializedLength => inputSerializedLength;

        /// <inheritdoc/>
        protected override int Drain(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, bool isComplete)
            => consumer.Consume(first, second, isComplete, key, ref input, context);
    }
}
