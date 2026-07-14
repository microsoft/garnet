// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;

namespace Tsavorite.core.Allocator.ObjectSerialization
{
    /// <summary>
    /// Serializes an <see cref="IHeapObject"/> into a bounded buffer, draining the buffer to an <see cref="IChunkedObjectConsumer"/>
    /// as it fills. This lets an arbitrarily large object be written as a sequence of chunks without materializing its entire
    /// serialized form at once (only up to <c>bufferSize</c> bytes are held at a time). The generic
    /// <see cref="ChunkedObjectSerializer{TKey, TInput}"/> adds the record's key and input, which are forwarded to the consumer.
    /// </summary>
    /// <remarks>
    /// The buffer is compacted (unconsumed bytes moved to the front) rather than treated as a true wrap-around ring, so the
    /// span handed to the consumer is always contiguous. Buffer-fill drains use <c>isComplete: false</c>; a final drain after
    /// serialization completes uses <c>isComplete: true</c> (its data length may be zero).
    /// </remarks>
    public abstract class ChunkedObjectSerializer
    {
        readonly IObjectSerializer<IHeapObject> serializer;
        readonly IHeapObject valueObject;

        /// <summary>Buffer holding serialized value bytes not yet consumed; unconsumed bytes are kept at the front.</summary>
        protected readonly byte[] buffer;

        /// <summary>Count of valid (unconsumed) bytes at the front of <see cref="buffer"/>.</summary>
        int count;

        protected ChunkedObjectSerializer(IObjectSerializer<IHeapObject> serializer, IHeapObject valueObject, int bufferSize)
        {
            this.serializer = serializer;
            this.valueObject = valueObject;
            this.buffer = new byte[bufferSize];
        }

        /// <summary>
        /// Serialize the value object, draining the buffer to the consumer as it fills, then perform a final drain with
        /// <c>isComplete: true</c> (which also carries the key/input tail on the generic subclass).
        /// </summary>
        public void Serialize()
        {
            using var stream = new ChunkStream(this);
            serializer.BeginSerialize(stream);
            serializer.Serialize(valueObject);
            serializer.EndSerialize();
            FlushFinal();
        }

        /// <summary>
        /// Drain the front <paramref name="length"/> bytes of <see cref="buffer"/> to the consumer.
        /// </summary>
        /// <returns>The number of bytes consumed (0..<paramref name="length"/>).</returns>
        protected abstract long Drain(int length, bool isComplete);

        // Append bytes into the buffer, draining (isComplete: false) whenever the buffer fills.
        void Write(ReadOnlySpan<byte> src)
        {
            while (src.Length > 0)
            {
                var space = buffer.Length - count;
                if (space == 0)
                {
                    DrainAndCompact(isComplete: false);
                    space = buffer.Length - count;
                    // If the consumer could not free any space, we cannot make progress.
                    if (space == 0)
                        throw new TsavoriteException("Chunk consumer did not consume any bytes on a full buffer");
                }
                var toCopy = Math.Min(space, src.Length);
                src.Slice(0, toCopy).CopyTo(buffer.AsSpan(count));
                count += toCopy;
                src = src.Slice(toCopy);
            }
        }

        void DrainAndCompact(bool isComplete)
        {
            var consumed = (int)Drain(count, isComplete);
            if (consumed < 0 || consumed > count)
                throw new TsavoriteException($"Chunk consumer returned invalid consumed count {consumed} for {count} bytes");
            var remaining = count - consumed;
            if (remaining > 0 && consumed > 0)
                buffer.AsSpan(consumed, remaining).CopyTo(buffer.AsSpan(0));
            count = remaining;
        }

        // Drain whatever remains as the final chunk (isComplete: true). Loops in case the consumer takes it in pieces.
        void FlushFinal()
        {
            do
            {
                DrainAndCompact(isComplete: true);
            } while (count > 0);
        }

        /// <summary>A write-only <see cref="Stream"/> that funnels serialized bytes into the owning serializer's buffer.</summary>
        sealed class ChunkStream : Stream
        {
            readonly ChunkedObjectSerializer owner;

            internal ChunkStream(ChunkedObjectSerializer owner) => this.owner = owner;

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
    /// This form of <see cref="ChunkedObjectSerializer"/> carries the record's key and input, which are passed to the consumer
    /// on each drain so the consumer can write the key (first chunk) and input (final chunk) alongside the value data.
    /// </summary>
    /// <typeparam name="TKey">The record key type.</typeparam>
    /// <typeparam name="TInput">The record input type.</typeparam>
    public sealed class ChunkedObjectSerializer<TKey, TInput> : ChunkedObjectSerializer
        where TKey : IKey
        where TInput : IStoreInput
    {
        /// <summary>The caller's key, passed to Consume.</summary>
        /// <remarks>SAFETY: This is safe as long as we do not exit the scope of any pinned or fixed memory.</remarks>
        TKey key;

        /// <summary>The caller's input, passed to Consume.</summary>
        /// <remarks>SAFETY: This is safe as long as we do not exit the scope of any pinned or fixed memory.</remarks>
        TInput input;

        /// <summary>The input's serialized length, captured at construction so it is available independent of <see cref="input"/>.</summary>
        readonly int inputSerializedLength;

        readonly IChunkedObjectConsumer consumer;

        public ChunkedObjectSerializer(TKey key, ref TInput input, IObjectSerializer<IHeapObject> serializer, IHeapObject valueObject, int bufferSize, IChunkedObjectConsumer consumer)
            : base(serializer, valueObject, bufferSize)
        {
            this.key = key;
            this.input = input;
            this.inputSerializedLength = input.SerializedLength;
            this.consumer = consumer;
        }

        /// <summary>The input's serialized length, captured at construction.</summary>
        public int InputSerializedLength => inputSerializedLength;

        protected override long Drain(int length, bool isComplete)
            => consumer.Consume(new ReadOnlySpan<byte>(buffer, 0, length), isComplete, key, ref input);
    }
}
