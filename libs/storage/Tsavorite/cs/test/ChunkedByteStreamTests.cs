// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using Tsavorite.core.Allocator.ObjectSerialization;

namespace Tsavorite.test
{
    /// <summary>
    /// Round-trip tests for the value-only chunked byte-stream path used by migration / replication:
    /// <see cref="ChunkedObjectSerializer{TContext}"/> raw-byte framing -> a value-only
    /// <see cref="IChunkedObjectSerializerConsumer"/> -> reassembly. A small ring forces multiple (wrapped) drains so both
    /// ring spans and the final <c>isComplete</c> chunk are exercised.
    /// </summary>
    [TestFixture]
    internal class ChunkedByteStreamTests
    {
        // The "receiver": a value-only IChunkedObjectSerializerConsumer that reassembles the chunks (network consumers
        // likewise implement this interface directly; the key/input overload does not apply to the network path).
        sealed class ReassemblingConsumer : IChunkedObjectSerializerConsumer
        {
            readonly MemoryStream stream = new();
            public bool SawFinal { get; private set; }
            public int ChunkCount { get; private set; }

            public int Consume<TContext>(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, bool isStart, bool isComplete, TContext context)
            {
                ClassicAssert.IsFalse(SawFinal, "no chunk may follow the final chunk");
                stream.Write(first);
                stream.Write(second);
                ChunkCount++;
                if (isComplete)
                    SawFinal = true;
                // We always take the whole available chunk (both ring spans).
                return first.Length + second.Length;
            }

            public int Consume<TContext, TKey, TInput>(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, bool isStart, bool isComplete, TKey key, ref TInput input, TContext context)
                where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
                where TInput : IStoreInput
                => throw new NotSupportedException("Chunked network (migration/replication) serialization carries value bytes only (no key/input).");

            public byte[] Reassembled => stream.ToArray();
        }

        static byte[] MakeBytes(int length)
        {
            var b = new byte[length];
            for (var i = 0; i < length; i++)
                b[i] = (byte)(i * 31 + 7);
            return b;
        }

        [Test]
        [Category("TsavoriteLog")]
        public void ChunkedRawByteRoundTripTest([Values(1, 3, 16, 64, 1000)] int totalLength, [Values(4, 16, 64)] int bufferSize)
        {
            var input = MakeBytes(totalLength);
            var receiver = new ReassemblingConsumer();

            // Value-only chunker (network path): raw bytes fed in several slices to exercise partial fills and wrapping.
            var chunker = new ChunkedObjectSerializer<int>(receiver, bufferSize);
            chunker.BeginSerialize(context: 0);
            var offset = 0;
            foreach (var sliceLen in new[] { 1, 7, 50, totalLength }) // arbitrary component boundaries, clamped below
            {
                if (offset >= totalLength)
                    break;
                var len = Math.Min(sliceLen, totalLength - offset);
                chunker.WriteBytes(new ReadOnlySpan<byte>(input, offset, len));
                offset += len;
            }
            chunker.EndSerialize();

            ClassicAssert.IsTrue(receiver.SawFinal, "the final chunk must be delivered");
            ClassicAssert.GreaterOrEqual(receiver.ChunkCount, 1);
            CollectionAssert.AreEqual(input, receiver.Reassembled, "reassembled bytes must equal the input");
        }

        [Test]
        [Category("TsavoriteLog")]
        public void ChunkedEmptyStreamStillSignalsFinalTest()
        {
            var receiver = new ReassemblingConsumer();
            var chunker = new ChunkedObjectSerializer<int>(receiver, bufferSize: 8);
            chunker.BeginSerialize(context: 0);
            chunker.EndSerialize(); // no bytes written

            ClassicAssert.IsTrue(receiver.SawFinal, "an empty stream still delivers a final (zero-length) chunk");
            ClassicAssert.AreEqual(0, receiver.Reassembled.Length);
        }
    }
}