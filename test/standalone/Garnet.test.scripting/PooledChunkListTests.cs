// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Ownership tests for <see cref="PooledChunkList"/>, the accumulator behind chunked AOF replay and chunked migration
    /// record reassembly.
    /// </summary>
    /// <remarks>
    /// Several paths may discard the same accumulation (an operation is dispatched, which releases it, and the buffer that
    /// held it is then cleared), so <see cref="PooledChunkList.Reset"/> must be idempotent. The failure it must not have is
    /// worse than a leak: returning one block twice pushes it onto a pool free list twice, and those lists are singly linked
    /// through the block itself, so the chain is corrupted and two renters are later handed the same memory.
    /// </remarks>
    [TestFixture]
    public class PooledChunkListTests
    {
        // Small buffers so a modest payload spans several of them; the pool only requires a legal alignment here.
        const int BufferSize = 4096;
        SectorAlignedBufferPool pool;

        [SetUp]
        public void Setup() => pool = new SectorAlignedBufferPool(1, 512);

        [TearDown]
        public void TearDown()
        {
            pool?.Free();
            pool = null;
        }

        static byte[] Payload(int length)
        {
            var data = new byte[length];
            for (var i = 0; i < length; i++)
                data[i] = (byte)(i * 31 + 7);
            return data;
        }

        /// <summary>Rent more blocks than were ever returned and assert none is handed out twice. A block returned more
        /// than once is reachable twice from the pool's free lists, so this is the observable form of the corruption.
        /// <see cref="SectorAlignedMemory"/> does not override equality, so the set compares by reference.</summary>
        void AssertPoolHandsOutDistinctBlocks()
        {
            var rented = new List<SectorAlignedMemory>();
            var seen = new HashSet<SectorAlignedMemory>();
            var duplicate = false;
            for (var i = 0; i < 16; i++)
            {
                var buffer = pool.Get(BufferSize, clearOnReturn: false);
                rented.Add(buffer);
                duplicate |= !seen.Add(buffer);
            }
            foreach (var buffer in rented)
                buffer.Return();
            ClassicAssert.IsFalse(duplicate, "the pool handed out the same block twice");
        }

        /// <summary>Resetting twice must return each buffer exactly once.</summary>
        [Test]
        public void DoubleResetDoesNotReturnABufferTwice()
        {
            var list = new PooledChunkList(pool, BufferSize);
            list.Append(Payload(BufferSize * 3 + 17));
            ClassicAssert.AreEqual(4, list.Count, "payload should span four buffers");

            list.Reset();
            list.Reset();
            list.Dispose();

            ClassicAssert.AreEqual(0, list.Count);
            ClassicAssert.AreEqual(0, list.TotalLength);
            ClassicAssert.IsTrue(list.IsEmpty);

            AssertPoolHandsOutDistinctBlocks();
        }

        /// <summary>A list must be reusable after reset, and must not return a buffer belonging to the previous round.</summary>
        [Test]
        public void ResetThenReuseIsClean()
        {
            var list = new PooledChunkList(pool, BufferSize);

            for (var round = 0; round < 4; round++)
            {
                var payload = Payload(BufferSize + round);
                list.Append(payload);
                ClassicAssert.AreEqual(payload.Length, list.TotalLength);
                CollectionAssert.AreEqual(payload, list.AsSequence().ToArray());

                list.Reset();
                ClassicAssert.AreEqual(0, list.Count);
                list.Reset();   // the overlapping discard paths
            }

            AssertPoolHandsOutDistinctBlocks();
        }

        /// <summary>The accumulated bytes must read back identically whether they fit one buffer or span many, since the
        /// sequence is consumed directly by the deserializer with no contiguous copy.</summary>
        [Test]
        public void SequenceRoundTripsAcrossBufferBoundaries([Values(1, BufferSize - 1, BufferSize, BufferSize + 1, BufferSize * 5 + 3)] int length)
        {
            using var list = new PooledChunkList(pool, BufferSize);
            var payload = Payload(length);

            // Append in irregular slices so buffer boundaries do not line up with append boundaries.
            var offset = 0;
            var slice = 1;
            while (offset < payload.Length)
            {
                var take = Math.Min(slice, payload.Length - offset);
                list.Append(payload.AsSpan(offset, take));
                offset += take;
                slice = slice * 3 + 1;
            }

            ClassicAssert.AreEqual(payload.Length, list.TotalLength);
            var sequence = list.AsSequence();
            ClassicAssert.AreEqual(payload.Length, sequence.Length);
            CollectionAssert.AreEqual(payload, sequence.ToArray());

            // Every chunk but the last is full, which is what lets GetChunk derive lengths without tracking them.
            for (var i = 0; i < list.Count - 1; i++)
                ClassicAssert.AreEqual(BufferSize, list.GetChunk(i).Length);
        }

        /// <summary>An empty list yields an empty sequence rather than throwing, since a record may carry no object bytes.</summary>
        [Test]
        public void EmptyListYieldsEmptySequence()
        {
            using var list = new PooledChunkList(pool, BufferSize);
            ClassicAssert.IsTrue(list.IsEmpty);
            ClassicAssert.AreEqual(0, list.AsSequence().Length);
            list.Reset();
            ClassicAssert.AreEqual(0, list.AsSequence().Length);
        }
    }
}