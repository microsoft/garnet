// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.networking;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Covers the release policy for per-session buffers that grow to the high-water mark of the
    /// largest request a session has served. Without it, one large or one unusually wide command
    /// permanently enlarges every session that saw it, so memory tracks session count rather than
    /// working set.
    ///
    /// The policy is a periodic checkpoint rather than per-batch demand tracking: the reset path runs
    /// on every batch and is hot enough that reading the buffer state there is measurable, so demand is
    /// inferred from whether the buffer grew between two checkpoints. A buffer that is needed on every
    /// batch is therefore released at most once per two intervals rather than never.
    /// </summary>
    [TestFixture]
    public class SessionBufferShrinkTests : TestBase
    {
        const int MaxRetained = 64 * 1024;
        const int Interval = ScratchBufferBuilder.ShrinkCheckInterval;

        [SetUp]
        public void Setup() => TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        [Test]
        public unsafe void ScratchBufferBuilderReleasesAfterOneLargeRequest()
        {
            var builder = new ScratchBufferBuilder(MaxRetained);
            var big = new byte[256 * 1024];

            var slice = builder.CreateArgSlice(big);
            ClassicAssert.IsTrue(builder.RewindScratchBuffer(slice));
            builder.ResetAtBatchBoundary();

            var grown = builder.ScratchBufferCapacity;
            ClassicAssert.GreaterOrEqual(grown, big.Length, "buffer should have grown to fit the request");

            // Sustained small work releases the capacity the session no longer needs.
            var small = new byte[64];
            for (var i = 0; i < Interval * 2; i++)
            {
                var s = builder.CreateArgSlice(small);
                ClassicAssert.IsTrue(builder.RewindScratchBuffer(s));
                builder.ResetAtBatchBoundary();
            }

            ClassicAssert.LessOrEqual(builder.ScratchBufferCapacity, MaxRetained,
                "scratch buffer should have been released back to the retained capacity");

            // Still usable after shrinking, including for another large request.
            var again = builder.CreateArgSlice(big);
            ClassicAssert.AreEqual(big.Length, again.Length);
            ClassicAssert.IsTrue(builder.RewindScratchBuffer(again));
        }

        /// <summary>
        /// A session that genuinely needs a large scratch buffer on every batch must not pay a pinned
        /// reallocation per batch. The checkpoint design permits bounded churn, so this pins the bound
        /// rather than asserting zero: without it the ratchet-free policy would be free to thrash.
        /// </summary>
        [Test]
        public unsafe void ScratchBufferBuilderChurnsAtMostOncePerCheckpointUnderSustainedLargeUse()
        {
            var builder = new ScratchBufferBuilder(MaxRetained);
            var big = new byte[256 * 1024];
            const int Batches = Interval * 8;

            var shrinks = 0;
            var previous = 0;
            for (var i = 0; i < Batches; i++)
            {
                var s = builder.CreateArgSlice(big);
                ClassicAssert.AreEqual(big.Length, s.Length, $"large request failed at iteration {i}");
                ClassicAssert.IsTrue(builder.RewindScratchBuffer(s));
                builder.ResetAtBatchBoundary();

                var capacity = builder.ScratchBufferCapacity;
                if (i > 0 && capacity < previous) shrinks++;
                previous = capacity;
            }

            // One release per two intervals is the design maximum; allow the boundary case.
            ClassicAssert.LessOrEqual(shrinks, Batches / (2 * Interval) + 1,
                "buffer churned more often than one release per two checkpoint intervals");
        }

        /// <summary>
        /// The checkpoint interval is counted in batches, so only the session's batch boundary may advance
        /// it. Plain <see cref="ScratchBufferBuilder.Reset"/> is called far more often -- the Lua
        /// interpreter resets once per string while decoding a JSON document, and once per response through
        /// <c>ScratchBufferNetworkSender</c> -- and driving the countdown from those would fire checkpoints
        /// many times inside a single command, releasing a buffer the very next element re-grows.
        /// </summary>
        [Test]
        public unsafe void NonBatchResetsDoNotAdvanceTheShrinkCheckpoint()
        {
            var builder = new ScratchBufferBuilder(MaxRetained);
            var big = new byte[256 * 1024];
            var small = new byte[64];

            var slice = builder.CreateArgSlice(big);
            ClassicAssert.IsTrue(builder.RewindScratchBuffer(slice));
            var grown = builder.ScratchBufferCapacity;
            ClassicAssert.GreaterOrEqual(grown, big.Length);

            // Far more resets than two checkpoint intervals, none of them at a batch boundary.
            for (var i = 0; i < Interval * 8; i++)
            {
                builder.Reset();
                var s = builder.CreateArgSlice(small);
                ClassicAssert.IsTrue(builder.RewindScratchBuffer(s));
            }

            ClassicAssert.AreEqual(grown, builder.ScratchBufferCapacity,
                "a mid-command reset advanced the checkpoint countdown, so the interval is not counted in batches");
        }

        [Test]
        public unsafe void ScratchBufferBuilderUnboundedByDefault()
        {
            var builder = new ScratchBufferBuilder();
            var big = new byte[256 * 1024];

            var slice = builder.CreateArgSlice(big);
            ClassicAssert.IsTrue(builder.RewindScratchBuffer(slice));

            var small = new byte[64];
            for (var i = 0; i < Interval * 8; i++)
            {
                builder.ResetAtBatchBoundary();
                var s = builder.CreateArgSlice(small);
                ClassicAssert.IsTrue(builder.RewindScratchBuffer(s));
            }

            ClassicAssert.GreaterOrEqual(builder.ScratchBufferCapacity, big.Length,
                "default construction must preserve legacy grow-forever behavior");
        }

        [Test]
        public unsafe void ScratchBufferBuilderZeroCapIsTreatedAsUnbounded()
        {
            // A zero or negative cap must never be read as "retain nothing", which would shrink every
            // buffer to an unusable size.
            var builder = new ScratchBufferBuilder(0);
            var big = new byte[256 * 1024];

            var slice = builder.CreateArgSlice(big);
            ClassicAssert.IsTrue(builder.RewindScratchBuffer(slice));

            for (var i = 0; i < Interval * 4; i++)
                builder.ResetAtBatchBoundary();

            ClassicAssert.GreaterOrEqual(builder.ScratchBufferCapacity, big.Length);
        }

        [Test]
        public unsafe void ParseStateRootBufferShrinksAfterWideCommand()
        {
            var parseState = new SessionParseState();
            parseState.Initialize();

            parseState.Initialize(20000);
            ClassicAssert.GreaterOrEqual(parseState.RootBufferLength, 20000);

            parseState.ShrinkRootBuffer(1024);
            ClassicAssert.AreEqual(1024, parseState.RootBufferLength,
                "root buffer should be released back to the retained argument capacity");

            // Still correct after shrinking: it regrows and arguments round-trip.
            parseState.Initialize(3);
            var arg = PinnedSpanByte.FromPinnedSpan("abc"u8);
            parseState.SetArgument(0, arg);
            ClassicAssert.AreEqual(3, parseState.Count);
            ClassicAssert.IsTrue(parseState.GetArgSliceByRef(0).ReadOnlySpan.SequenceEqual("abc"u8));

            parseState.Initialize(50000);
            ClassicAssert.GreaterOrEqual(parseState.RootBufferLength, 50000);
        }

        [Test]
        public unsafe void ParseStateShrinkNeverGoesBelowMinimum()
        {
            var parseState = new SessionParseState();
            parseState.Initialize();
            parseState.Initialize(20000);

            parseState.ShrinkRootBuffer(0);
            ClassicAssert.GreaterOrEqual(parseState.RootBufferLength, 1,
                "shrink must not leave an unusable root buffer");

            parseState.Initialize(2);
            var arg = PinnedSpanByte.FromPinnedSpan("hi"u8);
            parseState.SetArgument(0, arg);
            ClassicAssert.IsTrue(parseState.GetArgSliceByRef(0).ReadOnlySpan.SequenceEqual("hi"u8));
        }

        [Test]
        public unsafe void ParseStateShrinkIsANoOpWhenAlreadyWithinCap()
        {
            var parseState = new SessionParseState();
            parseState.Initialize();
            parseState.Initialize(16);

            var before = parseState.RootBufferLength;
            parseState.ShrinkRootBuffer(1024);
            ClassicAssert.AreEqual(before, parseState.RootBufferLength,
                "a buffer already within the cap must not be reallocated");
        }

        /// <summary>
        /// A request for exactly a size class must be served at that size. Rounding one byte past it doubles
        /// every such allocation permanently -- and the Lua reply window, at exactly 128 KiB, is one.
        /// </summary>
        [Test]
        public void ExactSizeClassRequestsAreNotRoundedToTheNextClass()
        {
            for (var size = 64; size <= 1 << 20; size <<= 1)
            {
                ClassicAssert.AreEqual(size, ScratchBufferBuilder.CapacityFor(size),
                    $"a request for exactly {size} bytes was rounded away from its size class");

                // The arithmetic helper is not the allocator. Assert the buffer a real expansion produces,
                // or the allocator can keep doubling exact requests while the helper stays honest.
                var builder = new ScratchBufferBuilder();
                _ = builder.CreateArgSlice(size);
                ClassicAssert.AreEqual(size, builder.ScratchBufferCapacity,
                    $"allocating exactly {size} bytes produced a {builder.ScratchBufferCapacity} byte buffer");
            }

            ClassicAssert.AreEqual(128, ScratchBufferBuilder.CapacityFor(65),
                "a request past a size class must still reach the next one");
            ClassicAssert.AreEqual(64, ScratchBufferBuilder.CapacityFor(1),
                "requests below the minimum must be served at the minimum");

            var past = new ScratchBufferBuilder();
            _ = past.CreateArgSlice(65);
            ClassicAssert.AreEqual(128, past.ScratchBufferCapacity,
                "allocating past a size class must still reach the next one");
        }

        /// <summary>
        /// The Lua script cache's dummy network sender owns a second scratch buffer, which the RESP fallback
        /// path for <c>redis.call</c> writes replies into. It was built uncapped, so one large reply on that
        /// path pinned it for the life of the connection. The sender has no batch boundary of its own, so the
        /// owning session drives its checkpoint; this pins that the cap is plumbed and releases.
        /// </summary>
        [Test]
        public unsafe void LuaReplySenderReleasesABufferGrownPastItsCap()
        {
            const int Cap = 16 * 1024;
            const int Big = 512 * 1024;

            var sender = new ScratchBufferNetworkSender(Cap);

            // Drive the buffer past the cap the way a large reply does: take a response window, declare it
            // written, and repeat until the cumulative offset exceeds Big.
            var written = 0;
            while (written < Big)
            {
                sender.EnterAndGetResponseObject(out var head, out var tail);
                var chunk = (int)(tail - head);
                _ = sender.SendResponse(0, chunk);
                written += chunk;
            }

            var grown = sender.ScratchBufferCapacityForTests;
            ClassicAssert.Greater(grown, Cap, "the probe did not grow the reply buffer past the cap");

            // Reclaim takes up to two checkpoints: the first records the new high, the second observes no
            // growth since and releases. The floor is one response window exactly -- a cap below that is
            // undone by the first subsequent redis.call, and a cap above it retains pinned memory nobody
            // asked for, so assert the value rather than merely that it fell.
            sender.ShrinkCheckpoint();
            sender.ShrinkCheckpoint();

            var window = ScratchBufferBuilder.CapacityFor(BufferSizeUtils.ServerBufferSize(new MaxSizeSettings()));
            var afterShrink = sender.ScratchBufferCapacityForTests;
            ClassicAssert.AreEqual(window, afterShrink,
                $"the reply buffer settled at {afterShrink} bytes rather than the {window} byte response window");
            ClassicAssert.Less(afterShrink, grown,
                $"the reply buffer stayed at {afterShrink} bytes after two checkpoints");

            // The cap must survive the next use. A response window is requested at the full server buffer
            // size on every call, so a cap below one window is undone by the first subsequent redis.call --
            // the buffer is released at the checkpoint and immediately reallocated larger than it started.
            sender.EnterAndGetResponseObject(out var h, out var t);
            _ = sender.SendResponse(0, 8);

            var afterNextUse = sender.ScratchBufferCapacityForTests;
            ClassicAssert.AreEqual(window, afterNextUse,
                $"one small reply took the buffer from {afterShrink} to {afterNextUse} bytes, against a {window} byte window");
            ClassicAssert.Less(afterNextUse, grown,
                $"the buffer returned to {afterNextUse} bytes, at or above the {grown} bytes the cap released");
            ClassicAssert.Greater((int)(t - h), 0, "the response window is empty");
        }
    }
}