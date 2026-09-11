// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
    }
}