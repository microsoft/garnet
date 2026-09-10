// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
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
    /// </summary>
    [TestFixture]
    public class SessionBufferShrinkTests : TestBase
    {
        const int MaxRetained = 64 * 1024;
        const int Hysteresis = 8;

        [SetUp]
        public void Setup() => TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        [Test]
        public void ShrinkPolicyReleasesOnlyAfterSustainedDisuse()
        {
            var policy = new BufferShrinkPolicy(MaxRetained, Hysteresis);

            // Within budget: never shrinks, regardless of how long it runs.
            for (var i = 0; i < Hysteresis * 4; i++)
                ClassicAssert.IsFalse(policy.ShouldShrink(MaxRetained, 1024));

            // Oversized but genuinely used every batch: keeps its buffer, so a steady large-payload
            // workload never churns pinned reallocations.
            for (var i = 0; i < Hysteresis * 4; i++)
                ClassicAssert.IsFalse(policy.ShouldShrink(1 << 20, MaxRetained + 1));

            // Oversized and unused: released, but only once hysteresis has elapsed.
            for (var i = 0; i < Hysteresis - 1; i++)
                ClassicAssert.IsFalse(policy.ShouldShrink(1 << 20, 16), $"shrank early at {i}");
            ClassicAssert.IsTrue(policy.ShouldShrink(1 << 20, 16));
        }

        [Test]
        public void ShrinkPolicyUsageResetsTheIdleCount()
        {
            var policy = new BufferShrinkPolicy(MaxRetained, Hysteresis);

            for (var i = 0; i < Hysteresis - 1; i++)
                ClassicAssert.IsFalse(policy.ShouldShrink(1 << 20, 16));

            // A single batch that needs the capacity restarts the countdown.
            ClassicAssert.IsFalse(policy.ShouldShrink(1 << 20, MaxRetained + 1));

            for (var i = 0; i < Hysteresis - 1; i++)
                ClassicAssert.IsFalse(policy.ShouldShrink(1 << 20, 16), $"shrank early at {i}");
            ClassicAssert.IsTrue(policy.ShouldShrink(1 << 20, 16));
        }

        [Test]
        public void ShrinkPolicyDisabledByDefaultAndByZero()
        {
            // Both the explicit sentinel and a default-initialized struct must be inert; a
            // default instance must never be read as "retain nothing".
            var unbounded = new BufferShrinkPolicy(BufferShrinkPolicy.Unbounded);
            ClassicAssert.IsFalse(unbounded.IsEnabled);

            var defaulted = default(BufferShrinkPolicy);
            ClassicAssert.IsFalse(defaulted.IsEnabled);
            for (var i = 0; i < 1000; i++)
                ClassicAssert.IsFalse(defaulted.ShouldShrink(1 << 24, 0));

            var zero = new BufferShrinkPolicy(0);
            ClassicAssert.IsFalse(zero.IsEnabled);
            for (var i = 0; i < 1000; i++)
                ClassicAssert.IsFalse(zero.ShouldShrink(1 << 24, 0));
        }

        [Test]
        public unsafe void ScratchBufferBuilderReleasesAfterOneLargeRequest()
        {
            var builder = new ScratchBufferBuilder(MaxRetained, Hysteresis);
            var big = new byte[256 * 1024];

            var slice = builder.CreateArgSlice(big);
            ClassicAssert.IsTrue(builder.RewindScratchBuffer(slice));
            builder.Reset();

            var grown = builder.ScratchBufferCapacity;
            ClassicAssert.GreaterOrEqual(grown, big.Length, "buffer should have grown to fit the request");

            // Sustained small work releases the capacity the session no longer needs.
            var small = new byte[64];
            for (var i = 0; i < Hysteresis; i++)
            {
                var s = builder.CreateArgSlice(small);
                ClassicAssert.IsTrue(builder.RewindScratchBuffer(s));
                builder.Reset();
            }

            ClassicAssert.LessOrEqual(builder.ScratchBufferCapacity, MaxRetained,
                "scratch buffer should have been released back to the retained capacity");

            // Still usable after shrinking, including for another large request.
            var again = builder.CreateArgSlice(big);
            ClassicAssert.AreEqual(big.Length, again.Length);
            ClassicAssert.IsTrue(builder.RewindScratchBuffer(again));
        }

        [Test]
        public unsafe void ScratchBufferBuilderKeepsBufferUnderSustainedLargeUse()
        {
            var builder = new ScratchBufferBuilder(MaxRetained, Hysteresis);
            var big = new byte[256 * 1024];

            for (var i = 0; i < Hysteresis * 4; i++)
            {
                var s = builder.CreateArgSlice(big);
                ClassicAssert.IsTrue(builder.RewindScratchBuffer(s));
                builder.Reset();
                ClassicAssert.GreaterOrEqual(builder.ScratchBufferCapacity, big.Length,
                    $"buffer churned at iteration {i} despite being needed every batch");
            }
        }

        [Test]
        public unsafe void ScratchBufferBuilderUnboundedByDefault()
        {
            var builder = new ScratchBufferBuilder();
            var big = new byte[256 * 1024];

            var slice = builder.CreateArgSlice(big);
            ClassicAssert.IsTrue(builder.RewindScratchBuffer(slice));

            var small = new byte[64];
            for (var i = 0; i < 1000; i++)
            {
                builder.Reset();
                var s = builder.CreateArgSlice(small);
                ClassicAssert.IsTrue(builder.RewindScratchBuffer(s));
            }

            ClassicAssert.GreaterOrEqual(builder.ScratchBufferCapacity, big.Length,
                "default construction must preserve legacy grow-forever behavior");
        }

        [Test]
        public unsafe void ParseStateRootBufferShrinksAfterWideCommand()
        {
            var parseState = new SessionParseState();
            parseState.Initialize();

            parseState.Initialize(20000);
            ClassicAssert.GreaterOrEqual(parseState.RootBufferLength, 20000);
            ClassicAssert.AreEqual(20000, parseState.BatchHighWater);

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
        public unsafe void ParseStateHighWaterTracksWidestCommandInBatch()
        {
            var parseState = new SessionParseState();
            parseState.Initialize();

            parseState.Initialize(10);
            parseState.Initialize(5000);
            parseState.Initialize(7);

            ClassicAssert.AreEqual(5000, parseState.BatchHighWater,
                "high-water must reflect the widest command in the batch, not the last one");

            parseState.ResetBatchHighWater();
            ClassicAssert.AreEqual(0, parseState.BatchHighWater);
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
    }
}