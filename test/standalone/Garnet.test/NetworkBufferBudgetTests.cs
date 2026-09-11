// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers the process-wide network buffer budget: the sizing formula, the hysteresis band that keeps it
    /// from oscillating between size classes, the conservation of the live buffer count that the formula
    /// divides by, and the fact that the published target is inert at default settings.
    /// </summary>
    /// <remarks>
    /// At this stage the target is published but consumed by no allocation site, so these tests pin the
    /// signal itself. That ordering is deliberate: it makes a drift bug in the live buffer count observable
    /// on its own, rather than tangled with a clamping bug at an allocation site.
    /// </remarks>
    [TestFixture]
    public class NetworkBufferBudgetTests : TestBase
    {
        const int Ceiling = 1 << 17;      // 128 KB, the shipped --network-buffer-size
        const int ReceiveFloor = 1 << 14; // 16 KB
        const int SendFloor = 1 << 16;    // 64 KB

        GarnetServer server;

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.OnTearDown();
        }

        static NetworkBufferBudget Budget(long budgetBytes = 1L << 30)
            => new(budgetBytes, Ceiling, ReceiveFloor, SendFloor);

        #region sizing formula

        /// <summary>
        /// The whole point of the design: while the quotient is at or above the configured size, the target
        /// is the configured size and adaptation is arithmetically inert.
        /// </summary>
        [Test]
        public void TargetPinsAtCeilingWhileBudgetIsSlack()
        {
            var budget = Budget();

            // 1 GB / 128 KB = 8,192 buffers before the quotient reaches the configured size.
            ClassicAssert.AreEqual(Ceiling, budget.TargetForCount(1));
            ClassicAssert.AreEqual(Ceiling, budget.TargetForCount(100));
            ClassicAssert.AreEqual(Ceiling, budget.TargetForCount(8192));
        }

        /// <summary>
        /// Past the threshold the target steps down through pool size classes, never to something the pool
        /// could not recycle.
        /// </summary>
        [Test]
        public void TargetStepsDownThroughPowersOfTwo()
        {
            var budget = Budget();

            ClassicAssert.AreEqual(1 << 16, budget.TargetForCount(8193));   // just past the threshold
            ClassicAssert.AreEqual(1 << 16, budget.TargetForCount(16384));
            ClassicAssert.AreEqual(1 << 15, budget.TargetForCount(32768));
            ClassicAssert.AreEqual(1 << 14, budget.TargetForCount(65536));
        }

        /// <summary>
        /// The incident shape. 9,212 live buffers against the default budget must land one step below the
        /// configured size, which is the halving the design predicts.
        /// </summary>
        [Test]
        public void IncidentShapeLandsOneStepBelowConfiguredSize()
        {
            ClassicAssert.AreEqual(1 << 16, Budget().TargetForCount(9212));
        }

        /// <summary>
        /// The target must never fall below the floor no matter how extreme the count, because a buffer
        /// below the pool's smallest size class can never be recycled.
        /// </summary>
        [Test]
        public void TargetNeverFallsBelowTheFloor()
        {
            var budget = Budget();
            ClassicAssert.AreEqual(ReceiveFloor, budget.TargetForCount(long.MaxValue));
            ClassicAssert.AreEqual(ReceiveFloor, budget.TargetForCount(1_000_000_000));
        }

        /// <summary>
        /// A zero budget is the production escape hatch and must restore the configured size unconditionally.
        /// </summary>
        [Test]
        public void DisabledBudgetAlwaysReportsTheConfiguredSize()
        {
            var budget = Budget(budgetBytes: 0);
            ClassicAssert.IsFalse(budget.IsEnabled);
            ClassicAssert.AreEqual(Ceiling, budget.TargetForCount(1));
            ClassicAssert.AreEqual(Ceiling, budget.TargetForCount(1_000_000));
            ClassicAssert.AreEqual(Ceiling, budget.TargetBufferSize);
        }

        /// <summary>
        /// Send buffers carry a higher floor than receive buffers, and neither floor may raise the base size
        /// above what was configured.
        /// </summary>
        [Test]
        public void SendAndReceiveFloorsAreSeparateAndNeverExceedTheCeiling()
        {
            var budget = Budget();
            for (var i = 0; i < 1_000_000; i++)
                budget.OnBufferAcquired();
            budget.Recompute();

            ClassicAssert.AreEqual(ReceiveFloor, budget.TargetReceiveBufferSize);
            ClassicAssert.AreEqual(SendFloor, budget.TargetSendBufferSize);

            // A floor configured above the buffer size must not raise the base size; adaptation only lowers.
            var tight = new NetworkBufferBudget(1L << 30, 1 << 14, 1 << 16, 1 << 18);
            ClassicAssert.AreEqual(1 << 14, tight.TargetReceiveBufferSize);
            ClassicAssert.AreEqual(1 << 14, tight.TargetSendBufferSize);
        }

        #endregion

        #region hysteresis

        /// <summary>
        /// The actuation step is a factor of two, so the deadband must be at least a factor of two or the
        /// target limit-cycles between adjacent size classes. Walking the count up one buffer at a time must
        /// produce a monotonically non-increasing target with no re-expansion.
        /// </summary>
        [Test]
        public void TargetNeverOscillatesWhileTheCountGrows()
        {
            var budget = Budget();
            var previous = budget.TargetBufferSize;
            var increases = 0;

            for (var i = 0; i < 200_000; i++)
            {
                budget.OnBufferAcquired();
                budget.Recompute();
                var current = budget.TargetBufferSize;
                if (current > previous) increases++;
                previous = current;
            }

            ClassicAssert.AreEqual(0, increases, "target grew while the live buffer count was only rising");
            ClassicAssert.AreEqual(ReceiveFloor, previous);
        }

        /// <summary>
        /// Growth requires the quotient to reach twice the published target, not merely to exceed it. Inside
        /// that band the target must be pinned, which is what stops a workload sitting near a size-class
        /// boundary from flapping.
        /// </summary>
        [Test]
        public void GrowthRequiresTwiceThePublishedTarget()
        {
            // 64 buffers of budget each, so the quotient starts at the ceiling and the target with it.
            var budget = new NetworkBufferBudget(Ceiling * 64L, Ceiling, ReceiveFloor, SendFloor);
            for (var i = 0; i < 256; i++)
                budget.OnBufferAcquired();
            budget.Recompute();

            // 64 * 128 KB / 256 = 32 KB.
            ClassicAssert.AreEqual(1 << 15, budget.TargetBufferSize);

            // Release back to 129 buffers, a quotient of ~63.5 KB: above the published 32 KB, but short of
            // twice it, so the target must hold.
            for (var i = 0; i < 127; i++)
                budget.OnBufferReleased();
            budget.Recompute();
            ClassicAssert.AreEqual(1 << 15, budget.TargetBufferSize, "target grew inside the deadband");

            // One more release takes the quotient to exactly 64 KB, twice the target, which permits a step.
            budget.OnBufferReleased();
            budget.Recompute();
            ClassicAssert.AreEqual(1 << 16, budget.TargetBufferSize);
        }

        /// <summary>
        /// Draining every buffer must restore the configured size exactly, not leave the target stuck low.
        /// </summary>
        [Test]
        public void TargetReturnsToTheCeilingWhenConnectionsDrain()
        {
            var budget = Budget();
            for (var i = 0; i < 100_000; i++)
                budget.OnBufferAcquired();
            budget.Recompute();
            ClassicAssert.AreEqual(ReceiveFloor, budget.TargetBufferSize);

            for (var i = 0; i < 100_000; i++)
                budget.OnBufferReleased();
            budget.Recompute();

            ClassicAssert.AreEqual(0, budget.LiveBufferCount);
            ClassicAssert.AreEqual(Ceiling, budget.TargetBufferSize);
        }

        #endregion

        #region conservation of the live buffer count

        /// <summary>
        /// The target is budget divided by this count, so a count that ratchets up does not degrade
        /// gracefully -- it collapses the target to the floor permanently. Every path out of the pool must
        /// therefore balance, including the one where a returned buffer is dropped rather than pooled.
        /// </summary>
        [Test]
        public void PoolGetAndReturnConserveTheLiveBufferCount()
        {
            var budget = Budget();
            var settings = new NetworkBufferSettings(Ceiling, Ceiling, 1 << 20);
            // A tiny idle cap, so most returns take the over-budget drop path rather than being pooled.
            using var pool = settings.CreateBufferPool(ownerType: PoolOwnerType.ServerNetwork, maxPooledBytes: Ceiling, budget: budget);

            var entries = new List<PoolEntry>();
            for (var i = 0; i < 64; i++)
                entries.Add(pool.Get(Ceiling));
            ClassicAssert.AreEqual(64, budget.LiveBufferCount);

            foreach (var entry in entries)
                entry.Dispose();
            ClassicAssert.AreEqual(0, budget.LiveBufferCount, "a return path skipped the live buffer count");

            // Out-of-bound sizes bypass the size classes entirely and must still balance.
            var oversized = pool.Get(1 << 22);
            ClassicAssert.AreEqual(1, budget.LiveBufferCount);
            oversized.Dispose();
            ClassicAssert.AreEqual(0, budget.LiveBufferCount);
        }

        /// <summary>
        /// Pools that are not connection-scaled must not touch the budget at all, or replication and
        /// migration traffic would drive live client connections toward the floor.
        /// </summary>
        [Test]
        public void PoolsWithoutABudgetDoNotAffectIt()
        {
            var budget = Budget();
            var settings = new NetworkBufferSettings(Ceiling, Ceiling, 1 << 20);
            using var unbudgeted = settings.CreateBufferPool(ownerType: PoolOwnerType.Replication);

            var entry = unbudgeted.Get(Ceiling);
            ClassicAssert.AreEqual(0, budget.LiveBufferCount);
            ClassicAssert.AreEqual(Ceiling, budget.TargetBufferSize);
            entry.Dispose();
        }

        /// <summary>
        /// The falsifying test for the clamp itself. Everything else pins the published signal; this pins that
        /// an allocation site consumes it, by measuring the bytes the server says its live connections hold.
        /// </summary>
        [Test]
        public void SmallBudgetShrinksTheBytesHeldByLiveConnections()
        {
            // Disabled budget first, so the control is today's behaviour on the same build.
            var unbudgeted = MeasureLiveBytesPerConnection("0");
            var budgeted = MeasureLiveBytesPerConnection("512k");

            // Receive buffers drop from 128 KB to their 16 KB floor and send buffers from 128 KB to their
            // higher 64 KB floor, so the per-connection total falls to roughly a third.
            ClassicAssert.Less(budgeted, unbudgeted * 0.5,
                $"live bytes per connection did not fall under budget pressure: {unbudgeted} -> {budgeted}");
        }

        /// <summary>
        /// Under pressure a grown buffer must come back without waiting out the long idle hysteresis, or the
        /// aggregate cannot converge in time to matter. The control arm -- identical workload, budget disabled
        /// -- must still be holding its grown buffers after the same handful of small receives, which is what
        /// proves the pressure gate rather than the countdown did the work.
        /// </summary>
        [Test]
        public void PressureShrinksGrownBuffersWithoutWaitingOutTheIdleHysteresis()
        {
            var (budgetedSettled, budgetedBaseline, pressureShrinks) = MeasureShrinkAfterBurst("1m");
            var (controlSettled, controlBaseline, controlPressureShrinks) = MeasureShrinkAfterBurst("0");

            ClassicAssert.Greater(pressureShrinks, 0, "no pressure shrink was recorded under a small budget");
            ClassicAssert.AreEqual(0, controlPressureShrinks, "an unbudgeted server must never record a pressure shrink");

            // Far fewer than ShrinkHysteresis receives happened, so only the pressure path can have released these.
            ClassicAssert.LessOrEqual(budgetedSettled, budgetedBaseline * 1.2,
                $"grown buffers were not released under pressure (settled={budgetedSettled}, baseline={budgetedBaseline})");
            ClassicAssert.Greater(controlSettled, controlBaseline * 1.5,
                $"the control arm released its buffers too, so the pressure gate is not what is being measured " +
                $"(settled={controlSettled}, baseline={controlBaseline})");
        }

        /// <summary>
        /// Connects, grows every connection's receive buffer with one large request, then does a handful of
        /// small round trips -- far short of the idle hysteresis. Returns settled and baseline live bytes
        /// together with the pressure-shrink count.
        /// </summary>
        (long Settled, long Baseline, long PressureShrinks) MeasureShrinkAfterBurst(string budget)
        {
            server?.Dispose();
            server = null;
            StartServer(networkBufferMemoryBudget: budget);

            const int Connections = 24;
            const int PayloadLength = 400 * 1024;
            var payload = new string('p', PayloadLength);

            var sockets = new List<Socket>();
            try
            {
                for (var i = 0; i < Connections; i++)
                    sockets.Add(Ping(Connect()));
                var baseline = SocketStatBytes("liveBytes");

                foreach (var s in sockets)
                {
                    Send(s, $"*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n${PayloadLength}\r\n{payload}\r\n");
                    ClassicAssert.AreEqual("+OK\r\n", ReadExactly(s, 5));
                }

                // Ten rounds, against a 256-receive idle hysteresis.
                for (var round = 0; round < 10; round++)
                    foreach (var s in sockets)
                        _ = Ping(s);

                return (SocketStatBytes("liveBytes"), baseline, StatValue("pressureShrinks"));
            }
            finally
            {
                foreach (var s in sockets) s.Dispose();
            }
        }

        /// <summary>
        /// Adaptation governs the base size only. A response far larger than the adapted send buffer, and a
        /// request far larger than the adapted receive buffer, must still succeed -- the send side by chunking
        /// through the buffer it was given, the receive side by growing on demand.
        /// </summary>
        [Test]
        public void LargePayloadsStillSucceedWhileTheBudgetIsAtItsFloor()
        {
            StartServer(networkBufferMemoryBudget: "512k");

            var connections = new List<Socket>();
            try
            {
                // Drive the target to the floor before the payload connection is opened.
                for (var i = 0; i < 60; i++)
                    connections.Add(Ping(Connect()));
                ClassicAssert.AreEqual(ReceiveFloor, StatBytes("targetBufferSize"));

                using var s = Connect();

                // A 1 MB value is well beyond both floors in both directions.
                const int ValueLength = 1 << 20;
                var value = new string('v', ValueLength);
                Send(s, $"*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n${ValueLength}\r\n{value}\r\n");
                ClassicAssert.AreEqual("+OK\r\n", ReadExactly(s, 5));

                Send(s, "*2\r\n$3\r\nGET\r\n$3\r\nbig\r\n");
                var header = $"${ValueLength}\r\n";
                var reply = ReadExactly(s, header.Length + ValueLength + 2);
                ClassicAssert.IsTrue(reply.StartsWith(header, StringComparison.Ordinal), "unexpected bulk string header");
                ClassicAssert.IsTrue(reply.EndsWith("\r\n", StringComparison.Ordinal));
                ClassicAssert.AreEqual(ValueLength, reply.Length - header.Length - 2);

                // A key longer than the send buffer exercises the chunked write path rather than the growing one.
                var longKey = new string('k', 200 * 1024);
                Send(s, $"*3\r\n$3\r\nSET\r\n${longKey.Length}\r\n{longKey}\r\n$1\r\nx\r\n");
                ClassicAssert.AreEqual("+OK\r\n", ReadExactly(s, 5));
            }
            finally
            {
                foreach (var c in connections) c.Dispose();
            }
        }

        double MeasureLiveBytesPerConnection(string networkBufferMemoryBudget)
        {
            server?.Dispose();
            server = null;
            StartServer(networkBufferMemoryBudget: networkBufferMemoryBudget);

            const int Count = 50;
            var connections = new List<Socket>();
            try
            {
                for (var i = 0; i < Count; i++)
                    connections.Add(Ping(Connect()));

                return (double)SocketStatBytes("liveBytes") / Count;
            }
            finally
            {
                foreach (var c in connections) c.Dispose();
            }
        }

        #endregion

        #region end to end

        /// <summary>
        /// The signal must be observable from outside the process, and must read as inert at the shipped
        /// defaults with an ordinary number of connections.
        /// </summary>
        [Test]
        public void DefaultConfigPublishesTheConfiguredSizeToInfo()
        {
            StartServer();

            var connections = new List<Socket>();
            try
            {
                for (var i = 0; i < 20; i++)
                    connections.Add(Ping(Connect()));

                ClassicAssert.AreEqual(1L << 30, StatBytes("budgetBytes"));
                ClassicAssert.AreEqual(Ceiling, StatBytes("targetBufferSize"));
                ClassicAssert.Greater(StatValue("liveBufferCount"), 0);
            }
            finally
            {
                foreach (var c in connections) c.Dispose();
            }
        }

        /// <summary>
        /// The falsifying half: with a budget small enough for the connections in the test to exhaust, the
        /// published target must actually fall. Without this the whole suite would pass against a target
        /// hardcoded to the configured size.
        /// </summary>
        [Test]
        public void SmallBudgetDrivesThePublishedTargetDown()
        {
            // 512 KB of budget, so a handful of connections is already past the threshold.
            StartServer(networkBufferMemoryBudget: "512k");

            var connections = new List<Socket>();
            try
            {
                for (var i = 0; i < 40; i++)
                    connections.Add(Ping(Connect()));

                var target = StatBytes("targetBufferSize");
                ClassicAssert.Less(target, Ceiling, "target did not adapt under a budget it cannot satisfy");
                ClassicAssert.GreaterOrEqual(target, ReceiveFloor, "target fell below the receive floor");
            }
            finally
            {
                foreach (var c in connections) c.Dispose();
            }
        }

        /// <summary>
        /// Zero restores today's behaviour exactly, which is the escape hatch operators are told to use.
        /// </summary>
        [Test]
        public void ZeroBudgetDisablesAdaptationEndToEnd()
        {
            StartServer(networkBufferMemoryBudget: "0");

            var connections = new List<Socket>();
            try
            {
                for (var i = 0; i < 40; i++)
                    connections.Add(Ping(Connect()));

                ClassicAssert.AreEqual(0, StatBytes("budgetBytes"));
                ClassicAssert.AreEqual(Ceiling, StatBytes("targetBufferSize"));
            }
            finally
            {
                foreach (var c in connections) c.Dispose();
            }
        }

        #endregion

        #region helpers

        void StartServer(string networkBufferMemoryBudget = null)
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, networkBufferMemoryBudget: networkBufferMemoryBudget);
            server.Start();
        }

        static Socket Connect()
        {
            var s = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
            s.Connect(TestUtils.EndPoint);
            return s;
        }

        /// <summary>
        /// Drive one round trip so the connection's buffers are actually allocated before the count is read.
        /// </summary>
        static Socket Ping(Socket s)
        {
            s.Send(Encoding.ASCII.GetBytes("*1\r\n$4\r\nPING\r\n"));
            var buf = new byte[64];
            _ = s.Receive(buf);
            return s;
        }

        static void Send(Socket s, string command)
        {
            var bytes = Encoding.ASCII.GetBytes(command);
            var sent = 0;
            while (sent < bytes.Length)
                sent += s.Send(bytes, sent, bytes.Length - sent, SocketFlags.None);
        }

        /// <summary>
        /// Reads exactly <paramref name="count"/> bytes, since a large reply arrives in many chunks.
        /// </summary>
        static string ReadExactly(Socket s, int count)
        {
            var buf = new byte[count];
            var read = 0;
            while (read < count)
            {
                var n = s.Receive(buf, read, count - read, SocketFlags.None);
                if (n == 0) throw new Exception("connection closed");
                read += n;
            }
            return Encoding.ASCII.GetString(buf, 0, count);
        }

        static string BpStats()
        {
            using var s = Connect();
            s.Send(Encoding.ASCII.GetBytes("*2\r\n$4\r\nINFO\r\n$7\r\nBPSTATS\r\n"));
            var buf = new byte[32 * 1024];
            var n = s.Receive(buf);
            return Encoding.ASCII.GetString(buf, 0, n);
        }

        static string StatRaw(string name)
        {
            var stats = BpStats();
            // Read from the shared budget section, not from a per-socket pool section.
            var section = stats.IndexOf("network_buffer_budget", StringComparison.Ordinal);
            ClassicAssert.GreaterOrEqual(section, 0, $"budget section not found in BPSTATS: {stats}");

            var marker = name + "=";
            var idx = stats.IndexOf(marker, section, StringComparison.Ordinal);
            ClassicAssert.GreaterOrEqual(idx, 0, $"'{name}' not found in BPSTATS: {stats}");
            idx += marker.Length;
            var end = stats.IndexOfAny([',', '\r', '\n'], idx);
            return stats[idx..end];
        }

        static long StatValue(string name) => long.Parse(StatRaw(name));

        /// <summary>
        /// Reads a stat from the listener's own pool section rather than the shared budget section.
        /// </summary>
        static long SocketStatBytes(string name)
        {
            var stats = BpStats();
            var section = stats.IndexOf("server_socket_0", StringComparison.Ordinal);
            ClassicAssert.GreaterOrEqual(section, 0, $"socket section not found in BPSTATS: {stats}");

            var marker = name + "=";
            var idx = stats.IndexOf(marker, section, StringComparison.Ordinal);
            ClassicAssert.GreaterOrEqual(idx, 0, $"'{name}' not found in BPSTATS: {stats}");
            idx += marker.Length;
            var end = stats.IndexOfAny([',', '\r', '\n'], idx);
            return ParseMemoryBytes(stats[idx..end]);
        }

        static long StatBytes(string name)
        {
            var value = StatRaw(name);
            return ParseMemoryBytes(value);
        }

        static long ParseMemoryBytes(string value)
        {
            // Format.MemoryBytes emits e.g. "123KB", "1.50MB", "2.00GB".
            var (suffix, scale) = value.EndsWith("KB", StringComparison.Ordinal) ? ("KB", 1L << 10)
                : value.EndsWith("MB", StringComparison.Ordinal) ? ("MB", 1L << 20)
                : value.EndsWith("GB", StringComparison.Ordinal) ? ("GB", 1L << 30)
                : ("", 1L);
            var number = suffix.Length == 0 ? value : value[..^suffix.Length];
            return (long)(double.Parse(number, System.Globalization.CultureInfo.InvariantCulture) * scale);
        }

        #endregion
    }
}