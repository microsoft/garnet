// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Garnet.client;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers reuse of a single <see cref="GarnetClient"/> across a reconnect. The client keeps one fixed-size
    /// completion ring for the life of the instance, but each reconnect builds a fresh NetworkWriter whose
    /// task-id numbering restarts at zero. If the completion cursor and per-slot state are not realigned with
    /// that fresh numbering, requests issued after the reconnect are matched against the wrong ring slot and
    /// never complete.
    /// </summary>
    [TestFixture]
    public class GarnetClientReconnectTests : TestBase
    {
        static readonly TimeSpan Deadline = TimeSpan.FromSeconds(10);

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.OnTearDown();
        }

        [Test]
        public async Task ReconnectRealignsCompletionRingAfterPriorRequests()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            using var db = new GarnetClient(TestUtils.EndPoint, useTimeoutChecker: false);
            await db.ConnectAsync();

            // Advance the completion cursor and per-slot task-id state to a non-zero, unaligned position so the
            // reconnect below leaves the reused ring out of phase with the fresh writer (which restarts at zero).
            for (var i = 0; i < 5; i++)
                ClassicAssert.AreEqual("PONG", await db.PingAsync());

            // Drop and re-establish the connection on the SAME client.
            await db.ReconnectAsync();

            // A single round-trip after the reconnect must still complete. A misaligned ring routes the reply to
            // the wrong slot (or blocks slot acquisition), so the request never finishes -- caught here as a timeout.
            var pong = db.PingAsync();
            var finished = await Task.WhenAny(pong, Task.Delay(Deadline));
            Assert.That(finished, Is.SameAs(pong),
                "a request issued after reconnect never completed -- the reused completion ring was left out of " +
                "phase with the fresh NetworkWriter");
            ClassicAssert.AreEqual("PONG", await pong);

            // A value must round-trip to its own request, guarding against a reply matching the wrong slot
            // rather than merely hanging.
            ClassicAssert.IsTrue(await db.StringSetAsync("reconnect-key", "reconnect-value"));
            ClassicAssert.AreEqual("reconnect-value", await db.StringGetAsync("reconnect-key"));
        }
    }
}