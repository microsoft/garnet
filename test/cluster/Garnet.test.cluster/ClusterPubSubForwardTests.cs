// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class ClusterPubSubForwardTests
    {
        ClusterTestContext context;
        readonly Dictionary<string, LogLevel> monitorTests = [];

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup(monitorTests);
        }

        [TearDown]
        public void TearDown()
        {
            context.TearDown();
        }

        /// <summary>
        /// Regression test for issues #1928 (NullReferenceException in TryClusterPublish) and
        /// #1929 (gossip client disposal cascade / process unresponsiveness).
        ///
        /// When a peer cluster node goes down, the surviving node still lists it as a peer and keeps
        /// forwarding publishes to it. The forwarding used to dereference a gossip client whose socket
        /// had been disposed, throwing on the network thread and permanently breaking cluster pub/sub
        /// (and eventually the whole process). After the fix, forwarding is skipped when the gossip
        /// client is not connected, so the surviving node stays responsive and keeps serving PUBLISH.
        ///
        /// The assertions are intentionally outcome-based (server stays responsive and PUBLISH
        /// recovers) rather than tied to a precise moment in time, so the test does not depend on
        /// gossip timing.
        /// </summary>
        [Test, Order(1), CancelAfter(120_000)]
        public void ClusterPublishSurvivesPeerNodeShutdown()
        {
            const int nodeCount = 2;
            const int publisherIndex = 0;
            const int peerIndex = 1;

            context.CreateInstances(nodeCount, disablePubSub: false);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: 2, replica_count: 0, logger: context.logger);

            // Verify both nodes are up and know about each other before we start publishing.
            context.clusterTestUtils.WaitUntilNodeIsKnown(publisherIndex, peerIndex, context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(peerIndex, publisherIndex, context.logger);

            var publisherEndpoint = context.clusterTestUtils.GetEndPoint(publisherIndex);
            var peerEndpoint = context.clusterTestUtils.GetEndPoint(peerIndex);

            var channel = RedisChannel.Literal("test-forward-channel");
            const string channelName = "test-forward-channel";
            const string message = "forwarded-message";

            // The cluster test multiplexer disables PUBLISH/SUBSCRIBE, so use dedicated connections
            // (default command map) for pub/sub: publish on the publisher node, subscribe on the peer
            // node. Delivery therefore proves that cross-node publish forwarding works.
            using var pubRedis = ConnectionMultiplexer.Connect(SingleNodeConfig(publisherEndpoint));
            using var subRedis = ConnectionMultiplexer.Connect(SingleNodeConfig(peerEndpoint));

            using var delivered = new ManualResetEventSlim(false);
            var subscriber = subRedis.GetSubscriber();
            subscriber.Subscribe(channel, (_, value) =>
            {
                if (value == message)
                    delivered.Set();
            });

            // Baseline: with both nodes up, a publish on the publisher node is forwarded and delivered.
            // StackExchange.Redis SUBSCRIBE can return before the subscription is actually active on
            // the server, so a single early publish may be silently dropped. Retry the publish until
            // the message is delivered (or we time out), which makes the baseline check robust against
            // that race rather than relying on one publish landing after the subscription is live.
            var baselineDeadline = Stopwatch.StartNew();
            var baselineDelivered = false;
            while (baselineDeadline.Elapsed < TimeSpan.FromSeconds(15))
            {
                Assert.DoesNotThrow(
                    () => pubRedis.GetServer(publisherEndpoint).Execute("PUBLISH", channelName, message),
                    "Baseline publish threw while both nodes were up");
                if (delivered.Wait(TimeSpan.FromMilliseconds(500)))
                {
                    baselineDelivered = true;
                    break;
                }
            }
            ClassicAssert.IsTrue(baselineDelivered,
                "Baseline publish was not forwarded/delivered while both nodes were up");

            subscriber.Unsubscribe(channel);

            // Shut down the peer node. The publisher still lists it as a cluster peer that owns slots,
            // so publish forwarding now targets a node whose gossip client connection is down.
            context.ShutdownNode(peerIndex);

            // Core assertions (timing-insensitive):
            //  1. The surviving node never hangs -> PING keeps returning PONG.
            //  2. PUBLISH on the surviving node recovers -> it does not throw permanently (pre-fix it
            //     kept throwing NullReferenceException and never recovered).
            //
            // We give the cluster a short settle window to detect the dead peer and recycle the
            // connection, then only count PUBLISH successes after that window, so the check is a
            // genuine recovery signal rather than incidental early successes on a stale socket.
            var settle = TimeSpan.FromSeconds(4);
            var total = TimeSpan.FromSeconds(15);
            var lateSuccesses = 0;
            const int requiredLateSuccesses = 8;
            Exception lastPublishError = null;

            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < total)
            {
                // (1) The surviving node must remain responsive throughout.
                ClassicAssert.IsTrue(NodeResponds(publisherIndex),
                    "Surviving node became unresponsive to PING after peer shutdown");

                // (2) Attempt to forward-publish from the surviving node.
                try
                {
                    _ = pubRedis.GetServer(publisherEndpoint).Execute("PUBLISH", channelName, message);
                    if (sw.Elapsed >= settle)
                        lateSuccesses++;
                }
                catch (Exception ex)
                {
                    // Transient errors are tolerated while the dead connection is torn down/recreated,
                    // but they must stop once forwarding to the down peer is skipped.
                    lastPublishError = ex;
                }

                if (lateSuccesses >= requiredLateSuccesses)
                    break;

                Thread.Sleep(100);
            }

            ClassicAssert.GreaterOrEqual(lateSuccesses, requiredLateSuccesses,
                $"PUBLISH on the surviving node did not recover after peer shutdown. Last error: {lastPublishError}");

            // Final sanity: the surviving node still serves commands.
            ClassicAssert.IsTrue(NodeResponds(publisherIndex), "Surviving node was unresponsive at end of test");
        }

        private static ConfigurationOptions SingleNodeConfig(EndPoint endpoint)
        {
            var config = new ConfigurationOptions
            {
                AbortOnConnectFail = false,
                ConnectRetry = 5,
                ConnectTimeout = 5000,
            };
            config.EndPoints.Add(endpoint);
            return config;
        }

        /// <summary>
        /// Returns true if the node answers PING with PONG. A brief client-side reconfiguration after
        /// a peer shutdown can surface as a transient connection or timeout exception; those are retried
        /// for a short bounded period. A node whose main thread is blocked will never answer and returns false.
        /// </summary>
        private bool NodeResponds(int nodeIndex)
        {
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < TimeSpan.FromSeconds(5))
            {
                try
                {
                    var pong = (string)context.clusterTestUtils.GetServer(nodeIndex).Execute("PING");
                    if (pong == "PONG")
                        return true;
                }
                catch (Exception ex) when (ex is RedisConnectionException or RedisTimeoutException)
                {
                    // Transient client-side reconfiguration or a slow/overloaded node after peer
                    // shutdown surfaces as a connection or timeout exception; retry within the window.
                }
                Thread.Sleep(100);
            }
            return false;
        }
    }
}