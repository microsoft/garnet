// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using Garnet.client;
using Garnet.cluster;
using Garnet.common;
using NUnit.Framework;

namespace Garnet.test.cluster
{
    [TestFixture]
    [NonParallelizable]
    public class AofSyncTaskInfoTests
    {
        /// <summary>
        /// Verifies that AofSyncTaskInfo.Dispose() disposes
        /// the owned GarnetClientSession. This prevents session
        /// leaks when replication tasks are replaced (dedup) or
        /// fail to be added to the AofTaskStore.
        /// </summary>
        [Test]
        public void DisposeReleasesGarnetClientSession()
        {
            // Arrange: create a GarnetClientSession (unconnected)
            var endpoint = new IPEndPoint(IPAddress.Loopback, 9999);
            var session = new GarnetClientSession(
                endpoint, new NetworkBufferSettings());

            Assert.That(session.Disposed, Is.False,
                "Session should not be disposed before test");

            // Create AofSyncTaskInfo with null clusterProvider
            // and aofTaskStore — Dispose() doesn't use them.
            var taskInfo = new AofSyncTaskInfo(
                clusterProvider: null,
                aofTaskStore: null,
                localNodeId: "local-node",
                remoteNodeId: "remote-node",
                garnetClient: session,
                startAddress: 64,
                logger: null);

            // Act
            taskInfo.Dispose();

            // Assert: the GarnetClientSession must be disposed
            Assert.That(session.Disposed, Is.True,
                "AofSyncTaskInfo.Dispose() must dispose the " +
                "owned GarnetClientSession to prevent leaks");
        }
    }
}