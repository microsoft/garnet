// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using Garnet.server;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class ScheduledCheckpointClusterTests : TestBase
    {
        ClusterTestContext context;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup(new Dictionary<string, LogLevel>(), testTimeoutSeconds: 120);
        }

        [TearDown]
        public void TearDown() => context.TearDown();

        DateTimeOffset LastSave(int nodeIndex) => context.nodes[nodeIndex].Provider.StoreWrapper.DefaultDatabase.LastSaveTime;

        void WaitForCheckpointAfter(int nodeIndex, DateTimeOffset prior)
        {
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (LastSave(nodeIndex) <= prior && DateTime.UtcNow < deadline)
                Thread.Sleep(25);
            Assert.That(LastSave(nodeIndex), Is.GreaterThan(prior));
        }

        void WaitForScheduledCheckpointTaskToStop(int nodeIndex)
        {
            var taskManager = context.nodes[nodeIndex].Provider.StoreWrapper.TaskManager;
            Assert.That(SpinWait.SpinUntil(() => !taskManager.IsRegistered(TaskType.ScheduledCheckpointTask), TimeSpan.FromSeconds(15)), Is.True);
        }

        [Test, CancelAfter(120_000)]
        public void ScheduledCheckpointFollowsPrimaryAndReplicates()
        {
            context.CreateInstances(2, enableAOF: true);
            context.MeetAndAssignSlotsAllNodes(2);
            context.clusterTestUtils.AttachReplicaToPrimary(1, 0, waitForRecovery: true, logger: context.logger);

            Assert.That(context.nodes[0].Provider.StoreWrapper.TaskManager.IsRegistered(TaskType.ScheduledCheckpointTask), Is.False);
            context.clusterTestUtils.ConfigSet(0, "checkpoint-freq", "1", context.logger);
            context.clusterTestUtils.ConfigSet(1, "checkpoint-freq", "1", context.logger);
            Assert.That(context.nodes[0].Provider.StoreWrapper.TaskManager.IsRunning(TaskType.ScheduledCheckpointTask), Is.True);
            Assert.That(context.nodes[1].Provider.StoreWrapper.TaskManager.IsRunning(TaskType.ScheduledCheckpointTask), Is.False);

            var primaryBefore = LastSave(0);
            var replicaBefore = LastSave(1);
            Assert.That(context.clusterTestUtils.Execute(context.clusterTestUtils.GetEndPoint(0), "SET", ["{checkpoint}key", "1"]).ToString(), Is.EqualTo("OK"));
            WaitForCheckpointAfter(0, primaryBefore);
            WaitForCheckpointAfter(1, replicaBefore);

            context.FailoverTo(1, 0);
            Assert.That(context.nodes[0].Provider.StoreWrapper.TaskManager.IsRunning(TaskType.ScheduledCheckpointTask), Is.False);
            Assert.That(context.nodes[1].Provider.StoreWrapper.TaskManager.IsRunning(TaskType.ScheduledCheckpointTask), Is.True);

            primaryBefore = LastSave(1);
            replicaBefore = LastSave(0);
            Assert.That(context.clusterTestUtils.Execute(context.clusterTestUtils.GetEndPoint(1), "SET", ["{checkpoint}key", "2"]).ToString(), Is.EqualTo("OK"));
            WaitForCheckpointAfter(1, primaryBefore);
            WaitForCheckpointAfter(0, replicaBefore);

            context.clusterTestUtils.ConfigSet(1, "checkpoint-freq", "0", context.logger);
            WaitForScheduledCheckpointTaskToStop(1);
        }

        [Test, CancelAfter(120_000)]
        public void ScheduledCheckpointFansOutToThreeReplicas()
        {
            context.CreateInstances(4, enableAOF: true);
            context.MeetAndAssignSlotsAllNodes(4);
            for (var replica = 1; replica < 4; replica++)
                context.clusterTestUtils.AttachReplicaToPrimary(replica, 0, waitForRecovery: true, logger: context.logger);

            context.clusterTestUtils.ConfigSet(0, "checkpoint-freq", "1", context.logger);
            for (var tick = 0; tick < 2; tick++)
            {
                var prior = new DateTimeOffset[4];
                for (var node = 0; node < prior.Length; node++)
                    prior[node] = LastSave(node);

                Assert.That(context.clusterTestUtils.Execute(context.clusterTestUtils.GetEndPoint(0), "SET", ["{checkpoint}key", tick]).ToString(), Is.EqualTo("OK"));
                for (var node = 0; node < prior.Length; node++)
                    WaitForCheckpointAfter(node, prior[node]);
            }
        }
    }
}