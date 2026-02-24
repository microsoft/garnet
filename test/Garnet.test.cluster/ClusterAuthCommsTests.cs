// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Linq;
using System.Net;
using Allure.NUnit;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [AllureNUnit]
    [TestFixture, NonParallelizable]
    internal class ClusterAuthCommsTests : AllureTestBase
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

        [Test, Order(1)]
        [Category("CLUSTER-AUTH"), CancelAfter(60000)]
        public void ClusterBasicACLTest([Values] bool useDefaultUserForInterNodeComms)
        {
            var nodes = 6;

            // Generate default ACL file
            context.GenerateCredentials();

            if (useDefaultUserForInterNodeComms)
            {
                // Create instances, feed generated acl file and use default user for cluster auth
                context.CreateInstances(nodes, useAcl: true, clusterCreds: context.credManager.GetUserCredentials("default"));
            }
            else
            {
                // Create instances, feed generated acl file and use admin user for cluster auth
                context.CreateInstances(nodes, useAcl: true, clusterCreds: context.credManager.GetUserCredentials("admin"));
            }

            context.CreateConnection(useTLS: false, clientCreds: context.credManager.GetUserCredentials("admin"));

            var creds = context.credManager.GetUserCredentials("admin");
            for (int i = 1; i < nodes; i++)
            {
                context.clusterTestUtils.Authenticate(0, creds.user, creds.password, context.logger);
                context.clusterTestUtils.Meet(0, i, context.logger);
            }

            context.clusterTestUtils.WaitAll(context.logger);
        }

        [Test, Order(2)]
        [Category("CLUSTER-AUTH"), CancelAfter(60000)]
        public void ClusterStartupWithoutAuthCreds([Values] bool useDefaultUserForInterNodeComms)
        {
            var shards = 3;

            // Generate default ACL file
            context.GenerateCredentials();

            // Create instances but do not provide credentials through server options
            context.CreateInstances(shards, useAcl: true);

            // Connect as admin
            context.CreateConnection(clientCreds: context.credManager.GetUserCredentials("admin"));

            var cred = useDefaultUserForInterNodeComms ?
                context.credManager.GetUserCredentials("default") :
                context.credManager.GetUserCredentials("admin");

            // Update cluster credential before setting up cluster
            for (var i = 0; i < shards; i++)
            {
                context.clusterTestUtils.ConfigSet(i, "cluster-username", cred.user);
                context.clusterTestUtils.ConfigSet(i, "cluster-password", cred.password);
            }

            // Setup cluster
            _ = context.clusterTestUtils.SimpleSetupCluster();

            // Validate convergence
            Dictionary<string, SlotRange> slots = new();
            for (var i = 0; i < shards; i++)
            {
                var nodes = context.clusterTestUtils.ClusterNodes(0).Nodes;

                foreach (var node in nodes)
                {
                    var endpoint = node.EndPoint.ToString();
                    if (slots.ContainsKey(endpoint))
                        ClassicAssert.AreEqual(node.Slots.First(), slots[endpoint]);
                    else
                        slots.Add(endpoint, node.Slots.First());
                }
            }
        }

        [Test, Order(3)]
        [Category("CLUSTER-AUTH"), CancelAfter(60000)]
        public void ClusterReplicationAuth()
        {
            var shards = 3;
            // Generate default ACL file
            context.GenerateCredentials();

            // Create instances but do not provide credentials through server options
            context.CreateInstances(shards, useAcl: true, enableAOF: true);

            // Connect as admin
            context.CreateConnection(clientCreds: context.credManager.GetUserCredentials("admin"));

            // Assign slots
            _ = context.clusterTestUtils.AddSlotsRange(0, new List<(int, int)> { (0, 16383) }, logger: context.logger);

            // Retrieve credentials
            var cred = context.credManager.GetUserCredentials("admin");
            for (var i = 0; i < shards; i++)
            {
                // Set config epoch
                context.clusterTestUtils.SetConfigEpoch(i, i + 1, logger: context.logger);

                // Set cluster creds
                context.clusterTestUtils.ConfigSet(i, "cluster-username", cred.user);
                context.clusterTestUtils.ConfigSet(i, "cluster-password", cred.password);
            }

            // Initiate meet now that we know the credentatial
            for (int i = 1; i < shards; i++)
                context.clusterTestUtils.Meet(0, i, logger: context.logger);

            // Wait for config convergence
            context.clusterTestUtils.WaitAll(logger: context.logger);

            context.kvPairs = new();
            //Populate Primary
            context.PopulatePrimary(ref context.kvPairs, keyLength: 8, kvpairCount: 100, primaryIndex: 0);

            // Get primary id to replicate
            var primaryId = context.clusterTestUtils.GetNodeIdFromNode(nodeIndex: 0, logger: context.logger);

            // Try attach replicas
            for (var i = 1; i < shards; i++)
            {
                context.clusterTestUtils.ClusterReplicate(
                    sourceNodeIndex: i,
                    primaryNodeId: primaryId,
                    failEx: true,
                    logger: context.logger);
                context.clusterTestUtils.BumpEpoch(i, logger: context.logger);
            }

            // Wait for sync and validate key/values pairs
            for (var i = 1; i < shards; i++)
            {
                context.clusterTestUtils.WaitForReplicaAofSync(
                    primaryIndex: 0,
                    secondaryIndex: i,
                    logger: context.logger);
                context.ValidateKVCollectionAgainstReplica(ref context.kvPairs,
                    replicaIndex: i,
                    primaryIndex: 0);
            }
        }

        [Test, Order(4)]
        [Category("CLUSTER-AUTH"), CancelAfter(60000)]
        public void ClusterSimpleFailoverAuth()
        {
            // Setup single primary populate and then attach replicas
            ClusterReplicationAuth();

            context.ClusterFailoverSpinWait(replicaNodeIndex: 1, logger: context.logger);
            context.ClusterFailoverSpinWait(replicaNodeIndex: 2, logger: context.logger);

            // Reconfigure slotMap to reflect new primary
            int[] slotMap = new int[16384];
            for (int i = 0; i < 16384; i++)
                slotMap[i] = 1;

            context.PopulatePrimary(ref context.kvPairs, keyLength: 8, kvpairCount: 100, primaryIndex: 1);
        }

        [Test, Order(4)]
        [Category("CLUSTER-AUTH"), CancelAfter(60000)]
        public void ClusterSimpleACLReload()
        {
            ClusterStartupWithoutAuthCreds(useDefaultUserForInterNodeComms: true);

            ServerCredential[] cc = [
                new ServerCredential("admin", "adminplaceholder", IsAdmin: true, UsedForClusterAuth: false, IsClearText: false),
                new ServerCredential("default", "defaultplaceholder2", IsAdmin: false, UsedForClusterAuth: true, IsClearText: true),
            ];

            // Wait for all nodes to converge
            context.clusterTestUtils.WaitAll(logger: context.logger);

            // Update loaded ACL file
            context.GenerateCredentials(cc);

            // Reload creds for node 1
            context.clusterTestUtils.AclLoad(1, logger: context.logger);

            // Restart node with new ACL file
            context.nodes[0].Dispose(false);
            context.nodes[0] = context.CreateInstance(context.clusterTestUtils.GetEndPoint(0), useAcl: true, cleanClusterConfig: false);
            context.nodes[0].Start();

            context.CreateConnection(clientCreds: cc[0]);

            // Since credential have changed for admin user both nodes will fail their gossip
            // Bump epoch on one node
            for (var i = 0; i < 10; i++)
                context.clusterTestUtils.BumpEpoch(nodeIndex: 0, logger: context.logger);

            // Update cluster credential
            for (var i = 0; i < context.nodes.Length; i++)
            {
                context.clusterTestUtils.ConfigSet(i, "cluster-username", cc[1].user);
                context.clusterTestUtils.ConfigSet(i, "cluster-password", cc[1].password);
            }

            // Get epoch value and port for node 0
            var epoch0 = context.clusterTestUtils.GetConfigEpoch(0, logger: context.logger);
            var port0 = context.clusterTestUtils.GetEndPoint(0).Port;

            // Wait until convergence after updating passwords
            for (var i = 1; i < context.nodes.Length;)
            {
                var config = context.clusterTestUtils.ClusterNodes(i, logger: context.logger);

                foreach (var node in config.Nodes)
                {
                    var port = ((IPEndPoint)node.EndPoint).Port;
                    var epoch = int.Parse(node.Raw.Split(" ")[6]);
                    if (port == port0 && epoch == epoch0)
                    {
                        i++;
                        break;
                    }
                }
            }
        }
    }
}