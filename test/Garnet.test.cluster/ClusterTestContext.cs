// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    internal class ClusterTestContext
    {
        public CredentialManager credManager;
        public string TestFolder;
        public GarnetServer[] nodes = null;
        public EndPointCollection endpoints;
        public TextWriter logTextWriter = TestContext.Progress;
        public ILoggerFactory loggerFactory;
        public ILogger logger;

        public Random r = new();
        public ManualResetEventSlim waiter;

        public Task checkpointTask;

        public ClusterTestUtils clusterTestUtils = null;

        public void Setup(HashSet<string> monitorTests)
        {
            TestFolder = TestUtils.UnitTestWorkingDir() + "\\";
            LogLevel logLevel = monitorTests.Contains(TestContext.CurrentContext.Test.MethodName) ? LogLevel.Warning : LogLevel.Error;
            loggerFactory = TestUtils.CreateLoggerFactoryInstance(logTextWriter, logLevel, scope: TestContext.CurrentContext.Test.Name);
            logger = loggerFactory.CreateLogger(TestContext.CurrentContext.Test.Name);
            logger.LogDebug("0. Setup >>>>>>>>>>>>");
            r = new Random(674386);
            waiter = new ManualResetEventSlim();
            credManager = new CredentialManager();
        }

        public void TearDown()
        {
            logger.LogDebug("0. Dispose <<<<<<<<<<<");
            waiter.Dispose();
            clusterTestUtils?.Dispose();
            loggerFactory.Dispose();
            DisposeCluster();
            TestUtils.DeleteDirectory(TestFolder, true);
        }

        /// <summary>
        /// Create instances with provided configuration
        /// </summary>
        /// <param name="shards"></param>
        /// <param name="cleanClusterConfig"></param>
        /// <param name="tryRecover"></param>
        /// <param name="disableObjects"></param>
        /// <param name="lowMemory"></param>
        /// <param name="MemorySize"></param>
        /// <param name="PageSize"></param>
        /// <param name="SegmentSize"></param>
        /// <param name="enableAOF"></param>
        /// <param name="MainMemoryReplication"></param>
        /// <param name="OnDemandCheckpoint"></param>
        /// <param name="AofMemorySize"></param>
        /// <param name="CommitFrequencyMs"></param>
        /// <param name="DisableStorageTier"></param>
        /// <param name="EnableIncrementalSnapshots"></param>
        /// <param name="FastCommit"></param>
        /// <param name="useAcl"></param>
        /// <param name="clusterCreds"></param>
        /// <param name="timeout"></param>
        /// <param name="useTLS"></param>
        /// <param name="certificates"></param>
        public void CreateInstances(
            int shards,
            bool cleanClusterConfig = true,
            bool tryRecover = false,
            bool disableObjects = false,
            bool lowMemory = false,
            string MemorySize = default,
            string PageSize = default,
            string SegmentSize = "1g",
            bool enableAOF = false,
            bool MainMemoryReplication = false,
            bool OnDemandCheckpoint = false,
            string AofMemorySize = "64m",
            int CommitFrequencyMs = 0,
            bool DisableStorageTier = false,
            bool EnableIncrementalSnapshots = false,
            bool FastCommit = false,
            int timeout = -1,
            bool useTLS = false,
            bool useAcl = false,
            X509CertificateCollection certificates = null,
            ServerCredentials clusterCreds = new ServerCredentials())
        {
            endpoints = TestUtils.GetEndPoints(shards, 7000);
            nodes = TestUtils.CreateGarnetCluster(
                TestFolder,
                disablePubSub: true,
                disableObjects: disableObjects,
                endpoints: endpoints,
                enableAOF: enableAOF,
                timeout: timeout,
                loggerFactory: loggerFactory,
                tryRecover: tryRecover,
                UseTLS: useTLS,
                cleanClusterConfig: cleanClusterConfig,
                lowMemory: lowMemory,
                MemorySize: MemorySize,
                PageSize: PageSize,
                SegmentSize: SegmentSize,
                MainMemoryReplication: MainMemoryReplication,
                AofMemorySize: AofMemorySize,
                CommitFrequencyMs: CommitFrequencyMs,
                DisableStorageTier: DisableStorageTier,
                OnDemandCheckpoint: OnDemandCheckpoint,
                EnableIncrementalSnapshots: EnableIncrementalSnapshots,
                FastCommit: FastCommit,
                useAcl: useAcl,
                aclFile: credManager.aclFilePath,
                authUsername: clusterCreds.user,
                authPassword: clusterCreds.password,
                certificates: certificates);

            foreach (var node in nodes)
                node.Start();
        }

        /// <summary>
        /// Create single cluster instance with corresponding options
        /// </summary>
        /// <param name="Port"></param>
        /// <param name="cleanClusterConfig"></param>
        /// <param name="tryRecover"></param>
        /// <param name="disableObjects"></param>
        /// <param name="lowMemory"></param>
        /// <param name="MemorySize"></param>
        /// <param name="PageSize"></param>
        /// <param name="SegmentSize"></param>
        /// <param name="enableAOF"></param>
        /// <param name="MainMemoryReplication"></param>
        /// <param name="OnDemandCheckpoint"></param>
        /// <param name="AofMemorySize"></param>
        /// <param name="CommitFrequencyMs"></param>
        /// <param name="DisableStorageTier"></param>
        /// <param name="EnableIncrementalSnapshots"></param>
        /// <param name="FastCommit"></param>
        /// <param name="timeout"></param>
        /// <param name="gossipDelay"></param>
        /// <param name="useTLS"></param>
        /// <param name="useAcl"></param>
        /// <param name="clusterCreds"></param>
        /// <param name="certificates"></param>
        /// <returns></returns>
        public GarnetServer CreateInstance(
            int Port,
            bool cleanClusterConfig = true,
            bool tryRecover = false,
            bool disableObjects = false,
            bool lowMemory = false,
            string MemorySize = default,
            string PageSize = default,
            string SegmentSize = "1g",
            bool enableAOF = false,
            bool MainMemoryReplication = false,
            bool OnDemandCheckpoint = false,
            string AofMemorySize = "64m",
            int CommitFrequencyMs = 0,
            bool DisableStorageTier = false,
            bool EnableIncrementalSnapshots = false,
            bool FastCommit = false,
            int timeout = -1,
            int gossipDelay = 5,
            bool useTLS = false,
            bool useAcl = false,
            X509CertificateCollection certificates = null,
            ServerCredentials clusterCreds = new ServerCredentials())
        {

            var opts = TestUtils.GetGarnetServerOptions(
                TestFolder,
                TestFolder,
                Port,
                disablePubSub: true,
                disableObjects: disableObjects,
                enableAOF: enableAOF,
                timeout: timeout,
                gossipDelay: gossipDelay,
                tryRecover: tryRecover,
                UseTLS: useTLS,
                cleanClusterConfig: cleanClusterConfig,
                lowMemory: lowMemory,
                MemorySize: MemorySize,
                PageSize: PageSize,
                SegmentSize: SegmentSize,
                MainMemoryReplication: MainMemoryReplication,
                AofMemorySize: AofMemorySize,
                CommitFrequencyMs: CommitFrequencyMs,
                DisableStorageTier: DisableStorageTier,
                OnDemandCheckpoint: OnDemandCheckpoint,
                EnableIncrementalSnapshots: EnableIncrementalSnapshots,
                FastCommit: FastCommit,
                useAcl: useAcl,
                aclFile: credManager.aclFilePath,
                authUsername: clusterCreds.user,
                authPassword: clusterCreds.password,
                certificates: certificates);

            return new GarnetServer(opts, loggerFactory);
        }


        /// <summary>
        /// Dispose created instances
        /// </summary>
        public void DisposeCluster()
        {
            if (nodes != null)
            {
                for (int i = 0; i < nodes.Length; i++)
                {
                    if (nodes[i] != null)
                    {
                        logger.LogDebug("\t a. Dispose node {testName}", TestContext.CurrentContext.Test.Name);
                        nodes[i].Dispose();
                        nodes[i] = null;
                        logger.LogDebug("\t b. Dispose node {testName}", TestContext.CurrentContext.Test.Name);
                    }
                }
            }
        }

        /// <summary>
        /// Establish connection to cluster.
        /// </summary>
        /// <param name="useTLS"></param>
        /// <param name="certificates"></param>
        /// <param name="clientCreds"></param>
        public void CreateConnection(
            bool useTLS = false,
            X509CertificateCollection certificates = null,
            ServerCredentials clientCreds = new ServerCredentials())
        {
            if (clusterTestUtils != null) clusterTestUtils.Dispose();
            clusterTestUtils = new ClusterTestUtils(
                endpoints,
                textWriter: logTextWriter,
                UseTLS: useTLS,
                authUsername: clientCreds.user,
                authPassword: clientCreds.password,
                certificates: certificates);
            clusterTestUtils.Connect(logger);
            clusterTestUtils.PingAll(logger);
        }

        /// <summary>
        /// Generate credential file through credManager
        /// </summary>
        /// <param name="customCreds"></param>
        public void GenerateCredentials(ServerCredentials[] customCreds = null)
            => credManager.GenerateCredentials(TestFolder, customCreds);

        public int keyOffset = 0;
        public bool orderedKeys = false;

        public Dictionary<string, int> kvPairs;
        public Dictionary<string, List<int>> kvPairsObj;

        public void PopulatePrimary(
            ref Dictionary<string, int> kvPairs,
            int keyLength,
            int kvpairCount,
            int primaryIndex,
            int[] slotMap = null,
            bool incrementalSnapshots = false,
            int ckptNode = 0,
            int randomSeed = -1)
        {
            if (randomSeed != -1) clusterTestUtils.InitRandom(randomSeed);
            for (int i = 0; i < kvpairCount; i++)
            {
                var key = orderedKeys ? (keyOffset++).ToString() : clusterTestUtils.RandomStr(keyLength);
                var value = r.Next();

                //Use slotMap
                var keyBytes = Encoding.ASCII.GetBytes(key);
                if (slotMap != null)
                {
                    var slot = ClusterTestUtils.HashSlot(keyBytes);
                    primaryIndex = slotMap[slot];
                }

                var resp = clusterTestUtils.SetKey(primaryIndex, keyBytes, Encoding.ASCII.GetBytes(value.ToString()), out int _, out string _, out int _, logger: logger);
                Assert.AreEqual(ResponseState.OK, resp);

                var retVal = clusterTestUtils.GetKey(primaryIndex, keyBytes, out int _, out string _, out int _, out ResponseState responseState, logger: logger);
                Assert.AreEqual(ResponseState.OK, responseState);
                Assert.AreEqual(value, int.Parse(retVal));

                kvPairs.Add(key, int.Parse(retVal));

                if (incrementalSnapshots && i == kvpairCount / 2)
                    clusterTestUtils.Checkpoint(ckptNode, logger: logger);
            }
        }

        public void PopulatePrimaryRMW(ref Dictionary<string, int> kvPairs, int keyLength, int kvpairCount, int primaryIndex, int addCount, int[] slotMap = null, bool incrementalSnapshots = false, int ckptNode = 0, int randomSeed = -1)
        {
            if (randomSeed != -1) clusterTestUtils.InitRandom(randomSeed);
            for (int i = 0; i < kvpairCount; i++)
            {
                var key = orderedKeys ? (keyOffset++).ToString() : clusterTestUtils.RandomStr(keyLength);

                //Use slotMap
                var keyBytes = Encoding.ASCII.GetBytes(key);
                if (slotMap != null)
                {
                    var slot = ClusterTestUtils.HashSlot(keyBytes);
                    primaryIndex = slotMap[slot];
                }

                int value = 0;
                for (int j = 0; j < addCount; j++)
                    value = clusterTestUtils.IncrBy(primaryIndex, key, randomSeed == -1 ? 1 : clusterTestUtils.r.Next(1, 100));

                kvPairs.Add(key, value);

                if (incrementalSnapshots && i == kvpairCount / 2)
                    clusterTestUtils.Checkpoint(ckptNode, logger: logger);
            }
        }

        public void PopulatePrimaryWithObjects(ref Dictionary<string, List<int>> kvPairsObj, int keyLength, int kvpairCount, int primaryIndex, int countPerList = 32, int itemSize = 1 << 20, int randomSeed = -1, bool set = false)
        {
            if (randomSeed != -1) clusterTestUtils.InitRandom(randomSeed);
            for (int i = 0; i < kvpairCount; i++)
            {
                var key = clusterTestUtils.RandomStr(keyLength);
                var value = !set ? clusterTestUtils.RandomList(countPerList, itemSize) : clusterTestUtils.RandomHset(countPerList, itemSize);
                while (kvPairsObj.ContainsKey(key))
                    key = clusterTestUtils.RandomStr(keyLength);
                kvPairsObj.Add(key, value);

                //int insertCount = 0;
                if (!set)
                    clusterTestUtils.Lpush(primaryIndex, key, value, logger);
                else
                    clusterTestUtils.Sadd(primaryIndex, key, value, logger);

                if (!set)
                {
                    var result = clusterTestUtils.Lrange(primaryIndex, key, logger);
                    Assert.AreEqual(value, result);
                }
                else
                {
                    var result = clusterTestUtils.Smembers(primaryIndex, key, logger);
                    Assert.IsTrue(result.ToHashSet().SetEquals(value.ToHashSet()));
                }
            }
        }

        public void PopulatePrimaryAndTakeCheckpointTask(bool performRMW, bool disableObjects, bool takeCheckpoint, int iter = 5)
        {
            int keyLength = 32;
            int kvpairCount = 64;
            int addCount = 5;
            for (int i = 0; i < iter; i++)
            {
                //Populate Primary
                if (disableObjects)
                {
                    if (!performRMW)
                        PopulatePrimary(ref kvPairs, keyLength, kvpairCount, 0);
                    else
                        PopulatePrimaryRMW(ref kvPairs, keyLength, kvpairCount, 0, addCount);
                }
                else
                {
                    PopulatePrimaryWithObjects(ref kvPairsObj, keyLength, kvpairCount, 0);
                }
                if (takeCheckpoint) clusterTestUtils.Checkpoint(0, logger: logger);
            }
        }

        public void ValidateKVCollectionAgainstReplica(
            ref Dictionary<string, int> kvPairs,
            int replicaIndex,
            int primaryIndex = 0,
            int[] slotMap = null)
        {
            var keys = orderedKeys ? kvPairs.Keys.Select(int.Parse).ToList().OrderBy(x => x).Select(x => x.ToString()) : kvPairs.Keys;
            foreach (var key in keys)
            {
                var value = kvPairs[key];
                var keyBytes = Encoding.ASCII.GetBytes(key);

                if (slotMap != null)
                {
                    var slot = ClusterTestUtils.HashSlot(keyBytes);
                    replicaIndex = slotMap[slot];
                }

                var retVal = clusterTestUtils.GetKey(replicaIndex, keyBytes, out int _, out string _, out int _, out ResponseState responseState, logger: logger);
                while (retVal == null || (value != int.Parse(retVal)))
                {
                    retVal = clusterTestUtils.GetKey(replicaIndex, keyBytes, out int _, out string _, out int _, out responseState, logger: logger);
                    Thread.Yield();
                }
                Assert.AreEqual(ResponseState.OK, responseState);
                Assert.AreEqual(value, int.Parse(retVal), $"replOffset > p:{clusterTestUtils.GetReplicationOffset(primaryIndex, logger: logger)}, s[{replicaIndex}]:{clusterTestUtils.GetReplicationOffset(replicaIndex)}");
            }
        }

        public void ValidateNodeObjects(ref Dictionary<string, List<int>> kvPairsObj, int nodeIndex, bool set = false)
        {
            foreach (var key in kvPairsObj.Keys)
            {
                var elements = kvPairsObj[key];
                List<int> result;
                if (!set)
                    result = clusterTestUtils.Lrange(nodeIndex, key, logger);
                else
                    result = clusterTestUtils.Smembers(nodeIndex, key, logger);

                while (result.Count == 0)
                {
                    if (!set)
                        result = clusterTestUtils.Lrange(nodeIndex, key, logger);
                    else
                        result = clusterTestUtils.Smembers(nodeIndex, key, logger);
                    Thread.Yield();
                }
                if (!set)
                    Assert.AreEqual(elements, result);
                else
                    Assert.IsTrue(result.ToHashSet().SetEquals(result.ToHashSet()));
            }
        }

        public void SendAndValidateKeys(int primaryIndex, int replicaIndex, int keyLength, int numKeys = 1)
        {
            for (int i = 0; i < numKeys; i++)
            {
                var key = orderedKeys ? (keyOffset++).ToString() : clusterTestUtils.RandomStr(keyLength);
                var keyBytes = Encoding.ASCII.GetBytes(key);
                var value = r.Next();
                var resp = clusterTestUtils.SetKey(primaryIndex, keyBytes, Encoding.ASCII.GetBytes(value.ToString()), out int _, out string _, out int _, logger: logger);
                Assert.AreEqual(ResponseState.OK, resp);

                clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex);

                var retVal = clusterTestUtils.GetKey(replicaIndex, keyBytes, out int _, out string _, out int _, out ResponseState responseState, logger: logger);
                while (retVal == null || (value != int.Parse(retVal)))
                {
                    retVal = clusterTestUtils.GetKey(replicaIndex, keyBytes, out int _, out string _, out int _, out responseState, logger: logger);
                    Thread.Yield();
                }
                Assert.AreEqual(ResponseState.OK, responseState);
                Assert.AreEqual(value, int.Parse(retVal), $"replOffset > p:{clusterTestUtils.GetReplicationOffset(primaryIndex, logger: logger)}, s[{replicaIndex}]:{clusterTestUtils.GetReplicationOffset(replicaIndex)}");
            }
        }

        public void ClusterFailoveSpinWait(int replicaNodeIndex, ILogger logger)
        {
            //Failover primary
            clusterTestUtils.ClusterFailover(replicaNodeIndex, "ABORT", logger);
            clusterTestUtils.ClusterFailover(replicaNodeIndex, logger: logger);

            int retryCount = 0;
            while (true)
            {
                var role = clusterTestUtils.GetReplicationRole(replicaNodeIndex, logger: logger);
                if (role.Equals("master")) break;
                if (retryCount++ > 10000)
                {
                    logger?.LogError("CLUSTER FAILOVER retry count reached");
                    Assert.Fail();
                }
                Thread.Sleep(1000);
            }
        }

        public void ClusterFailoverRetry(int replicaNodeIndex)
        {
            //Failover primary
            clusterTestUtils.ClusterFailover(replicaNodeIndex, "ABORT", logger);
            clusterTestUtils.ClusterFailover(replicaNodeIndex, logger: logger);

            int retryCount = 0;
            while (true)
            {
                var role = clusterTestUtils.GetReplicationRole(replicaNodeIndex, logger: logger);
                if (role.Equals("master")) break;
                if (retryCount++ > 10000)
                {
                    logger?.LogError("CLUSTER FAILOVER retry count reached");
                    Assert.Fail();
                }
                Thread.Sleep(1000);
            }
        }

        public void AttachAndWaitForSync(int primary_count, int replica_count, bool disableObjects)
        {
            var primaryId = clusterTestUtils.GetNodeIdFromNode(0, logger);
            //Issue meet to replicas
            for (int i = primary_count; i < primary_count + replica_count; i++)
                clusterTestUtils.Meet(i, 0);

            //Wait until primary node is known so as not to fail replicate
            for (int i = primary_count; i < primary_count + replica_count; i++)
                clusterTestUtils.WaitUntilNodeIdIsKnown(i, primaryId, logger: logger);

            //Issue cluster replicate and bump epoch manually to capture config.
            for (int i = primary_count; i < primary_count + replica_count; i++)
            {
                clusterTestUtils.ClusterReplicate(i, primaryId, async: true, logger: logger);
                clusterTestUtils.BumpEpoch(i, logger: logger);
            }

            if (!checkpointTask.Wait(TimeSpan.FromSeconds(100))) Assert.Fail("Checkpoint task timeout");

            //Wait for recovery and AofSync
            for (int i = primary_count; i < replica_count; i++)
            {
                clusterTestUtils.WaitForReplicaRecovery(i, logger);
                clusterTestUtils.WaitForReplicaAofSync(0, i, logger);
            }

            clusterTestUtils.WaitForConnectedReplicaCount(0, replica_count, logger: logger);

            //Validate data on replicas
            for (int i = primary_count; i < replica_count; i++)
            {
                if (disableObjects)
                    ValidateKVCollectionAgainstReplica(ref kvPairs, i);
                else
                    ValidateNodeObjects(ref kvPairsObj, i);
            }
        }
    }
}