// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Garnet.server.Auth.Settings;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test.cluster
{
    public class ClusterTestContext
    {
        public CredentialManager credManager;
        public string TestFolder;
        public GarnetServer[] nodes = null;
        public GarnetServerOptions[] nodeOptions = null;
        public EndPointCollection endpoints;
        public TextWriter logTextWriter = TestContext.Progress;
        public ILoggerFactory loggerFactory;
        public NUnitLoggerProvider loggerProvider;
        public ILogger logger;

        public int defaultShards = 3;
        public static int Port = 7000;

        public Random r = new();
        public ManualResetEventSlim waiter;

        public Task checkpointTask;

        public ClusterTestUtils clusterTestUtils = null;

        public CancellationTokenSource cts;

        public void EnableGarnetLoggingEvents(GarnetTestLoggingEventType[] events)
        {
            foreach (var e in events)
                loggerProvider.GarnetTestLoggingEvents[(int)e] = true;
        }

        public void Setup(Dictionary<string, LogLevel> monitorTests, int testTimeoutSeconds = 60)
        {
            // Pull timeout off [CancelAfter] if its specified, otherwise use default
            if (TestContext.CurrentContext.Test.Properties.ContainsKey("Timeout"))
            {
                testTimeoutSeconds = ((int)TestContext.CurrentContext.Test.Properties["Timeout"].First()) / 1_000;
            }

            cts = new CancellationTokenSource(TimeSpan.FromSeconds(testTimeoutSeconds));

            TestFolder = TestUtils.UnitTestWorkingDir() + "\\";
            var logLevel = LogLevel.Error;
            if (!string.IsNullOrEmpty(TestContext.CurrentContext.Test.MethodName) && monitorTests.TryGetValue(TestContext.CurrentContext.Test.MethodName, out var value))
                logLevel = value;
            (loggerFactory, loggerProvider) = TestUtils.CreateLoggerFactoryInstance(logTextWriter, logLevel, scope: TestContext.CurrentContext.Test.FullName);
            logger = loggerFactory.CreateLogger(TestContext.CurrentContext.Test.FullName);
            logger.LogDebug("0. Setup >>>>>>>>>>>>");
            r = new Random(674386);
            waiter = new ManualResetEventSlim();
            credManager = new CredentialManager();
        }

        public void ShutdownNode(IPEndPoint endpoint)
        {
            for (var i = 0; i < endpoints.Count; i++)
            {
                if (endpoints[i] == endpoint)
                {
                    ShutdownNode(i);
                    return;
                }
            }

            throw new InvalidOperationException($"Could not find node for {endpoint}");
        }

        public void ShutdownNode(int nodeIndex)
        {
            if (nodeIndex < 0 || nodeIndex >= nodes.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(nodeIndex));
            }

            nodes[nodeIndex].Dispose();
            nodes[nodeIndex] = null;
        }

        public void RestartNode(IPEndPoint endpoint)
        {
            for (var i = 0; i < endpoints.Count; i++)
            {
                if (endpoints[i] == endpoint)
                {
                    RestartNode(i);
                    return;
                }
            }

            throw new InvalidOperationException($"Could not find node for {endpoint}");
        }

        public void RestartNode(int nodeIndex)
        {
            if (nodeIndex < 0 || nodeIndex >= nodes.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(nodeIndex));
            }

            if (nodes[nodeIndex] != null)
            {
                ShutdownNode(nodeIndex);
            }

            // We're restarting, leave state unaltered
            nodeOptions[nodeIndex].CleanClusterConfig = false;

            nodes[nodeIndex] = new GarnetServer(nodeOptions[nodeIndex], loggerFactory);
            nodes[nodeIndex].Start();
        }

        public void TearDown()
        {
            // Capture test outcome before any teardown work to distinguish
            // primary teardown failures from secondary ones caused by a hung/failed test.
            var testOutcome = TestContext.CurrentContext.Result.Outcome;
            var testAlreadyFailed = testOutcome.Status == TestStatus.Failed;

            if (testAlreadyFailed)
            {
                logger?.LogError(
                    "TearDown: test already failed ({label}): {message}",
                    testOutcome.Label,
                    TestContext.CurrentContext.Result.Message);
            }

            cts.Cancel();
            cts.Dispose();
            waiter?.Dispose();
            clusterTestUtils?.Dispose();

            var timeoutSeconds = 60;
            string failureReason = null;

            // Phase 1: Dispose cluster nodes (may timeout if handlers are stuck)
            try
            {
                if (!Task.Run(() => DisposeCluster()).Wait(TimeSpan.FromSeconds(timeoutSeconds)))
                {
                    failureReason = "Timed out waiting for DisposeCluster";
                    logger?.LogError("Timed out waiting for DisposeCluster");
                }
            }
            catch (Exception ex)
            {
                failureReason = $"DisposeCluster threw: {ex.Message}";
                logger?.LogError(ex, "DisposeCluster failed");
            }

            // Phase 2: Dispose logger factory (always, even after timeout)
            loggerFactory?.Dispose();

            // Phase 3: Delete test directory (may timeout if files locked from Phase 1 timeout)
            try
            {
                if (!Task.Run(() => TestUtils.DeleteDirectory(TestFolder, true)).Wait(TimeSpan.FromSeconds(timeoutSeconds)))
                {
                    failureReason ??= "Timed out DeleteDirectory";
                    logger?.LogError("Timed out DeleteDirectory");
                }
            }
            catch (Exception ex)
            {
                failureReason ??= $"DeleteDirectory threw: {ex.Message}";
                logger?.LogError(ex, "DeleteDirectory failed");
            }

            // Phase 4: Always runs — resets LightEpoch instances to prevent cross-test contamination
            TestUtils.OnTearDown();

            // Fail the test at the end, after all cleanup is done
            if (failureReason != null)
            {
                var context = testAlreadyFailed
                    ? $" (secondary failure — test already failed with '{testOutcome.Label}')"
                    : " (primary failure — test itself passed)";
                Assert.Fail(failureReason + context);
            }
        }

        public void RegisterCustomTxn(string name, Func<CustomTransactionProcedure> proc, RespCommandsInfo commandInfo = null, RespCommandDocs commandDocs = null)
        {
            foreach (var node in nodes)
                node.Register.NewTransactionProc(name, proc, commandInfo, commandDocs);
        }

        /// <summary>
        /// Create instances with provided configuration
        /// </summary>
        /// <param name="shards"></param>
        /// <param name="enableCluster"></param>
        /// <param name="cleanClusterConfig"></param>
        /// <param name="tryRecover"></param>
        /// <param name="disableObjects"></param>
        /// <param name="lowMemory"></param>
        /// <param name="memorySize"></param>
        /// <param name="pageSize"></param>
        /// <param name="segmentSize"></param>
        /// <param name="enableAOF"></param>
        /// <param name="FastAofTruncate"></param>
        /// <param name="OnDemandCheckpoint"></param>
        /// <param name="AofMemorySize"></param>
        /// <param name="CommitFrequencyMs"></param>
        /// <param name="useAofNullDevice"></param>
        /// <param name="DisableStorageTier"></param>
        /// <param name="FastCommit"></param>
        /// <param name="timeout"></param>
        /// <param name="useTLS"></param>
        /// <param name="useAcl"></param>
        /// <param name="certificates"></param>
        /// <param name="clusterCreds"></param>
        /// <param name="authenticationSettings"></param>
        /// <param name="disablePubSub"></param>
        /// <param name="metricsSamplingFrequency"></param>
        /// <param name="enableLua"></param>
        /// <param name="asyncReplay"></param>
        /// <param name="enableDisklessSync"></param>
        /// <param name="replicaDisklessSyncDelay"></param>
        /// <param name="replicaDisklessSyncFullSyncAofThreshold"></param>
        /// <param name="luaMemoryMode"></param>
        /// <param name="luaMemoryLimit"></param>
        /// <param name="useHostname"></param>
        /// <param name="luaTransactionMode"></param>
        /// <param name="deviceType"></param>
        /// <param name="sublogCount"></param>
        /// <param name="clusterReplicationReestablishmentTimeout"></param>
        /// <param name="aofSizeLimit"></param>
        /// <param name="compactionFrequencySecs"></param>
        /// <param name="compactionType"></param>
        /// <param name="latencyMonitory"></param>
        /// <param name="loggingFrequencySecs"></param>
        /// <param name="checkpointThrottleFlushDelayMs"></param>
        /// <param name="clusterReplicaResumeWithData"></param>
        /// <param name="replicaSyncTimeout"></param>
        /// <param name="expiredObjectCollectionFrequencySecs"></param>
        /// <param name="clusterPreferredEndpointType"></param>
        /// <param name="useClusterAnnounceHostname"></param>
        /// <param name="vectorSetReplayTaskCount"></param>
        /// <param name="threadPoolMinIOCompletionThreads"></param>
        public void CreateInstances(
            int shards,
            bool enableCluster = true,
            bool cleanClusterConfig = true,
            bool tryRecover = false,
            bool disableObjects = false,
            bool lowMemory = false,
            string memorySize = default,
            string pageSize = default,
            string segmentSize = "1g",
            bool enableAOF = false,
            bool FastAofTruncate = false,
            bool OnDemandCheckpoint = false,
            string AofMemorySize = "64m",
            int CommitFrequencyMs = 0,
            bool useAofNullDevice = false,
            bool DisableStorageTier = false,
            bool FastCommit = true,
            int timeout = -1,
            bool useTLS = false,
            bool useAcl = false,
            X509CertificateCollection certificates = null,
            ServerCredential clusterCreds = new ServerCredential(),
            AadAuthenticationSettings authenticationSettings = null,
            bool disablePubSub = true,
            int metricsSamplingFrequency = 0,
            bool enableLua = false,
            bool asyncReplay = false,
            bool enableDisklessSync = false,
            int replicaDisklessSyncDelay = 1,
            string replicaDisklessSyncFullSyncAofThreshold = null,
            LuaMemoryManagementMode luaMemoryMode = LuaMemoryManagementMode.Native,
            string luaMemoryLimit = "",
            bool useHostname = false,
            bool luaTransactionMode = false,
            DeviceType deviceType = DeviceType.Default,
            int clusterReplicationReestablishmentTimeout = 0,
            string aofSizeLimit = "",
            int compactionFrequencySecs = 0,
            LogCompactionType compactionType = LogCompactionType.Scan,
            bool latencyMonitory = false,
            int loggingFrequencySecs = 5,
            int checkpointThrottleFlushDelayMs = 0,
            bool clusterReplicaResumeWithData = false,
            int replicaSyncTimeout = 60,
            int sublogCount = 1,
            int replayTaskCount = 1,
            int expiredObjectCollectionFrequencySecs = 0,
            ClusterPreferredEndpointType clusterPreferredEndpointType = ClusterPreferredEndpointType.Ip,
            bool useClusterAnnounceHostname = false,
            int vectorSetReplayTaskCount = 0,
            int threadPoolMinIOCompletionThreads = 0)
        {
            var ipAddress = IPAddress.Loopback;
            TestUtils.EndPoint = new IPEndPoint(ipAddress, 7000);
            endpoints = TestUtils.GetShardEndPoints(shards, useHostname ? IPAddress.Any : ipAddress, 7000);

            (nodes, nodeOptions) = TestUtils.CreateGarnetCluster(
                TestFolder,
                disablePubSub: disablePubSub,
                disableObjects: disableObjects,
                enableCluster: enableCluster,
                endpoints: endpoints,
                enableAOF: enableAOF,
                timeout: timeout,
                loggerFactory: loggerFactory,
                tryRecover: tryRecover,
                UseTLS: useTLS,
                cleanClusterConfig: cleanClusterConfig,
                lowMemory: lowMemory,
                MemorySize: memorySize,
                PageSize: pageSize,
                SegmentSize: segmentSize,
                FastAofTruncate: FastAofTruncate,
                AofMemorySize: AofMemorySize,
                CommitFrequencyMs: CommitFrequencyMs,
                useAofNullDevice: useAofNullDevice,
                DisableStorageTier: DisableStorageTier,
                OnDemandCheckpoint: OnDemandCheckpoint,
                FastCommit: FastCommit,
                useAcl: useAcl,
                aclFile: credManager.aclFilePath,
                authUsername: clusterCreds.user,
                authPassword: clusterCreds.password,
                certificates: certificates,
                authenticationSettings: authenticationSettings,
                metricsSamplingFrequency: metricsSamplingFrequency,
                enableLua: enableLua,
                asyncReplay: asyncReplay,
                enableDisklessSync: enableDisklessSync,
                replicaDisklessSyncDelay: replicaDisklessSyncDelay,
                replicaDisklessSyncFullSyncAofThreshold: replicaDisklessSyncFullSyncAofThreshold,
                luaMemoryMode: luaMemoryMode,
                luaMemoryLimit: luaMemoryLimit,
                luaTransactionMode: luaTransactionMode,
                deviceType: deviceType,
                clusterReplicationReestablishmentTimeout: clusterReplicationReestablishmentTimeout,
                aofSizeLimit: aofSizeLimit,
                compactionFrequencySecs: compactionFrequencySecs,
                compactionType: compactionType,
                latencyMonitory: latencyMonitory,
                loggingFrequencySecs: loggingFrequencySecs,
                checkpointThrottleFlushDelayMs: checkpointThrottleFlushDelayMs,
                clusterReplicaResumeWithData: clusterReplicaResumeWithData,
                replicaSyncTimeout: replicaSyncTimeout,
                sublogCount: sublogCount,
                replayTaskCount: replayTaskCount,
                expiredObjectCollectionFrequencySecs: expiredObjectCollectionFrequencySecs,
                clusterPreferredEndpointType: clusterPreferredEndpointType,
                clusterAnnounceHostname: useClusterAnnounceHostname ? "localhost" : null,
                vectorSetReplayTaskCount: vectorSetReplayTaskCount,
                threadPoolMinIOCompletionThreads: threadPoolMinIOCompletionThreads);

            foreach (var node in nodes)
                node.Start();

            endpoints = TestUtils.GetShardEndPoints(shards, ipAddress, 7000);
        }

        /// <summary>
        /// Create single cluster instance with corresponding options
        /// </summary>
        /// <param name="endpoint"></param>
        /// <param name="enableCluster"></param>
        /// <param name="cleanClusterConfig"></param>
        /// <param name="disableEpochCollision"></param>
        /// <param name="tryRecover"></param>
        /// <param name="disableObjects"></param>
        /// <param name="lowMemory"></param>
        /// <param name="MemorySize"></param>
        /// <param name="PageSize"></param>
        /// <param name="SegmentSize"></param>
        /// <param name="enableAOF"></param>
        /// <param name="FastAofTruncate"></param>
        /// <param name="OnDemandCheckpoint"></param>
        /// <param name="AofMemorySize"></param>
        /// <param name="CommitFrequencyMs"></param>
        /// <param name="DisableStorageTier"></param>
        /// <param name="FastCommit"></param>
        /// <param name="timeout"></param>
        /// <param name="gossipDelay"></param>
        /// <param name="useTLS"></param>
        /// <param name="useAcl"></param>
        /// <param name="asyncReplay"></param>
        /// <param name="sublogCount"></param>
        /// <param name="vectorSetReplayTaskCount"></param>
        /// <param name="clusterAnnounceEndpoint"></param>
        /// <param name="certificates"></param>
        /// <param name="clusterCreds"></param>
        /// <param name="threadPoolMinIOCompletionThreads"></param>
        /// <returns></returns>
        public GarnetServer CreateInstance(
            EndPoint endpoint,
            bool enableCluster = true,
            bool cleanClusterConfig = true,
            bool disableEpochCollision = false,
            bool tryRecover = false,
            bool disableObjects = false,
            bool lowMemory = false,
            string MemorySize = default,
            string PageSize = default,
            string SegmentSize = "1g",
            bool enableAOF = false,
            bool FastAofTruncate = false,
            bool OnDemandCheckpoint = false,
            string AofMemorySize = "64m",
            int CommitFrequencyMs = 0,
            bool DisableStorageTier = false,
            bool FastCommit = true,
            int timeout = -1,
            int gossipDelay = 5,
            bool useTLS = false,
            bool useAcl = false,
            bool asyncReplay = false,
            int sublogCount = 1,
            int vectorSetReplayTaskCount = 0,
            EndPoint clusterAnnounceEndpoint = null,
            X509CertificateCollection certificates = null,
            ServerCredential clusterCreds = new ServerCredential(),
            int threadPoolMinIOCompletionThreads = 0)
        {

            var opts = TestUtils.GetGarnetServerOptions(
                TestFolder,
                TestFolder,
                endpoint,
                logger: loggerFactory?.CreateLogger("GarnetServer"),
                enableCluster: enableCluster,
                disablePubSub: true,
                disableObjects: disableObjects,
                enableAOF: enableAOF,
                timeout: timeout,
                gossipDelay: gossipDelay,
                tryRecover: tryRecover,
                useTLS: useTLS,
                cleanClusterConfig: cleanClusterConfig,
                lowMemory: lowMemory,
                memorySize: MemorySize,
                pageSize: PageSize,
                segmentSize: SegmentSize,
                fastAofTruncate: FastAofTruncate,
                aofMemorySize: AofMemorySize,
                commitFrequencyMs: CommitFrequencyMs,
                disableStorageTier: DisableStorageTier,
                onDemandCheckpoint: OnDemandCheckpoint,
                fastCommit: FastCommit,
                useAcl: useAcl,
                asyncReplay: asyncReplay,
                sublogCount: sublogCount,
                aclFile: credManager.aclFilePath,
                authUsername: clusterCreds.user,
                authPassword: clusterCreds.password,
                certificates: certificates,
                clusterAnnounceEndpoint: clusterAnnounceEndpoint,
                vectorSetReplayTaskCount: vectorSetReplayTaskCount,
                threadPoolMinIOCompletionThreads: threadPoolMinIOCompletionThreads);

            return new GarnetServer(opts, loggerFactory);
        }

        /// <summary>
        /// Dispose created instances
        /// </summary>
        public void DisposeCluster()
        {
            if (nodes != null)
            {
                for (var i = 0; i < nodes.Length; i++)
                {
                    if (nodes[i] != null)
                    {
                        logger.LogDebug("\t a. Before dispose node {i}{testName}", i, TestContext.CurrentContext.Test.Name);
                        var node = nodes[i];
                        nodes[i] = null;
                        node.Dispose(true);
                        logger.LogDebug("\t b. After dispose node {i}{testName}", i, TestContext.CurrentContext.Test.Name);
                    }
                }
            }
        }

        /// <summary>
        /// Establish connection to cluster.
        /// </summary>
        /// <param name="enabledCluster"></param>
        /// <param name="useTLS"></param>
        /// <param name="certificates"></param>
        /// <param name="clientCreds"></param>
        public void CreateConnection(
            bool enabledCluster = true,
            bool useTLS = false,
            X509CertificateCollection certificates = null,
            ServerCredential clientCreds = new ServerCredential())
        {
            clusterTestUtils?.Dispose();
            clusterTestUtils = new ClusterTestUtils(
                endpoints,
                context: this,
                textWriter: logTextWriter,
                UseTLS: useTLS,
                authUsername: clientCreds.user,
                authPassword: clientCreds.password,
                certificates: certificates);
            clusterTestUtils.Connect(cluster: enabledCluster, logger: logger);
            clusterTestUtils.PingAll(logger);
        }

        /// <summary>
        /// Generate credential file through credManager
        /// </summary>
        /// <param name="customCreds"></param>
        public void GenerateCredentials(ServerCredential[] customCreds = null)
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
            int randomSeed = -1)
        {
            if (randomSeed != -1) clusterTestUtils.InitRandom(randomSeed);
            for (var i = 0; i < kvpairCount; i++)
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

                var resp = clusterTestUtils.SetKey(primaryIndex, keyBytes, Encoding.ASCII.GetBytes(value.ToString()), out int _, out _, logger: logger);
                ClassicAssert.AreEqual(ResponseState.OK, resp);

                var retVal = clusterTestUtils.GetKey(primaryIndex, keyBytes, out int _, out _, out ResponseState responseState, logger: logger);
                ClassicAssert.AreEqual(ResponseState.OK, responseState);
                ClassicAssert.AreEqual(value, int.Parse(retVal));

                kvPairs.Add(key, int.Parse(retVal));
            }
        }

        public void SimplePrimaryReplicaSetup()
        {
            var primaryIndex = 0;
            var replicaIndex = 1;
            // Setup cluster
            var resp = clusterTestUtils.AddDelSlotsRange(primaryIndex, [(0, 16383)], true, logger);
            ClassicAssert.AreEqual("OK", resp);
            clusterTestUtils.SetConfigEpoch(primaryIndex, primaryIndex + 1, logger);
            clusterTestUtils.SetConfigEpoch(replicaIndex, replicaIndex + 1, logger);
            clusterTestUtils.Meet(primaryIndex, replicaIndex, logger);
            clusterTestUtils.WaitUntilNodeIsKnown(primaryIndex, replicaIndex);
            clusterTestUtils.WaitUntilNodeIsKnown(replicaIndex, primaryIndex);
        }

        public void SimplePopulateDB(bool disableObjects, int keyLength, int kvpairCount, int primaryIndex, int addCount = 0, bool performRMW = false)
        {
            //Populate Primary
            if (disableObjects)
            {
                if (!performRMW)
                    PopulatePrimary(ref kvPairs, keyLength, kvpairCount, primaryIndex);
                else
                    PopulatePrimaryRMW(ref kvPairs, keyLength, kvpairCount, primaryIndex, addCount);
            }
            else
            {
                PopulatePrimaryWithObjects(ref kvPairsObj, keyLength, kvpairCount, primaryIndex);
            }
        }

        public void SimpleValidateDB(bool disableObjects, int replicaIndex)
        {
            // Validate database
            if (disableObjects)
            {
                ValidateKVCollectionAgainstReplica(ref kvPairs, replicaIndex);
            }
            else
            {
                ValidateNodeObjects(ref kvPairsObj, replicaIndex);
            }
        }

        public void PopulatePrimaryRMW(ref Dictionary<string, int> kvPairs, int keyLength, int kvpairCount, int primaryIndex, int addCount, int[] slotMap = null, int randomSeed = -1)
        {
            if (randomSeed != -1) clusterTestUtils.InitRandom(randomSeed);
            for (int i = 0; i < kvpairCount; i++)
            {
                var key = orderedKeys ? (keyOffset++).ToString() : clusterTestUtils.RandomStr(keyLength);

                // Use slotMap
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
            }
        }

        public void PopulatePrimaryWithObjects(ref Dictionary<string, List<int>> kvPairsObj, int keyLength, int kvpairCount, int primaryIndex, int countPerList = 32, int itemSize = 1 << 20, int randomSeed = -1, bool set = false)
        {
            if (randomSeed != -1) clusterTestUtils.InitRandom(randomSeed);
            for (var i = 0; i < kvpairCount; i++)
            {
                var key = clusterTestUtils.RandomStr(keyLength);
                var value = !set ? clusterTestUtils.RandomList(countPerList, itemSize) : clusterTestUtils.RandomHset(countPerList, itemSize);
                while (kvPairsObj.ContainsKey(key))
                    key = clusterTestUtils.RandomStr(keyLength);
                kvPairsObj.Add(key, value);
                int count;
                if (!set)
                {
                    count = clusterTestUtils.Lpush(primaryIndex, key, value, logger);
                }
                else
                    clusterTestUtils.Sadd(primaryIndex, key, value, logger);

                if (!set)
                {
                    var result = clusterTestUtils.Lrange(primaryIndex, key, logger);
                    ClassicAssert.AreEqual(value, result);
                }
                else
                {
                    var result = clusterTestUtils.Smembers(primaryIndex, key, logger);
                    ClassicAssert.IsTrue(result.ToHashSet().SetEquals(value));
                }
            }
        }

        public void PopulatePrimaryAndTakeCheckpointTask(bool performRMW, bool disableObjects, bool takeCheckpoint, int iter = 5)
        {
            var keyLength = 32;
            var kvpairCount = 64;
            var addCount = 5;
            for (var i = 0; i < iter; i++)
            {
                // Populate Primary
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

                var retVal = clusterTestUtils.GetKey(replicaIndex, keyBytes, out _, out _, out var responseState, logger: logger);
                while (responseState != ResponseState.OK || retVal == null || (value != int.Parse(retVal)))
                {
                    retVal = clusterTestUtils.GetKey(replicaIndex, keyBytes, out _, out _, out responseState, logger: logger);
                    ClusterTestUtils.BackOff(cancellationToken: cts.Token, msg: $"{clusterTestUtils.GetEndPoint(primaryIndex)} > {clusterTestUtils.GetEndPoint(replicaIndex)}");
                }
                ClassicAssert.AreEqual(ResponseState.OK, responseState);
                ClassicAssert.AreEqual(value, int.Parse(retVal), $"replOffset > p:{clusterTestUtils.GetReplicationOffset(primaryIndex, logger: logger)}, s[{replicaIndex}]:{clusterTestUtils.GetReplicationOffset(replicaIndex)}");
            }
        }

        public void ValidateNodeObjects(ref Dictionary<string, List<int>> kvPairsObj, int replicaIndex, bool set = false)
        {
            foreach (var key in kvPairsObj.Keys)
            {
                var elements = kvPairsObj[key];
                List<int> result;
                if (!set)
                    result = clusterTestUtils.Lrange(replicaIndex, key, logger);
                else
                    result = clusterTestUtils.Smembers(replicaIndex, key, logger);

                while (result.Count == 0)
                {
                    if (!set)
                        result = clusterTestUtils.Lrange(replicaIndex, key, logger);
                    else
                        result = clusterTestUtils.Smembers(replicaIndex, key, logger);
                    ClusterTestUtils.BackOff(cancellationToken: cts.Token);
                }
                if (!set)
                    ClassicAssert.AreEqual(elements, result);
                else
                    ClassicAssert.IsTrue(result.ToHashSet().SetEquals(result));
            }
        }

        public void SendAndValidateKeys(int primaryIndex, int replicaIndex, int keyLength, int numKeys = 1)
        {
            for (var i = 0; i < numKeys; i++)
            {
                var key = orderedKeys ? (keyOffset++).ToString() : clusterTestUtils.RandomStr(keyLength);
                var keyBytes = Encoding.ASCII.GetBytes(key);
                var value = r.Next();
                var resp = clusterTestUtils.SetKey(primaryIndex, keyBytes, Encoding.ASCII.GetBytes(value.ToString()), out int _, out _, logger: logger);
                ClassicAssert.AreEqual(ResponseState.OK, resp);

                clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex);

                var retVal = clusterTestUtils.GetKey(replicaIndex, keyBytes, out _, out _, out ResponseState responseState, logger: logger);
                while (responseState != ResponseState.OK || retVal == null || (value != int.Parse(retVal)))
                {
                    retVal = clusterTestUtils.GetKey(replicaIndex, keyBytes, out _, out _, out responseState, logger: logger);
                    ClusterTestUtils.BackOff(cancellationToken: cts.Token);
                }
                ClassicAssert.AreEqual(ResponseState.OK, responseState);
                ClassicAssert.AreEqual(value, int.Parse(retVal), $"replOffset > p:{clusterTestUtils.GetReplicationOffset(primaryIndex, logger: logger)}, s[{replicaIndex}]:{clusterTestUtils.GetReplicationOffset(replicaIndex)}");
            }
        }

        public void ClusterFailoverSpinWait(int replicaNodeIndex, ILogger logger)
        {
            // Failover primary
            _ = clusterTestUtils.ClusterFailover(replicaNodeIndex, "ABORT", logger);
            _ = clusterTestUtils.ClusterFailover(replicaNodeIndex, logger: logger);

            var retryCount = 0;
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

        public async Task AttachAndWaitForSyncAsync(int primaryIndex, int replicaStartIndex, int replicaCount, bool disableObjects)
        {
            var primaryId = clusterTestUtils.GetNodeIdFromNode(primaryIndex, logger);

            // Wait until primary node is known so as not to fail replicate
            for (var i = replicaStartIndex; i < replicaStartIndex + replicaCount; i++)
                clusterTestUtils.WaitUntilNodeIdIsKnown(i, primaryId, logger: logger);

            // Issue cluster replicate and bump epoch manually to capture config.
            for (var i = replicaStartIndex; i < replicaStartIndex + replicaCount; i++)
                _ = clusterTestUtils.ClusterReplicate(i, primaryId, async: true, logger: logger);

            if (!checkpointTask.Wait(TimeSpan.FromSeconds(100))) Assert.Fail("Checkpoint task timeout");

            // Wait for recovery and AofSync
            for (var i = replicaStartIndex; i < replicaStartIndex + replicaCount; i++)
            {
                clusterTestUtils.WaitForReplicaRecovery(i, logger);
                clusterTestUtils.WaitForReplicaAofSync(primaryIndex, i, logger);
            }

            await clusterTestUtils.WaitForConnectedReplicaCountAsync(0, replicaCount, logger: logger).ConfigureAwait(false);

            // Validate data on replicas
            for (var i = replicaStartIndex; i < replicaStartIndex + replicaCount; i++)
            {
                if (disableObjects)
                    ValidateKVCollectionAgainstReplica(ref kvPairs, i);
                else
                    ValidateNodeObjects(ref kvPairsObj, i);
            }
        }

        public List<byte[]> GenerateKeysWithPrefix(string prefix, int keyCount, int suffixLength)
        {
            var keyBuffer = new byte[2 + prefix.Length + suffixLength];
            Encoding.ASCII.GetBytes("{" + prefix + "}").CopyTo(keyBuffer, 0);

            var keys = new List<byte[]>();
            for (var i = 0; i < keyCount; i++)
            {
                clusterTestUtils.RandomBytes(ref keyBuffer, 2 + prefix.Length);
                keys.Add(keyBuffer.ToArray());
            }
            return keys;
        }

        public List<byte[]> GenerateIncreasingSizeValues(int minSize, int maxSize)
        {
            var values = new List<byte[]>();
            for (var i = minSize; i <= maxSize; i++)
            {
                var valueBuffer = new byte[minSize];
                clusterTestUtils.RandomBytes(ref valueBuffer, valueBuffer.Length);
                values.Add(valueBuffer.ToArray());
            }
            return values;
        }

        public void ExecuteTxnBulkIncrement(string[] keys, string[] values)
        {
            try
            {
                var db = clusterTestUtils.GetDatabase();
                var txn = db.CreateTransaction();
                for (var i = 0; i < keys.Length; i++)
                    _ = txn.StringIncrementAsync(keys[i], long.Parse(values[i]));
                _ = txn.Execute();
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        public string[] ExecuteTxnBulkRead(IServer server, string[] keys)
        {
            try
            {
                var resp = server.Execute("MULTI");
                ClassicAssert.AreEqual("OK", (string)resp);

                foreach (var key in keys)
                {
                    resp = server.Execute("GET", key);
                    ClassicAssert.AreEqual("QUEUED", (string)resp);
                }

                resp = server.Execute("EXEC");
                return (string[])resp;
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
            return null;
        }

        public static void ExecuteStoredProcBulkIncrement(IServer server, string[] keys, string[] values)
        {
            try
            {
                var args = new object[1 + (keys.Length * 2)];
                args[0] = keys.Length;
                for (var i = 0; i < keys.Length; i++)
                {
                    args[1 + (i * 2)] = keys[i];
                    args[1 + (i * 2) + 1] = values[i];
                }
                var resp = server.Execute("BULKINCRBY", args);
                ClassicAssert.AreEqual("OK", (string)resp);
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        public static string[] ExecuteBulkReadStoredProc(IServer server, string[] keys)
        {
            try
            {
                var args = new object[1 + keys.Length];
                args[0] = keys.Length;
                for (var i = 0; i < keys.Length; i++)
                    args[1 + i] = keys[i];
                var resp = server.Execute("BULKREAD", args);
                var result = (string[])resp;
                ClassicAssert.AreEqual(keys.Length, result.Length);
                return result;
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
            return null;
        }
    }
}