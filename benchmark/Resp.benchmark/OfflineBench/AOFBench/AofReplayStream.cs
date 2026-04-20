// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using Embedded.server;
using Garnet.cluster;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Resp.benchmark
{
    internal sealed class AofReplayStream : IDisposable
    {
        const int maxChunkSize = 1 << 20;
        readonly Options options;
        readonly int threadId;
        readonly CancellationTokenSource cts = new();
        readonly long startAddress;
        long previousAddress;
        readonly ILogger logger = null;
        readonly string primaryId;

        public long Size => previousAddress - startAddress;

        readonly GarnetServerInstance instance;

        // Direct replay mode (InProc): bypass RESP, call ReplicaReplayDriver directly
        ReplicaReplayDriver replayDriver;

        public byte[] buffer;

        public AofReplayStream(
            GarnetServerInstance instance,
            int threadId,
            long startAddress,
            Options options)
        {
            this.options = options;
            this.instance = instance;
            this.threadId = threadId;
            this.primaryId = instance.primaryId;
            this.startAddress = startAddress;
            previousAddress = startAddress;
            buffer = GC.AllocateArray<byte>(2 << options.AofPageSizeBits(), pinned: true);

            instance.GetClusterSession(0).UnsafeSetConfig(replicaOf: primaryId);
        }

        public void Dispose()
        {
            cts.Cancel();
            cts.Dispose();
        }

        ClusterNode GetClusterNodes(Options opts)
        {
            var redis = ConnectionMultiplexer.Connect(
                BenchUtils.GetConfig(
                    opts.Address,
                    opts.Port,
                    useTLS: opts.EnableTLS,
                    tlsHost: opts.TlsHost,
                    allowAdmin: true));

            var servers = redis.GetServers();
            if (servers.Length < 2)
                throw new Exception("Too few nodes for AOF bench to run");

            var endpoint = new IPEndPoint(IPAddress.Parse(opts.Address), opts.Port);
            var primaryServer = redis.GetServer(endpoint);
            var nodes = primaryServer.ClusterNodes();
            var primaryNodeId = (string)primaryServer.Execute("cluster", "myid");

            ClusterNode replicaNode = null;
            foreach (var node in nodes.Nodes)
            {
                if (node.ParentNodeId != null && node.ParentNodeId.Equals(primaryNodeId))
                    replicaNode = node;
            }

            if (replicaNode == null)
                throw new Exception($"No replica found for [{endpoint}] to run AOF bench!");
            return replicaNode;
        }

        public unsafe void InitializeReplayStream()
        {
            if (options.AofBenchType == AofBenchType.ReplayDirect)
            {
                // Direct mode: initialize ReplicaReplayDriver without going through RESP
                var clusterProvider = (ClusterProvider)instance.server.StoreWrapper.clusterProvider;
                var networkSender = new EmbeddedNetworkSender();
                clusterProvider.replicationManager.InitializeReplicaReplayDriver(threadId, networkSender);
                replayDriver = clusterProvider.replicationManager.ReplicaReplayDriverStore.GetReplayDriver(threadId);
                replayDriver.ResumeReplay();
            }
            else
            {
                fixed (byte* ptr = buffer)
                {
                    var respMessageSize = AofGen.WriterClusterAppendLog(
                        ptr,
                        buffer.Length,
                        nodeId: primaryId,
                        physicalSublogIdx: threadId,
                        previousAddress: -1,
                        currentAddress: -1,
                        nextAddress: -1,
                        payloadPtr: -1,
                        payloadLength: 0);
                    _ = instance.sessions[threadId].TryConsumeMessages(ptr, respMessageSize);
                }
            }
        }

        public unsafe void Consume(byte* payloadPtr, int payloadLength, long currentAddress, long nextAddress, bool isProtected)
        {
            try
            {
                fixed (byte* ptr = buffer)
                {
                    var respMessageSize = AofGen.WriterClusterAppendLog(
                        ptr,
                        buffer.Length,
                        nodeId: primaryId,
                        physicalSublogIdx: threadId,
                        previousAddress,
                        currentAddress,
                        nextAddress,
                        (long)payloadPtr,
                        payloadLength);
                    _ = instance.sessions[threadId].TryConsumeMessages(ptr, respMessageSize);
                }

                previousAddress = nextAddress;
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.AofSyncTaskInfo.Consume");
                throw;
            }
        }

        public unsafe void ConsumeResp(byte* respMessagePtr, int respMessageLength)
        {
            try
            {
                _ = instance.sessions[threadId].TryConsumeMessages(respMessagePtr, respMessageLength);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.AofSyncTaskInfo.ConsumeResp");
                throw;
            }
        }

        public unsafe void ConsumeNoResp(byte* payloadPtr, int payloadLength, long currentAddress, long nextAddress, bool isProtected)
        {
            try
            {
                instance.sessions[threadId].clusterSession.ProcessPrimaryStream(
                    physicalSublogIdx: threadId,
                    payloadPtr,
                    payloadLength,
                    previousAddress,
                    currentAddress,
                    nextAddress);

                previousAddress = nextAddress;
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.AofSyncTaskInfo.ConsumeNoResp");
                throw;
            }
        }

        public unsafe void ConsumeDirect(byte* payloadPtr, int payloadLength, long currentAddress, long nextAddress, bool isProtected)
        {
            try
            {
                replayDriver.Consume(
                    payloadPtr,
                    payloadLength,
                    currentAddress,
                    nextAddress,
                    isProtected: false);

                previousAddress = nextAddress;
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.AofSyncTaskInfo.ConsumeNoResp");
                throw;
            }
        }

        public void Throttle()
        { }
    }
}