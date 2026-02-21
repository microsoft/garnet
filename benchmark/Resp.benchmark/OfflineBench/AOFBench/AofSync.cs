// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using Garnet.client;
using Garnet.common;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Tsavorite.core;

namespace Resp.benchmark
{
    internal sealed class AofSync : IBulkLogEntryConsumer, IDisposable
    {
        const int maxChunkSize = 1 << 20;
        readonly Options options;
        readonly AofBench aofBench;
        readonly int threadId;
        readonly CancellationTokenSource cts = new();
        GarnetClientSession garnetClient;
        string primaryId;
        readonly long startAddress;
        long previousAddress;
        readonly ILogger logger = null;

        public long Size => previousAddress - startAddress;

        public byte[] buffer;

        public AofSync(AofBench aofBench, int threadId, long startAddress, Options options, AofGen aofGen)
        {
            this.options = options;
            this.aofBench = aofBench;
            this.threadId = threadId;
            primaryId = null;
            garnetClient = null;
            this.startAddress = startAddress;
            previousAddress = startAddress;

            if (options.Client == ClientType.InProc)
            {
                this.buffer = GC.AllocateArray<byte>(2 << options.AofPageSizeBits(), pinned: true);
                primaryId = aofBench.primaryId;
                if (options.EnableCluster)
                    aofBench.sessions[0].clusterSession.UnsafeSetConfig(replicaOf: primaryId);
            }
            else
            {
                // Get replica information
                var replicaNode = GetClusterNodes(options);
                primaryId = replicaNode.ParentNodeId;

                // Initialize client
                var aofSyncNetworkBufferSettings = aofGen.GetAofSyncNetworkBufferSettings();
                garnetClient = new GarnetClientSession(
                            replicaNode.EndPoint,
                            aofSyncNetworkBufferSettings,
                            aofSyncNetworkBufferSettings.CreateBufferPool(),
                            tlsOptions: null,
                            logger: logger);
                garnetClient.Connect();
            }
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

        public unsafe int WriterClusterAppendLog(
            byte* bufferPtr,
            int bufferLength,
            string nodeId,
            int physicalSublogIdx,
            long previousAddress,
            long currentAddress,
            long nextAddress,
            long payloadPtr,
            int payloadLength)
        {
            var CLUSTER = "$7\r\nCLUSTER\r\n"u8;
            var appendLog = "APPENDLOG"u8;

            var curr = bufferPtr;
            var end = bufferPtr + bufferLength;

            var arraySize = 8;

            // 
            if (!RespWriteUtils.TryWriteArrayLength(arraySize, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            // 1
            if (!RespWriteUtils.TryWriteDirect(CLUSTER, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            // 2
            if (!RespWriteUtils.TryWriteBulkString(appendLog, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            // 3
            if (!RespWriteUtils.TryWriteAsciiBulkString(nodeId, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            // 4
            if (!RespWriteUtils.TryWriteArrayItem(physicalSublogIdx, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            // 5
            if (!RespWriteUtils.TryWriteArrayItem(previousAddress, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            // 6
            if (!RespWriteUtils.TryWriteArrayItem(currentAddress, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            // 7
            if (!RespWriteUtils.TryWriteArrayItem(nextAddress, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            // 8
            if (!RespWriteUtils.TryWriteBulkString(new Span<byte>((void*)payloadPtr, payloadLength), ref curr, end))
                throw new GarnetException("Not enough space in buffer");

            return (int)(curr - bufferPtr);
        }

        public unsafe void Consume(byte* payloadPtr, int payloadLength, long currentAddress, long nextAddress, bool isProtected)
        {
            try
            {
                if (options.Client == ClientType.InProc)
                {
                    fixed (byte* ptr = buffer)
                    {
                        var respMessageSize = WriterClusterAppendLog(
                            ptr,
                            buffer.Length,
                            nodeId: primaryId,
                            physicalSublogIdx: threadId,
                            previousAddress,
                            currentAddress,
                            nextAddress,
                            (long)payloadPtr,
                            payloadLength);
                        _ = aofBench.sessions[threadId].TryConsumeMessages(ptr, respMessageSize);
                    }
                }
                else
                {
                    garnetClient.ExecuteClusterAppendLog(
                        primaryId,
                        physicalSublogIdx: threadId,
                        previousAddress,
                        currentAddress,
                        nextAddress,
                        (long)payloadPtr,
                        payloadLength);
                }

                previousAddress = nextAddress;
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.AofSyncTaskInfo.Consume");
                throw;
            }
        }

        public void Throttle()
        {
            // Trigger flush while we are out of epoch protection
            garnetClient.CompletePending(false);
            garnetClient.Throttle();
        }
    }
}