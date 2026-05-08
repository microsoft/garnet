// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using Embedded.server;
using Garnet.common;
using Garnet.server;

namespace Resp.benchmark
{
    public class GarnetServerInstance
    {
        public static GarnetServerOptions GetServerOptions(Options options)
        {
            var serverOptions = new GarnetServerOptions
            {
                ClusterAnnounceEndpoint = new IPEndPoint(IPAddress.Loopback, 6379),
                QuietMode = true,
                IndexMemorySize = options.IndexMemorySize,
                EnableAOF = options.EnableAOF || options.AofBench,
                EnableCluster = options.EnableCluster,
                ClusterConfigFlushFrequencyMs = -1,
                FastAofTruncate = options.EnableCluster && options.UseAofNullDevice,
                UseAofNullDevice = options.UseAofNullDevice,
                AofMemorySize = options.AofMemorySize,
                AofPageSize = options.AofPageSize,
                CommitFrequencyMs = options.CommitFrequencyMs,
                AofPhysicalSublogCount = options.AofPhysicalSublogCount,
                AofReplayTaskCount = options.AofReplayTaskCount,
                ReplicationOffsetMaxLag = 0,
                CheckpointDir = OperatingSystem.IsLinux() ? "/tmp" : null,
            };
            return serverOptions;
        }

        internal EmbeddedRespServer server;
        internal RespServerSession[] sessions;
        internal readonly string primaryId;

        public GarnetServerInstance(Options options)
        {
            var serverOptions = AofBench.GetServerOptions(options);
            primaryId = Generator.CreateHexId();
            server = new EmbeddedRespServer(serverOptions, Program.loggerFactory, new GarnetServerEmbedded());
            sessions = server.GetRespSessions(options.AofPhysicalSublogCount);
            sessions[0].clusterSession.UnsafeSetConfig(replicaOf: primaryId);
        }

        public IClusterSession GetClusterSession(int idx)
            => sessions[idx].clusterSession;

        internal RespServerSession GetRespServerSession(int idx)
            => sessions[idx];
    }
}