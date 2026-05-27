// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using System.Runtime.InteropServices;
using Embedded.server;
using Garnet.cluster;
using Garnet.common;
using Garnet.server;

namespace BDN.benchmark.Cluster.ConsistentRead
{
    /// <summary>
    /// Context for consistent read benchmarks — sets up an embedded Garnet server
    /// with cluster + AOF + optional multilog, and optionally forces replica mode.
    /// </summary>
    unsafe class ConsistentReadContext
    {
        EmbeddedRespServer server;
        RespServerSession session;
        readonly BenchUtils benchUtils = new();
        readonly int port = 7100;

        public static ReadOnlySpan<byte> keyTag => "{0}"u8;
        public Request getRequest;
        public Request mgetRequest;

        public void Dispose()
        {
            session.Dispose();
            server.Dispose();
        }

        public void SetupInstance(ConsistentReadParams parameters)
        {
            var opt = new GarnetServerOptions
            {
                QuietMode = true,
                EnableCluster = true,
                EndPoints = [new IPEndPoint(IPAddress.Loopback, port)],
                CleanClusterConfig = true,
                ClusterAnnounceEndpoint = new IPEndPoint(IPAddress.Loopback, port),
                EnableAOF = true,
                UseAofNullDevice = true,
                FastAofTruncate = true,
                CommitFrequencyMs = -1,
                AofPageSize = "128m",
                AofMemorySize = "256m",
            };

            if (parameters.multiLogEnabled)
            {
                opt.AofPhysicalSublogCount = 1;
                opt.AofReplayTaskCount = 4;
                opt.EnableFastCommit = true;
            }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                opt.CheckpointDir = "/tmp";

            server = new EmbeddedRespServer(opt, null, new GarnetServerEmbedded());
            session = server.GetRespSession();

            // Assign all slots
            AddSlotRange([(0, 16383)]);

            // Populate keys while still primary
            PopulateKeys();

            // If replica mode, force replica role and pre-seed sequence numbers
            if (parameters.replicaMode && parameters.multiLogEnabled)
            {
                ForceReplicaRole();
                PreSeedSequenceNumbers();
            }
        }

        void AddSlotRange(List<(int, int)> slotRanges)
        {
            foreach (var slotRange in slotRanges)
            {
                var cmd = $"*4\r\n$7\r\nCLUSTER\r\n$13\r\nADDSLOTSRANGE\r\n" +
                    $"${NumUtils.CountDigits(slotRange.Item1)}\r\n{slotRange.Item1}\r\n" +
                    $"${NumUtils.CountDigits(slotRange.Item2)}\r\n{slotRange.Item2}\r\n";
                var bytes = System.Text.Encoding.ASCII.GetBytes(cmd);
                fixed (byte* req = bytes)
                    _ = session.TryConsumeMessages(req, bytes.Length);
            }
        }

        void PopulateKeys()
        {
            const int batchSize = 100;
            const int keySize = 8;
            const int valueSize = 32;

            var pairs = new (byte[], byte[])[batchSize];
            for (var i = 0; i < batchSize; i++)
            {
                pairs[i] = (new byte[keySize], new byte[valueSize]);
                keyTag.CopyTo(pairs[i].Item1.AsSpan());
                benchUtils.RandomBytes(ref pairs[i].Item1, startOffset: keyTag.Length);
                benchUtils.RandomBytes(ref pairs[i].Item2);
            }

            // SET all keys
            var setByteCount = batchSize * ("*3\r\n$3\r\nSET\r\n"u8.Length + 1 + NumUtils.CountDigits(keySize) + 2 + keySize + 2 + 1 + NumUtils.CountDigits(valueSize) + 2 + valueSize + 2);
            var setReq = new Request(setByteCount);
            var curr = setReq.ptr;
            var end = curr + setReq.buffer.Length;
            for (var i = 0; i < batchSize; i++)
            {
                _ = RespWriteUtils.TryWriteArrayLength(3, ref curr, end);
                _ = RespWriteUtils.TryWriteBulkString("SET"u8, ref curr, end);
                _ = RespWriteUtils.TryWriteBulkString(pairs[i].Item1, ref curr, end);
                _ = RespWriteUtils.TryWriteBulkString(pairs[i].Item2, ref curr, end);
            }
            _ = session.TryConsumeMessages(setReq.ptr, setReq.buffer.Length);

            // Build GET request buffer
            var getByteCount = batchSize * ("*2\r\n$3\r\nGET\r\n"u8.Length + 1 + NumUtils.CountDigits(keySize) + 2 + keySize + 2);
            getRequest = new Request(getByteCount);
            curr = getRequest.ptr;
            end = curr + getRequest.buffer.Length;
            for (var i = 0; i < batchSize; i++)
            {
                _ = RespWriteUtils.TryWriteArrayLength(2, ref curr, end);
                _ = RespWriteUtils.TryWriteBulkString("GET"u8, ref curr, end);
                _ = RespWriteUtils.TryWriteBulkString(pairs[i].Item1, ref curr, end);
            }

            // Build MGET request buffer
            var mGetHeaderSize = 1 + NumUtils.CountDigits(1 + batchSize) + 2 + "$4\r\nMGET\r\n"u8.Length;
            var getRespSize = 1 + NumUtils.CountDigits(keySize) + 2 + keySize + 2;
            var mGetByteCount = mGetHeaderSize + (batchSize * getRespSize);
            mgetRequest = new Request(mGetByteCount);
            curr = mgetRequest.ptr;
            end = curr + mgetRequest.buffer.Length;
            _ = RespWriteUtils.TryWriteArrayLength(1 + batchSize, ref curr, end);
            _ = RespWriteUtils.TryWriteBulkString("MGET"u8, ref curr, end);
            for (var i = 0; i < batchSize; i++)
                _ = RespWriteUtils.TryWriteBulkString(pairs[i].Item1, ref curr, end);
        }

        void ForceReplicaRole()
        {
            var clusterProvider = (ClusterProvider)server.StoreWrapper.clusterProvider;
            clusterProvider.clusterManager.TrySetLocalNodeRole(NodeRole.REPLICA);
        }

        void PreSeedSequenceNumbers()
        {
            // Advance all virtual sublog frontier sequence numbers to long.MaxValue
            // so that consistent reads never block waiting for replay to catch up.
            var appendOnlyFile = server.StoreWrapper.appendOnlyFile;
            var readConsistencyManager = appendOnlyFile.readConsistencyManager;
            if (readConsistencyManager == null)
                return;

            var physicalSublogCount = appendOnlyFile.serverOptions.AofPhysicalSublogCount;
            for (var i = 0; i < physicalSublogCount; i++)
                readConsistencyManager.UpdatePhysicalSublogMaxSequenceNumber(i, long.MaxValue);
        }

        public void Consume(byte* ptr, int length)
            => session.TryConsumeMessages(ptr, length);
    }
}
