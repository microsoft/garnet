// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;

namespace Garnet.cluster
{
    internal sealed class GossipVersionNegotiation
    {
        private readonly GarnetClient client;
        private readonly ClusterManager manager;
        private readonly SemaphoreSlim exchangeLock = new(1, 1);
        private ClusterConfig lastConfig;
        private byte version = ClusterConfig.OutboundGossipVersion;
        private volatile bool confirmed;

        internal bool IsNegotiated => confirmed;

        internal GossipVersionNegotiation(GarnetClient client, ClusterManager manager)
        {
            this.client = client;
            this.manager = manager;
        }

        internal async Task<ClusterConfig> ExchangeAsync(ClusterConfig config, bool withMeet, string expectedNodeId, CancellationToken token)
        {
            await exchangeLock.WaitAsync(token).ConfigureAwait(false);
            try
            {
                bool sendFullConfig = withMeet || !confirmed || config != lastConfig;
                byte[] payload = sendFullConfig ? config.ToGossipByteArray(version) : [];
                using MemoryResult<byte> response = await SendAsync(payload, withMeet, token).ConfigureAwait(false);
                ClusterConfig remote = null;
                if (response.Length == 0)
                {
                    if (!confirmed || withMeet)
                        throw new InvalidDataException("Initial gossip and MEET require a full configuration reply.");
                }
                else
                {
                    remote = ValidateResponse(response, expectedNodeId, out byte replyVersion);
                    if (replyVersion != version)
                    {
                        if (confirmed || replyVersion >= version)
                            throw new InvalidDataException($"Gossip response does not match negotiated version {version}.");

                        // An older reply does not acknowledge the rejected configuration, so resend it in full.
                        byte[] retryPayload = config.ToGossipByteArray(replyVersion);
                        using MemoryResult<byte> retryResponse = await SendAsync(retryPayload, withMeet, token).ConfigureAwait(false);
                        if (retryResponse.Length > 0)
                        {
                            remote = ValidateResponse(retryResponse, remote.LocalNodeId, out byte retryVersion);
                            if (retryVersion != replyVersion)
                                throw new InvalidDataException("Gossip retry did not confirm the selected version.");
                        }
                        else if (withMeet)
                            throw new InvalidDataException("MEET retry requires a full configuration reply.");
                        version = replyVersion;
                    }
                }

                lastConfig = config;
                confirmed = true;
                return remote;
            }
            finally
            {
                exchangeLock.Release();
            }
        }

        private async Task<MemoryResult<byte>> SendAsync(byte[] payload, bool withMeet, CancellationToken token)
        {
            manager.gossipStats.UpdateGossipBytesSend(payload.Length);
            if (payload.Length == 0)
                Interlocked.Increment(ref manager.gossipStats.gossip_empty_send);
            else
                Interlocked.Increment(ref manager.gossipStats.gossip_full_send);
            MemoryResult<byte> response = withMeet
                ? await client.GossipWithMeetAsync(payload, token).ConfigureAwait(false)
                : await client.GossipAsync(payload, token).ConfigureAwait(false);
            manager.gossipStats.UpdateGossipBytesRecv(response.Length);
            return response;
        }

        private static ClusterConfig ValidateResponse(MemoryResult<byte> response, string expectedNodeId, out byte replyVersion)
        {
            if (!ClusterConfig.TryPeekVersion(response.Span, out replyVersion) || !ClusterConfig.IsSupportedVersion(replyVersion))
                throw new InvalidDataException("Gossip response contains an unsupported configuration version.");
            ClusterConfig remote = ClusterConfig.FromByteArray(response.Span.ToArray());
            if (string.IsNullOrEmpty(remote.LocalNodeId) || (expectedNodeId != null && remote.LocalNodeId != expectedNodeId))
                throw new InvalidDataException("Gossip response does not match the expected node identity.");
            return remote;
        }
    }
}