// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.networking;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Embedded.server
{
    class GarnetServerEmbedded : GarnetServerBase, IServerHook
    {
        public GarnetServerEmbedded() : base("0.0.0.0", 0, 1 << 10)
        {
        }

        public void DisposeMessageConsumer(INetworkHandler session)
        {
            if (activeHandlers.TryRemove(session, out _))
            {
                Interlocked.Decrement(ref activeHandlerCount);
                incr_conn_disp();
                try
                {
                    session.Session?.Dispose();
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "Error disposing RespServerSession");
                }
            }
        }

        public override void Start()
        {
        }

        public bool TryCreateMessageConsumer(Span<byte> bytes, INetworkSender networkSender, out IMessageConsumer session)
        {
            session = null;

            // We need at least 4 bytes to determine session            
            if (bytes.Length < 4)
                return false;

            WireFormat protocol = WireFormat.ASCII;

            if (!GetSessionProviders().TryGetValue(protocol, out var provider))
            {
                var input = System.Text.Encoding.ASCII.GetString(bytes);
                logger?.LogError("Cannot identify wire protocol {bytes}", input);
                throw new Exception($"Unsupported incoming wire format {protocol} {input}");
            }

            if (!AddSession(protocol, ref provider, networkSender, out session))
                throw new Exception($"Unable to add session");

            return true;
        }
    }
}