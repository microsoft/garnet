﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using System.Net.Security;
using Garnet.common;
using Garnet.networking;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Embedded.server
{
    internal class GarnetServerEmbedded : GarnetServerBase, IServerHook
    {
        public GarnetServerEmbedded() : base(new IPEndPoint(IPAddress.Loopback, 0), 1 << 10)
        {
        }

        public EmbeddedNetworkHandler CreateNetworkHandler(SslClientAuthenticationOptions tlsOptions = null, string remoteEndpointName = null)
        {
            var networkSender = new EmbeddedNetworkSender();
            var networkSettings = new NetworkBufferSettings();
            var networkPool = networkSettings.CreateBufferPool();
            EmbeddedNetworkHandler handler = null;

            if (activeHandlerCount >= 0)
            {
                var currentActiveHandlerCount = Interlocked.Increment(ref activeHandlerCount);
                if (currentActiveHandlerCount > 0)
                {
                    try
                    {
                        handler = new EmbeddedNetworkHandler(this, networkSender, networkSettings, networkPool, tlsOptions != null);
                        if (!activeHandlers.TryAdd(handler, default))
                            throw new Exception("Unable to add handler to dictionary");

                        handler.Start(tlsOptions, remoteEndpointName);
                        IncrementConnectionsReceived();
                        return handler;
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "Error starting network handler");
                        Interlocked.Decrement(ref activeHandlerCount);
                        handler?.Dispose();
                    }
                }
                else
                {
                    Interlocked.Decrement(ref activeHandlerCount);
                }
            }
            return handler;
        }

        public void DisposeMessageConsumer(INetworkHandler session)
        {
            if (activeHandlers.TryRemove(session, out _))
            {
                Interlocked.Decrement(ref activeHandlerCount);
                IncrementConnectionsDisposed();
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