// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Garnet server - common base class
    /// </summary>
    public abstract class GarnetServerBase : IGarnetServer
    {
        /// <summary>
        /// Active network handlers
        /// </summary>
        protected readonly ConcurrentDictionary<INetworkHandler, byte> activeHandlers;

        /// <summary>
        /// Count of active network handlers sessions
        /// </summary>
        protected int activeHandlerCount;

        /// <summary>
        /// Session providers
        /// </summary>
        readonly ConcurrentDictionary<WireFormat, ISessionProvider> sessionProviders;

        readonly int networkBufferSize;

        /// <summary>
        /// The endpoint server listener socket is bound to.
        /// </summary>
        public EndPoint EndPoint { get; }

        /// <summary>
        /// Server NetworkBufferSize
        /// </summary>        
        public int NetworkBufferSize => networkBufferSize;

        /// <summary>
        /// Check if server has been disposed
        /// </summary>
        public bool Disposed { get; set; }

        /// <summary>
        /// Logger
        /// </summary>
        protected readonly ILogger logger;


        long totalConnectionsReceived = 0;
        long totalConnectionsDisposed = 0;

        /// <summary>
        /// Add to total_connections_received
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementConnectionsReceived() => Interlocked.Increment(ref totalConnectionsReceived);

        /// <summary>
        /// Add to total_connections_disposed
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementConnectionsDisposed() => Interlocked.Increment(ref totalConnectionsDisposed);

        /// <summary>
        /// Returns all the active message consumers.
        /// </summary>
        public abstract IEnumerable<IMessageConsumer> ActiveConsumers();

        /// <summary>
        /// Return all active <see cref="IClusterSession"/>s.
        /// </summary>
        public abstract IEnumerable<IClusterSession> ActiveClusterSessions();

        /// <summary>
        /// Get total_connections_active
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long get_conn_active() => this.activeHandlers.Count;

        /// <summary>
        /// Get the total number of connections received.
        /// </summary>
        public long TotalConnectionsReceived => totalConnectionsReceived;

        /// <summary>
        /// Get the total number of connections received.
        /// </summary>
        public long TotalConnectionsDisposed => totalConnectionsDisposed;

        /// <summary>
        /// Reset connections received counter. Multiplier for accounting for pub/sub
        /// </summary>
        public void ResetConnectionsReceived() => Interlocked.Exchange(ref totalConnectionsReceived, activeHandlers.Count);

        /// <summary>
        /// Reset connections disposed counter
        /// </summary>
        public void ResetConnectionsDiposed() => Interlocked.Exchange(ref totalConnectionsDisposed, 0);

        public GarnetServerBase(EndPoint endpoint, int networkBufferSize, ILogger logger = null)
        {
            this.logger = logger;
            this.networkBufferSize = networkBufferSize;
            if (networkBufferSize == default)
                this.networkBufferSize = BufferSizeUtils.ClientBufferSize(new MaxSizeSettings());

            activeHandlers = new();
            sessionProviders = new();
            activeHandlerCount = 0;

            EndPoint = endpoint;
            Disposed = false;
        }

        /// <inheritdoc />
        public void Register(WireFormat wireFormat, ISessionProvider backendProvider)
        {
            if (!sessionProviders.TryAdd(wireFormat, backendProvider))
                throw new GarnetException($"Wire format {wireFormat} already registered");
        }

        /// <inheritdoc />
        public void Unregister(WireFormat wireFormat, out ISessionProvider provider)
            => sessionProviders.TryRemove(wireFormat, out provider);

        /// <inheritdoc />
        public ConcurrentDictionary<WireFormat, ISessionProvider> GetSessionProviders() => sessionProviders;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool AddSession(WireFormat protocol, ref ISessionProvider provider, INetworkSender networkSender, out IMessageConsumer session)
        {
            session = provider.GetSession(protocol, networkSender);

            // RESP sessions need to be able to enumerate other sessions.
            // So stash a reference back to the GarnetServer if we created a RespServerSessions.
            if (session is RespServerSession respSession)
            {
                respSession.Server = this;
            }

            return true;
        }

        /// <inheritdoc />
        public abstract void Start();

        /// <inheritdoc />
        public virtual void StopListening()
        {
            // Base implementation does nothing; derived classes should override
        }

        /// <inheritdoc />
        public virtual void Dispose()
        {
            Disposed = true;
            DisposeActiveHandlers();
            sessionProviders.Clear();
        }

        internal void DisposeActiveHandlers()
        {
            logger?.LogTrace("Begin disposing active handlers");
#if HANGDETECT
            int count = 0;
#endif
            while (activeHandlerCount >= 0)
            {
                while (activeHandlerCount > 0)
                {
                    foreach (var kvp in activeHandlers)
                    {
                        var _handler = kvp.Key;
                        _handler?.Dispose();
                    }
                    Thread.Yield();
#if HANGDETECT
                    if (++count % 10000 == 0)
                        logger?.LogTrace("Dispose iteration {count}, {activeHandlerCount}", count, activeHandlerCount);
#endif
                }
                if (Interlocked.CompareExchange(ref activeHandlerCount, int.MinValue, 0) == 0)
                    break;
            }
            logger?.LogTrace("End disposing active handlers");
        }
    }
}