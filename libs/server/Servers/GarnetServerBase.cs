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
    public abstract class GarnetServerBase : IGarnetServer, IConnectionSource
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
        long totalConnectionsRejected = 0;

        /// <summary>
        /// Add to total_connections_received
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementConnectionsReceived() => Interlocked.Increment(ref totalConnectionsReceived);

        /// <summary>
        /// Add to rejected_connections. Counts connections refused because the configured
        /// connection limit was already reached, and nothing else: a socket that dies during
        /// setup, or a handler that fails to construct, is not a rejection.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementConnectionsRejected() => Interlocked.Increment(ref totalConnectionsRejected);

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
        /// Get the number of connections rejected because the connection limit was reached.
        /// </summary>
        public long TotalConnectionsRejected => totalConnectionsRejected;

        /// <summary>
        /// Process-wide connection admission control consulted on every accept, and the object a
        /// <c>CONFIG SET maxclients</c> writes through. Never null; a listener with no shared limit
        /// owns an unlimited one.
        /// </summary>
        public ConnectionLimit ConnectionLimit { get; protected set; } = new(ConnectionLimit.Unlimited);

        /// <summary>
        /// Live connections held by this listener, contributed to the process-wide connection
        /// limit. Clamped at zero because a disposed listener parks the count at int.MinValue as a
        /// sentinel, which must not subtract from its peers' populations.
        /// </summary>
        public int LiveConnectionCount
        {
            get
            {
                var count = Volatile.Read(ref activeHandlerCount);
                return count > 0 ? count : 0;
            }
        }

        /// <summary>
        /// Reset connections received counter. Multiplier for accounting for pub/sub
        /// </summary>
        public void ResetConnectionsReceived() => Interlocked.Exchange(ref totalConnectionsReceived, activeHandlers.Count);

        /// <summary>
        /// Reset connections disposed counter
        /// </summary>
        public void ResetConnectionsDiposed() => Interlocked.Exchange(ref totalConnectionsDisposed, 0);

        /// <summary>
        /// Reset connections rejected counter. Unlike the received counter, which resets to the
        /// live handler count so that received minus disposed stays equal to connected_clients,
        /// rejections track no live population and so reset to zero.
        /// </summary>
        public void ResetConnectionsRejected() => Interlocked.Exchange(ref totalConnectionsRejected, 0);

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
        public abstract void Close();

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
#if DEBUG
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var diagnosed = false;
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
#if DEBUG
                    if (!diagnosed && sw.ElapsedMilliseconds > 5_000)
                    {
                        diagnosed = true;
                        logger?.LogError("DisposeActiveHandlers blocked with activeHandlerCount={activeHandlerCount}. Active handlers:", activeHandlerCount);
                        foreach (var kvp in activeHandlers)
                            logger?.LogError("  Stuck handler: {handlerType}", kvp.Key?.GetType().FullName);
                    }
#endif
                    Thread.Yield();
                }
                if (Interlocked.CompareExchange(ref activeHandlerCount, int.MinValue, 0) == 0)
                    break;
            }
            logger?.LogTrace("End disposing active handlers");
        }
    }
}