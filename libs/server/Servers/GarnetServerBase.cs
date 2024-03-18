// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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

        readonly string address;
        readonly int port;
        readonly int networkBufferSize;

        /// <summary>
        /// Server Address
        /// </summary>        
        public string Address => address;

        /// <summary>
        /// Server Port
        /// </summary>
        public int Port => port;

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


        long total_connections_received = 0;
        long total_connections_disposed = 0;

        /// <summary>
        /// Add to total_connections_received
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void incr_conn_recv() => Interlocked.Increment(ref total_connections_received);

        /// <summary>
        /// Add to total_connections_disposed
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void incr_conn_disp() => Interlocked.Increment(ref total_connections_disposed);

        /// <summary>
        /// Returns all the active message consumers.
        /// </summary>
        public virtual IEnumerable<IMessageConsumer> ActiveConsumers()
        {
            return this.activeHandlers.Keys.Select(k => k.Session);
        }

        /// <summary>
        /// Get total_connections_received
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long get_conn_recv() => total_connections_received;

        /// <summary>
        /// Get total_connections_disposed
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long get_conn_disp() => total_connections_disposed;

        /// <summary>
        /// Reset connection recv counter. Multiplier for accounting for pub/sub
        /// </summary>
        public void reset_conn_recv() => Interlocked.Exchange(ref total_connections_received, activeHandlers.Count);

        /// <summary>
        /// Reset connection disposed counter
        /// </summary>
        public void reset_conn_disp() => Interlocked.Exchange(ref total_connections_disposed, 0);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <param name="port"></param>
        /// <param name="networkBufferSize"></param>
        /// <param name="logger"></param>
        public GarnetServerBase(string address, int port, int networkBufferSize, ILogger logger = null)
        {
            this.logger = logger == null ? null : new SessionLogger(logger, $"[{address ?? StoreWrapper.GetIp()}:{port}] ");
            this.address = address;
            this.port = port;
            this.networkBufferSize = networkBufferSize;
            if (networkBufferSize == default)
                this.networkBufferSize = BufferSizeUtils.ClientBufferSize(new MaxSizeSettings());

            activeHandlers = new();
            sessionProviders = new();
            activeHandlerCount = 0;
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
            return true;
        }

        /// <inheritdoc />
        public abstract void Start();

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