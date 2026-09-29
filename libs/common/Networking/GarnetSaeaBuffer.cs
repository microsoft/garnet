// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net.Sockets;

namespace Garnet.common
{
    /// <summary>
    /// Buffer of SocketAsyncEventArgs and pinned byte array for transport
    /// </summary>
    public unsafe class GarnetSaeaBuffer : IDisposable
    {
        /// <summary>
        /// SocketAsyncEventArgs
        /// </summary>
        public readonly SocketAsyncEventArgs socketEventAsyncArgs;

        /// <summary>
        /// Byte buffer used by instance
        /// </summary>
        public readonly PoolEntry buffer;

        /// <summary>
        /// Construct new instance
        /// </summary>
        /// <param name="eventHandler">Event handler</param>
        /// <param name="networkBufferSettings"></param>
        /// <param name="networkPool"></param>
        public GarnetSaeaBuffer(EventHandler<SocketAsyncEventArgs> eventHandler, NetworkBufferSettings networkBufferSettings, LimitedFixedBufferPool networkPool)
        {
            socketEventAsyncArgs = new SocketAsyncEventArgs();

            // Send buffers do not grow, so an oversized response is chunked through the buffer it was given
            // rather than needing one large enough to hold it. That makes the size safe to adapt, and every
            // consumer reads the length off the entry rather than off the configured setting.
            var budget = networkPool.Budget;
            var size = budget.IsEnabled
                ? Math.Min(networkBufferSettings.sendBufferSize, budget.TargetSendBufferSize)
                : networkBufferSettings.sendBufferSize;

            buffer = networkPool.Get(size, PoolEntryBufferType.SaeaSendBuffer);
            socketEventAsyncArgs.SetBuffer(buffer.entry, 0, buffer.entry.Length);
            socketEventAsyncArgs.Completed += eventHandler;
        }

        /// <summary>
        /// Dispose instance
        /// </summary>
        public void Dispose()
        {
            buffer.Dispose();
            socketEventAsyncArgs.UserToken = null;
            socketEventAsyncArgs.Dispose();
        }
    }
}