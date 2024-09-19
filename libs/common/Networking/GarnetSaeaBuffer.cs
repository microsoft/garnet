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
        public GarnetSaeaBuffer(EventHandler<SocketAsyncEventArgs> eventHandler, NetworkBufferSettings networkBufferSettings, LimitedFixedBufferPool networkPool)
        {
            socketEventAsyncArgs = new SocketAsyncEventArgs();

            buffer = networkPool.Get(networkBufferSettings.sendBufferSize);
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