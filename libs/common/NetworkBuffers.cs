// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Garnet.common
{
    /// <summary>
    /// Create a NetworkBuffers instance
    /// </summary>
    public struct NetworkBuffers
    {
        public LimitedFixedBufferPool sendBufferPool;
        public LimitedFixedBufferPool recvBufferPool;
        public readonly int sendBufferPoolSize;
        public readonly int recvBufferPoolSize;

        /// <summary>
        /// If this NetworkBuffers object has allocated separate pools for send and receive
        /// </summary>
        public bool UseSeparatePools { get; private set; }

        /// <summary>
        /// Indicates if underlying buffer pools have been allocated
        /// </summary>
        public bool IsAllocated => sendBufferPool != null || recvBufferPool != null;

        /// <summary>
        /// Default constructor
        /// </summary>
        public NetworkBuffers() : this(1 << 17, 1 << 17, false) { }

        /// <summary>
        /// Set network buffer sizes without allocating them
        /// </summary>
        /// <param name="sendBufferPoolSize"></param>
        /// <param name="recvBufferPoolSize"></param>
        /// <param name="useSeparatePools"></param>
        public NetworkBuffers(int sendBufferPoolSize = 1 << 17, int recvBufferPoolSize = 1 << 17, bool useSeparatePools = false)
        {
            this.sendBufferPoolSize = sendBufferPoolSize;
            this.recvBufferPoolSize = recvBufferPoolSize;
            this.sendBufferPool = null;
            this.recvBufferPool = null;
            UseSeparatePools = useSeparatePools;
        }

        /// <summary>
        /// Set network buffer using same pool for both send and receive
        /// </summary>
        /// <param name="networkPool"></param>
        public NetworkBuffers(LimitedFixedBufferPool networkPool) : this(networkPool, networkPool) { }

        /// <summary>
        /// Set network buffer using the provided buffer pools
        /// </summary>
        /// <param name="sendBufferPool"></param>
        /// <param name="recvBufferPool"></param>
        public NetworkBuffers(LimitedFixedBufferPool sendBufferPool, LimitedFixedBufferPool recvBufferPool)
        {
            this.sendBufferPool = sendBufferPool;
            this.recvBufferPool = recvBufferPool;
            this.sendBufferPoolSize = sendBufferPool.MinAllocationSize;
            this.recvBufferPoolSize = recvBufferPool.MinAllocationSize;
            UseSeparatePools = sendBufferPool != recvBufferPool;
        }

        /// <summary>
        /// Allocate network buffer pool
        /// </summary>
        /// <param name="logger"></param>
        /// <returns></returns>
        public NetworkBuffers Allocate(ILogger logger = null)
        {
            Debug.Assert(sendBufferPool == null && recvBufferPool == null);
            sendBufferPool = new LimitedFixedBufferPool(sendBufferPoolSize, logger: logger);
            recvBufferPool = UseSeparatePools ? new LimitedFixedBufferPool(recvBufferPoolSize, logger: logger) : sendBufferPool;
            return this;
        }

        /// <summary>
        /// Dispose associated network buffer pool
        /// </summary>
        public void Dispose()
        {
            if (!UseSeparatePools)
            {
                Debug.Assert(sendBufferPool == recvBufferPool);
                sendBufferPool?.Dispose();
            }
            else
            {
                Debug.Assert(sendBufferPool != recvBufferPool);
                sendBufferPool?.Dispose();
                recvBufferPool?.Dispose();
            }
        }
    }
}