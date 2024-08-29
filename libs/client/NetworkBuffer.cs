// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.client
{
    public struct NetworkBuffers
    {
        public LimitedFixedBufferPool sendBufferPool;
        public LimitedFixedBufferPool recvBufferPool;
        public readonly int sendBufferPoolSize;
        public readonly int recvBufferPoolSize;

        public bool IsAllocated => sendBufferPool != null || recvBufferPool != null;

        bool DistinctSendRecvPool => sendBufferPool != recvBufferPool;

        public static NetworkBuffers Default => new(1 << 17, 1 << 17);

        /// <summary>
        /// Set network buffer sizes without allocating them
        /// </summary>
        /// <param name="sendBufferPoolSize"></param>
        /// <param name="recvBufferPoolSize"></param>
        public NetworkBuffers(int sendBufferPoolSize = 1 << 17, int recvBufferPoolSize = 1 << 17)
        {
            this.sendBufferPoolSize = sendBufferPoolSize;
            this.recvBufferPoolSize = recvBufferPoolSize;
            this.sendBufferPool = null;
            this.recvBufferPool = null;
        }

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
        }

        /// <summary>
        /// Allocate network buffer pool
        /// </summary>
        /// <param name="logger"></param>
        /// <returns></returns>
        public NetworkBuffers Create(ILogger logger = null)
        {
            Debug.Assert(sendBufferPool == null && recvBufferPool == null);
            sendBufferPool = new LimitedFixedBufferPool(sendBufferPoolSize, logger: logger);
            recvBufferPool = sendBufferPoolSize != recvBufferPoolSize ? new LimitedFixedBufferPool(recvBufferPoolSize, logger: logger) : sendBufferPool;
            return this;
        }

        /// <summary>
        /// Dispose associated network buffer pool
        /// </summary>
        public void Dispose()
        {
            if (!DistinctSendRecvPool)
                sendBufferPool?.Dispose();
            else
            {
                sendBufferPool?.Dispose();
                recvBufferPool?.Dispose();
            }
        }
    }
}
