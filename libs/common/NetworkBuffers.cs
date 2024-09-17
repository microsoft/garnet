// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Garnet.common
{
    /// <summary>
    /// Create a NetworkBuffers instance
    /// </summary>
    public struct NetworkBuffers
    {
        /// <summary>
        /// Buffer pool
        /// </summary>
        public LimitedFixedBufferPool bufferPool;

        /// <summary>
        /// Min allocation size for send network buffer
        /// </summary>
        public int sendMinAllocationSize;

        /// <summary>
        /// Min allocation size for recv network buffer
        /// </summary>
        public int recvMinAllocationSize;

        /// <summary>
        /// Indicates if underlying buffer pools have been allocated
        /// </summary>
        public readonly bool IsAllocated => bufferPool != null;

        /// <summary>
        /// Default constructor
        /// </summary>
        public NetworkBuffers() : this(1 << 17, 1 << 17) { }

        /// <summary>
        /// Set network buffer sizes without allocating them
        /// </summary>
        /// <param name="sendBufferPoolSize"></param>
        /// <param name="recvBufferPoolSize"></param>
        public NetworkBuffers(int sendBufferPoolSize = 1 << 17, int recvBufferPoolSize = 1 << 17)
        {
            this.sendMinAllocationSize = sendBufferPoolSize;
            this.recvMinAllocationSize = recvBufferPoolSize;
            this.bufferPool = null;
        }

        /// <summary>
        /// Allocate network buffer pool
        /// </summary>
        /// <param name="maxEntriesPerLevel"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public NetworkBuffers Allocate(int maxEntriesPerLevel = 16, ILogger logger = null)
        {
            Debug.Assert(bufferPool == null);
            var minSize = Math.Min(sendMinAllocationSize, recvMinAllocationSize);
            var maxSize = Math.Max(sendMinAllocationSize, recvMinAllocationSize);

            var levels = LimitedFixedBufferPool.GetLevel(recvMinAllocationSize, sendMinAllocationSize) + 1;
            Debug.Assert(levels >= 0);
            levels = Math.Max(4, levels);
            bufferPool = new LimitedFixedBufferPool(recvMinAllocationSize, maxEntriesPerLevel: maxEntriesPerLevel, numLevels: levels, logger: logger);
            return this;
        }

        /// <summary>
        /// Purge buffer pool
        /// </summary>
        /// <returns></returns>
        public void Purge() => bufferPool?.Purge();

        /// <summary>
        /// Get buffer pool statistics
        /// </summary>
        /// <returns></returns>
        public string GetStats() => bufferPool.GetStats();

        /// <summary>
        /// Dispose associated network buffer pool
        /// </summary>
        public void Dispose()
        {
            bufferPool?.Dispose();
        }
    }
}