// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Garnet.common
{
    /// <summary>
    /// Definition of NetworkBufferSettings object
    /// </summary>
    public class NetworkBufferSettings
    {
        /// <summary>
        /// Send buffer size.
        /// (NOTE: Send buffers are fixed and cannot grow automatically. Caller responsible for allocating correct amount and handling larger payloads.)
        /// </summary>
        public readonly int sendBufferSize;

        /// <summary>
        /// Initial allocation size for receive network buffer.
        /// (NOTE: Receive buffers can automatically grow to accommodate larger payloads.)
        /// </summary>
        public readonly int initialReceiveBufferSize;

        /// <summary>
        /// Max allocation size for receive network buffer
        /// </summary>
        public readonly int maxReceiveBufferSize;

        /// <summary>
        /// Default constructor
        /// </summary>
        public NetworkBufferSettings() : this(1 << 17, 1 << 17, 1 << 20) { }

        /// <summary>
        /// Set network buffer sizes without allocating them
        /// </summary>
        /// <param name="sendBufferSize"></param>
        /// <param name="initialReceiveBufferSize"></param>
        /// <param name="maxReceiveBufferSize"></param>
        public NetworkBufferSettings(int sendBufferSize = 1 << 17, int initialReceiveBufferSize = 1 << 17, int maxReceiveBufferSize = 1 << 20)
        {
            this.sendBufferSize = sendBufferSize;
            this.initialReceiveBufferSize = initialReceiveBufferSize;
            this.maxReceiveBufferSize = maxReceiveBufferSize;
        }

        /// <summary>
        /// Allocate network buffer pool
        /// </summary>
        /// <param name="maxEntriesPerLevel"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public LimitedFixedBufferPool Create(int maxEntriesPerLevel = 16, ILogger logger = null)
        {
            var minSize = Math.Min(Math.Min(sendBufferSize, initialReceiveBufferSize), maxReceiveBufferSize);
            var maxSize = Math.Max(Math.Max(sendBufferSize, initialReceiveBufferSize), maxReceiveBufferSize);

            var levels = LimitedFixedBufferPool.GetLevel(minSize, maxSize) + 1;
            Debug.Assert(levels >= 0);
            levels = Math.Max(4, levels);
            return new LimitedFixedBufferPool(minSize, maxEntriesPerLevel: maxEntriesPerLevel, numLevels: levels, logger: logger);
        }
    }
}