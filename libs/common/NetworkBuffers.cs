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
    public struct NetworkBufferSettings
    {
        /// <summary>
        /// Send buffer size.
        /// (NOTE: Send buffers are fixed and cannot grow automatically. Caller responsible for allocating correct amount and handling larger payloads.)
        /// </summary>
        public int sendBufferSize;

        /// <summary>
        /// Initial allocation size for receive network buffer.
        /// (NOTE: Receive buffers can automatically grow to accomodate larger payloads.)
        /// </summary>
        public int initialReceiveBufferSize;

        /// <summary>
        /// Default constructor
        /// </summary>
        public NetworkBufferSettings() : this(1 << 17, 1 << 17) { }

        /// <summary>
        /// Set network buffer sizes without allocating them
        /// </summary>
        /// <param name="sendBufferSize"></param>
        /// <param name="initialBufferSize"></param>
        public NetworkBufferSettings(int sendBufferSize = 1 << 17, int initialBufferSize = 1 << 17)
        {
            this.sendBufferSize = sendBufferSize;
            this.initialReceiveBufferSize = initialBufferSize;
        }

        /// <summary>
        /// Allocate network buffer pool
        /// </summary>
        /// <param name="maxEntriesPerLevel"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public LimitedFixedBufferPool Create(int maxEntriesPerLevel = 16, ILogger logger = null)
        {

            var minSize = Math.Min(sendBufferSize, initialReceiveBufferSize);
            var maxSize = Math.Max(sendBufferSize, initialReceiveBufferSize);

            var levels = LimitedFixedBufferPool.GetLevel(initialReceiveBufferSize, sendBufferSize) + 1;
            Debug.Assert(levels >= 0);
            levels = Math.Max(4, levels);
            return new LimitedFixedBufferPool(initialReceiveBufferSize, maxEntriesPerLevel: maxEntriesPerLevel, numLevels: levels, logger: logger);
        }
    }
}