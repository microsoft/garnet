// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// This class is created by <see cref="TsavoriteKV{TStoreFunctions, TAllocator}"/> to pass parameters to the allocator factory function.
    /// </summary>
    public struct AllocatorSettings
    {
        /// <summary>The Log settings, usually from <see cref="KVSettings"/></summary>
        internal LogSettings LogSettings;

        /// <summary>The epoch created for the <see cref="TsavoriteKV{TStoreFunctions, TAllocator}"/></summary>
        internal LightEpoch epoch;

        /// <summary>The logger to use, either from <see cref="KVSettings"/> or created by <see cref="TsavoriteKV{TStoreFunctions, TAllocator}"/></summary>
        internal ILogger logger;

        /// <summary>The action to call on page eviction; used only for readcache</summary>
        internal Action<long, long> evictCallback;

        /// <summary>The action to execute on flush completion; used only for <see cref="TsavoriteLog"/></summary>
        internal Action<CommitInfo> flushCallback;

        internal AllocatorSettings(LogSettings logSettings, LightEpoch epoch, ILogger logger)
        {
            this.LogSettings = logSettings;
            this.epoch = epoch;
            this.logger = logger;
        }
    }
}