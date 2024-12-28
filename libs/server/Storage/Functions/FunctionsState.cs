// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// State for Functions - one instance per session is created
    /// </summary>
    internal sealed class FunctionsState
    {
        public readonly TsavoriteLog appendOnlyFile;
        public readonly CustomRawStringCommand[] customCommands;
        public readonly CustomObjectCommandWrapper[] customObjectCommands;
        public readonly WatchVersionMap watchVersionMap;
        public readonly MemoryPool<byte> memoryPool;
        public readonly CacheSizeTracker objectStoreSizeTracker;
        public readonly GarnetObjectSerializer garnetObjectSerializer;
        public readonly ILogger logger;
        public bool StoredProcMode;

        public FunctionsState(TsavoriteLog appendOnlyFile, WatchVersionMap watchVersionMap, CustomRawStringCommand[] customCommands, CustomObjectCommandWrapper[] customObjectCommands,
            MemoryPool<byte> memoryPool, CacheSizeTracker objectStoreSizeTracker, GarnetObjectSerializer garnetObjectSerializer, ILogger logger)
        {
            this.appendOnlyFile = appendOnlyFile;
            this.watchVersionMap = watchVersionMap;
            this.customCommands = customCommands;
            this.customObjectCommands = customObjectCommands;
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
            this.objectStoreSizeTracker = objectStoreSizeTracker;
            this.garnetObjectSerializer = garnetObjectSerializer;
            this.logger = logger;
        }
    }
}