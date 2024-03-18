// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// State for Functions - one instance per session is created
    /// </summary>
    internal sealed class FunctionsState
    {
        public readonly TsavoriteLog appendOnlyFile;
        public readonly CustomCommand[] customCommands;
        public readonly CustomObjectCommandWrapper[] customObjectCommands;
        public readonly WatchVersionMap watchVersionMap;
        public readonly MemoryPool<byte> memoryPool;
        public readonly CacheSizeTracker objectStoreSizeTracker;
        public bool StoredProcMode;

        public FunctionsState(TsavoriteLog appendOnlyFile, WatchVersionMap watchVersionMap, CustomCommand[] customCommands, CustomObjectCommandWrapper[] customObjectCommands,
            MemoryPool<byte> memoryPool, CacheSizeTracker objectStoreSizeTracker)
        {
            this.appendOnlyFile = appendOnlyFile;
            this.watchVersionMap = watchVersionMap;
            this.customCommands = customCommands;
            this.customObjectCommands = customObjectCommands;
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
            this.objectStoreSizeTracker = objectStoreSizeTracker;
        }
    }
}