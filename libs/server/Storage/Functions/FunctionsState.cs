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
        private readonly CustomCommandManager customCommandManager;

        public readonly TsavoriteLog appendOnlyFile;
        public readonly WatchVersionMap watchVersionMap;
        public readonly MemoryPool<byte> memoryPool;
        public readonly CacheSizeTracker objectStoreSizeTracker;
        public readonly GarnetObjectSerializer garnetObjectSerializer;
        public bool StoredProcMode;

        public FunctionsState(TsavoriteLog appendOnlyFile, WatchVersionMap watchVersionMap, CustomCommandManager customCommandManager,
            MemoryPool<byte> memoryPool, CacheSizeTracker objectStoreSizeTracker, GarnetObjectSerializer garnetObjectSerializer)
        {
            this.appendOnlyFile = appendOnlyFile;
            this.watchVersionMap = watchVersionMap;
            this.customCommandManager = customCommandManager;
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
            this.objectStoreSizeTracker = objectStoreSizeTracker;
            this.garnetObjectSerializer = garnetObjectSerializer;
        }

        public CustomRawStringFunctions GetCustomCommandFunctions(int id)
            => customCommandManager.TryGetCustomCommand(id, out var cmd) ? cmd.functions : null;

        public CustomObjectFactory GetCustomObjectFactory(int id)
            => customCommandManager.TryGetCustomObjectCommand(id, out var cmd) ? cmd.factory : null;

        public CustomObjectFunctions GetCustomObjectSubCommandFunctions(int id, int subId)
            => customCommandManager.TryGetCustomObjectSubCommand(id, subId, out var cmd) ? cmd.functions : null;
    }
}