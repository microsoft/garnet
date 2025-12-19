// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
        public EtagState etagState;
        public byte respProtocolVersion;
        public bool StoredProcMode;
        public readonly VectorManager vectorManager;

        internal ReadOnlySpan<byte> nilResp => respProtocolVersion >= 3 ? CmdStrings.RESP3_NULL_REPLY : CmdStrings.RESP_ERRNOTFOUND;

        public FunctionsState(TsavoriteLog appendOnlyFile, WatchVersionMap watchVersionMap, CustomCommandManager customCommandManager,
            MemoryPool<byte> memoryPool, CacheSizeTracker objectStoreSizeTracker, GarnetObjectSerializer garnetObjectSerializer, VectorManager vectorManager,
            byte respProtocolVersion = ServerOptions.DEFAULT_RESP_VERSION)
        {
            this.appendOnlyFile = appendOnlyFile;
            this.watchVersionMap = watchVersionMap;
            this.customCommandManager = customCommandManager;
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
            this.objectStoreSizeTracker = objectStoreSizeTracker;
            this.garnetObjectSerializer = garnetObjectSerializer;
            this.etagState = new EtagState();
            this.vectorManager = vectorManager;
            this.respProtocolVersion = respProtocolVersion;
        }

        public CustomRawStringFunctions GetCustomCommandFunctions(int id)
            => customCommandManager.TryGetCustomCommand(id, out var cmd) ? cmd.functions : null;

        public CustomObjectFactory GetCustomObjectFactory(int id)
            => customCommandManager.TryGetCustomObjectCommand(id, out var cmd) ? cmd.factory : null;

        public CustomObjectFunctions GetCustomObjectSubCommandFunctions(int id, int subId)
            => customCommandManager.TryGetCustomObjectSubCommand(id, subId, out var cmd) ? cmd.functions : null;
    }
}