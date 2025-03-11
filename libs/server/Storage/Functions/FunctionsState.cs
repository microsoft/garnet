// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// State for Functions - one instance per session is created
    /// </summary>
    internal sealed class FunctionsState
    {
        private readonly CustomCommandManager customCommandManager;

        public readonly TsavoriteAof appendOnlyFile;
        public readonly WatchVersionMap watchVersionMap;
        public readonly MemoryPool<byte> memoryPool;
        public readonly CacheSizeTracker objectStoreSizeTracker;
        public readonly GarnetObjectSerializer garnetObjectSerializer;
        public ETagState etagState;
        public readonly ILogger logger;
        public bool StoredProcMode;

        public FunctionsState(TsavoriteAof appendOnlyFile, WatchVersionMap watchVersionMap, CustomCommandManager customCommandManager,
            MemoryPool<byte> memoryPool, CacheSizeTracker objectStoreSizeTracker, GarnetObjectSerializer garnetObjectSerializer, ILogger logger)
        {
            this.appendOnlyFile = appendOnlyFile;
            this.watchVersionMap = watchVersionMap;
            this.customCommandManager = customCommandManager;
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
            this.objectStoreSizeTracker = objectStoreSizeTracker;
            this.garnetObjectSerializer = garnetObjectSerializer;
            this.etagState = new ETagState();
            this.logger = logger;
        }

        internal void CopyDefaultResp(ReadOnlySpan<byte> resp, ref SpanByteAndMemory dst)
        {
            if (resp.Length < dst.SpanByte.Length)
            {
                resp.CopyTo(dst.SpanByte.AsSpan());
                dst.SpanByte.Length = resp.Length;
                return;
            }

            dst.ConvertToHeap();
            dst.Length = resp.Length;
            dst.Memory = memoryPool.Rent(resp.Length);
            resp.CopyTo(dst.Memory.Memory.Span);
        }

        internal unsafe void CopyRespNumber(long number, ref SpanByteAndMemory dst)
        {
            byte* curr = dst.SpanByte.ToPointer();
            byte* end = curr + dst.SpanByte.Length;
            if (RespWriteUtils.TryWriteInt64(number, ref curr, end, out int integerLen, out int totalLen))
            {
                dst.SpanByte.Length = (int)(curr - dst.SpanByte.ToPointer());
                return;
            }

            //handle resp buffer overflow here
            dst.ConvertToHeap();
            dst.Length = totalLen;
            dst.Memory = memoryPool.Rent(totalLen);
            fixed (byte* ptr = dst.Memory.Memory.Span)
            {
                byte* cc = ptr;
                *cc++ = (byte)':';
                NumUtils.WriteInt64(number, integerLen, ref cc);
                *cc++ = (byte)'\r';
                *cc++ = (byte)'\n';
            }
        }

        public CustomRawStringFunctions GetCustomCommandFunctions(int id)
            => customCommandManager.TryGetCustomCommand(id, out var cmd) ? cmd.functions : null;

        public CustomObjectFactory GetCustomObjectFactory(int id)
            => customCommandManager.TryGetCustomObjectCommand(id, out var cmd) ? cmd.factory : null;

        public CustomObjectFunctions GetCustomObjectSubCommandFunctions(int id, int subId)
            => customCommandManager.TryGetCustomObjectSubCommand(id, subId, out var cmd) ? cmd.functions : null;
    }
}