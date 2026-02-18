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

        public readonly TsavoriteLog appendOnlyFile;
        public readonly WatchVersionMap watchVersionMap;
        public readonly MemoryPool<byte> memoryPool;
        public readonly CacheSizeTracker objectStoreSizeTracker;
        public readonly GarnetObjectSerializer garnetObjectSerializer;
        public IStoreFunctions storeFunctions;
        public ObjectIdMap transientObjectIdMap;
        public ETagState etagState;
        public readonly ILogger logger;
        public byte respProtocolVersion;
        public bool StoredProcMode;

        internal ReadOnlySpan<byte> nilResp => respProtocolVersion >= 3 ? CmdStrings.RESP3_NULL_REPLY : CmdStrings.RESP_ERRNOTFOUND;

        public FunctionsState(TsavoriteLog appendOnlyFile, WatchVersionMap watchVersionMap, StoreWrapper storeWrapper,
            MemoryPool<byte> memoryPool, CacheSizeTracker objectStoreSizeTracker, ILogger logger,
            byte respProtocolVersion = ServerOptions.DEFAULT_RESP_VERSION)
        {
            this.appendOnlyFile = appendOnlyFile;
            this.watchVersionMap = watchVersionMap;
            this.customCommandManager = storeWrapper.customCommandManager;
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
            this.objectStoreSizeTracker = objectStoreSizeTracker;
            this.garnetObjectSerializer = storeWrapper.GarnetObjectSerializer;
            this.storeFunctions = storeWrapper.storeFunctions;
            this.transientObjectIdMap = storeWrapper.store.TransientObjectIdMap;

            this.etagState = new ETagState();
            this.logger = logger;
            this.respProtocolVersion = respProtocolVersion;
        }

        public CustomRawStringFunctions GetCustomCommandFunctions(int id)
            => customCommandManager.TryGetCustomCommand(id, out var cmd) ? cmd.functions : null;

        public CustomObjectFactory GetCustomObjectFactory(int id)
            => customCommandManager.TryGetCustomObjectCommand(id, out var cmd) ? cmd.factory : null;

        public CustomObjectFunctions GetCustomObjectSubCommandFunctions(int id, int subId)
            => customCommandManager.TryGetCustomObjectSubCommand(id, subId, out var cmd) ? cmd.functions : null;

        /// <summary>
        /// Copies the specified RESP response bytes into the destination <see cref="SpanByteAndMemory"/> buffer.
        /// If the response fits within the stack-allocated buffer, it is copied directly; otherwise, the buffer is converted to heap allocation and the response is copied there.
        /// </summary>
        /// <param name="resp">The response bytes to copy.</param>
        /// <param name="dst">The destination buffer to receive the copied response.</param>
        internal void CopyDefaultResp(ReadOnlySpan<byte> resp, ref SpanByteAndMemory dst)
        {
            if (resp.Length < dst.SpanByte.Length)
            {
                resp.CopyTo(dst.SpanByte.Span);
                dst.SpanByte.Length = resp.Length;
                return;
            }

            dst.ConvertToHeap();
            dst.Length = resp.Length;
            dst.Memory = memoryPool.Rent(resp.Length);
            resp.CopyTo(dst.MemorySpan);
        }

        /// <summary>
        /// Copies a RESP-formatted integer response into the destination <see cref="SpanByteAndMemory"/> buffer.
        /// If the buffer has sufficient stack-allocated space, the number is written directly; otherwise, the buffer is converted to heap allocation and the response is written there.
        /// </summary>
        /// <param name="number">The integer value to encode in RESP format.</param>
        /// <param name="dst">The destination buffer to receive the encoded response.</param>
        internal unsafe void CopyRespNumber(long number, ref SpanByteAndMemory dst)
        {
            var curr = dst.SpanByte.ToPointer();
            var end = curr + dst.SpanByte.Length;
            if (RespWriteUtils.TryWriteInt64(number, ref curr, end, out int integerLen, out int totalLen))
            {
                dst.SpanByte.Length = (int)(curr - dst.SpanByte.ToPointer());
                return;
            }

            // Handle resp buffer overflow here
            dst.ConvertToHeap();
            dst.Length = totalLen;
            dst.Memory = memoryPool.Rent(totalLen);
            fixed (byte* ptr = dst.MemorySpan)
            {
                var cc = ptr;
                *cc++ = (byte)':';
                NumUtils.WriteInt64(number, integerLen, ref cc);
                *cc++ = (byte)'\r';
                *cc = (byte)'\n';
            }
        }

        internal byte GetRespProtocolVersion(in ObjectInput input)
        {
            return input.header.type switch
            {
                GarnetObjectType.SortedSet =>
                    input.header.SortedSetOp switch
                    {
                        SortedSetOperation.ZINCRBY or
                            SortedSetOperation.ZPOPMIN or
                            SortedSetOperation.ZPOPMAX => input.arg2 > 0 ? (byte)input.arg2 : respProtocolVersion,
                        SortedSetOperation.ZRANGE => ((SortedSetRangeOptions)input.arg2 & SortedSetRangeOptions.Store) != 0 ? (byte)2 : respProtocolVersion,
                        _ => respProtocolVersion
                    },
                _ => respProtocolVersion
            };
        }

        internal bool HandleSkippedExecution(in RespInputHeader inputHeader, ref SpanByteAndMemory output)
        {
            if (inputHeader.CheckSkipRespOutputFlag())
                return true;

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            writer.WriteNull();

            return true;
        }
    }
}