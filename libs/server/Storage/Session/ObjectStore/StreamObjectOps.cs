// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        /// <summary>
        /// Execute a mutating stream sub-operation (XADD, XDEL, XGROUP*, XREADGROUP, ...) through the
        /// object store RMW path. The stream is created on demand by the RMW initial-update path.
        /// </summary>
        public GarnetStatus StreamObjectRMW(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => RMWObjectStoreOperation(key.ReadOnlySpan, ref input, ref objectBasicContext, ref output);

        /// <summary>
        /// Execute a read-only stream sub-operation (XLEN, XRANGE, XINFO, XPENDING, ...) through the
        /// object store read path. Returns NOTFOUND when the stream key does not exist.
        /// </summary>
        public GarnetStatus StreamObjectRead(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output)
            => ReadObjectStoreOperation(key.ReadOnlySpan, ref input, ref objectBasicContext, ref output);

        /// <summary>
        /// XREAD against a single stream: builds a one-arg ([id]) input carrying the count and reads
        /// entries after <paramref name="id"/>. Used by the multi-key XREAD handler per stream.
        /// </summary>
        public GarnetStatus StreamReadOne(PinnedSpanByte key, PinnedSpanByte id, int count, ref ObjectOutput output)
        {
            parseState.InitializeWithArgument(id);
            var header = new RespInputHeader(GarnetObjectType.Stream) { StreamOp = StreamOperation.XREAD };
            var input = new ObjectInput(header, ref parseState) { arg2 = count };
            return ReadObjectStoreOperation(key.ReadOnlySpan, ref input, ref objectBasicContext, ref output);
        }

        /// <summary>
        /// XREADGROUP against a single stream: builds a three-arg ([group, consumer, id]) input
        /// carrying NOACK (arg1) and count (arg2). Used by the multi-key XREADGROUP handler per stream.
        /// </summary>
        public GarnetStatus StreamReadGroupOne(PinnedSpanByte key, PinnedSpanByte group, PinnedSpanByte consumer, PinnedSpanByte id, int count, bool noAck, ref ObjectOutput output)
        {
            parseState.InitializeWithArguments(group, consumer, id);
            var header = new RespInputHeader(GarnetObjectType.Stream) { StreamOp = StreamOperation.XREADGROUP };
            var input = new ObjectInput(header, ref parseState) { arg1 = noAck ? 1 : 0, arg2 = count };
            return RMWObjectStoreOperation(key.ReadOnlySpan, ref input, ref objectBasicContext, ref output);
        }
    }
}
