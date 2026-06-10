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
    }
}
