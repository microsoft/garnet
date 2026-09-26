// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Garnet.client
{
    /// <summary>
    /// A single fixed-size page in the <see cref="LightNetworkWriter"/> circular buffer.
    /// The page stores fixed-size out-of-line payload descriptors (self-referential log
    /// addresses), not payload bytes; the payload bytes live in separately rented buffers.
    /// </summary>
    unsafe struct LightPage
    {
        public readonly byte[] value;
        public readonly long pointer;
        public FullPageStatus PageStatusIndicator;
        public long lastOffset;

        public LightPage(int pageSize)
        {
            value = GC.AllocateArray<byte>(pageSize, true);
            pointer = (long)Unsafe.AsPointer(ref value[0]);
            PageStatusIndicator = default;
            lastOffset = 0;
        }
    }
}
