// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace NoOpModule
{
    /// <summary>
    /// Represents a no-op read operation on a dummy object
    /// </summary>
    public class DummyObjectNoOpRead : CustomObjectFunctions
    {
        /// <inheritdoc />
        public override bool Reader(ReadOnlySpan<byte> key, ref ObjectInput input, IGarnetObject value,
            ref RespMemoryWriter writer, ref ReadInfo readInfo)
        {
            return true;
        }
    }
}