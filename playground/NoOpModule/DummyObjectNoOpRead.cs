// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
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
        public override bool Reader(ReadOnlyMemory<byte> key, ref ObjectInput input, IGarnetObject value,
            ref (IMemoryOwner<byte>, int) output, ref ReadInfo readInfo)
        {
            WriteNullBulkString(ref output);
            return true;
        }
    }
}