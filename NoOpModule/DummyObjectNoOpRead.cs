// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using Garnet.server;
using Tsavorite.core;

namespace NoOpModule
{
    public class DummyObjectNoOpRead : CustomObjectFunctions
    {
        public override bool Reader(ReadOnlyMemory<byte> key, ref ObjectInput input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref ReadInfo readInfo)
        {
            WriteNullBulkString(ref output);
            return true;
        }
    }
}
