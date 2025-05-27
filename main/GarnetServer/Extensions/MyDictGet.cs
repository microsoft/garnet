// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    public class MyDictGet : CustomObjectFunctions
    {
        public override bool Reader(ReadOnlySpan<byte> key, ref ObjectInput input, IGarnetObject value, ref RespMemoryWriter writer, ref ReadInfo readInfo)
        {
            Debug.Assert(value is MyDict);

            var entryKey = GetFirstArg(ref input);

            var dictObject = (MyDict)value;
            if (dictObject.TryGetValue(entryKey.ToArray(), out var result))
                writer.WriteBulkString(result);
            else
                writer.WriteNull();

            return true;
        }
    }
}