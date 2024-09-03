// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    public class MyDictGet : CustomObjectFunctions
    {
        public override bool Reader(ReadOnlyMemory<byte> key, ref ObjectInput input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref ReadInfo readInfo)
        {
            Debug.Assert(value is MyDict);

            var entryKey = GetFirstArg(ref input);

            var dictObject = (MyDict)value;
            if (dictObject.TryGetValue(entryKey.ToArray(), out var result))
                WriteBulkString(ref output, result);
            else
                WriteNullBulkString(ref output);

            return true;
        }
    }
}