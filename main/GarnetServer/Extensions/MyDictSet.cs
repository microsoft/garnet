// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    public class MyDictSet : CustomObjectFunctions
    {
        public override bool NeedInitialUpdate(ReadOnlyMemory<byte> key, ref ObjectInput input, ref (IMemoryOwner<byte>, int) output) => true;

        public override bool Updater(ReadOnlyMemory<byte> key, ref ObjectInput input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo)
        {
            Debug.Assert(value is MyDict);

            var offset = 0;
            var keyArg = GetNextArg(ref input, ref offset).ToArray();
            var valueArg = GetNextArg(ref input, ref offset).ToArray();

            _ = ((MyDict)value).Set(keyArg, valueArg);
            WriteSimpleString(ref output, "OK");
            return true;
        }
    }
}