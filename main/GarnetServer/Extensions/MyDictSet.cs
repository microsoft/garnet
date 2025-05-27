// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    public class MyDictSet : CustomObjectFunctions
    {
        public override bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref ObjectInput input, ref RespMemoryWriter writer) => true;

        public override bool Updater(ReadOnlySpan<byte> key, ref ObjectInput input, IGarnetObject value, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
        {
            Debug.Assert(value is MyDict);

            var offset = 0;
            var keyArg = GetNextArg(ref input, ref offset).ToArray();
            var valueArg = GetNextArg(ref input, ref offset).ToArray();

            _ = ((MyDict)value).Set(keyArg, valueArg);
            return true;
        }
    }
}