// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet;

public class TestSortedSetRank : CustomTransactionProcedure
{
    public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
    {
        var offset = 0;
        var key = GetNextArg(input, ref offset);
        var increment = GetNextArg(input, ref offset);
        
        AddKey(key, LockType.Exclusive, false);
        
        return true;
    }

    public override void Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
    {
        var offset = 0;
        var key = GetNextArg(input, ref offset);
        var increment = GetNextArg(input, ref offset);
        
        var incrementNumber = long.Parse(Encoding.UTF8.GetString(increment.Span));
        
        var status = api.Increment(key, incrementNumber, out var error, out var value);
        
        WriteSimpleString(ref output, $"{status} {value}");
    }
}