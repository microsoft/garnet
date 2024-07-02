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
        public void AddEntry(ReadOnlySpan<byte> input, IGarnetObject garnetObject, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo)
        {
            Debug.Assert(garnetObject is MyDict);

            int offset = 0;
            var key = CustomCommandUtils.GetNextArg(input, ref offset).ToArray();
            var value = CustomCommandUtils.GetNextArg(input, ref offset).ToArray();

            var dictObject = (MyDict)garnetObject;
            dictObject.TryAdd(key, value);
            CustomCommandUtils.WriteSimpleString(ref output, "OK");
        }

        public override bool InitialUpdater(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo) => throw new NotImplementedException();

        public override bool InPlaceUpdater(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo) => throw new NotImplementedException();

        public override bool CopyUpdater(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, IGarnetObject oldValue, IGarnetObject newValue, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo) => throw new NotImplementedException();

        public override bool Reader(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref ReadInfo readInfo)
        {
            Debug.Assert(value is MyDict);

            var entryKey = CustomCommandUtils.GetFirstArg(input);

            var dictObject = (MyDict)value;
            if (dictObject.TryGetValue(entryKey.ToArray(), out var result))
                CustomCommandUtils.WriteBulkString(ref output, result);
            else
                CustomCommandUtils.WriteNullBulkString(ref output);

            return true;
        }
    }
}