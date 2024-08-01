// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using System.Diagnostics;
using System.Text;
using Garnet.server;
using Tsavorite.core;

namespace GarnetJSON
{
    class JsonSET : CustomObjectFunctions
    {
        public override bool Updater(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, IGarnetObject jsonObject, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo)
        {
            Debug.Assert(jsonObject is JsonObject);

            int offset = 0;
            var path = CustomCommandUtils.GetNextArg(input, ref offset);
            var value = CustomCommandUtils.GetNextArg(input, ref offset);

            ((JsonObject)jsonObject).Set(Encoding.UTF8.GetString(path), Encoding.UTF8.GetString(value));
            return true;
        }
    }

    class JsonGET : CustomObjectFunctions
    {
        public override bool NeedInitialUpdate(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, ref (IMemoryOwner<byte>, int) output) => throw new NotImplementedException();

        public override bool Reader(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref ReadInfo readInfo)
        {
            Debug.Assert(value is JsonObject);

            int offset = 0;
            var path = CustomCommandUtils.GetNextArg(input, ref offset);

            var result = ((JsonObject)value).Get(Encoding.UTF8.GetString(path));
            CustomCommandUtils.WriteBulkString(ref output, Encoding.UTF8.GetBytes(result));
            return true;
        }
    }
}