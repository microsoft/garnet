// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using System.Diagnostics;
using System.Text;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace GarnetJSON
{
    public class JsonSET : CustomObjectFunctions
    {
        private ILogger? logger;

        public JsonSET(ILogger? logger = null) => this.logger = logger;

        public override bool NeedInitialUpdate(ReadOnlyMemory<byte> key, ref ObjectInput input, ref (IMemoryOwner<byte>, int) output) => true;

        public override bool Updater(ReadOnlyMemory<byte> key, ref ObjectInput input, IGarnetObject jsonObject, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo)
        {
            Debug.Assert(jsonObject is JsonObject);

            int offset = 0;
            var path = CustomCommandUtils.GetNextArg(ref input, ref offset);
            var value = CustomCommandUtils.GetNextArg(ref input, ref offset);

            if (((JsonObject)jsonObject).TrySet(Encoding.UTF8.GetString(path), Encoding.UTF8.GetString(value), logger))
                WriteSimpleString(ref output, "OK");
            else
                WriteError(ref output, "ERR Invalid input");

            return true;
        }
    }

    public class JsonGET : CustomObjectFunctions
    {
        private ILogger? logger;

        public JsonGET(ILogger? logger = null) => this.logger = logger;

        public override bool Reader(ReadOnlyMemory<byte> key, ref ObjectInput input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref ReadInfo readInfo)
        {
            Debug.Assert(value is JsonObject);

            var offset = 0;
            var path = CustomCommandUtils.GetNextArg(ref input, ref offset);

            if (((JsonObject)value).TryGet(Encoding.UTF8.GetString(path), out var result, logger))
                CustomCommandUtils.WriteBulkString(ref output, Encoding.UTF8.GetBytes(result));
            else
                WriteNullBulkString(ref output);
            return true;
        }
    }
}