// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc />
        public bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (!srcLogRecord.Info.ValueIsObject)
            {
                readInfo.Action = ReadAction.WrongType;
                return false;
            }

            if (srcLogRecord.Info.HasExpiration && srcLogRecord.Expiration < DateTimeOffset.Now.UtcTicks)
            {
                // Do not set 'value = null' or otherwise mark this; Reads should not update the database. We rely on consistently checking for expiration everywhere.
                readInfo.Action = ReadAction.Expire;
                return false;
            }

            if (input.header.type != 0)
            {
                if ((byte)input.header.type < CustomCommandManager.CustomTypeIdStartOffset)
                {
                    var opResult = ((IGarnetObject)srcLogRecord.ValueObject).Operate(ref input, ref output, functionsState.respProtocolVersion, out _);
                    if (output.HasWrongType)
                        return true;

                    return opResult;
                }

                if (IncorrectObjectType(ref input, (IGarnetObject)srcLogRecord.ValueObject, ref output.SpanByteAndMemory))
                {
                    output.OutputFlags |= OutputFlags.WrongType;
                    return true;
                }

                var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);
                var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
                try
                {
                    var result = customObjectCommand.Reader(srcLogRecord.Key, ref input, Unsafe.As<IGarnetObject>(srcLogRecord.ValueObject), ref writer, ref readInfo);
                    return result;
                }
                finally
                {
                    writer.Dispose();
                }
            }

            output.GarnetObject = (IGarnetObject)srcLogRecord.ValueObject;
            return true;
        }
    }
}