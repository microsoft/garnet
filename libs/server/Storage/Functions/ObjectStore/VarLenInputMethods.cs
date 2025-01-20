// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc/>
        public RecordFieldInfo GetRMWInitialFieldInfo(SpanByte key, ref ObjectInput input)
        {
            var fieldInfo = new RecordFieldInfo()
            {
                KeySize = key.TotalSize,
                ValueSize = ObjectIdMap.ObjectIdSize
            };

            switch (input.header.cmd)
            {
                case RespCommand.SETBIT:
                case RespCommand.BITFIELD:
                case RespCommand.PFADD:
                case RespCommand.PFMERGE:
                    return fieldInfo;

                case RespCommand.SET:
                case RespCommand.SETEXNX:
                    fieldInfo.HasExpiration = input.arg1 != 0;
                    return fieldInfo;

                case RespCommand.SETKEEPTTL:
                case RespCommand.SETRANGE:
                case RespCommand.APPEND:
                case RespCommand.INCR:
                case RespCommand.DECR:
                case RespCommand.INCRBY:
                case RespCommand.DECRBY:
                    return fieldInfo;

                default:
                     fieldInfo.HasExpiration = input.arg1 != 0;
                    return fieldInfo;
            }
        }

        /// <inheritdoc/>
        public RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref ObjectInput input)
            where TSourceLogRecord : ISourceLogRecord<IGarnetObject>
        {
            var fieldInfo = new RecordFieldInfo()
            {
                KeySize = srcLogRecord.Key.TotalSize,
                ValueSize = ObjectIdMap.ObjectIdSize,
                HasExpiration = srcLogRecord.Info.HasExpiration
            };

            if (input.header.cmd != RespCommand.NONE)
            {
                var cmd = input.header.cmd;
                switch (cmd)
                {
                    case RespCommand.INCR:
                    case RespCommand.INCRBY:
                    case RespCommand.DECR:
                    case RespCommand.DECRBY:
                    case RespCommand.INCRBYFLOAT:
                    case RespCommand.SETBIT:
                    case RespCommand.BITFIELD:
                    case RespCommand.PFADD:
                    case RespCommand.PFMERGE:
                    case RespCommand.SETKEEPTTLXX:
                    case RespCommand.SETKEEPTTL:
                    case RespCommand.SET:
                    case RespCommand.SETEXXX:
                        return fieldInfo;

                    case RespCommand.PERSIST:
                        fieldInfo.HasExpiration = false;
                        return fieldInfo;

                    case RespCommand.EXPIRE:
                    case RespCommand.PEXPIRE:
                    case RespCommand.EXPIREAT:
                    case RespCommand.PEXPIREAT:
                        fieldInfo.HasExpiration = true;
                        return fieldInfo;

                    case RespCommand.SETRANGE:
                    case RespCommand.GETDEL:
                        return fieldInfo;

                    case RespCommand.GETEX:
                        fieldInfo.HasExpiration = input.arg1 > 0;
                        return fieldInfo;

                    case RespCommand.APPEND:
                        return fieldInfo;

                    default:
                        if ((ushort)cmd >= CustomCommandManager.StartOffset)
                        {
                            fieldInfo.HasExpiration = input.arg1 != 0;
                            return fieldInfo;
                        }
                        throw new GarnetException("Unsupported operation on input");
                }
            }

            fieldInfo.HasExpiration = input.arg1 != 0;
            return fieldInfo;
        }

        public RecordFieldInfo GetUpsertFieldInfo(SpanByte key, IGarnetObject value, ref ObjectInput input)
        {
            var fieldInfo = new RecordFieldInfo()
            {
                KeySize = key.TotalSize,
                ValueSize = ObjectIdMap.ObjectIdSize
            };

            switch (input.header.cmd)
            {
                case RespCommand.SET:
                case RespCommand.SETEX:
                    fieldInfo.HasExpiration = input.arg1 != 0;
                    break;
            }
            return fieldInfo;
        }
    }
}