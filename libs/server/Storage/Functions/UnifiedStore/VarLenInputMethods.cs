// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Unified store functions
    /// </summary>
    public readonly unsafe partial struct UnifiedSessionFunctions : ISessionFunctions<UnifiedInput, UnifiedOutput, long>
    {
        public RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref UnifiedInput input)
        {
            return new RecordFieldInfo
            {
                KeySize = key.Length,
                ValueSize = 0,
                HasETag = input.metaCommandInfo.MetaCommand.IsEtagCommand()
            };
        }

        public RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord,
            ref UnifiedInput input) where TSourceLogRecord : ISourceLogRecord
        {
            var fieldInfo = new RecordFieldInfo
            {
                KeySize = srcLogRecord.Key.Length,
                ValueSize = srcLogRecord.Info.ValueIsObject ? ObjectIdMap.ObjectIdSize : 0,
                ValueIsObject = srcLogRecord.Info.ValueIsObject,
                HasETag = SessionFunctionsUtils.CheckModifiedRecordHasEtag(srcLogRecord.ETag, ref input.metaCommandInfo),
                HasExpiration = srcLogRecord.Info.HasExpiration
            };

            if (input.header.cmd != RespCommand.NONE)
            {
                var cmd = input.header.cmd;

                switch (cmd)
                {
                    case RespCommand.DEL:
                        // Min allocation (only metadata) needed since this is going to be used for tombstoning anyway.
                        return fieldInfo;

                    case RespCommand.EXPIRE:
                        {
                            // Set HasExpiration to match with EvaluateExpireInPlace.
                            if (srcLogRecord.Info.HasExpiration)
                            {
                                // case ExpireOption.NX:                // HasExpiration is true so we will retain it
                                // case ExpireOption.XX:
                                // case ExpireOption.None:
                                // case ExpireOption.GT:
                                // case ExpireOption.XXGT:
                                // case ExpireOption.LT:
                                // case ExpireOption.XXLT:
                                fieldInfo.HasExpiration = true;         // Will update or retain
                            }
                            else
                            {
                                var expirationWithOption = new ExpirationWithOption(input.arg1);
                                switch (expirationWithOption.ExpireOption)
                                {
                                    case ExpireOption.NX:
                                    case ExpireOption.None:
                                    case ExpireOption.LT:
                                        // If expiry doesn't exist, LT should treat the current expiration as infinite, so the new value must be less
                                        fieldInfo.HasExpiration = true;     // Will update or retain
                                        break;
                                    default:
                                        // case ExpireOption.XX:
                                        // case ExpireOption.GT:            // If expiry doesn't exist, GT should treat the current expiration as infinite, so the new value cannot be greater
                                        // case ExpireOption.XXGT:
                                        // case ExpireOption.XXLT:
                                        fieldInfo.HasExpiration = false;    // Will not add one and there is not one there now
                                        break;
                                }
                            }
                        }

                        if (!srcLogRecord.Info.ValueIsObject)
                            fieldInfo.ValueSize = srcLogRecord.ValueSpan.Length;
                        return fieldInfo;
                    case RespCommand.PERSIST:
                        fieldInfo.HasExpiration = false;
                        if (!srcLogRecord.Info.ValueIsObject)
                            fieldInfo.ValueSize = srcLogRecord.ValueSpan.Length;
                        return fieldInfo;
                    default:
                        return fieldInfo;
                }
            }

            fieldInfo.ValueSize = input.parseState.GetArgSliceByRef(0).Length;
            fieldInfo.HasExpiration = input.arg1 != 0;
            return fieldInfo;
        }

        public RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value,
            ref UnifiedInput input)
        {
            return new RecordFieldInfo
            {
                KeySize = key.Length,
                ValueSize = value.Length,
                ValueIsObject = false,
                HasETag = input.metaCommandInfo.MetaCommand.IsEtagCommand()
            };
        }

        public RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref UnifiedInput input)
        {
            return new RecordFieldInfo
            {
                KeySize = key.Length,
                ValueSize = ObjectIdMap.ObjectIdSize,
                ValueIsObject = true,
                HasETag = input.metaCommandInfo.MetaCommand.IsEtagCommand()
            };
        }

        public RecordFieldInfo GetUpsertFieldInfo<TSourceLogRecord>(ReadOnlySpan<byte> key,
            in TSourceLogRecord inputLogRecord,
            ref UnifiedInput input) where TSourceLogRecord : ISourceLogRecord
        {
            return new RecordFieldInfo
            {
                KeySize = key.Length,
                ValueSize = inputLogRecord.Info.ValueIsObject ? ObjectIdMap.ObjectIdSize : inputLogRecord.ValueSpan.Length,
                ValueIsObject = inputLogRecord.Info.ValueIsObject,
                HasETag = input.metaCommandInfo.MetaCommand.IsEtagCommand(),
                HasExpiration = inputLogRecord.Info.HasExpiration
            };
        }
    }
}