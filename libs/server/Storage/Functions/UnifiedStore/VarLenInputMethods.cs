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
        public RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord,
            ref UnifiedInput input) where TSourceLogRecord : ISourceLogRecord
        {
            var fieldInfo = new RecordFieldInfo
            {
                KeySize = srcLogRecord.Key.Length,
                ValueSize = srcLogRecord.Info.ValueIsObject ? ObjectIdMap.ObjectIdSize : 0,
                ValueIsObject = srcLogRecord.Info.ValueIsObject,
                HasETag = !srcLogRecord.Info.ValueIsObject && srcLogRecord.Info.HasETag,
                HasExpiration = srcLogRecord.Info.HasExpiration
            };

            if (input.header.cmd != RespCommand.NONE)
            {
                var cmd = input.header.cmd;

                switch (cmd)
                {
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

        public RecordFieldInfo GetRMWInitialFieldInfo<TKey>(TKey key, ref UnifiedInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            // We know namespaces aren't present in string/object functions, so don't populate
            return new RecordFieldInfo
            {
                KeySize = key.KeyBytes.Length,
                ValueSize = 0,
                HasETag = false
            };
        }

        public RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, ReadOnlySpan<byte> value,
            ref UnifiedInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            // We know namespaces aren't present in string/object functions, so don't populate
            return new RecordFieldInfo
            {
                KeySize = key.KeyBytes.Length,
                ValueSize = value.Length,
                ValueIsObject = false,
                HasETag = false
            };
        }

        public RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, IHeapObject value, ref UnifiedInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            return new RecordFieldInfo
            {
                KeySize = key.KeyBytes.Length,
                ValueSize = ObjectIdMap.ObjectIdSize,
                ValueIsObject = true,
                HasETag = false
            };
        }

        public RecordFieldInfo GetUpsertFieldInfo<TKey, TSourceLogRecord>(TKey key,
            in TSourceLogRecord inputLogRecord,
            ref UnifiedInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSourceLogRecord : ISourceLogRecord
        {
            return new RecordFieldInfo
            {
                KeySize = key.KeyBytes.Length,
                ValueSize = inputLogRecord.Info.ValueIsObject ? ObjectIdMap.ObjectIdSize : inputLogRecord.ValueSpan.Length,
                ValueIsObject = inputLogRecord.Info.ValueIsObject,
                HasETag = false,
                HasExpiration = inputLogRecord.Info.HasExpiration
            };
        }
    }
}