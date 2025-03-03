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
            return new RecordFieldInfo()
            {
                KeyTotalSize = key.TotalSize,
                ValueTotalSize = ObjectIdMap.ObjectIdSize,
                ValueIsObject = true
                // No object commands take an Expiration for InitialUpdater.
                // TODO ETag?
            };
        }

        /// <inheritdoc/>
        public RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref ObjectInput input)
            where TSourceLogRecord : ISourceLogRecord<IGarnetObject>
        {
            var fieldInfo = new RecordFieldInfo()
            {
                KeyTotalSize = srcLogRecord.Key.TotalSize,
                ValueTotalSize = ObjectIdMap.ObjectIdSize,
                ValueIsObject = true,
                HasExpiration = srcLogRecord.Info.HasExpiration
            };

            switch (input.header.type)
            {
                case GarnetObjectType.Expire:
                case GarnetObjectType.PExpire:
                    fieldInfo.HasExpiration = true;
                    return fieldInfo;

                case GarnetObjectType.Persist:
                    fieldInfo.HasExpiration = false;
                    return fieldInfo;

                default:
                    return fieldInfo;
            }
        }

        public RecordFieldInfo GetUpsertFieldInfo(SpanByte key, IGarnetObject value, ref ObjectInput input)
        {
            return new RecordFieldInfo()
            {
                KeyTotalSize = key.TotalSize,
                ValueTotalSize = ObjectIdMap.ObjectIdSize,
                ValueIsObject = true
                // No object commands take an Expiration for Upsert.
                // TODO ETag?
            };
        }
    }
}