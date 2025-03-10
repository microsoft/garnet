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
                KeyDataSize = key.Length,
                ValueDataSize = ObjectIdMap.ObjectIdSize,
                ValueIsObject = true,
                HasETag = input.header.CheckWithETagFlag()
                // No object commands take an Expiration for InitialUpdater.
            };
        }

        /// <inheritdoc/>
        public RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref ObjectInput input)
            where TSourceLogRecord : ISourceLogRecord<IGarnetObject>
        {
            var fieldInfo = new RecordFieldInfo()
            {
                KeyDataSize = srcLogRecord.Key.Length,
                ValueDataSize = ObjectIdMap.ObjectIdSize,
                ValueIsObject = true,
                HasETag = input.header.CheckWithETagFlag(),
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
                KeyDataSize = key.Length,
                ValueDataSize = ObjectIdMap.ObjectIdSize,
                ValueIsObject = true,
                 HasETag = input.header.CheckWithETagFlag()
                // No object commands take an Expiration for Upsert.
            };
        }
    }
}