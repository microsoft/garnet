// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc/>
        public RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref ObjectInput input)
        {
            return new RecordFieldInfo()
            {
                KeyDataSize = key.Length,
                ValueDataSize = ObjectIdMap.ObjectIdSize,
                ValueIsObject = true,
                HasETag = false     // TODO ETag not supported in Object store yet: input.header.CheckWithETagFlag()
                // No object commands take an Expiration for InitialUpdater.
            };
        }

        /// <inheritdoc/>
        public RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref ObjectInput input)
            where TSourceLogRecord : ISourceLogRecord
        {
            var fieldInfo = new RecordFieldInfo()
            {
                KeyDataSize = srcLogRecord.Key.Length,
                ValueDataSize = ObjectIdMap.ObjectIdSize,
                ValueIsObject = true,
                HasETag = false, // TODO ETag not supported in Object store yet: input.header.CheckWithETagFlag(),
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

        public RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref ObjectInput input)
        {
            return new RecordFieldInfo()
            {
                KeyDataSize = key.Length,
                ValueDataSize = value.Length,
                ValueIsObject = false,
                HasETag = false     // TODO ETag not supported in Object store yet: input.header.CheckWithETagFlag()
                // No object commands take an Expiration for Upsert.
            };
        }

        public RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref ObjectInput input)
        {
            return new RecordFieldInfo()
            {
                KeyDataSize = key.Length,
                ValueDataSize = ObjectIdMap.ObjectIdSize,
                ValueIsObject = true,
                HasETag = false     // TODO ETag not supported in Object store yet: input.header.CheckWithETagFlag()
                // No object commands take an Expiration for Upsert.
            };
        }

        public RecordFieldInfo GetUpsertFieldInfo<TSourceLogRecord>(ReadOnlySpan<byte> key, in TSourceLogRecord inputLogRecord, ref ObjectInput input)
            where TSourceLogRecord : ISourceLogRecord
        {
            return new RecordFieldInfo()
            {
                KeyDataSize = key.Length,
                ValueDataSize = inputLogRecord.Info.ValueIsObject ? ObjectIdMap.ObjectIdSize : inputLogRecord.ValueSpan.Length,
                ValueIsObject = true,
                HasETag = false     // TODO ETag not supported in Object store yet: input.header.CheckWithETagFlag()
                // No object commands take an Expiration for Upsert.
            };
        }
    }
}