// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, ObjectOutput, long>
    {
        /// <inheritdoc/>
        public RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref ObjectInput input)
        {
            return new RecordFieldInfo()
            {
                KeySize = key.Length,
                ValueSize = ObjectIdMap.ObjectIdSize,
                ValueIsObject = true,
                HasETag = input.metaCommandInfo.MetaCommand.IsETagCommand(),
                // No object commands take an Expiration for InitialUpdater.
            };
        }

        /// <inheritdoc/>
        public RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref ObjectInput input)
            where TSourceLogRecord : ISourceLogRecord
        {
            return new RecordFieldInfo()
            {
                KeySize = srcLogRecord.Key.Length,
                ValueSize = ObjectIdMap.ObjectIdSize,
                ValueIsObject = true,
                HasETag = SessionFunctionsUtils.CheckModifiedRecordHasEtag(srcLogRecord.ETag, ref input.metaCommandInfo),
                HasExpiration = srcLogRecord.Info.HasExpiration
            };
        }

        public RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref ObjectInput input)
        {
            return new RecordFieldInfo()
            {
                KeySize = key.Length,
                ValueSize = value.Length,
                ValueIsObject = false,
                HasETag = input.metaCommandInfo.MetaCommand.IsETagCommand()
                // No object commands take an Expiration for Upsert.
            };
        }

        public RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref ObjectInput input)
        {
            return new RecordFieldInfo()
            {
                KeySize = key.Length,
                ValueSize = ObjectIdMap.ObjectIdSize,
                ValueIsObject = true,
                HasETag = input.metaCommandInfo.MetaCommand.IsETagCommand()
                // No object commands take an Expiration for Upsert.
            };
        }

        public RecordFieldInfo GetUpsertFieldInfo<TSourceLogRecord>(ReadOnlySpan<byte> key, in TSourceLogRecord inputLogRecord, ref ObjectInput input)
            where TSourceLogRecord : ISourceLogRecord
        {
            return new RecordFieldInfo()
            {
                KeySize = key.Length,
                ValueSize = inputLogRecord.Info.ValueIsObject ? ObjectIdMap.ObjectIdSize : inputLogRecord.ValueSpan.Length,
                ValueIsObject = true,
                HasETag = input.metaCommandInfo.MetaCommand.IsETagCommand()
                // No object commands take an Expiration for Upsert.
            };
        }
    }
}