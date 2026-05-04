// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, ObjectOutput, long>
    {
        /// <inheritdoc/>
        public RecordFieldInfo GetRMWInitialFieldInfo<TKey>(TKey key, ref ObjectInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            // We know namespaces aren't present in object functions, so don't populate
            return new RecordFieldInfo()
            {
                KeySize = key.KeyBytes.Length,
                ValueSize = ObjectIdMap.ObjectIdSize,
                ValueIsObject = true,
                HasETag = false
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
                HasETag = false,
                HasExpiration = srcLogRecord.Info.HasExpiration
            };
        }

        public RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, ref ObjectInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            // We know namespaces aren't present in object functions, so don't populate
            if (input.garnetObject != null)
            {
                return new RecordFieldInfo()
                {
                    KeySize = key.KeyBytes.Length,
                    ValueSize = ObjectIdMap.ObjectIdSize,
                    ValueIsObject = true,
                    HasETag = false,
                    HasExpiration = input.arg1 != 0
                };
            }
            return new RecordFieldInfo()
            {
                KeySize = key.KeyBytes.Length,
                ValueSize = input.parseState.GetArgSliceByRef(0).Length,
                ValueIsObject = false,
                HasETag = false,
                HasExpiration = input.arg1 != 0
            };
        }
    }
}