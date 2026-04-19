// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, ObjectOutput, long>
    {
        /// <inheritdoc />
        public bool NeedInitialUpdate<TKey>(TKey key, ref ObjectInput input, ref ObjectOutput output, ref RMWInfo rmwInfo)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            var type = input.header.type;

            if ((byte)type < CustomCommandManager.CustomTypeIdStartOffset)
                return GarnetObject.NeedToCreate(input.header);

            var customObjectCommand = GetCustomObjectCommand(ref input, type);

            // TODO: Cannot use 'using' statement because writer is passed by ref. Change that to non-ref parameter and convert to 'using'.
            var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
            try
            {
                // Deliberately hiding key type complexity from custom object commands
                return customObjectCommand.NeedInitialUpdate(key.KeyBytes, ref input, ref writer);
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <inheritdoc />
        public bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, ref ObjectOutput output, ref RMWInfo rmwInfo)
        {
            Debug.Assert(!logRecord.Info.HasETag && !logRecord.Info.HasExpiration, "Should not have Expiration or ETag on InitialUpdater log records");

            var type = input.header.type;
            IGarnetObject value;
            if ((byte)type < CustomCommandManager.CustomTypeIdStartOffset)
            {
                value = GarnetObject.Create(type);
                _ = value.Operate(ref input, ref output, functionsState.respProtocolVersion);
                _ = logRecord.TrySetValueObjectAndPrepareOptionals(value, in sizeInfo);
                return true;
            }

            var customObjectCommand = GetCustomObjectCommand(ref input, type);
            value = functionsState.GetCustomObjectFactory((byte)type).Create((byte)type);

            var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
            try
            {
                var result = customObjectCommand.InitialUpdater(logRecord.Key, ref input, value, ref writer, ref rmwInfo);
                _ = logRecord.TrySetValueObjectAndPrepareOptionals(value, in sizeInfo);
                if (result)
                    sizeInfo.AssertOptionalsIfSet(logRecord.Info);
                return result;
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <inheritdoc />
        public void PostInitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, ref ObjectOutput output, ref RMWInfo rmwInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                input.header.SetExpiredFlag();
                rmwInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
            }
        }

        /// <inheritdoc />
        public bool InPlaceUpdater(ref LogRecord logRecord, ref ObjectInput input, ref ObjectOutput output, ref RMWInfo rmwInfo)
        {
            if (!logRecord.Info.ValueIsObject)
            {
                rmwInfo.Action = RMWAction.WrongType;
                output.OutputFlags |= ObjectOutputFlags.WrongType;
                return true;
            }

            if (InPlaceUpdaterWorker(ref logRecord, ref input, ref output, ref rmwInfo))
            {
                if (!logRecord.Info.Modified)
                    functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
                if (functionsState.appendOnlyFile != null)
                    rmwInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
                return true;
            }
            return false;
        }

        bool InPlaceUpdaterWorker(ref LogRecord logRecord, ref ObjectInput input, ref ObjectOutput output, ref RMWInfo rmwInfo)
        {
            // Expired data
            if (logRecord.Info.HasExpiration && input.header.CheckExpiry(logRecord.Expiration))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }

            if ((byte)input.header.type < CustomCommandManager.CustomTypeIdStartOffset)
            {
                var operateSuccessful = ((IGarnetObject)logRecord.ValueObject).Operate(ref input, ref output, functionsState.respProtocolVersion);
                if (output.HasWrongType)
                    return true;
                if (output.HasRemoveKey)
                {
                    if (!logRecord.Info.Modified)
                        functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
                    if (functionsState.appendOnlyFile != null)
                        rmwInfo.UserData |= NeedAofLog;
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;
                }

                return operateSuccessful;
            }

            var garnetValueObject = Unsafe.As<IGarnetObject>(logRecord.ValueObject);
            if (IncorrectObjectType(ref input, garnetValueObject, ref output.SpanByteAndMemory))
            {
                output.OutputFlags |= ObjectOutputFlags.WrongType;
                return true;
            }

            var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);
            var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
            try
            {
                var result = customObjectCommand.Updater(logRecord.Key, ref input, garnetValueObject, ref writer, ref rmwInfo);
                if (!result)
                    return false;
            }
            finally
            {
                writer.Dispose();
            }

            return true;
        }

        /// <inheritdoc />
        public bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref ObjectInput input,
            ref ObjectOutput output, ref RMWInfo rmwInfo) where TSourceLogRecord : ISourceLogRecord
            => true;

        /// <inheritdoc />
        public bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, ref ObjectOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            // Expired data
            if (srcLogRecord.Info.HasExpiration && input.header.CheckExpiry(srcLogRecord.Expiration))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }
            // Defer the actual copying of data to PostCopyUpdater, so we know the record has been successfully CASed into the hash chain before we potentially
            // create large allocations (e.g. if srcLogRecord is from disk, we would have to allocate the overflow byte[]). Because we are doing an update we have
            // and XLock, so nobody will see the unset data even after the CAS. Tsavorite will handle cloning the ValueObject and caching serialized data as needed,
            // based on whether srcLogRecord is in-memory or a DiskLogRecord.
            return true;
        }

        /// <inheritdoc />
        public bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, ref ObjectOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            // We perform the object update here (and not in CopyUpdater) so that we are guaranteed that the record
            // was CASed into the hash chain before it gets modified. Tsavorite's CacheSerializedObjectData (called
            // by InternalRMW right before PCU, for both memory and disk sources) has already cloned src.ValueObject
            // into dstLogRecord. Reuse it directly — do not clone again.
            var value = Unsafe.As<IGarnetObject>(dstLogRecord.ValueObject);

            // Do not set actually set dstLogRecord.Expiration until we know it is a command for which we allocated length in the LogRecord for it.
            // TODO: Object store ETags

            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);

            if ((byte)input.header.type < CustomCommandManager.CustomTypeIdStartOffset)
            {
                value.Operate(ref input, ref output, functionsState.respProtocolVersion);
                if (output.HasWrongType)
                    return true;
                if (output.HasRemoveKey)
                {
                    // Log to AOF before returning, so the mutation that emptied the collection
                    // is persisted and replayed correctly on recovery.
                    if (functionsState.appendOnlyFile != null)
                        rmwInfo.UserData |= NeedAofLog;
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;
                }
            }
            else
            {
                // TODO: Update to invoke CopyUpdater of custom object command without creating a new object
                // using Clone. Currently, expire and persist commands are performed on the new copy of the object.
                if (IncorrectObjectType(ref input, value, ref output.SpanByteAndMemory))
                {
                    output.OutputFlags |= ObjectOutputFlags.WrongType;
                    return true;
                }

                var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);
                var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
                try
                {
                    var result = customObjectCommand.Updater(srcLogRecord.Key, ref input, value, ref writer, ref rmwInfo);
                    return result;
                }
                finally
                {
                    writer.Dispose();
                }
            }

            sizeInfo.AssertOptionalsIfSet(dstLogRecord.Info);

            if (functionsState.appendOnlyFile != null)
                rmwInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
            return true;
        }


        /// <inheritdoc />
        public void PostRMWOperation<TKey, TEpochAccessor>(TKey key, ref ObjectInput input, ref RMWInfo rmwInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        {
            if ((rmwInfo.UserData & NeedAofLog) == NeedAofLog) // Check if we need to write to AOF
                WriteLogRMW(key.KeyBytes, ref input, rmwInfo.Version, rmwInfo.SessionID, epochAccessor);
        }
    }
}