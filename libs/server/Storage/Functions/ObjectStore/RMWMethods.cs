// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc />
        public bool NeedInitialUpdate(ref byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
            var type = input.header.type;

            switch (type)
            {
                case GarnetObjectType.Expire:
                case GarnetObjectType.Persist:
                case GarnetObjectType.DelIfExpIm:
                    return false;
                default:
                    if ((byte)type < CustomCommandManager.CustomTypeIdStartOffset)
                        return GarnetObject.NeedToCreate(input.header);
                    else
                    {
                        var customObjectCommand = GetCustomObjectCommand(ref input, type);

                        var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
                        try
                        {
                            var ret = customObjectCommand.NeedInitialUpdate(key, ref input, ref writer);
                            return ret;
                        }
                        finally
                        {
                            writer.Dispose();
                        }
                    }
            }
        }

        /// <inheritdoc />
        public bool InitialUpdater(ref byte[] key, ref ObjectInput input, ref IGarnetObject value, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            var type = input.header.type;
            if ((byte)type < CustomCommandManager.CustomTypeIdStartOffset)
            {
                value = GarnetObject.Create(type);
                value.Operate(ref input, ref output, functionsState.respProtocolVersion, out _);
                return true;
            }
            else
            {
                Debug.Assert(type != GarnetObjectType.Expire && type != GarnetObjectType.Persist, "Expire and Persist commands should have been handled already by NeedInitialUpdate.");

                var customObjectCommand = GetCustomObjectCommand(ref input, type);
                value = functionsState.GetCustomObjectFactory((byte)type).Create((byte)type);

                var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
                try
                {
                    var result = customObjectCommand.InitialUpdater(key, ref input, value, ref writer, ref rmwInfo);
                    return result;
                }
                finally
                {
                    writer.Dispose();
                }
            }
        }

        /// <inheritdoc />
        public void PostInitialUpdater(ref byte[] key, ref ObjectInput input, ref IGarnetObject value, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                input.header.SetExpiredFlag();
                WriteLogRMW(ref key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            }

            functionsState.objectStoreSizeTracker?.AddTrackedSize(MemoryUtils.CalculateKeyValueSize(key, value));
        }

        /// <inheritdoc />
        public bool InPlaceUpdater(ref byte[] key, ref ObjectInput input, ref IGarnetObject value, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            if (InPlaceUpdaterWorker(ref key, ref input, ref value, ref output, ref rmwInfo, out var sizeChange))
            {
                if (!rmwInfo.RecordInfo.Modified)
                    functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
                if (functionsState.appendOnlyFile != null) WriteLogRMW(ref key, ref input, rmwInfo.Version, rmwInfo.SessionID);
                functionsState.objectStoreSizeTracker?.AddTrackedSize(sizeChange);
                return true;
            }
            return false;
        }

        bool InPlaceUpdaterWorker(ref byte[] key, ref ObjectInput input, ref IGarnetObject value, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo, out long sizeChange)
        {
            sizeChange = 0;

            // Expired data
            if (value.Expiration > 0 && input.header.CheckExpiry(value.Expiration))
            {
                functionsState.objectStoreSizeTracker?.AddTrackedSize(-value.Size);
                value = null;
                rmwInfo.Action = input.header.type == GarnetObjectType.DelIfExpIm ? RMWAction.ExpireAndStop : RMWAction.ExpireAndResume;
                return false;
            }

            switch (input.header.type)
            {
                case GarnetObjectType.Expire:
                    var expiryExists = value.Expiration > 0;

                    var expirationWithOption = new ExpirationWithOption(input.arg1, input.arg2);

                    return EvaluateObjectExpireInPlace(expirationWithOption.ExpireOption, expiryExists, expirationWithOption.ExpirationTimeInTicks, ref value, ref output);
                case GarnetObjectType.Persist:
                    if (value.Expiration > 0)
                    {
                        value.Expiration = 0;
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output.SpanByteAndMemory);
                    }
                    else
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output.SpanByteAndMemory);
                    return true;
                case GarnetObjectType.DelIfExpIm:
                    return true;
                default:
                    if ((byte)input.header.type < CustomCommandManager.CustomTypeIdStartOffset)
                    {
                        var operateSuccessful = value.Operate(ref input, ref output, functionsState.respProtocolVersion, out sizeChange);
                        if (output.HasWrongType)
                            return true;

                        if (output.HasRemoveKey)
                        {
                            functionsState.objectStoreSizeTracker?.AddTrackedSize(-value.Size);
                            value = null;
                            rmwInfo.Action = RMWAction.ExpireAndStop;
                            return false;
                        }

                        return operateSuccessful;
                    }
                    else
                    {
                        if (IncorrectObjectType(ref input, value, ref output.SpanByteAndMemory))
                        {
                            output.OutputFlags |= ObjectStoreOutputFlags.WrongType;
                            return true;
                        }

                        var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);
                        var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
                        try
                        {
                            var result = customObjectCommand.Updater(key, ref input, value, ref writer, ref rmwInfo);
                            return result;
                            //return customObjectCommand.InPlaceUpdateWorker(key, ref input, value, ref output.spanByteAndMemory, ref rmwInfo);
                        }
                        finally
                        {
                            writer.Dispose();
                        }
                    }
            }
        }

        /// <inheritdoc />
        public bool NeedCopyUpdate(ref byte[] key, ref ObjectInput input, ref IGarnetObject oldValue, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
            if (input.header.type == GarnetObjectType.DelIfExpIm && oldValue.Expiration > 0 && input.header.CheckExpiry(oldValue.Expiration))
            {
                rmwInfo.Action = RMWAction.ExpireAndStop;
                return false;
            }

            return true;
        }

        /// <inheritdoc />
        public bool CopyUpdater(ref byte[] key, ref ObjectInput input, ref IGarnetObject oldValue, ref IGarnetObject newValue, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            // Expired data
            if (oldValue.Expiration > 0 && input.header.CheckExpiry(oldValue.Expiration))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }
            return true;
        }

        /// <inheritdoc />
        public bool PostCopyUpdater(ref byte[] key, ref ObjectInput input, ref IGarnetObject oldValue, ref IGarnetObject value, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
            // We're performing the object update here (and not in CopyUpdater) so that we are guaranteed that
            // the record was CASed into the hash chain before it gets modified
            var oldValueSize = oldValue.Size;
            oldValue.CopyUpdate(ref oldValue, ref value, rmwInfo.RecordInfo.IsInNewVersion);

            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);

            switch (input.header.type)
            {
                case GarnetObjectType.Expire:
                    var expiryExists = value.Expiration > 0;

                    var expirationWithOption = new ExpirationWithOption(input.arg1, input.arg2);

                    EvaluateObjectExpireInPlace(expirationWithOption.ExpireOption, expiryExists, expirationWithOption.ExpirationTimeInTicks, ref value, ref output);
                    break;
                case GarnetObjectType.Persist:
                    if (value.Expiration > 0)
                    {
                        value.Expiration = 0;
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output.SpanByteAndMemory);
                    }
                    else
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output.SpanByteAndMemory);
                    break;
                case GarnetObjectType.DelIfExpIm:
                    break;
                default:
                    if ((byte)input.header.type < CustomCommandManager.CustomTypeIdStartOffset)
                    {
                        value.Operate(ref input, ref output, functionsState.respProtocolVersion, out _);
                        if (output.HasWrongType)
                            return true;

                        if (output.HasRemoveKey)
                        {
                            rmwInfo.Action = RMWAction.ExpireAndStop;
                            return false;
                        }
                        break;
                    }
                    else
                    {
                        // TODO: Update to invoke CopyUpdater of custom object command without creating a new object
                        // using Clone. Currently, expire and persist commands are performed on the new copy of the object.
                        if (IncorrectObjectType(ref input, value, ref output.SpanByteAndMemory))
                        {
                            output.OutputFlags |= ObjectStoreOutputFlags.WrongType;
                            return true;
                        }

                        var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);
                        var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
                        try
                        {
                            var result = customObjectCommand.Updater(key, ref input, value, ref writer, ref rmwInfo);
                            return result;
                        }
                        finally
                        {
                            writer.Dispose();
                        }
                    }
            }

            // If oldValue has been set to null, subtract it's size from the tracked heap size
            var sizeAdjustment = oldValue == null ? value.Size - oldValueSize : value.Size;
            functionsState.objectStoreSizeTracker?.AddTrackedSize(sizeAdjustment);

            if (functionsState.appendOnlyFile != null)
                WriteLogRMW(ref key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            return true;
        }
    }
}