// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectStoreFunctions : IFunctions<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc />
        public bool NeedInitialUpdate(ref byte[] key, ref SpanByte input, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
            return GarnetObject.NeedToCreate(*(RespInputHeader*)input.ToPointer());
        }

        /// <inheritdoc />
        public bool InitialUpdater(ref byte[] key, ref SpanByte input, ref IGarnetObject value, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            var type = ((RespInputHeader*)input.ToPointer())->type;
            if ((byte)type < CustomCommandManager.StartOffset)
                value = GarnetObject.Create(type);
            else
            {
                byte objectId = (byte)((byte)type - CustomCommandManager.StartOffset);
                value = functionsState.customObjectCommands[objectId].factory.Create((byte)type);
            }
            value.Operate(ref input, ref output.spanByteAndMemory, out _);
            return true;
        }

        /// <inheritdoc />
        public void PostInitialUpdater(ref byte[] key, ref SpanByte input, ref IGarnetObject value, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                var header = (RespInputHeader*)input.ToPointer();
                header->SetExpiredFlag();
                WriteLogRMW(ref key, ref input, ref value, rmwInfo.Version, rmwInfo.SessionID);
            }

            functionsState.objectStoreSizeTracker?.AddTrackedSize(MemoryUtils.CalculateKeyValueSize(key, value));
        }

        /// <inheritdoc />
        public bool InPlaceUpdater(ref byte[] key, ref SpanByte input, ref IGarnetObject value, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            if (InPlaceUpdaterWorker(ref key, ref input, ref value, ref output, ref rmwInfo, out long sizeChange))
            {
                if (!rmwInfo.RecordInfo.Modified)
                    functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
                if (functionsState.appendOnlyFile != null) WriteLogRMW(ref key, ref input, ref value, rmwInfo.Version, rmwInfo.SessionID);
                functionsState.objectStoreSizeTracker?.AddTrackedSize(sizeChange);
                return true;
            }
            return false;
        }

        bool InPlaceUpdaterWorker(ref byte[] key, ref SpanByte input, ref IGarnetObject value, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo, out long sizeChange)
        {
            var header = (RespInputHeader*)input.ToPointer();
            sizeChange = 0;

            // Expired data
            if (value.Expiration > 0 && header->CheckExpiry(value.Expiration))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }

            switch (header->type)
            {
                case GarnetObjectType.Expire:
                    ExpireOption optionType = (ExpireOption)(*(input.ToPointer() + RespInputHeader.Size));
                    bool expiryExists = (value.Expiration > 0);
                    return EvaluateObjectExpireInPlace(optionType, expiryExists, ref input, ref value, ref output);
                case GarnetObjectType.Persist:
                    if (value.Expiration > 0)
                    {
                        value.Expiration = 0;
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output.spanByteAndMemory);
                    }
                    else
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output.spanByteAndMemory);
                    return true;
                default:
                    return value.Operate(ref input, ref output.spanByteAndMemory, out sizeChange);
            }
        }

        /// <inheritdoc />
        public bool NeedCopyUpdate(ref byte[] key, ref SpanByte input, ref IGarnetObject oldValue, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
            => true;

        /// <inheritdoc />
        public bool CopyUpdater(ref byte[] key, ref SpanByte input, ref IGarnetObject oldValue, ref IGarnetObject newValue, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            var header = (RespInputHeader*)input.ToPointer();

            // Expired data
            if (oldValue.Expiration > 0 && header->CheckExpiry(oldValue.Expiration))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }

            oldValue.CopyUpdate(ref newValue);
            return true;
        }

        /// <inheritdoc />
        public void PostCopyUpdater(ref byte[] key, ref SpanByte input, ref IGarnetObject oldValue, ref IGarnetObject value, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
            var header = (RespInputHeader*)input.ToPointer();
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            switch (header->type)
            {
                case GarnetObjectType.Expire:
                    ExpireOption optionType = (ExpireOption)(*(input.ToPointer() + RespInputHeader.Size));
                    bool expiryExists = (value.Expiration > 0);
                    EvaluateObjectExpireInPlace(optionType, expiryExists, ref input, ref value, ref output);
                    break;
                case GarnetObjectType.Persist:
                    if (value.Expiration > 0)
                    {
                        value.Expiration = 0;
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output.spanByteAndMemory);
                    }
                    else
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output.spanByteAndMemory);
                    break;
                default:
                    value.Operate(ref input, ref output.spanByteAndMemory, out _);
                    break;
            }

            functionsState.objectStoreSizeTracker?.AddTrackedSize(MemoryUtils.CalculateKeyValueSize(key, value));

            if (functionsState.appendOnlyFile != null)
                WriteLogRMW(ref key, ref input, ref oldValue, rmwInfo.Version, rmwInfo.SessionID);
        }
    }
}