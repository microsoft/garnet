// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, GarnetObjectStoreOutput, long>
    {
        /// <summary>
        /// Logging upsert from
        /// a. InPlaceWriter
        /// b. PostInitialWriter
        /// </summary>
        void WriteLogUpsert(ReadOnlySpan<byte> key, ref ObjectInput input, ReadOnlySpan<byte> value, long version, int sessionID)
        {
            if (functionsState.StoredProcMode)
                return;
            input.header.flags |= RespInputFlags.Deterministic;

            if (functionsState.appendOnlyFile.serverOptions.MultiLogEnabled)
            {
                var header = new AofHeader
                {
                    opType = AofEntryType.ObjectStoreUpsert,
                    storeVersion = version,
                    sessionID = sessionID
                };
                functionsState.appendOnlyFile.Log.SigleLog.Enqueue(
                    header,
                    key,
                    value,
                    out _);
            }
            else
            {
                var header = new AofShardedHeader
                {
                    basicHeader = new AofHeader
                    {
                        padding = (byte)AofHeaderType.ShardedHeader,
                        opType = AofEntryType.ObjectStoreUpsert,
                        storeVersion = version,
                        sessionID = sessionID
                    },
                    sequenceNumber = functionsState.appendOnlyFile.seqNumGen.GetSequenceNumber()
                };

                functionsState.appendOnlyFile.Log.Enqueue(
                    header,
                    key,
                    value,
                    out _);
            }
        }

        /// <summary>
        /// Logging upsert from
        /// a. InPlaceWriter
        /// b. PostInitialWriter
        /// </summary>
        void WriteLogUpsert(ReadOnlySpan<byte> key, ref ObjectInput input, IGarnetObject value, long version, int sessionID)
        {
            if (functionsState.StoredProcMode)
                return;
            input.header.flags |= RespInputFlags.Deterministic;

            GarnetObjectSerializer.Serialize(value, out var valueBytes);
            fixed (byte* valPtr = valueBytes)
            {
                if (functionsState.appendOnlyFile.serverOptions.MultiLogEnabled)
                {
                    var header = new AofHeader
                    {
                        opType = AofEntryType.ObjectStoreUpsert,
                        storeVersion = version,
                        sessionID = sessionID
                    };

                    functionsState.appendOnlyFile.Log.SigleLog.Enqueue(
                        header,
                        key,
                        new ReadOnlySpan<byte>(valPtr, valueBytes.Length),
                        out _);
                }
                else
                {
                    var header = new AofShardedHeader
                    {
                        basicHeader = new AofHeader
                        {
                            padding = (byte)AofHeaderType.ShardedHeader,
                            opType = AofEntryType.ObjectStoreUpsert,
                            storeVersion = version,
                            sessionID = sessionID
                        },
                        sequenceNumber = functionsState.appendOnlyFile.seqNumGen.GetSequenceNumber(),
                    };

                    functionsState.appendOnlyFile.Log.Enqueue(
                        header,
                        key,
                        new ReadOnlySpan<byte>(valPtr, valueBytes.Length),
                        out _);
                }
            }
        }

        /// <summary>
        /// Logging RMW from
        /// a. PostInitialUpdater
        /// b. InPlaceUpdater
        /// c. PostCopyUpdater
        /// </summary>
        void WriteLogRMW(ReadOnlySpan<byte> key, ref ObjectInput input, long version, int sessionID)
        {
            if (functionsState.StoredProcMode) return;
            input.header.flags |= RespInputFlags.Deterministic;

            // Serializing key & ObjectInput to RMW log
            fixed (byte* keyPtr = key)
            {
                var sbKey = SpanByte.FromPinnedPointer(keyPtr, key.Length);

                if (functionsState.appendOnlyFile.serverOptions.MultiLogEnabled)
                {
                    var header = new AofHeader
                    {
                        opType = AofEntryType.ObjectStoreRMW,
                        storeVersion = version,
                        sessionID = sessionID
                    };

                    functionsState.appendOnlyFile.Log.SigleLog.Enqueue(
                        header,
                        sbKey,
                        ref input,
                        out _);
                }
                else
                {
                    var header = new AofShardedHeader
                    {
                        basicHeader = new AofHeader
                        {
                            padding = (byte)AofHeaderType.ShardedHeader,
                            opType = AofEntryType.ObjectStoreRMW,
                            storeVersion = version,
                            sessionID = sessionID
                        },
                        sequenceNumber = functionsState.appendOnlyFile.seqNumGen.GetSequenceNumber(),
                    };

                    functionsState.appendOnlyFile.Log.Enqueue(
                        header,
                        sbKey,
                        ref input,
                        out _);
                }
            }
        }

        /// <summary>
        ///  Logging Delete from
        ///  a. InPlaceDeleter
        ///  b. PostInitialDeleter
        /// </summary>
        void WriteLogDelete(ReadOnlySpan<byte> key, long version, int sessionID)
        {

            if (functionsState.StoredProcMode)
                return;

            if (functionsState.appendOnlyFile.serverOptions.MultiLogEnabled)
            {
                var header = new AofHeader
                {
                    opType = AofEntryType.ObjectStoreDelete,
                    storeVersion = version,
                    sessionID = sessionID
                };

                functionsState.appendOnlyFile.Log.SigleLog.Enqueue(
                    header,
                    key,
                    item2: default,
                    out _);
            }
            else
            {
                var header = new AofShardedHeader
                {
                    basicHeader = new AofHeader
                    {
                        padding = (byte)AofHeaderType.ShardedHeader,
                        opType = AofEntryType.ObjectStoreDelete,
                        storeVersion = version,
                        sessionID = sessionID
                    },
                    sequenceNumber = functionsState.appendOnlyFile.seqNumGen.GetSequenceNumber(),
                };

                functionsState.appendOnlyFile.Log.Enqueue(
                    header,
                    key,
                    value: default,
                    out _);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CustomObjectFunctions GetCustomObjectCommand(ref ObjectInput input, GarnetObjectType type)
        {
            var cmdId = input.header.SubId;
            var customObjectCommand = functionsState.GetCustomObjectSubCommandFunctions((byte)type, cmdId);
            return customObjectCommand;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe bool IncorrectObjectType(ref ObjectInput input, IGarnetObject value, ref SpanByteAndMemory output)
        {
            var inputType = (byte)input.header.type;
            if (inputType != value.Type) // Indicates an incorrect type of key
            {
                output.Length = 0;
                return true;
            }

            return false;
        }
    }
}