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
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, ObjectOutput, long>
    {
        /// <summary>
        /// Logging upsert from
        /// a. InPlaceWriter
        /// b. PostInitialWriter
        /// </summary>
        void WriteLogUpsert<TEpochAccessor>(ReadOnlySpan<byte> key, ref ObjectInput input, ReadOnlySpan<byte> value, long version, int sessionID, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        {
            if (functionsState.StoredProcMode)
                return;
            input.header.flags |= RespInputFlags.Deterministic;

            functionsState.appendOnlyFile.Log.Enqueue(
                AofEntryType.ObjectStoreUpsert,
                version,
                sessionID,
                key,
                value,
                ref input,
                epochAccessor,
                out _);
        }

        /// <summary>
        /// Logging upsert from
        /// a. InPlaceWriter
        /// b. PostInitialWriter
        /// </summary>
        void WriteLogUpsert<TEpochAccessor>(ReadOnlySpan<byte> key, ref ObjectInput input, IGarnetObject value, long version, int sessionID, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        {
            if (functionsState.StoredProcMode)
                return;
            input.header.flags |= RespInputFlags.Deterministic;

            GarnetObjectSerializer.Serialize(value, out var valueBytes);
            fixed (byte* valPtr = valueBytes)
            {
                functionsState.appendOnlyFile.Log.Enqueue(
                    AofEntryType.ObjectStoreUpsert,
                    version,
                    sessionID,
                    key,
                    new ReadOnlySpan<byte>(valPtr, valueBytes.Length),
                    ref input,
                    epochAccessor,
                    out _);
            }
        }

        /// <summary>
        /// Logging RMW from
        /// a. PostInitialUpdater
        /// b. InPlaceUpdater
        /// c. PostCopyUpdater
        /// </summary>
        void WriteLogRMW<TEpochAccessor>(ReadOnlySpan<byte> key, ref ObjectInput input, long version, int sessionID, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        {
            if (functionsState.StoredProcMode) return;
            input.header.flags |= RespInputFlags.Deterministic;

            functionsState.appendOnlyFile.Log.Enqueue(
                AofEntryType.ObjectStoreRMW,
                version,
                sessionID,
                key,
                ref input,
                epochAccessor,
                out _);
        }

        /// <summary>
        ///  Logging Delete from
        ///  a. InPlaceDeleter
        ///  b. PostInitialDeleter
        /// </summary>
        void WriteLogDelete<TEpochAccessor>(ReadOnlySpan<byte> key, long version, int sessionID, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        {

            if (functionsState.StoredProcMode)
                return;

            functionsState.appendOnlyFile.Log.Enqueue(
                AofEntryType.ObjectStoreDelete,
                version,
                sessionID,
                key,
                value: default,
                epochAccessor,
                out _);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CustomObjectFunctions GetCustomObjectCommand(ref ObjectInput input, GarnetObjectType type)
        {
            var cmdId = input.header.SubId;
            var customObjectCommand = functionsState.GetCustomObjectSubCommandFunctions((byte)type, cmdId);
            return customObjectCommand;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IncorrectObjectType(ref ObjectInput input, IGarnetObject value, ref SpanByteAndMemory output)
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