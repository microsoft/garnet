// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Text;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Garnet checkpoint manager, inherits from Tsavorite's DeviceLogCommitCheckpointManager
    /// </summary>
    public class GarnetCheckpointManager : DeviceLogCommitCheckpointManager
    {
        public string CurrentHistoryId { get; set; }
        public string RecoveredHistoryId { get; set; }
        public AofAddress CurrentSafeAofAddress { get; private set; }
        public AofAddress RecoveredSafeAofAddress { get; private set; }

        /// <summary>
        /// Create new instance of Garnet checkpoint manager
        /// </summary>
        /// <param name="AofPhysicalSublogCount">Number of sublog for Aof</param>
        /// <param name="deviceFactoryCreator">Factory for getting devices</param>
        /// <param name="checkpointNamingScheme">Checkpoint naming helper</param>
        /// <param name="removeOutdated">Remove older Tsavorite log commits</param>
        /// <param name="fastCommitThrottleFreq">FastCommit throttle frequency - use only in FastCommit mode</param>
        /// <param name="logger">Logger</param>
        public GarnetCheckpointManager(int AofPhysicalSublogCount, INamedDeviceFactoryCreator deviceFactoryCreator, ICheckpointNamingScheme checkpointNamingScheme, bool removeOutdated = true, int fastCommitThrottleFreq = 0, ILogger logger = null)
            : base(deviceFactoryCreator, checkpointNamingScheme, removeOutdated, fastCommitThrottleFreq, logger)
        {
            CurrentHistoryId = null;
            RecoveredHistoryId = null;
            CurrentSafeAofAddress = AofAddress.Create(AofPhysicalSublogCount, 0);
            RecoveredSafeAofAddress = AofAddress.Create(AofPhysicalSublogCount, 0);
        }

        /// <summary>
        /// Set current AOF address
        /// </summary>
        /// <param name="safeAofTailAddress"></param>
        public void SetCurrentSafeAofAddress(ref AofAddress safeAofTailAddress) => CurrentSafeAofAddress = safeAofTailAddress;

        /// <summary>
        /// Set recovered AOF address
        /// </summary>
        /// <param name="recoveredSafeAofAddress"></param>
        public void SetRecoveredSafeAofAddress(ref AofAddress recoveredSafeAofAddress) => RecoveredSafeAofAddress = recoveredSafeAofAddress;

        /// <inheritdoc />
        public override unsafe byte[] GetCookie()
        {
            if (CurrentHistoryId == null) return null;

            if (CurrentSafeAofAddress.Length == 1)
            {
                // Legacy single log serialization
                var cookie = new byte[sizeof(int) + sizeof(long) + CurrentHistoryId.Length];
                var primaryReplIdBytes = Encoding.ASCII.GetBytes(CurrentHistoryId);
                fixed (byte* ptr = cookie)
                fixed (byte* pridPtr = primaryReplIdBytes)
                {
                    *(int*)ptr = sizeof(long) + CurrentHistoryId.Length;
                    *(long*)(ptr + 4) = CurrentSafeAofAddress[0];
                    Buffer.MemoryCopy(pridPtr, ptr + 12, primaryReplIdBytes.Length, primaryReplIdBytes.Length);
                }
                return cookie;
            }
            else
            {
                // Multi-log serialization
                using var ms = new MemoryStream();
                using var writer = new BinaryWriter(ms, Encoding.ASCII);

                //1. Write history-Id
                writer.Write(CurrentHistoryId == null ? 0 : 1);
                if (CurrentHistoryId != null) writer.Write(CurrentHistoryId);
                //2. Write checkpoint covered aof address
                CurrentSafeAofAddress.Serialize(writer);

                var byteArray = ms.ToArray();
                return byteArray;
            }
        }
    }
}