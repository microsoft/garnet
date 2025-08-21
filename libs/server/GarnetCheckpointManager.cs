// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
        public long CurrentSafeAofAddress { get; set; }
        public long RecoveredSafeAofAddress { get; set; }

        /// <summary>
        /// Create new instance of Garnet checkpoint manager
        /// </summary>
        /// <param name="deviceFactoryCreator">Factory for getting devices</param>
        /// <param name="checkpointNamingScheme">Checkpoint naming helper</param>
        /// <param name="removeOutdated">Remove older Tsavorite log commits</param>
        /// <param name="fastCommitThrottleFreq">FastCommit throttle frequency - use only in FastCommit mode</param>
        /// <param name="logger">Logger</param>
        public GarnetCheckpointManager(INamedDeviceFactoryCreator deviceFactoryCreator, ICheckpointNamingScheme checkpointNamingScheme, bool removeOutdated = true, int fastCommitThrottleFreq = 0, ILogger logger = null)
            : base(deviceFactoryCreator, checkpointNamingScheme, removeOutdated, fastCommitThrottleFreq, logger)
        {
            CurrentHistoryId = null;
            RecoveredHistoryId = null;
            CurrentSafeAofAddress = 0;
            RecoveredSafeAofAddress = 0;
        }

        /// <inheritdoc />
        public override unsafe byte[] GetCookie()
        {
            if (CurrentHistoryId == null) return null;
            var cookie = new byte[sizeof(int) + sizeof(long) + CurrentHistoryId.Length];
            var primaryReplIdBytes = Encoding.ASCII.GetBytes(CurrentHistoryId);
            fixed (byte* ptr = cookie)
            fixed (byte* pridPtr = primaryReplIdBytes)
            {
                *(int*)ptr = sizeof(long) + CurrentHistoryId.Length;
                *(long*)(ptr + 4) = CurrentSafeAofAddress;
                Buffer.MemoryCopy(pridPtr, ptr + 12, primaryReplIdBytes.Length, primaryReplIdBytes.Length);
            }
            return cookie;
        }
    }
}