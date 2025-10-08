// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
        public ref AofAddress CurrentSafeAofAddress { get { return ref currentSafeAofAddress; } }
        AofAddress currentSafeAofAddress;
        public ref AofAddress RecoveredSafeAofAddress { get { return ref recoveredSafeAofAddress; } }
        AofAddress recoveredSafeAofAddress;

        /// <summary>
        /// Create new instance of Garnet checkpoint manager
        /// </summary>
        /// <param name="AofSublogCount">Number of sublog for Aof</param>
        /// <param name="deviceFactoryCreator">Factory for getting devices</param>
        /// <param name="checkpointNamingScheme">Checkpoint naming helper</param>
        /// <param name="removeOutdated">Remove older Tsavorite log commits</param>
        /// <param name="fastCommitThrottleFreq">FastCommit throttle frequency - use only in FastCommit mode</param>
        /// <param name="logger">Logger</param>
        public GarnetCheckpointManager(int AofSublogCount, INamedDeviceFactoryCreator deviceFactoryCreator, ICheckpointNamingScheme checkpointNamingScheme, bool removeOutdated = true, int fastCommitThrottleFreq = 0, ILogger logger = null)
            : base(deviceFactoryCreator, checkpointNamingScheme, removeOutdated, fastCommitThrottleFreq, logger)
        {
            CurrentHistoryId = null;
            RecoveredHistoryId = null;
            currentSafeAofAddress = AofAddress.SetValue(AofSublogCount, 0);
            recoveredSafeAofAddress = AofAddress.SetValue(AofSublogCount, 0);
        }

        /// <inheritdoc />
        public override unsafe byte[] GetCookie()
        {
            if (CurrentHistoryId == null) return null;
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