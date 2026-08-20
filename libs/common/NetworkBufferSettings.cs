// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Garnet.common
{
    /// <summary>
    /// Definition of NetworkBufferSettings object
    /// </summary>
    public class NetworkBufferSettings
    {
        /// <summary>
        /// Send buffer size.
        /// (NOTE: Send buffers are fixed and cannot grow automatically. Caller responsible for allocating correct amount and handling larger payloads.)
        /// </summary>
        public readonly int sendBufferSize;

        /// <summary>
        /// Initial allocation size for receive network buffer.
        /// (NOTE: Receive buffers can automatically grow to accommodate larger payloads.)
        /// </summary>
        public readonly int initialReceiveBufferSize;

        /// <summary>
        /// Max allocation size for receive network buffer
        /// </summary>
        public readonly int maxReceiveBufferSize;

        /// <summary>
        /// Reserve in the send buffer for per-batch/per-record overhead when computing <see cref="MaxSendBufferContentSize"/>,
        /// the largest single record or record-chunk we write. It must cover the once-per-batch cluster header
        /// (<c>SetClusterMigrateHeader</c> — a 6-element RESP array <c>CLUSTER MIGRATE nodeId replace vector</c> plus the
        /// payload length reservation — is the larger of the two, ~104 bytes for a 40-char nodeId) plus per-record framing
        /// (a type byte + a 4-byte chunk length + the batch trailer). 256 leaves comfortable margin (was 64, which was too small).
        /// </summary>
        public const int SendBufferOverheadReserve = 256;

        /// <summary>
        /// The largest single record, or record-chunk, that fits in the send buffer once the per-batch header and per-record
        /// framing (<see cref="SendBufferOverheadReserve"/>) are accounted for. Used by both migration and replication (both are
        /// based on <see cref="sendBufferSize"/>): a record whose serialized length exceeds this is split into chunks of at most
        /// this size.
        /// </summary>
        public int MaxSendBufferContentSize => sendBufferSize - SendBufferOverheadReserve;

        /// <summary>
        /// Default constructor
        /// </summary>
        public NetworkBufferSettings() : this(sendBufferSize: 1 << 17, initialReceiveBufferSize: 1 << 17, maxReceiveBufferSize: 1 << 20) { }

        /// <summary>
        /// Set network buffer sizes without allocating them
        /// </summary>
        /// <param name="sendBufferSize"></param>
        /// <param name="initialReceiveBufferSize"></param>
        /// <param name="maxReceiveBufferSize"></param>
        public NetworkBufferSettings(int sendBufferSize = 1 << 17, int initialReceiveBufferSize = 1 << 17, int maxReceiveBufferSize = 1 << 20)
        {
            this.sendBufferSize = sendBufferSize;
            this.initialReceiveBufferSize = initialReceiveBufferSize;
            this.maxReceiveBufferSize = maxReceiveBufferSize;
        }

        /// <summary>
        /// Return inclusive size for array of settings
        /// </summary>
        /// <param name="settings"></param>
        /// <returns></returns>
        public static NetworkBufferSettings GetInclusive(NetworkBufferSettings[] settings)
        {
            var maxSendBufferSize = 1 << 17;
            var minInitialReceiveBufferSize = 1 << 17;
            var maxReceiveBufferSize = 1 << 20;
            foreach (var setting in settings)
            {
                maxSendBufferSize = Math.Max(maxSendBufferSize, setting.sendBufferSize);
                minInitialReceiveBufferSize = Math.Min(minInitialReceiveBufferSize, setting.initialReceiveBufferSize);
                maxReceiveBufferSize = Math.Min(maxReceiveBufferSize, setting.maxReceiveBufferSize);
            }
            return new NetworkBufferSettings(maxSendBufferSize, minInitialReceiveBufferSize, maxReceiveBufferSize);
        }

        /// <summary>
        /// Allocate network buffer pool
        /// </summary>
        /// <param name="maxEntriesPerLevel"></param>
        /// <param name="ownerType"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public LimitedFixedBufferPool CreateBufferPool(int maxEntriesPerLevel = 16, PoolOwnerType ownerType = PoolOwnerType.Unknown, ILogger logger = null)
        {
            var minSize = Math.Min(Math.Min(sendBufferSize, initialReceiveBufferSize), maxReceiveBufferSize);
            var maxSize = Math.Max(Math.Max(sendBufferSize, initialReceiveBufferSize), maxReceiveBufferSize);

            var levels = LimitedFixedBufferPool.GetLevel(minSize, maxSize) + 1;
            Debug.Assert(levels >= 0);
            levels = Math.Max(4, levels);
            return new LimitedFixedBufferPool(minSize, maxEntriesPerLevel: maxEntriesPerLevel, numLevels: levels, logger: logger, ownerType: ownerType);
        }

        public void Log(ILogger logger, string category)
            => logger?.LogInformation("[{category}] network settings: {sendBufferSize}, {initialReceiveBufferSize}, {maxReceiveBufferSize}", category, sendBufferSize, initialReceiveBufferSize, maxReceiveBufferSize);
    }
}