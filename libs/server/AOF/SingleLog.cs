// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;
using Tsavorite.core;


namespace Garnet.server
{
    /// <summary>
    /// Provides a wrapper for a single Tsavorite log instance, exposing key log addresses and management operations.
    /// </summary>
    /// <remarks>This class simplifies access to the core addresses and lifecycle operations of a Tsavorite
    /// log. It is intended for scenarios where a single log instance is managed directly
    /// </remarks>
    /// <param name="logSettings">The settings used to configure the underlying Tsavorite log. Cannot be null.</param>
    /// <param name="logger">An optional logger used for diagnostic and operational messages. If null, logging is disabled.</param>
    public class SingleLog(TsavoriteLogSettings logSettings, ILogger logger = null)
    {
        readonly TsavoriteLogSettings logSettings = logSettings;
        /// <summary>
        /// The underlying TsavoriteLog instance for direct log operations.
        /// </summary>
        public readonly TsavoriteLog log = new(logSettings, logger: logger);

        public long HeaderSize => log.HeaderSize;

        public AofAddress BeginAddress => AofAddress.Create(1, value: log.BeginAddress);

        public AofAddress TailAddress => AofAddress.Create(1, value: log.TailAddress);

        public AofAddress CommittedUntilAddress => AofAddress.Create(1, value: log.CommittedUntilAddress);

        public AofAddress CommittedBeginAddress => AofAddress.Create(1, value: log.CommittedBeginAddress);

        public AofAddress FlushedUntilAddress => AofAddress.Create(1, value: log.FlushedUntilAddress);

        public AofAddress MaxMemorySizeBytes => AofAddress.Create(1, value: log.MaxMemorySizeBytes);

        public AofAddress MemorySizeBytes => AofAddress.Create(1, value: log.MemorySizeBytes);

        public void Recover() => log.Recover();
        public void Reset() => log.Reset();

        public void Dispose()
        {
            logSettings.LogDevice.Dispose();
            log.Dispose();
        }
    }
}