// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.common
{
    /// <summary>
    /// Identifies the buffer role when a <see cref="PoolEntry"/> is acquired from <see cref="LimitedFixedBufferPool"/>.
    /// </summary>
    public enum PoolEntryBufferType : byte
    {
        /// <summary>Default/unknown buffer type.</summary>
        Unknown = 0,

        /// <summary>Initial network receive buffer (TcpNetworkHandlerBase).</summary>
        NetworkReceiveBuffer = 1,

        /// <summary>Transport receive buffer for TLS (NetworkHandler).</summary>
        TransportReceiveBuffer = 2,

        /// <summary>Transport send buffer for TLS (NetworkHandler).</summary>
        TransportSendBuffer = 3,

        /// <summary>Doubled network receive buffer (NetworkHandler).</summary>
        DoubleNetworkReceiveBuffer = 4,

        /// <summary>Shrunk network receive buffer (NetworkHandler).</summary>
        ShrinkNetworkReceiveBuffer = 5,

        /// <summary>Doubled transport receive buffer for TLS (NetworkHandler).</summary>
        DoubleTransportReceiveBuffer = 6,

        /// <summary>Send buffer for async socket operations (GarnetSaeaBuffer).</summary>
        SaeaSendBuffer = 7,
    }

    /// <summary>
    /// Identifies the owner of a <see cref="LimitedFixedBufferPool"/> instance.
    /// Set at pool construction time to indicate which subsystem created the pool.
    /// </summary>
    public enum PoolOwnerType : byte
    {
        /// <summary>Default/unknown owner.</summary>
        Unknown = 0,

        /// <summary>Server-side network pool (GarnetServerTcp).</summary>
        ServerNetwork = 1,

        /// <summary>Replication network pool (ReplicationManager).</summary>
        Replication = 2,

        /// <summary>Client-side network pool (GarnetClientSession, self-managed).</summary>
        GarnetClientSession = 3,

        /// <summary>Migration network pool (MigrationManager).</summary>
        Migration = 4,

        /// <summary>Client-side network pool (LightClient, self-managed).</summary>
        LightClient = 5,

        /// <summary>Client-side network pool (GarnetClient, self-managed).</summary>
        GarnetClient = 6,
    }
}