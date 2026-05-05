// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Drives checkpoint transmission by iterating over checkpoint readers and sending segments.
    /// Supports both unicast (single GCS) and broadcast (multiple GCS) modes.
    /// </summary>
    internal sealed class SnapshotTransmissionDriver : IDisposable
    {
        readonly List<ISnapshotReader> checkpointReaders;
        readonly GarnetClientSession[] sessions;
        readonly bool[] sessionActive;
        readonly TimeSpan timeout;
        readonly ILogger logger;

        /// <summary>
        /// Creates a unicast driver targeting a single replica.
        /// </summary>
        public SnapshotTransmissionDriver(List<ISnapshotReader> checkpointReaders, GarnetClientSession gcs, TimeSpan timeout, ILogger logger = null)
            : this(checkpointReaders, [gcs], timeout, logger)
        {
        }

        /// <summary>
        /// Creates a broadcast driver targeting multiple replicas simultaneously.
        /// </summary>
        public SnapshotTransmissionDriver(List<ISnapshotReader> checkpointReaders, GarnetClientSession[] sessions, TimeSpan timeout, ILogger logger = null)
        {
            this.checkpointReaders = checkpointReaders;
            this.sessions = sessions;
            this.sessionActive = new bool[sessions.Length];
            Array.Fill(sessionActive, true);
            this.timeout = timeout;
            this.logger = logger;
        }

        public void Dispose()
        {
        }

        /// <summary>
        /// Returns the number of sessions still active after transmission.
        /// </summary>
        public int ActiveSessionCount
        {
            get
            {
                var count = 0;
                for (var i = 0; i < sessionActive.Length; i++)
                    if (sessionActive[i]) count++;
                return count;
            }
        }

        /// <summary>
        /// Checks if the session at the given index is still active.
        /// </summary>
        public bool IsSessionActive(int index) => sessionActive[index];

        /// <summary>
        /// Sends all checkpoint data by iterating transmit sources from each reader.
        /// In unicast mode, delegates to <see cref="ISnapshotTransmitSource.TransmitAsync"/>.
        /// In broadcast mode, reads data once and sends to all active sessions.
        /// </summary>
        public async Task SendCheckpointAsync(CancellationToken cancellationToken = default)
        {
            var isBroadcast = sessions.Length > 1;

            foreach (var checkpointReader in checkpointReaders)
            {
                foreach (var transmitSource in checkpointReader.GetTransmitSources())
                {
                    try
                    {
                        logger?.LogInformation("<Begin sending checkpoint data {token} {type} {startAddress} {endAddress}",
                            transmitSource.DataSource.Token, transmitSource.DataSource.Type, transmitSource.DataSource.StartOffset, transmitSource.DataSource.EndOffset);

                        if (isBroadcast)
                            await BroadcastTransmitSourceAsync(transmitSource, cancellationToken).ConfigureAwait(false);
                        else
                            await transmitSource.TransmitAsync(sessions[0], timeout, cancellationToken).ConfigureAwait(false);

                        logger?.LogInformation("<Complete sending checkpoint data {token} {type} {startAddress} {endAddress}",
                            transmitSource.DataSource.Token, transmitSource.DataSource.Type, transmitSource.DataSource.StartOffset, transmitSource.DataSource.EndOffset);
                    }
                    finally
                    {
                        transmitSource.Dispose();
                    }
                }
            }
        }

        async Task BroadcastTransmitSourceAsync(ISnapshotTransmitSource transmitSource, CancellationToken cancellationToken)
        {
            var dataSource = transmitSource.DataSource;
            var fileTokenBytes = dataSource.Token.ToByteArray();

            // Handle special types with headers
            if (transmitSource is RangeIndexFileTransmitSource riSource)
            {
                var keyHashDirBytes = Encoding.ASCII.GetBytes(riSource.KeyHashDir);
                await SendByteArrayToAllAsync(fileTokenBytes, (int)dataSource.Type, -1, keyHashDirBytes, cancellationToken).ConfigureAwait(false);
            }
            else if (transmitSource is TsavoriteMetadataTransmitSource)
            {
                byte[] checkpointMetadata = [];
                if (dataSource.HasNextChunk)
                {
                    var result = await dataSource.ReadNextChunkAsync(cancellationToken).ConfigureAwait(false);
                    checkpointMetadata = result.Data;
                }

                await SendByteArrayToAllAsync(fileTokenBytes, (int)dataSource.Type, -1, checkpointMetadata, cancellationToken).ConfigureAwait(false);
                return;
            }

            // Stream file chunks to all sessions
            while (dataSource.HasNextChunk)
            {
                var result = await dataSource.ReadNextChunkAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    await SendBufferToAllAsync(fileTokenBytes, (int)dataSource.Type, result.ChunkStartAddress, result.Buffer, result.BytesRead, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    result.Buffer.Return();
                }
            }

            // Send empty end-of-transmission to all sessions
            await SendByteArrayToAllAsync(fileTokenBytes, (int)dataSource.Type, dataSource.CurrentOffset, [], cancellationToken).ConfigureAwait(false);
        }

        async Task SendBufferToAllAsync(byte[] fileTokenBytes, int type, long startAddress, SectorAlignedMemory buffer, int bytesRead, CancellationToken cancellationToken)
        {
            for (var i = 0; i < sessions.Length; i++)
            {
                if (!sessionActive[i]) continue;

                try
                {
                    var resp = await sessions[i].ExecuteClusterSnapshotData(
                        fileTokenBytes, type, startAddress, buffer.GetSlice(bytesRead))
                        .WaitAsync(timeout, cancellationToken).ConfigureAwait(false);

                    if (!resp.Equals("OK"))
                    {
                        logger?.LogError("Broadcast error for session {index}: {resp}", i, resp);
                        sessionActive[i] = false;
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger?.LogError(ex, "Broadcast failed for session {index}", i);
                    sessionActive[i] = false;
                }
            }

            ThrowIfNoActiveSessions();
        }

        async Task SendByteArrayToAllAsync(byte[] fileTokenBytes, int type, long startAddress, byte[] data, CancellationToken cancellationToken)
        {
            for (var i = 0; i < sessions.Length; i++)
            {
                if (!sessionActive[i]) continue;

                try
                {
                    var resp = await sessions[i].ExecuteClusterSnapshotData(
                        fileTokenBytes, type, startAddress, data)
                        .WaitAsync(timeout, cancellationToken).ConfigureAwait(false);

                    if (!resp.Equals("OK"))
                    {
                        logger?.LogError("Broadcast error for session {index}: {resp}", i, resp);
                        sessionActive[i] = false;
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger?.LogError(ex, "Broadcast failed for session {index}", i);
                    sessionActive[i] = false;
                }
            }

            ThrowIfNoActiveSessions();
        }

        void ThrowIfNoActiveSessions()
        {
            if (ActiveSessionCount == 0)
                throw new GarnetException("All broadcast sessions have failed");
        }
    }
}