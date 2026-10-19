// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    /// <summary>
    /// MigrateSession
    /// </summary>
    internal sealed partial class MigrateSession : IDisposable
    {
        static readonly Memory<byte> IMPORTING = "IMPORTING"u8.ToArray();
        static readonly Memory<byte> NODE = "NODE"u8.ToArray();
        static readonly Memory<byte> STABLE = "STABLE"u8.ToArray();

        readonly ClusterSession clusterSession;
        readonly ClusterProvider clusterProvider;

        /// <summary>
        /// Get/Set migration status
        /// </summary>
        internal MigrateState Status { get; set; }

        readonly ILogger logger;
        readonly string _targetAddress;
        readonly int _targetPort;
        readonly string _targetNodeId;
        readonly string _username;
        readonly string _passwd;
        readonly string _sourceNodeId;
        readonly bool _copyOption;
        readonly bool _replaceOption;
        readonly TimeSpan _timeout;
        readonly List<(int, int)> _slotRanges;
        SingleWriterMultiReaderLock _disposed;

        readonly HashSet<int> _sslots;
        readonly CancellationTokenSource _cts = new();

        /// <summary>
        /// References to <see cref="migrateOperation"/> and <see cref="_cts"/>: one for the session's place in the
        /// task store, released by <see cref="Dispose"/>, one for the migration task started on it, and one for each
        /// scan task that one starts. The last release disposes them, so a client is never disposed while a task is
        /// writing through it.
        /// </summary>
        int references = 1;

        HashSet<ulong> _namespaces;
        FrozenDictionary<ulong, ulong> _namespaceMap;

        /// <summary>
        /// Get endpoint of target node
        /// </summary>
        public string GetTargetEndpoint => _targetAddress + ":" + _targetPort;

        /// <summary>
        /// Source nodeId of migration task
        /// </summary>
        public string GetSourceNodeId => _sourceNodeId;

        /// <summary>
        /// Target nodeId of migration task
        /// </summary>
        public string GetTargetNodeId => _targetNodeId;

        /// <summary>
        /// Return slots for migration
        /// </summary>
        public HashSet<int> GetSlots => _sslots;

        /// <summary>
        /// Get network buffer specs
        /// </summary>
        public NetworkBufferSettings NetworkBufferSettings => clusterProvider.migrationManager.GetNetworkBufferSettings;

        /// <summary>
        /// Get network pool
        /// </summary>
        public LimitedFixedBufferPool GetNetworkPool => clusterProvider.migrationManager.GetNetworkPool;

        /// <summary>
        /// Check for overlapping slots between migrate sessions
        /// </summary>
        /// <param name="session"></param>
        /// <returns></returns>
        public bool Overlap(MigrateSession session)
            => session._sslots.Overlaps(_sslots);

        /// <summary>
        /// Transfer option used for this migrateSession
        /// </summary>
        readonly TransferOption transferOption;

        /// <summary>
        /// MigrateTask for background slot migrate tasks
        /// </summary>
        readonly MigrateOperation[] migrateOperation;

        /// <summary>
        /// MigrateSession Constructor
        /// </summary>
        /// <param name="clusterSession"></param>
        /// <param name="clusterProvider"></param>
        /// <param name="_targetAddress"></param>
        /// <param name="_targetPort"></param>
        /// <param name="_targetNodeId"></param>
        /// <param name="_username"></param>
        /// <param name="_passwd"></param>
        /// <param name="_sourceNodeId"></param>
        /// <param name="_copyOption"></param>
        /// <param name="_replaceOption"></param>
        /// <param name="_timeout"></param>
        /// <param name="_slots"></param>
        /// <param name="sketch"></param>
        /// <param name="transferOption"></param>
        internal MigrateSession(
            ClusterSession clusterSession,
            ClusterProvider clusterProvider,
            string _targetAddress,
            int _targetPort,
            string _targetNodeId,
            string _username,
            string _passwd,
            string _sourceNodeId,
            bool _copyOption,
            bool _replaceOption,
            int _timeout,
            HashSet<int> _slots,
            Sketch sketch,
            TransferOption transferOption)
        {
            this.logger = clusterProvider.loggerFactory.CreateLogger($"MigrateSession - {GetHashCode()}"); ;
            this.clusterSession = clusterSession;
            this.clusterProvider = clusterProvider;
            this._targetAddress = _targetAddress;
            this._targetPort = _targetPort;
            this._targetNodeId = _targetNodeId;
            this._username = _username;
            this._passwd = _passwd;
            this._sourceNodeId = _sourceNodeId;
            this._copyOption = _copyOption;
            this._replaceOption = _replaceOption;
            this._timeout = TimeSpan.FromMilliseconds(_timeout);
            this._sslots = _slots;
            this._slotRanges = GetRanges();
            this.transferOption = transferOption;

            Status = MigrateState.PENDING;

            if (transferOption == TransferOption.SLOTS)
            {
                migrateOperation = new MigrateOperation[clusterProvider.serverOptions.ParallelMigrateTaskCount];
                for (var i = 0; i < migrateOperation.Length; i++)
                    migrateOperation[i] = new MigrateOperation(this);
            }
            else
            {
                migrateOperation = new MigrateOperation[1];
                migrateOperation[0] = new MigrateOperation(this, sketch: sketch);
            }
        }

        public GarnetClientSession GetGarnetClient()
            => new(
                new IPEndPoint(IPAddress.Parse(_targetAddress), _targetPort),
                networkBufferSettings: NetworkBufferSettings,
                networkPool: GetNetworkPool,
                clusterProvider?.serverOptions.TlsOptions?.TlsClientOptions,
                authUsername: _username,
                authPassword: _passwd,
                logger: logger);

        public LocalServerSession GetLocalSession()
            => new(clusterProvider.storeWrapper);

        /// <summary>
        /// Ends the migration: cancels it and closes its connections, so a task waiting on a send or a reply fails
        /// fast. The clients expect a single user, so they are disposed by the last task to release them rather than
        /// here, where a task may still be writing through one.
        /// </summary>
        public void Dispose()
        {
            if (!_disposed.TryWriteLock()) return;
            _cts.Cancel();

            for (var i = 0; i < migrateOperation.Length; i++)
                migrateOperation[i].Client.CloseConnection();

            ReleaseReference();
        }

        /// <summary>
        /// Takes another reference while one is held, so the session is live.
        /// </summary>
        internal void AddReference()
        {
            var taken = Interlocked.Increment(ref references);
            Debug.Assert(taken > 1, "MigrateSession reference taken without one held");
        }

        /// <summary>
        /// Releases a reference, and disposes the migration operations if it is the last.
        /// </summary>
        void ReleaseReference()
        {
            var remaining = Interlocked.Decrement(ref references);
            Debug.Assert(remaining >= 0, "MigrateSession reference released more often than taken");
            if (remaining > 0)
                return;

            for (var i = 0; i < migrateOperation.Length; i++)
                migrateOperation[i].Dispose();
            _cts.Dispose();
        }

        private async ValueTask<bool> CheckConnectionAsync(GarnetClientSession client)
        {
            if (!client.IsConnected)
            {
                await client.ReconnectAsync((int)_timeout.TotalMilliseconds).ConfigureAwait(false);
                if (_passwd != null)
                {
                    try
                    {
                        var authResp = await client.Authenticate(_username, _passwd).WaitAsync(_timeout, _cts.Token).ConfigureAwait(false);

                        if (!authResp.Equals("OK", StringComparison.Ordinal))
                        {
                            logger?.LogError("Migrate CheckConnection Authentication Error: {resp}", authResp);
                            Status = MigrateState.FAIL;
                            return false;
                        }

                        return true;
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "An error occurred");
                        return false;
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Get slot ranges from slot list
        /// </summary>
        /// <returns></returns>
        private List<(int, int)> GetRanges()
        {
            if (_sslots.Count == 1) return new() { (_sslots.First(), _sslots.First()) };

            var slotRanges = new List<(int, int)>();
            var slots = _sslots.ToList();
            slots.Sort();
            int slotStart;
            int slotEnd;

            for (slotStart = 0; slotStart < slots.Count; slotStart++)
            {
                slotEnd = slotStart + 1;
                while (slotEnd < slots.Count && slots[slotEnd - 1] + 1 == slots[slotEnd])
                    slotEnd++;

                slotEnd--;
                slotRanges.Add((slots[slotStart], slots[slotEnd]));
                slotStart = slotEnd;
            }
            return slotRanges;
        }

        /// <summary>
        /// Reset local slot state
        /// </summary>
        public ValueTask ResetLocalSlotAsync() => clusterProvider.clusterManager.TryResetSlotStateAsync(_sslots);

        /// <summary>
        /// Prepare remote node for importing
        /// </summary>
        /// <returns></returns>
        public async ValueTask<bool> TryPrepareLocalForMigrationAsync()
        {
            var (success, errorMessage) = await clusterProvider.clusterManager.TryPrepareSlotsForMigrationAsync(_sslots, _targetNodeId).ConfigureAwait(false);
            if (!success)
            {
                Status = MigrateState.FAIL;
                logger?.LogError("{errorMsg}", Encoding.ASCII.GetString(errorMessage.Span));
                return false;
            }
            return true;
        }

        /// <summary>
        /// Clean migration state
        /// </summary>
        /// <returns></returns>
        public async ValueTask<bool> RelinquishOwnershipAsync()
            => (await clusterProvider.clusterManager.TryPrepareSlotsForOwnershipChangeAsync(_sslots, _targetNodeId).ConfigureAwait(false)).Success;
    }
}