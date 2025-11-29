// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
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
    internal sealed unsafe partial class MigrateSession : IDisposable
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
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            if (!_disposed.TryWriteLock()) return;
            _cts?.Cancel();
            _cts?.Dispose();

            for (var i = 0; i < migrateOperation.Length; i++)
                migrateOperation[i].Dispose();
        }

        private bool CheckConnection(GarnetClientSession client)
        {
            var status = true;
            if (!client.IsConnected)
            {
                client.Reconnect((int)_timeout.TotalMilliseconds);
                if (_passwd != null)
                {
                    try
                    {
                        status = client.Authenticate(_username, _passwd).ContinueWith(resp =>
                        {
                            // Check if authenticate succeeded
                            if (!resp.Result.Equals("OK", StringComparison.Ordinal))
                            {
                                logger?.LogError("Migrate CheckConnection Authentication Error: {resp}", resp);
                                Status = MigrateState.FAIL;
                                return false;
                            }
                            return true;
                        }, TaskContinuationOptions.OnlyOnRanToCompletion).WaitAsync(_timeout, _cts.Token).Result;
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "An error occurred");
                        return false;
                    }
                }
            }
            return status;
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
        /// Change remote slot state
        /// </summary>
        /// <param name="nodeid"></param>
        /// <param name="state"></param>
        /// <returns></returns>
        public bool TrySetSlotRanges(string nodeid, MigrateState state)
        {
            var status = false;
            var client = migrateOperation[0].Client;
            try
            {
                if (!CheckConnection(client))
                    return false;
                var stateBytes = state switch
                {
                    MigrateState.IMPORT => IMPORTING,
                    MigrateState.STABLE => STABLE,
                    MigrateState.NODE => NODE,
                    _ => throw new Exception("Invalid SETSLOT Operation"),
                };

                status = client.SetSlotRange(stateBytes, nodeid, _slotRanges).ContinueWith(resp =>
                {
                    // Check if setslotsrange executed correctly
                    if (!resp.Result.Equals("OK", StringComparison.Ordinal))
                    {
                        logger?.LogError("TrySetSlot error: {error}", resp);
                        Status = MigrateState.FAIL;
                        return false;
                    }
                    logger?.LogTrace("[Completed] SETSLOT {slots} {state} {nodeid}", ClusterManager.GetRange([.. _sslots]), state, nodeid == null ? "" : nodeid);
                    return true;
                }, TaskContinuationOptions.OnlyOnRanToCompletion).WaitAsync(_timeout, _cts.Token).Result;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error occurred");
                return false;
            }

            return status;
        }

        /// <summary>
        /// Reset local slot state
        /// </summary>
        public void ResetLocalSlot() => clusterProvider.clusterManager.TryResetSlotState(_sslots);

        /// <summary>
        /// Prepare remote node for importing
        /// </summary>
        /// <returns></returns>
        public bool TryPrepareLocalForMigration()
        {
            if (!clusterProvider.clusterManager.TryPrepareSlotsForMigration(_sslots, _targetNodeId, out var resp))
            {
                Status = MigrateState.FAIL;
                logger?.LogError("{errorMsg}", Encoding.ASCII.GetString(resp));
                return false;
            }
            return true;
        }

        /// <summary>
        /// Clean migration state
        /// </summary>
        /// <returns></returns>
        public bool RelinquishOwnership()
        {
            if (!clusterProvider.clusterManager.TryPrepareSlotsForOwnershipChange(_sslots, _targetNodeId, out var _))
                return false;
            return true;
        }

        /// <summary>
        /// Try recover to cluster state before migration task.
        /// Used only for MIGRATE SLOTS option.
        /// </summary>
        public bool TryRecoverFromFailure()
        {
            // Set slot at target to stable state when migrate slots fails
            // This issues a SETSLOTRANGE STABLE for the slots of the failed migration task
            if (!TrySetSlotRanges(null, MigrateState.STABLE))
            {
                logger?.LogError("MigrateSession.RecoverFromFailure failed to make slots STABLE");
                return false;
            }

            // Set slots at source node to their original state when migrate fails
            // This will execute the equivalent of SETSLOTRANGE STABLE for the slots of the failed migration task
            ResetLocalSlot();

            // Log explicit migration failure.
            Status = MigrateState.FAIL;
            return true;
        }
    }
}