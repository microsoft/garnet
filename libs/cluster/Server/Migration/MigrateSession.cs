// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
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

        readonly ClusterProvider clusterProvider;
        readonly LocalServerSession localServerSession;

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
        readonly List<(long, long)> _keysWithSize;

        readonly HashSet<int> _sslots;
        readonly CancellationTokenSource _cts = new();

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

        readonly GarnetClientSession _gcs;

        /// <summary>
        /// Check for overlapping slots between migrate sessions
        /// </summary>
        /// <param name="session"></param>
        /// <returns></returns>
        public bool Overlap(MigrateSession session)
            => session._sslots.Overlaps(_sslots);

        readonly int _clientBufferSize;

        /// <summary>
        /// MigrateSession Constructor
        /// </summary>
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
        /// <param name="keysWithSize"></param>
        /// <param name="logger"></param>
        internal MigrateSession(
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
            List<(long, long)> keysWithSize,
            ILogger logger = null)
        {
            this.logger = logger;
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
            this._keysWithSize = keysWithSize;

            if (clusterProvider != null)
                localServerSession = new LocalServerSession(clusterProvider.storeWrapper);
            Status = MigrateState.PENDING;

            // Single key value size + few bytes for command header and arguments
            _clientBufferSize = 256 + (1 << clusterProvider.serverOptions.PageSizeBits());
            _gcs = new(
                _targetAddress,
                _targetPort,
                clusterProvider?.serverOptions.TlsOptions?.TlsClientOptions,
                authUsername: _username,
                authPassword: _passwd,
                bufferSize: _clientBufferSize,
                logger: logger);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            _cts?.Cancel();
            _cts?.Dispose();
            _gcs.Dispose();
            localServerSession?.Dispose();
        }

        private bool CheckConnection()
        {
            bool status = true;
            if (!_gcs.IsConnected)
            {
                _gcs.Reconnect((int)_timeout.TotalMilliseconds);
                if (_passwd != null)
                {
                    try
                    {
                        status = _gcs.Authenticate(_username, _passwd).ContinueWith(resp =>
                        {
                            // Check if authenticate succeeded
                            if (!resp.Result.Equals("OK"))
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
            if (_sslots.Count == 1) return new List<(int, int)> { (_sslots.First(), _sslots.First()) };

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
            bool status = false;
            try
            {
                CheckConnection();
                Memory<byte> stateBytes = state switch
                {
                    MigrateState.IMPORT => IMPORTING,
                    MigrateState.STABLE => STABLE,
                    MigrateState.NODE => NODE,
                    _ => throw new Exception("Invalid SETSLOT Operation"),
                };

                status = _gcs.SetSlotRange(stateBytes, nodeid, _slotRanges).ContinueWith(resp =>
                {
                    // Check if setslotsrange executed correctly
                    if (!resp.Result.Equals("OK"))
                    {
                        logger?.LogError("TrySetSlot error: {error}", resp);
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

            return status;
        }

        /// <summary>
        /// Reset local slot state
        /// </summary>
        /// <returns></returns>
        public bool TryResetLocalSlot()
        {
            if (!clusterProvider.clusterManager.ResetSlotsState(_sslots, out var resp))
            {
                Status = MigrateState.FAIL;
                logger?.LogError("{resp}", Encoding.ASCII.GetString(resp));
                return false;
            }
            return true;
        }

        /// <summary>
        /// Prepare remote node for importing
        /// </summary>
        /// <returns></returns>
        public bool TryPrepareLocalForMigration()
        {
            if (!clusterProvider.clusterManager.PrepareSlotsForMigration(_sslots, _targetNodeId, out var resp))
            {
                Status = MigrateState.FAIL;
                logger?.LogError("{resp}", Encoding.ASCII.GetString(resp));
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
            if (!clusterProvider.clusterManager.PrepareSlotsForOwnershipChange(_sslots, _targetNodeId, out var _))
                return false;
            return true;
        }

        /// <summary>
        /// Recover to cluster state before migration task.
        /// Used only for MIGRATE SLOTS option.
        /// </summary>
        public bool RecoverFromFailure()
        {
            // Set slot at target to stable state when migrate slots fails
            // This issues a SETSLOTRANGE STABLE for the slots of the failed migration task
            if (!TrySetSlotRanges(null, MigrateState.STABLE))
                return false;

            // Set slots at source node to their original state when migrate fails
            // This will execute the equivalent of SETSLOTRANGE STABLE for the slots of the failed migration task
            if (!TryResetLocalSlot())
                return false;

            // Log explicit migration failure.
            Status = MigrateState.FAIL;
            return true;
        }
    }
}