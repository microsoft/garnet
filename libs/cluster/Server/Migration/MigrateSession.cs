// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
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
        readonly Dictionary<ArgSlice, KeyMigrationStatus> _keys;
        SingleWriterMultiReaderLock _keyDictLock;
        SingleWriterMultiReaderLock _disposed;

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

        /// <summary>
        /// Add key to the migrate dictionary for tracking progress during migration
        /// </summary>
        /// <param name="key"></param>
        public void AddKey(ArgSlice key)
        {
            try
            {
                _keyDictLock.WriteLock();
                _keys.TryAdd(key, KeyMigrationStatus.QUEUED);
            }
            finally
            {
                _keyDictLock.WriteUnlock();
            }
        }

        /// <summary>
        /// Check if it is safe to operate on the provided key when a slot state is set to MIGRATING
        /// </summary>
        /// <param name="key"></param>
        /// <param name="slot"></param>
        /// <param name="readOnly"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        public bool CanAccessKey(ref ArgSlice key, int slot, bool readOnly)
        {
            try
            {
                _keyDictLock.ReadLock();
                // Skip operation check since this session is not responsible for migrating the associated slot
                if (!_sslots.Contains(slot))
                    return true;

                // If key is not queued for migration then
                if (!_keys.TryGetValue(key, out var state))
                    return true;

                // NOTE:
                // Caller responsible for spin-wait
                // Check definition of KeyMigrationStatus for more info
                return state switch
                {
                    KeyMigrationStatus.QUEUED or KeyMigrationStatus.MIGRATED => true,// Both reads and write commands can access key if it exists
                    KeyMigrationStatus.MIGRATING => readOnly, // If key exists read commands can access key but write commands will be delayed
                    KeyMigrationStatus.DELETING => false, // Neither read or write commands can access key
                    _ => throw new GarnetException($"Invalid KeyMigrationStatus: {state}")
                };
            }
            finally
            {
                _keyDictLock.ReadUnlock();
            }
        }

        /// <summary>
        /// Clear keys from dictionary
        /// </summary>
        public void ClearKeys()
        {
            try
            {
                _keyDictLock.WriteLock();
                _keys.Clear();
            }
            finally
            {
                _keyDictLock.WriteUnlock();
            }
        }

        readonly GarnetClientSession _gcs;

        /// <summary>
        /// Check for overlapping slots between migrate sessions
        /// </summary>
        /// <param name="session"></param>
        /// <returns></returns>
        public bool Overlap(MigrateSession session)
            => session._sslots.Overlaps(_sslots);

        readonly int _clientBufferSize;

        TransferOption transferOption;

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
        /// <param name="keys"></param>
        /// <param name="transferOption"></param>
        /// <param name="logger"></param>
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
            Dictionary<ArgSlice, KeyMigrationStatus> keys,
            TransferOption transferOption,
            ILogger logger = null)
        {
            this.logger = logger;
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
            this._keys = keys == null ? new Dictionary<ArgSlice, KeyMigrationStatus>(ArgSliceComparer.Instance) : keys;
            this.transferOption = transferOption;

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
            if (!_disposed.TryWriteLock()) return;
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
                    if (!resp.Result.Equals("OK", StringComparison.Ordinal))
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
        public void ResetLocalSlot() => clusterProvider.clusterManager.ResetSlotsState(_sslots);

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