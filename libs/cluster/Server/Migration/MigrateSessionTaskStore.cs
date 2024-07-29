// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed class MigrateSessionTaskStore
    {
        readonly MigrateSession[] sessions;
        SingleWriterMultiReaderLock _lock;
        readonly ILogger logger;
        private bool _disposed;

        public MigrateSessionTaskStore(ILogger logger = null)
        {
            this.sessions = new MigrateSession[ClusterConfig.MAX_HASH_SLOT_VALUE];
            this.logger = logger;
        }

        public void Dispose()
        {
            try
            {
                _lock.WriteLock();
                _disposed = true;
                for (var i = 0; i < sessions.Length; i++)
                    sessions[i]?.Dispose();
                Array.Clear(sessions);
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        /// <summary>
        /// Count active MigrateSessions
        /// </summary>
        /// <returns></returns>
        public int GetNumSessions()
        {
            var count = 0;
            try
            {
                _lock.ReadLock();
                if (_disposed) return 0;

                for (var i = 0; i < sessions.Length; i++)
                    count += sessions[i] != null ? 1 : 0;
            }
            finally
            {
                _lock.ReadUnlock();
            }
            return count;
        }

        public bool TryAddMigrateSession(
            ClusterSession clusterSession,
            ClusterProvider clusterProvider,
            string sourceNodeId,
            string targetAddress,
            int targetPort,
            string targetNodeId,
            string username,
            string passwd,
            bool copyOption,
            bool replaceOption,
            int timeout,
            HashSet<int> slots,
            Dictionary<ArgSlice, KeyMigrationStatus> keysWithSize,
            TransferOption transferOption,
            out MigrateSession mSession)
        {
            var success = true;
            mSession = new MigrateSession(
                clusterSession,
                clusterProvider,
                targetAddress,
                targetPort,
                targetNodeId,
                username,
                passwd,
                sourceNodeId,
                copyOption,
                replaceOption,
                timeout,
                slots,
                keysWithSize,
                transferOption,
                clusterProvider.loggerFactory.CreateLogger("MigrateSession"));

            try
            {
                _lock.WriteLock();
                if (_disposed) return false;

                foreach (var slot in mSession.GetSlots)
                {
                    if (sessions[slot] != null)
                    {
                        logger?.LogError("Failed to add new session due to an existing MigrateSession operating on overlapping slots");
                        success = false;
                        return false;
                    }
                    sessions[slot] = mSession;
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error at TryAddMigrateSession");
                return false;
            }
            finally
            {
                _lock.WriteUnlock();
                if (!success)
                {
                    mSession.Status = MigrateState.FAIL;
                    mSession?.Dispose();
                }
            }
            return success;
        }

        public bool TryRemove(MigrateSession mSession)
        {
            try
            {
                _lock.WriteLock();
                if (_disposed) return false;

                foreach (var slot in mSession.GetSlots)
                {
                    Debug.Assert(sessions[slot] == mSession, "MigrateSession not found in slot");
                    sessions[slot] = null;
                }

                mSession.Dispose();
                return true;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error at TryRemove");
                return false;
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        public bool TryRemove(string targetNodeId)
        {
            try
            {
                _lock.WriteLock();
                if (_disposed) return false;
                HashSet<MigrateSession> mSessions = null;
                for (var i = 0; i < sessions.Length; i++)
                {
                    var s = sessions[i];
                    if (s != null && s.GetTargetNodeId.Equals(targetNodeId, StringComparison.Ordinal))
                    {
                        sessions[i] = null;
                        mSessions ??= [];
                        _ = mSessions.Add(s);
                    }
                }

                if (mSessions == null)
                    return false;

                foreach (var session in mSessions)
                    session.Dispose();
                return true;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error at TryRemove");
                return false;
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        /// <summary>
        /// Check if provided key can be operated on.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="slot"></param>
        /// <param name="readOnly"></param>
        /// <returns>True if we can operate on the key, otherwise false (i.e. key is being migrated)</returns>
        public bool CanAccessKey(ref ArgSlice key, int slot, bool readOnly)
        {
            try
            {
                _lock.ReadLock();
                if (_disposed) return true;

                // Search slotMap
                var s = sessions[slot];
                if (s == null) // Slot is not managed by any session so can safely operate on it
                    return true;

                Debug.Assert(s != null);
                // Check owner of slot if can operate on key
                if (!s.CanAccessKey(ref key, slot, readOnly))
                    return false;
            }
            finally
            {
                _lock.ReadUnlock();
            }

            return true;
        }
    }
}