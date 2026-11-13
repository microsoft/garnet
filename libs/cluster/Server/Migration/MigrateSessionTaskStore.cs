// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed class MigrateSessionTaskStore
    {
        readonly MigrateSession[] sessions;
        SingleWriterMultiReaderLock _lock;
        readonly ILogger logger;
        int sessionCount;
        private bool _disposed;

        public MigrateSessionTaskStore(ILogger logger = null)
        {
            this.sessions = new MigrateSession[ClusterConfig.MAX_HASH_SLOT_VALUE];
            this.logger = logger;
        }

        /// <summary>
        /// Disposes all managed resources held by this instance.
        /// </summary>
        public void Dispose()
        {
            _lock.WriteLock();
            var skipDispose = _disposed;

            Volatile.Write(ref _disposed, true);
            Volatile.Write(ref sessionCount, 0);
            _lock.WriteUnlock();

            if (skipDispose)
            {
                return;
            }

            for (var i = 0; i < sessions.Length; i++)
            {
                sessions[i]?.Dispose();
                Volatile.Write(ref sessions[i], null);
            }
        }

        /// <summary>
        /// Count active MigrateSessions
        /// </summary>
        /// <returns></returns>
        public int GetNumSessions()
            => Volatile.Read(ref sessionCount);

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
            MigrationBatch sketch,
            TransferOption transferOption,
            out MigrateSession mSession)
        {
            if (slots.Count == 0)
            {
                mSession = null;
                logger?.LogError("Failed to add new MigrateSession with no slots");
                return false;
            }

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
                sketch,
                transferOption);

            try
            {
                _lock.WriteLock();
                if (_disposed)
                {
                    success = false;
                    return false;
                }

                // First iterate and check if corresponding slot is associated to another active migrate session
                foreach (var slot in mSession.GetSlots)
                {
                    if (sessions[slot] != null)
                    {
                        logger?.LogError("Failed to add new session due to an existing MigrateSession operating on overlapping slots");
                        success = false;
                        return false;
                    }
                }

                // If reached this point all slots to be migrated are not associated with any other session
                // so we can mark them as being associated with this newly added session
                foreach (var slot in mSession.GetSlots)
                    Volatile.Write(ref sessions[slot], mSession);

                Volatile.Write(ref sessionCount, sessionCount + 1);
            }
            catch (Exception ex)
            {
                success = false;
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

        /// <summary>
        /// Remove only the provided session instance
        /// </summary>
        /// <param name="mSession"></param>
        /// <returns></returns>
        public bool TryRemove(MigrateSession mSession)
        {
            try
            {
                _lock.WriteLock();
                if (_disposed) return false;

                RemoveSessionUnderLock(mSession);

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

        void RemoveSessionUnderLock(MigrateSession mSession)
        {
            // Keep protection published until cancellation and connection shutdown complete.
            mSession.Dispose();
            var removed = false;
            foreach (var slot in mSession.GetSlots)
            {
                if (sessions[slot] != mSession)
                    continue;

                Volatile.Write(ref sessions[slot], null);
                removed = true;
            }

            if (removed)
                Volatile.Write(ref sessionCount, sessionCount - 1);
        }

        /// <summary>
        /// Remove all sessions associated with the provided targetNodeId
        /// </summary>
        /// <param name="targetNodeId"></param>
        /// <returns></returns>
        public bool TryRemove(string targetNodeId)
        {
            try
            {
                _lock.WriteLock();
                if (_disposed) return false;
                for (var i = 0; i < sessions.Length; i++)
                {
                    var s = sessions[i];
                    if (s != null && s.GetTargetNodeId.Equals(targetNodeId, StringComparison.Ordinal))
                        RemoveSessionUnderLock(s);
                }

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
        public bool CanAccessKey(PinnedSpanByte key, int slot, bool readOnly)
        {
            if (Volatile.Read(ref _disposed)) return true;

            // Admission reads only managed session/sketch state, which remains valid after disposal.
            var session = Volatile.Read(ref sessions[slot]);
            return session == null || session.CanAccessKey(key, slot, readOnly);
        }
    }
}