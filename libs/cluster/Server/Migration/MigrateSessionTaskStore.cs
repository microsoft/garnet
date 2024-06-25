// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed class MigrateSessionTaskStore(ILogger logger = null)
    {
        MigrateSession[] sessions = new MigrateSession[1];
        int numSessions = 0;
        SingleWriterMultiReaderLock _lock;
        readonly ILogger logger = logger;

        private bool _disposed;

        public void Dispose()
        {
            try
            {
                _lock.WriteLock();
                _disposed = true;
                for (var i = 0; i < numSessions; i++)
                {
                    var s = sessions[i];
                    s.Dispose();
                }
                numSessions = 0;
                Array.Clear(sessions);
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        public int GetNumSession() => numSessions;

        private void GrowSessionArray()
        {
            if (numSessions == sessions.Length)
            {
                var _sessions = new MigrateSession[sessions.Length << 1];
                Array.Copy(sessions, _sessions, sessions.Length);
                sessions = _sessions;
            }
        }

        private void ShrinkSessionArray()
        {
            // Shrink the array if it got too big but avoid often shrinking/growing
            if (numSessions > 0 && (numSessions << 2) < sessions.Length)
            {
                var oldSessions = sessions;
                var _sessions = new MigrateSession[sessions.Length >> 1];
                Array.Copy(sessions, _sessions, sessions.Length >> 2);
                sessions = _sessions;
                Array.Clear(oldSessions);
            }
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

                // Check if an existing migration session is migrating slots that are already scheduled for migration from another task
                for (var i = 0; i < numSessions; i++)
                {
                    if (sessions[i].Overlap(mSession))
                    {
                        logger?.LogError("Migrate slot range overlaps with range of ongoing task");
                        success = false;
                        return false;
                    }
                }

                GrowSessionArray();
                sessions[numSessions++] = mSession;
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
                for (var i = 0; i < numSessions; i++)
                {
                    var s = sessions[i];

                    if (s == mSession)
                    {
                        sessions[i] = null;
                        if (i < numSessions - 1)
                        {
                            sessions[i] = sessions[numSessions - 1];
                            sessions[numSessions - 1] = null;
                        }
                        numSessions--;
                        s.Dispose();

                        ShrinkSessionArray();
                    }
                }
                return true;
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        /// <summary>
        /// Check if provided key can be operated on.
        /// </summary>
        /// <param name="slot"></param>
        /// <param name="key"></param>
        /// <param name="readOnly"></param>
        /// <returns>True if we can operate on the key, otherwise false (i.e. key is being migrated)</returns>
        public bool CanModifyKey(int slot, ArgSlice key, bool readOnly)
        {
            try
            {
                _lock.ReadLock();
                if (_disposed) return true;
                for (var i = 0; i < numSessions; i++)
                {
                    var s = sessions[i];
                    if (!s.CanOperateOnKey(slot, key, readOnly))
                        return false;
                }
            }
            finally
            {
                _lock.ReadUnlock();
            }

            return true;
        }
    }
}