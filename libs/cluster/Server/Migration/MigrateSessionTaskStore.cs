// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal class MigrateSessionTaskStore
    {
        MigrateSession[] sessions;
        int numSessions;
        SingleWriterMultiReaderLock _lock;
        readonly ILogger logger;

        private bool _disposed;

        public MigrateSessionTaskStore(ILogger logger = null)
        {
            sessions = new MigrateSession[1];
            numSessions = 0;
            this.logger = logger;
        }

        public void Dispose()
        {
            try
            {
                _lock.WriteLock();
                _disposed = true;
                for (int i = 0; i < numSessions; i++)
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
            List<(long, long)> keysWithSize,
            out MigrateSession mSession)
        {
            bool success = true;
            mSession = new MigrateSession(
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
                clusterProvider.loggerFactory.CreateLogger("MigrateSession"));

            try
            {
                _lock.WriteLock();
                if (_disposed) return false;

                // Check if an existing migration session is migrating slots that are already scheduled for migration from another task
                for (int i = 0; i < numSessions; i++)
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
                if (_disposed) return true;
                for (int i = 0; i < numSessions; i++)
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
                return false;
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }
    }
}