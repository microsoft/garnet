// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal class ReplicaSyncSessionTaskStore
    {
        readonly StoreWrapper storeWrapper;
        readonly ClusterProvider clusterProvider;
        ReplicaSyncSession[] sessions;
        int numSessions;
        SingleWriterMultiReaderLock _lock;
        readonly ILogger logger;

        private bool _disposed;

        public ReplicaSyncSessionTaskStore(StoreWrapper storeWrapper, ClusterProvider clusterProvider, ILogger logger = null)
        {
            this.storeWrapper = storeWrapper;
            this.clusterProvider = clusterProvider;
            sessions = new ReplicaSyncSession[1];
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

        public bool TryAddReplicaSyncSession(string remoteNodeId, string remote_primary_replid, CheckpointEntry remoteEntry, long replicaAofBeginAddress, long replicaAofTailAddress)
        {
            var retSession = new ReplicaSyncSession(storeWrapper, clusterProvider, remoteNodeId, remote_primary_replid, remoteEntry, replicaAofBeginAddress, replicaAofTailAddress, logger);
            bool success = false;
            try
            {
                _lock.WriteLock();
                for (int i = 0; i < numSessions; i++)
                {
                    var s = sessions[i];
                    if (s.remoteNodeId == retSession.remoteNodeId)
                    {
                        success = false;
                        return false;
                    }
                }

                GrowSessionArray();
                sessions[numSessions++] = retSession;
                success = true;

                return true;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error at TryAddCheckpointRetrievalSession");
                return false;
            }
            finally
            {
                _lock.WriteUnlock();
                if (!success)
                    retSession?.Dispose();
            }
        }

        private void GrowSessionArray()
        {
            if (numSessions == sessions.Length)
            {
                var _sessions = new ReplicaSyncSession[sessions.Length << 1];
                Array.Copy(sessions, _sessions, sessions.Length);
                sessions = _sessions;
            }
        }

        public bool TryRemove(string remoteNodeId)
        {
            try
            {
                _lock.WriteLock();
                if (_disposed) return true;
                for (int i = 0; i < numSessions; i++)
                {
                    var s = sessions[i];
                    if (s.remoteNodeId == remoteNodeId)
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
                        return true;
                    }
                }
                return false;
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        private void ShrinkSessionArray()
        {
            //Shrink the array if it got too big but avoid often shrinking/growing
            if (numSessions > 0 && (numSessions << 2) < sessions.Length)
            {
                var oldSessions = sessions;
                var _sessions = new ReplicaSyncSession[sessions.Length >> 1];
                Array.Copy(sessions, _sessions, sessions.Length >> 2);
                sessions = _sessions;
                Array.Clear(oldSessions);
            }
        }

        public bool TryGetSession(string remoteNodeId, out ReplicaSyncSession session)
        {
            session = null;
            try
            {
                _lock.ReadLock();
                if (_disposed) return false;

                for (int i = 0; i < numSessions; i++)
                {
                    session = sessions[i];
                    if (session.remoteNodeId == remoteNodeId)
                        return true;
                }
                return false;
            }
            finally
            {
                _lock.ReadUnlock();
            }
        }
    }
}