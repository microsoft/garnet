// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed class ReplicaSyncSessionTaskStore
    {
        readonly StoreWrapper storeWrapper;
        readonly ClusterProvider clusterProvider;
        ReplicaSyncSession[] sessions;
        int numSessions;
        SingleWriterMultiReaderLock _lock;
        readonly ILogger logger;
        private bool _disposed;

        public ReplicaSyncSession[] GetSessions() => sessions;

        public int GetNumSessions() => numSessions;

        public ReplicaSyncSessionTaskStore(StoreWrapper storeWrapper, ClusterProvider clusterProvider, ILogger logger = null)
        {
            this.storeWrapper = storeWrapper;
            this.clusterProvider = clusterProvider;
            sessions = new ReplicaSyncSession[1];
            numSessions = 0;
            this.logger = logger;
        }

        /// <summary>
        /// Dispose this Replica Sync Session Task Store
        /// </summary>
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

        /// <summary>
        /// Check if the session is the first in the array
        /// NOTE: used for leader task spawn in AttachSync
        /// </summary>
        /// <param name="session"></param>
        /// <returns></returns>
        public bool IsFirst(ReplicaSyncSession session)
        {
            try
            {
                _lock.ReadLock();
                if (_disposed) return false;
                return numSessions > 0 && sessions[0] == session;
            }
            finally
            {
                _lock.ReadUnlock();
            }
        }

        /// <summary>
        /// Add a new replica sync session
        /// </summary>
        /// <param name="session"></param>
        /// <returns></returns>
        public bool TryAddReplicaSyncSession(ReplicaSyncSession session)
        {
            try
            {
                _lock.WriteLock();
                for (var i = 0; i < numSessions; i++)
                {
                    var s = sessions[i];
                    if (s.replicaNodeId == session.replicaSyncMetadata.originNodeId)
                    {
                        logger?.LogError("Error syncSession for {replicaNodeId} already exists", session.replicaNodeId);
                        return false;
                    }
                }

                GrowSessionArray();
                sessions[numSessions++] = session;
                return true;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, $"{nameof(TryAddReplicaSyncSession)}");
                return false;
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        /// <summary>
        /// Add a new replica sync session
        /// </summary>
        /// <param name="replicaNodeId"></param>
        /// <param name="replicaAssignedPrimaryId"></param>
        /// <param name="replicaCheckpointEntry"></param>
        /// <param name="replicaAofBeginAddress"></param>
        /// <param name="replicaAofTailAddress"></param>
        /// <returns></returns>
        public bool TryAddReplicaSyncSession(string replicaNodeId, string replicaAssignedPrimaryId, CheckpointEntry replicaCheckpointEntry, long replicaAofBeginAddress, long replicaAofTailAddress)
        {
            var retSession = new ReplicaSyncSession(storeWrapper, clusterProvider, replicaSyncMetadata: null, token: default, replicaNodeId, replicaAssignedPrimaryId, replicaCheckpointEntry, replicaAofBeginAddress, replicaAofTailAddress, logger);
            var success = false;
            try
            {
                _lock.WriteLock();
                for (var i = 0; i < numSessions; i++)
                {
                    var s = sessions[i];
                    if (s.replicaNodeId == retSession.replicaNodeId)
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

        public bool TryRemove(string remoteNodeId)
        {
            try
            {
                _lock.WriteLock();
                if (_disposed) return true;
                for (int i = 0; i < numSessions; i++)
                {
                    var s = sessions[i];
                    if (s.replicaNodeId == remoteNodeId)
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

        /// <summary>
        /// Get session associated with replica sync session
        /// </summary>
        /// <param name="remoteNodeId"></param>
        /// <param name="session"></param>
        /// <returns></returns>
        public bool TryGetSession(string remoteNodeId, out ReplicaSyncSession session)
        {
            session = null;
            try
            {
                _lock.ReadLock();
                if (_disposed) return false;

                for (var i = 0; i < numSessions; i++)
                {
                    session = sessions[i];
                    if (session.replicaNodeId == remoteNodeId)
                        return true;
                }
                return false;
            }
            finally
            {
                _lock.ReadUnlock();
            }
        }


        /// <summary>
        /// Clear references to task store
        /// </summary>
        /// <returns></returns>
        public void Clear()
        {
            try
            {
                _lock.WriteLock();
                if (_disposed) return;
                for (var i = 0; i < numSessions; i++)
                {
                    var s = sessions[i];
                    sessions[i] = null;
                }
                numSessions = 0;
            }
            finally
            {
                _lock.WriteUnlock();
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
    }
}