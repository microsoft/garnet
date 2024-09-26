// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        internal Dictionary<int, SessionInfo> _activeSessions = new();

        /// <summary>
        /// Start a new client session with Tsavorite.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionName">Name of session (optional)</param>
        /// <param name="readCopyOptions"><see cref="ReadCopyOptions"/> for this session; override those specified at TsavoriteKV level, and may be overridden on individual Read operations</param>
        /// <returns>Session instance</returns>
        public ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> NewSession<TInput, TOutput, TContext, TSessionFunctions>(TSessionFunctions functions, string sessionName = null,
                ReadCopyOptions readCopyOptions = default, DualRole dualRole = DualRole.None)
            where TSessionFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        {
            if (functions == null)
                throw new ArgumentNullException(nameof(functions));
            if (sessionName == "")
                throw new TsavoriteException("Cannot use empty string as session name");

            int sessionID = Interlocked.Increment(ref maxSessionID);
            var ctx = new ExecutionContext<TInput, TOutput, TContext>();
            InitContext(ctx, sessionID, sessionName);
            ctx.MergeReadCopyOptions(ReadCopyOptions, readCopyOptions);
            var prevCtx = new ExecutionContext<TInput, TOutput, TContext>();
            InitContext(prevCtx, sessionID, sessionName);
            prevCtx.version--;
            prevCtx.ReadCopyOptions = ctx.ReadCopyOptions;

            ctx.prevCtx = prevCtx;

            if (_activeSessions == null)
                _ = Interlocked.CompareExchange(ref _activeSessions, new Dictionary<int, SessionInfo>(), null);

            var session = new ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>(this, ctx, functions, dualRole);
            lock (_activeSessions)
                _activeSessions.Add(sessionID, new SessionInfo { sessionName = sessionName, session = session, isActive = true });
            return session;
        }

        /// <summary>
        /// Dispose session with Tsavorite
        /// </summary>
        /// <param name="sessionID"></param>
        /// <param name="sessionPhase"></param>
        /// <returns></returns>
        internal void DisposeClientSession(int sessionID, Phase sessionPhase)
        {
            // If a session is disposed during a checkpoint cycle, we mark the session
            // as inactive, but wait until the end of checkpoint before disposing it
            lock (_activeSessions)
            {
                if (_activeSessions.TryGetValue(sessionID, out SessionInfo sessionInfo))
                {
                    var session = sessionInfo.session;
                    if (RevivificationManager.IsEnabled)
                        session.MergeRevivificationStatsTo(ref RevivificationManager.stats, reset: true);
                    if (sessionPhase == Phase.REST || sessionPhase == Phase.PREPARE_GROW || sessionPhase == Phase.IN_PROGRESS_GROW)
                        _ = _activeSessions.Remove(sessionID);
                    else
                        sessionInfo.isActive = false;
                }
            }
        }

        /// <summary>
        /// Dumps the revivification stats to a string.
        /// </summary>
        public string DumpRevivificationStats()
        {
            lock (_activeSessions)
            {
                // Merge the session-level stats into the global stats, clear the session-level stats, and keep the cumulative stats.
                foreach (var sessionInfo in _activeSessions.Values)
                    sessionInfo.session.MergeRevivificationStatsTo(ref RevivificationManager.stats, reset: true);
                return RevivificationManager.stats.Dump();
            }
        }

        /// <summary>
        /// Resets the revivification stats.
        /// </summary>
        public void ResetRevivificationStats()
        {
            lock (_activeSessions)
            {
                foreach (var sessionInfo in _activeSessions.Values)
                    sessionInfo.session.ResetRevivificationStats();
                RevivificationManager.stats.Reset();
            }
        }
    }
}