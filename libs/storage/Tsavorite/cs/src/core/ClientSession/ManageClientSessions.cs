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
        /// <param name="readCopyOptions"><see cref="ReadCopyOptions"/> for this session; override those specified at TsavoriteKV level, and may be overridden on individual Read operations</param>
        /// <returns>Session instance</returns>
        public ClientSession<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> NewSession<TInput, TOutput, TContext, TFunctions>(TFunctions functions, ReadCopyOptions readCopyOptions = default)
            where TFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        {
            if (functions == null)
                throw new ArgumentNullException(nameof(functions));

            int sessionID = Interlocked.Increment(ref maxSessionID);
            var ctx = new TsavoriteExecutionContext<TInput, TOutput, TContext>(sessionID);
            ctx.MergeReadCopyOptions(ReadCopyOptions, readCopyOptions);

            var session = new ClientSession<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>(this, ctx, functions);
            if (RevivificationManager.IsEnabled)
            {
                if (_activeSessions == null)
                    _ = Interlocked.CompareExchange(ref _activeSessions, [], null);

                lock (_activeSessions)
                    _activeSessions.Add(sessionID, new SessionInfo { session = session, isActive = true });
            }
            return session;
        }

        /// <summary>
        /// Dispose session with Tsavorite
        /// </summary>
        /// <param name="sessionID"></param>
        /// <returns></returns>
        internal void DisposeClientSession(int sessionID)
        {
            if (_activeSessions != null)
            {
                lock (_activeSessions)
                {
                    if (_activeSessions.TryGetValue(sessionID, out SessionInfo sessionInfo))
                    {
                        var session = sessionInfo.session;
                        session.MergeRevivificationStatsTo(ref RevivificationManager.stats, reset: true);
                        _ = _activeSessions.Remove(sessionID);
                    }
                }
            }
        }

        /// <summary>
        /// Dumps the revivification stats to a string.
        /// </summary>
        public string DumpRevivificationStats()
        {
            if (_activeSessions != null)
            {
                lock (_activeSessions)
                {
                    // Merge the session-level stats into the global stats, clear the session-level stats, and keep the cumulative stats.
                    foreach (var sessionInfo in _activeSessions.Values)
                        sessionInfo.session.MergeRevivificationStatsTo(ref RevivificationManager.stats, reset: true);

                }
            }
            return RevivificationManager.stats.Dump();
        }

        /// <summary>
        /// Resets the revivification stats.
        /// </summary>
        public void ResetRevivificationStats()
        {
            if (_activeSessions != null)
            {
                lock (_activeSessions)
                {
                    foreach (var sessionInfo in _activeSessions.Values)
                        sessionInfo.session.ResetRevivificationStats();
                }
            }
            RevivificationManager.stats.Reset();
        }
    }
}