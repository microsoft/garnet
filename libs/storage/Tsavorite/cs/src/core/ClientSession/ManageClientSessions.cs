// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        internal Dictionary<object, SessionInfo> _activeSessions = [];

        /// <summary>
        /// Start a new client session with Tsavorite.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="enableConsistentRead">Enable consistent read context</param>
        /// <param name="readCopyOptions"><see cref="ReadCopyOptions"/> for this session; override those specified at TsavoriteKV level, and may be overridden on individual Read operations</param>
        /// <param name="initialIORecordSize">Initial IO record size for disk reads in this session;
        ///     <see cref="KVSettings.UseDefaultInitialIORecordSize"/> means inherit from the store-level setting, and may be overridden on individual Read operations via <see cref="ReadOptions.InitialIORecordSize"/>.</param>
        /// <param name="sessionIdOverride">Specifies the id for this session - used to tie related sessions together, should come from earlier allocated session.</param>
        /// <returns>Session instance</returns>
        public ClientSession<TKey, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> NewSession<TKey, TInput, TOutput, TContext, TFunctions>(
            TFunctions functions,
            bool enableConsistentRead = false,
            ReadCopyOptions readCopyOptions = default,
            int initialIORecordSize = KVSettings.UseDefaultInitialIORecordSize,
            int? sessionIdOverride = null)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TFunctions : ISessionFunctions<TInput, TOutput, TContext>
        {
            if (functions == null)
                throw new ArgumentNullException(nameof(functions));

            var sessionID = sessionIdOverride ?? Interlocked.Increment(ref maxSessionID);
            var ctx = new TsavoriteExecutionContext<TInput, TOutput, TContext>(sessionID);
            ctx.MergeReadCopyOptions(ReadCopyOptions, readCopyOptions);
            ctx.InitialIORecordSize = initialIORecordSize;

            var session = new ClientSession<TKey, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>(this, ctx, functions, enableConsistentRead);
            lock (_activeSessions)
            {
                _ = _activeSessions.TryAdd(session, new SessionInfo { session = session, isActive = true });
            }
            return session;
        }

        /// <summary>
        /// Dispose session with Tsavorite
        /// </summary>
        /// <param name="sessionRef"></param>
        /// <returns></returns>
        internal void DisposeClientSession(object sessionRef)
        {
            lock (_activeSessions)
            {
                if (_activeSessions.TryGetValue(sessionRef, out var sessionInfo))
                {
                    var session = sessionInfo.session;
                    session.MergeRevivificationStatsTo(ref RevivificationManager.stats, reset: true);

                    _ = _activeSessions.Remove(sessionRef);
                }
            }
        }

        /// <summary>
        /// Dumps the revivification stats to a string.
        /// </summary>
        public string DumpRevivificationStats()
        {
            // Merge the session-level stats into the global stats, clear the session-level stats, and keep the cumulative stats.
            lock (_activeSessions)
            {
                foreach (var sessionInfo in _activeSessions.Values)
                    sessionInfo.session.MergeRevivificationStatsTo(ref RevivificationManager.stats, reset: true);
            }

            return RevivificationManager.stats.Dump();
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
            }

            RevivificationManager.stats.Reset();
        }
    }
}