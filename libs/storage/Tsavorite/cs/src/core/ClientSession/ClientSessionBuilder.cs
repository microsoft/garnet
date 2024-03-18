// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<Key, Value> : TsavoriteBase
    {
        internal Dictionary<int, SessionInfo> _activeSessions = new();

        /// <summary>
        /// Start a new client session with Tsavorite.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionName">Name of session (optional)</param>
        /// <param name="readCopyOptions"><see cref="ReadCopyOptions"/> for this session; override those specified at TsavoriteKV level, and may be overridden on individual Read operations</param>
        /// <returns>Session instance</returns>
        public ClientSession<Key, Value, Input, Output, Context, Functions> NewSession<Input, Output, Context, Functions>(Functions functions, string sessionName = null,
                ReadCopyOptions readCopyOptions = default)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            if (functions == null)
                throw new ArgumentNullException(nameof(functions));
            if (sessionName == "")
                throw new TsavoriteException("Cannot use empty string as session name");
            if (sessionName != null && _recoveredSessionNameMap != null && _recoveredSessionNameMap.ContainsKey(sessionName))
                throw new TsavoriteException($"Session named {sessionName} already exists in recovery info, use RecoverSession to resume it");

            int sessionID = Interlocked.Increment(ref maxSessionID);
            var ctx = new TsavoriteExecutionContext<Input, Output, Context>();
            InitContext(ctx, sessionID, sessionName);
            ctx.MergeReadCopyOptions(ReadCopyOptions, readCopyOptions);
            var prevCtx = new TsavoriteExecutionContext<Input, Output, Context>();
            InitContext(prevCtx, sessionID, sessionName);
            prevCtx.version--;
            prevCtx.ReadCopyOptions = ctx.ReadCopyOptions;

            ctx.prevCtx = prevCtx;

            if (_activeSessions == null)
                Interlocked.CompareExchange(ref _activeSessions, new Dictionary<int, SessionInfo>(), null);

            var session = new ClientSession<Key, Value, Input, Output, Context, Functions>(this, ctx, functions);
            lock (_activeSessions)
                _activeSessions.Add(sessionID, new SessionInfo { sessionName = sessionName, session = session, isActive = true });
            return session;
        }

        /// <summary>
        /// Resume (continue) prior client session with Tsavorite; used during recovery from failure.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionName">Name of previous session to resume</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="readCopyOptions"><see cref="ReadCopyOptions"/> for this session; override those specified at TsavoriteKV level, and may be overridden on individual Read operations</param>
        /// <returns>Session instance</returns>
        internal ClientSession<Key, Value, Input, Output, Context, Functions> ResumeSession<Input, Output, Context, Functions>(Functions functions, string sessionName, out CommitPoint commitPoint,
                ReadCopyOptions readCopyOptions = default)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            // Map from sessionName to sessionID and call through
            if (_recoveredSessionNameMap == null || !_recoveredSessionNameMap.TryRemove(sessionName, out int sessionID))
                throw new TsavoriteException($"Unable to find session named {sessionName} to recover");
            return ResumeSession<Input, Output, Context, Functions>(functions, sessionID, out commitPoint, readCopyOptions);
        }

        /// <summary>
        /// Resume (continue) prior client session with Tsavorite; used during recovery from failure.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionID">ID of previous session to resume</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="readCopyOptions"><see cref="ReadCopyOptions"/> for this session; override those specified at TsavoriteKV level, and may be overridden on individual Read operations</param>
        /// <returns>Session instance</returns>
        internal ClientSession<Key, Value, Input, Output, Context, Functions> ResumeSession<Input, Output, Context, Functions>(Functions functions, int sessionID, out CommitPoint commitPoint,
                ReadCopyOptions readCopyOptions = default)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            if (functions == null)
                throw new ArgumentNullException(nameof(functions));

            string sessionName;
            (sessionName, commitPoint) = InternalContinue<Input, Output, Context>(sessionID, out var ctx);
            if (commitPoint.UntilSerialNo == -1)
                throw new Exception($"Unable to find session {sessionID} to recover");
            ctx.MergeReadCopyOptions(ReadCopyOptions, readCopyOptions);

            var session = new ClientSession<Key, Value, Input, Output, Context, Functions>(this, ctx, functions);

            if (_activeSessions == null)
                Interlocked.CompareExchange(ref _activeSessions, new Dictionary<int, SessionInfo>(), null);
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
                        _activeSessions.Remove(sessionID);
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