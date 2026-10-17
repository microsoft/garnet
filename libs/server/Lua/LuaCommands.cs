// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
        /// </summary>
        /// <returns></returns>
        private unsafe bool TryEVALSHA()
        {
            if (!CheckLuaEnabled())
            {
                return true;
            }

            var count = parseState.Count;
            if (count < 2)
            {
                return AbortWithWrongNumberOfArguments("EVALSHA");
            }

            if (!parseState.TryGetInt(1, out var n) || (n < 0) || (n > count - 2))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }

            ref var digest = ref parseState.GetArgSliceByRef(0);

            var executor = RentScriptExecutor();
            if (executor == null)
                return true; // The server is shutting down

            try
            {
                var convertedToLower = false;

                LuaRunner runner = null;
                ScriptHashKey scriptKey = default;

                // Length check is mandatory, as ScriptHashKey assumes correct length
                if (digest.Length == ScriptExecutor.SHA1Len)
                {
                tryAgain:
                    scriptKey = new ScriptHashKey(digest.Span);

                    if (!executor.TryGetFromDigest(scriptKey, out runner, out _))
                    {
                        if (storeWrapper.storeScriptCache.TryGetValue(scriptKey, out var globalScriptHandle))
                        {
                            if (!executor.TryGetOrCreateRunnerFromCachedScript(this, scriptKey, globalScriptHandle, out runner))
                            {
                                // The loading error was already written, if any
                                //
                                // Note we DON'T dispose the script handle because this is just the executor's cache
                                _ = storeWrapper.storeScriptCache.TryRemove(scriptKey, out _);
                                return true;
                            }
                        }
                        else if (!convertedToLower)
                        {
                            // On a miss (which should be rare) make sure the hash is lower case and try again.
                            //
                            // We assume that hashes will be sent in the same format as we return them (lower)
                            // most of the time, so optimize for that.

                            AsciiUtils.ToLowerInPlace(digest.Span);
                            convertedToLower = true;
                            goto tryAgain;
                        }
                    }
                }

                if (runner == null)
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_NO_SCRIPT, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    RunScriptForSession(executor, count, runner, scriptKey);
                }
            }
            finally
            {
                ReturnScriptExecutor(executor);
            }

            return true;
        }


        /// <summary>
        /// EVAL script numkeys [key [key ...]] [arg [arg ...]]
        /// </summary>
        /// <returns></returns>
        private unsafe bool TryEVAL()
        {
            if (!CheckLuaEnabled())
            {
                return true;
            }

            var count = parseState.Count;
            if (count < 2)
            {
                return AbortWithWrongNumberOfArguments("EVAL");
            }

            if (!parseState.TryGetInt(1, out var n) || (n < 0) || (n > count - 2))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }

            ref var script = ref parseState.GetArgSliceByRef(0);

            // that this is stack allocated is load bearing - if it moves, things will break
            Span<byte> digest = stackalloc byte[ScriptExecutor.SHA1Len];
            ScriptExecutor.GetScriptDigest(script.ReadOnlySpan, digest);

            var onStackScriptKey = new ScriptHashKey(digest);

            var executor = RentScriptExecutor();
            if (executor == null)
                return true; // The server is shutting down

            try
            {
                if (!TryGetOrCreateScriptRunner(executor, script.ReadOnlySpan, digest, onStackScriptKey, out var runner))
                    return true; // The loading error was written to the response.

                if (runner == null)
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_NO_SCRIPT, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    RunScriptForSession(executor, count, runner, onStackScriptKey);
                }
            }
            finally
            {
                ReturnScriptExecutor(executor);
            }

            return true;
        }

        /// <summary>
        /// SCRIPT|EXISTS
        /// </summary>
        private bool NetworkScriptExists()
        {
            if (!CheckLuaEnabled())
            {
                return true;
            }

            if (parseState.Count == 0)
            {
                return AbortWithWrongNumberOfArguments("script|exists");
            }

            // Returns an array where each element is a 0 if the script does not exist, and a 1 if it does

            while (!RespWriteUtils.TryWriteArrayLength(parseState.Count, ref dcurr, dend))
                SendAndReset();

            for (var shaIx = 0; shaIx < parseState.Count; shaIx++)
            {
                ref var sha1 = ref parseState.GetArgSliceByRef(shaIx);
                var exists = 0;

                // Length check is required, as ScriptHashKey makes a hard assumption
                if (sha1.Length == ScriptExecutor.SHA1Len)
                {
                    AsciiUtils.ToLowerInPlace(sha1.Span);

                    var sha1Arg = new ScriptHashKey(sha1.Span);

                    exists = storeWrapper.storeScriptCache.ContainsKey(sha1Arg) ? 1 : 0;
                }

                while (!RespWriteUtils.TryWriteInt32(exists, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// SCRIPT|FLUSH
        /// </summary>
        private bool NetworkScriptFlush()
        {
            if (!CheckLuaEnabled())
            {
                return true;
            }

            if (parseState.Count > 1)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_SCRIPT_FLUSH_OPTIONS);
            }
            else if (parseState.Count == 1)
            {
                // We ignore this, but should validate it
                ref var arg = ref parseState.GetArgSliceByRef(0);

                AsciiUtils.ToUpperInPlace(arg.Span);

                var valid = arg.Span.SequenceEqual(CmdStrings.ASYNC) || arg.Span.SequenceEqual(CmdStrings.SYNC);

                if (!valid)
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_SCRIPT_FLUSH_OPTIONS);
                }
            }

            // Flush store script cache
            //
            // Disposing each script handle (that we actually remove) along the way
            // to signal to executor level caches that the script needs to be discarded
            foreach (var digest in storeWrapper.storeScriptCache.Keys)
            {
                if (storeWrapper.storeScriptCache.TryRemove(digest, out var scriptHandle))
                {
                    scriptHandle.Dispose();
                }
            }

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// SCRIPT|LOAD
        /// </summary>
        private bool NetworkScriptLoad()
        {
            if (!CheckLuaEnabled())
            {
                return true;
            }

            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments("script|load");
            }

            ref var source = ref parseState.GetArgSliceByRef(0);

            Span<byte> digest = stackalloc byte[ScriptExecutor.SHA1Len];
            ScriptExecutor.GetScriptDigest(source.Span, digest);

            var onStackScriptHashKey = new ScriptHashKey(digest);

            var executor = RentScriptExecutor();
            if (executor == null)
                return true; // The server is shutting down

            try
            {
                if (TryGetOrCreateScriptRunner(executor, source.ReadOnlySpan, digest, onStackScriptHashKey, out _))
                {
                    while (!RespWriteUtils.TryWriteBulkString(digest, ref dcurr, dend))
                        SendAndReset();
                }
            }
            finally
            {
                ReturnScriptExecutor(executor);
            }

            return true;
        }

        /// <summary>
        /// Rents a script executor that runs as this session's user. Returns null if the server is shutting down.
        /// </summary>
        private ScriptExecutor RentScriptExecutor()
        {
            var executor = storeWrapper.scriptExecutorPool.Rent(_userHandle);
            rentedScriptExecutor = executor;
            return executor;
        }

        private void ReturnScriptExecutor(ScriptExecutor executor)
        {
            rentedScriptExecutor = null;
            storeWrapper.scriptExecutorPool.Return(executor);
        }

        private bool TryGetOrCreateScriptRunner(ScriptExecutor executor, ReadOnlySpan<byte> source, ReadOnlySpan<byte> digest, ScriptHashKey scriptKey, out LuaRunner runner)
        {
            if (storeWrapper.storeScriptCache.TryGetValue(scriptKey, out var globalScriptHandle))
                return executor.TryGetOrCreateRunnerFromCachedScript(this, scriptKey, globalScriptHandle, out runner);

            if (!executor.TryGetOrCreateRunnerFromSource(this, source, scriptKey, out var sessionScriptHandle, out runner, out var digestOnHeap))
                return false;

            ScriptHashKey globalScriptKey;
            if (digestOnHeap != null)
            {
                globalScriptKey = digestOnHeap.Value;
            }
            else
            {
                var digestCopy = GC.AllocateUninitializedArray<byte>(ScriptExecutor.SHA1Len, pinned: true);
                digest.CopyTo(digestCopy);
                globalScriptKey = new(digestCopy);
            }

            if (!storeWrapper.storeScriptCache.TryAdd(globalScriptKey, sessionScriptHandle))
                sessionScriptHandle.Dispose();

            return true;
        }

        /// <summary>
        /// Returns true if Lua is enabled.
        /// 
        /// Otherwise writes out an error and returns false.
        /// </summary>
        private bool CheckLuaEnabled()
        {
            if (!storeWrapper.serverOptions.EnableLua || !scriptsEnabled)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_LUA_DISABLED, ref dcurr, dend))
                    SendAndReset();

                return false;
            }

            return true;
        }

        internal void WriteLuaCompilationError(string error)
        {
            while (!RespWriteUtils.TryWriteError($"Compilation error: {error}", ref dcurr, dend))
                SendAndReset();
        }

        /// <summary>
        /// Run a resolved script for the current session.
        /// 
        /// Gated so it can't execute while the executor's Lua state is being disposed. If disposing,
        /// <see cref="ScriptExecutor.StartRunningScript"/> returns false and this is a no-op: no reply
        /// is written since the server is shutting down. On failure the runner is removed inside the
        /// guarded window so it can't race disposal.
        /// </summary>
        private void RunScriptForSession(ScriptExecutor executor, int count, LuaRunner runner, ScriptHashKey scriptKey)
        {
            // We assume here that ExecuteScript does not raise exceptions
            if (!executor.StartRunningScript(runner))
                return;

            try
            {
                if (!TryExecuteScript(count - 1, runner))
                {
                    // Note we DON'T dispose the script handle because this is just the executor's cache
                    executor.Remove(scriptKey);
                }
            }
            finally
            {
                executor.StopRunningScript();
            }
        }

        /// <summary>
        /// Invoke the execution of a server-side Lua script.
        /// 
        /// Returns false if the <see cref="LuaRunner"/> should be discarded rather than reused.
        /// </summary>
        private bool TryExecuteScript(int count, LuaRunner scriptRunner)
        {
            try
            {
                scriptRunner.RunForSession(count, this);
                return !scriptRunner.NeedsDispose;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error executing Lua script");
                while (!RespWriteUtils.TryWriteError("ERR " + ex.Message, ref dcurr, dend))
                    SendAndReset();

                // Exceptions shouldn't happen, so if they did the runner is probably in a bad state
                return false;
            }
        }
    }
}