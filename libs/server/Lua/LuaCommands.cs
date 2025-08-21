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

            var convertedToLower = false;

            LuaRunner runner = null;
            ScriptHashKey scriptKey = default;

            // Length check is mandatory, as ScriptHashKey assumes correct length
            if (digest.Length == SessionScriptCache.SHA1Len)
            {
            tryAgain:
                scriptKey = new ScriptHashKey(digest.Span);

                if (!sessionScriptCache.TryGetFromDigest(scriptKey, out runner, out _))
                {
                    if (storeWrapper.storeScriptCache.TryGetValue(scriptKey, out var globalScriptHandle))
                    {
                        if (!sessionScriptCache.TryLoad(this, globalScriptHandle.ScriptData.Span, scriptKey, ref globalScriptHandle, out runner, out _))
                        {
                            // TryLoad will have written an error out, it any
                            //
                            // Note we DON'T dispose the script handle because this is just the session cache
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
                // We assume here that ExecuteScript does not raise exceptions
                sessionScriptCache.StartRunningScript(runner);
                var res = TryExecuteScript(count - 1, runner);
                sessionScriptCache.StopRunningScript();

                if (!res)
                {
                    // Note we DON'T dispose the script handle because this is just the session cache
                    sessionScriptCache.Remove(scriptKey);
                }
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
            Span<byte> digest = stackalloc byte[SessionScriptCache.SHA1Len];
            sessionScriptCache.GetScriptDigest(script.ReadOnlySpan, digest);

            var onStackScriptKey = new ScriptHashKey(digest);
            _ = storeWrapper.storeScriptCache.TryGetValue(onStackScriptKey, out var globalScriptHandle);

            var sessionScriptHandle = globalScriptHandle;

            if (!sessionScriptCache.TryLoad(this, script.ReadOnlySpan, onStackScriptKey, ref sessionScriptHandle, out var runner, out var digestOnHeap))
            {
                // TryLoad will have written any errors out
                return true;
            }
            else if (sessionScriptHandle != globalScriptHandle)
            {
                // Add script to the store dictionary IF we didn't already have it cached
                //
                // This may strike you as odd, but it is how Redis behaves
                if (digestOnHeap == null)
                {
                    var newAlloc = GC.AllocateUninitializedArray<byte>(SessionScriptCache.SHA1Len, pinned: true);
                    digest.CopyTo(newAlloc);
                    if (!storeWrapper.storeScriptCache.TryAdd(new(newAlloc), sessionScriptHandle))
                    {
                        // Some other session loaded the script, toss our new handle
                        //
                        // Next time this script is run, it'll be pulled from the global cache
                        sessionScriptHandle.Dispose();
                    }
                }
                else
                {
                    if (!storeWrapper.storeScriptCache.TryAdd(digestOnHeap.Value, sessionScriptHandle))
                    {
                        // Some other session loaded the script, toss our new handle
                        //
                        // Next time this script is run, it'll be pulled from the global cache
                        sessionScriptHandle.Dispose();
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
                // We assume here that ExecuteScript does not raise exceptions
                sessionScriptCache.StartRunningScript(runner);
                var res = TryExecuteScript(count - 1, runner);
                sessionScriptCache.StopRunningScript();

                if (!res)
                {
                    sessionScriptCache.Remove(onStackScriptKey);
                }
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
                if (sha1.Length == SessionScriptCache.SHA1Len)
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
            // to signal to session level caches that the script needs to be discarded
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

            Span<byte> digest = stackalloc byte[SessionScriptCache.SHA1Len];
            sessionScriptCache.GetScriptDigest(source.Span, digest);

            var onStackScriptHashKey = new ScriptHashKey(digest);
            _ = storeWrapper.storeScriptCache.TryGetValue(onStackScriptHashKey, out var globalScriptHandle);

            var sessionScriptHandle = globalScriptHandle;
            if (sessionScriptCache.TryLoad(this, source.ReadOnlySpan, onStackScriptHashKey, ref sessionScriptHandle, out _, out var digestOnHeap))
            {
                // TryLoad will write any errors out

                // Add script to the global store dictionary if not already in there
                if (globalScriptHandle != sessionScriptHandle)
                {
                    if (digestOnHeap == null)
                    {
                        var newAlloc = GC.AllocateUninitializedArray<byte>(SessionScriptCache.SHA1Len, pinned: true);
                        digest.CopyTo(newAlloc);
                        if (!storeWrapper.storeScriptCache.TryAdd(new(newAlloc), sessionScriptHandle))
                        {
                            // Some other caller added the script already, our new handle is dead
                            // but we'll load it from the shared cache on next invocation
                            sessionScriptHandle.Dispose();
                        }
                    }
                    else
                    {
                        if (!storeWrapper.storeScriptCache.TryAdd(digestOnHeap.Value, sessionScriptHandle))
                        {
                            // Some other caller added the script already, our new handle is dead
                            // but we'll load it from the shared cache on next invocation
                            sessionScriptHandle.Dispose();
                        }
                    }
                }

                while (!RespWriteUtils.TryWriteBulkString(digest, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Returns true if Lua is enabled.
        /// 
        /// Otherwise writes out an error and returns false.
        /// </summary>
        private bool CheckLuaEnabled()
        {
            if (!storeWrapper.serverOptions.EnableLua)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_LUA_DISABLED, ref dcurr, dend))
                    SendAndReset();

                return false;
            }

            return true;
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