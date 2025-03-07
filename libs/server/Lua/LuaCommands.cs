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

            // Length check is mandatory, as ScriptHashKey assumes correct length
            if (digest.length == SessionScriptCache.SHA1Len)
            {
            tryAgain:
                var scriptKey = new ScriptHashKey(digest.Span);

                if (!sessionScriptCache.TryGetFromDigest(scriptKey, out runner))
                {
                    if (storeWrapper.storeScriptCache.TryGetValue(scriptKey, out var source))
                    {
                        if (!sessionScriptCache.TryLoad(this, source, scriptKey, out runner, out _, out var error))
                        {
                            // TryLoad will have written an error out, it any

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
                ExecuteScript(count - 1, runner);
                sessionScriptCache.StopRunningScript();
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

            if (!sessionScriptCache.TryLoad(this, script.ReadOnlySpan, new ScriptHashKey(digest), out var runner, out _, out var error))
            {
                // TryLoad will have written any errors out
                return true;
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
                ExecuteScript(count - 1, runner);
                sessionScriptCache.StopRunningScript();
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
                if (sha1.length == SessionScriptCache.SHA1Len)
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
            storeWrapper.storeScriptCache.Clear();

            // Flush session script cache
            sessionScriptCache.Clear();

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

            if (sessionScriptCache.TryLoad(this, source.ReadOnlySpan, new(digest), out _, out var digestOnHeap, out var error))
            {
                // TryLoad will write any errors out

                // Add script to the store dictionary
                if (digestOnHeap == null)
                {
                    var newAlloc = GC.AllocateUninitializedArray<byte>(SessionScriptCache.SHA1Len, pinned: true);
                    digest.CopyTo(newAlloc);
                    _ = storeWrapper.storeScriptCache.TryAdd(new(newAlloc), source.ToArray());
                }
                else
                {
                    _ = storeWrapper.storeScriptCache.TryAdd(digestOnHeap.Value, source.ToArray());
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
        /// </summary>
        private void ExecuteScript(int count, LuaRunner scriptRunner)
        {
            try
            {
                scriptRunner.RunForSession(count, this);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error executing Lua script");
                while (!RespWriteUtils.TryWriteError("ERR " + ex.Message, ref dcurr, dend))
                    SendAndReset();
            }
        }
    }
}