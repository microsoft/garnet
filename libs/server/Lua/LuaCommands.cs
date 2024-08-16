// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Microsoft.Extensions.Logging;
using NLua.Exceptions;

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
            if (!storeWrapper.serverOptions.EnableLua)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_LUA_DISABLED, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var count = parseState.count;
            if (count < 2)
            {
                return AbortWithWrongNumberOfArguments("EVALSHA", count);
            }
            var digest = parseState.GetArgSliceByRef(0).ReadOnlySpan;

            var result = false;
            if (!sessionScriptCache.TryGetFromDigest(digest, out var runner))
            {
                var d = digest.ToArray();
                if (storeWrapper.storeScriptCache.TryGetValue(d, out var source))
                {
                    if (!sessionScriptCache.TryLoad(source, d, out runner, out var error))
                    {
                        while (!RespWriteUtils.WriteError(error, ref dcurr, dend))
                            SendAndReset();
                        _ = storeWrapper.storeScriptCache.TryRemove(d, out _);
                        return result;
                    }
                }
            }
            if (runner == null)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_NO_SCRIPT, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                result = ExecuteScript(count - 1, runner);
            }
            return result;
        }


        /// <summary>
        /// EVAL script numkeys [key [key ...]] [arg [arg ...]]
        /// </summary>
        /// <returns></returns>
        private unsafe bool TryEVAL()
        {
            if (!storeWrapper.serverOptions.EnableLua)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_LUA_DISABLED, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var count = parseState.count;
            if (count < 2)
            {
                return AbortWithWrongNumberOfArguments("EVAL", count);
            }
            var script = parseState.GetArgSliceByRef(0).ReadOnlySpan;
            var digest = sessionScriptCache.GetScriptDigest(script);

            var result = false;
            if (!sessionScriptCache.TryLoad(script, digest, out var runner, out var error))
            {
                while (!RespWriteUtils.WriteError(error, ref dcurr, dend))
                    SendAndReset();
                return result;
            }
            if (runner == null)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_NO_SCRIPT, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                result = ExecuteScript(count - 1, runner);
            }
            return result;
        }

        /// <summary>
        /// SCRIPT Commands (load, exists, flush)
        /// </summary>
        /// <returns></returns>
        private unsafe bool TrySCRIPT()
        {
            if (!storeWrapper.serverOptions.EnableLua)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_LUA_DISABLED, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var count = parseState.count;
            var option = parseState.GetArgSliceByRef(0).ReadOnlySpan;
            if (parseState.count == 2 && option.EqualsUpperCaseSpanIgnoringCase("LOAD"u8))
            {
                var source = parseState.GetArgSliceByRef(1).ReadOnlySpan;
                if (!sessionScriptCache.TryLoad(source, out var digest, out _, out var error))
                {
                    while (!RespWriteUtils.WriteError(error, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                // Add script to the store dictionary
                storeWrapper.storeScriptCache.TryAdd(digest, source.ToArray());

                while (!RespWriteUtils.WriteBulkString(digest, ref dcurr, dend))
                    SendAndReset();
            }
            else if (parseState.count == 2 && option.EqualsUpperCaseSpanIgnoringCase("EXISTS"u8))
            {
                var sha1Exists = parseState.GetArgSliceByRef(1).ToArray();

                // Check whether script exists at the store level
                if (storeWrapper.storeScriptCache.ContainsKey(sha1Exists))
                {
                    while (!RespWriteUtils.WriteBulkString(CmdStrings.RESP_OK.ToArray(), ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteBulkString(CmdStrings.RESP_RETURN_VAL_N1.ToArray(), ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (parseState.count == 1 && option.EqualsUpperCaseSpanIgnoringCase("FLUSH"u8))
            {
                // Flush store script cache
                storeWrapper.storeScriptCache.Clear();

                // Flush session script cache
                sessionScriptCache.Clear();

                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK.ToArray(), ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                // Send error to output
                return AbortWithWrongNumberOfArguments("SCRIPT", count);
            }
            return true;
        }

        /// <summary>
        /// Invoke the execution of a server-side Lua script.
        /// </summary>
        /// <param name="count"></param>
        /// <param name="scriptRunner"></param>
        /// <returns></returns>
        private unsafe bool ExecuteScript(int count, LuaRunner scriptRunner)
        {
            try
            {
                object scriptResult = scriptRunner.Run(count, parseState);
                if (scriptResult != null)
                {
                    if (scriptResult is string s)
                    {
                        while (!RespWriteUtils.WriteAsciiBulkString(s, ref dcurr, dend))
                            SendAndReset();
                    }
                    else if ((scriptResult as byte?) != null && (byte)scriptResult == 36) //equals to $
                    {
                        while (!RespWriteUtils.WriteDirect((byte[])scriptResult, ref dcurr, dend))
                            SendAndReset();
                    }
                    else if (scriptResult as Int64? != null)
                    {
                        while (!RespWriteUtils.WriteInteger((Int64)scriptResult, ref dcurr, dend))
                            SendAndReset();
                    }
                    else if (scriptResult as ArgSlice? != null)
                    {
                        while (!RespWriteUtils.WriteBulkString(((ArgSlice)scriptResult).ToArray(), ref dcurr, dend))
                            SendAndReset();
                    }
                    else if (scriptResult as Object[] != null)
                    {
                        // Two objects one boolean value and the result from the Lua Call
                        while (!RespWriteUtils.WriteAsciiBulkString((scriptResult as Object[])[1].ToString().AsSpan(), ref dcurr, dend))
                            SendAndReset();
                    }
                }
                else
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                }
            }
            catch (LuaScriptException ex)
            {
                logger?.LogError(ex.InnerException, "Error executing Lua script callback");
                while (!RespWriteUtils.WriteError("ERR " + ex.InnerException.Message, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error executing Lua script");
                while (!RespWriteUtils.WriteError("ERR " + ex.Message, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            return true;
        }
    }
}