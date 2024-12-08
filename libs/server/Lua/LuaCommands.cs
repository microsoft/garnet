// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using Garnet.common;
using Microsoft.Extensions.Logging;
using NLua;
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
            if (!CheckLuaEnabled())
            {
                return true;
            }

            var count = parseState.Count;
            if (count < 2)
            {
                return AbortWithWrongNumberOfArguments("EVALSHA");
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
            if (!CheckLuaEnabled())
            {
                return true;
            }

            var count = parseState.Count;
            if (count < 2)
            {
                return AbortWithWrongNumberOfArguments("EVAL");
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
                return AbortWithWrongNumberOfArguments("SCRIPT|EXISTS");
            }

            // returns an array where each element is a 0 if the script does not exist, and a 1 if it does

            // todo: can we remove this alloc?
            byte[] sha1Buff = new byte[20];

            // todo: does Redis accept hashes of the wrong length?
            //       if so we could get rid of this intoArry stuff
            //       and just write the results out as we calculate them

            var intoArr = parseState.Count <= 16 ? null : ArrayPool<bool>.Shared.Rent(parseState.Count);
            Span<bool> into = intoArr == null ? stackalloc bool[parseState.Count] : intoArr.AsSpan()[..parseState.Count];

            for (var shaIx = 0; shaIx < parseState.Count; shaIx++)
            {
                var sha1 = parseState.GetArgSliceByRef(shaIx);
                if (sha1.length != sha1Buff.Length)
                {
                    into[shaIx] = false;
                }
                else
                {
                    sha1.Span.CopyTo(sha1Buff);
                    into[shaIx] = storeWrapper.storeScriptCache.ContainsKey(sha1Buff);
                }
            }

            while (!RespWriteUtils.WriteArrayLength(into.Length, ref dcurr, dend))
                SendAndReset();

            for (var i = 0; i < into.Length; i++)
            {
                var toWrite = into[i] ? 1 : 0;
                while (!RespWriteUtils.WriteArrayItem(toWrite, ref dcurr, dend))
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
                return AbortWithWrongNumberOfArguments("SCRIPT");
            }
            else if (parseState.Count == 1)
            {
                // we ignore this, but should validate it
                ref var arg = ref parseState.GetArgSliceByRef(0);

                AsciiUtils.ToUpperInPlace(arg.Span);

                var valid = arg.Span.SequenceEqual(CmdStrings.ASYNC) || arg.Span.SequenceEqual(CmdStrings.SYNC);

                if (!valid)
                {
                    // todo: match what redis does
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                }
            }

            // Flush store script cache
            storeWrapper.storeScriptCache.Clear();

            // Flush session script cache
            sessionScriptCache.Clear();

            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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
                return AbortWithWrongNumberOfArguments("SCRIPT|LOAD");
            }

            var source = parseState.GetArgSliceByRef(0).ReadOnlySpan;
            if (!sessionScriptCache.TryLoad(source, out var digest, out _, out var error))
            {
                while (!RespWriteUtils.WriteError(error, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {

                // Add script to the store dictionary
                storeWrapper.storeScriptCache.TryAdd(digest, source.ToArray());

                while (!RespWriteUtils.WriteBulkString(digest, ref dcurr, dend))
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
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_LUA_DISABLED, ref dcurr, dend))
                    SendAndReset();
                return false;
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
                var scriptResult = scriptRunner.Run(count, parseState);
                WriteObject(scriptResult);
            }
            catch (LuaScriptException ex)
            {
                logger?.LogError(ex.InnerException ?? ex, "Error executing Lua script callback");
                while (!RespWriteUtils.WriteError("ERR " + (ex.InnerException ?? ex).Message, ref dcurr, dend))
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

        void WriteObject(object scriptResult)
        {
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
                else if (scriptResult is bool b)
                {
                    if (b)
                    {
                        while (!RespWriteUtils.WriteInteger(1, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                            SendAndReset();
                    }
                }
                else if (scriptResult is long l)
                {
                    while (!RespWriteUtils.WriteInteger(l, ref dcurr, dend))
                        SendAndReset();
                }
                else if (scriptResult is ArgSlice a)
                {
                    while (!RespWriteUtils.WriteBulkString(a.ReadOnlySpan, ref dcurr, dend))
                        SendAndReset();
                }
                else if (scriptResult is object[] o)
                {
                    // Two objects one boolean value and the result from the Lua Call
                    while (!RespWriteUtils.WriteAsciiBulkString(o[1].ToString().AsSpan(), ref dcurr, dend))
                        SendAndReset();
                }
                else if (scriptResult is LuaTable luaTable)
                {
                    try
                    {
                        var retVal = luaTable["err"];
                        if (retVal != null)
                        {
                            while (!RespWriteUtils.WriteError((string)retVal, ref dcurr, dend))
                                SendAndReset();
                        }
                        else
                        {
                            retVal = luaTable["ok"];
                            if (retVal != null)
                            {
                                while (!RespWriteUtils.WriteAsciiBulkString((string)retVal, ref dcurr, dend))
                                    SendAndReset();
                            }
                            else
                            {
                                int count = luaTable.Values.Count;
                                while (!RespWriteUtils.WriteArrayLength(count, ref dcurr, dend))
                                    SendAndReset();
                                foreach (var value in luaTable.Values)
                                {
                                    WriteObject(value);
                                }
                            }
                        }
                    }
                    finally
                    {
                        luaTable.Dispose();
                    }
                }
                else
                {
                    throw new LuaScriptException("Unknown return type", "");
                }
            }
            else
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                    SendAndReset();
            }
        }
    }
}