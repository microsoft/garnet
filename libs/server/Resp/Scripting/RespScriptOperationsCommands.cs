// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {

        /// <summary>
        /// EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
        /// </summary>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <returns></returns>
        private unsafe bool TryEVALSHA(int count, byte* ptr)
        {
            var digest = parseState.GetArgSliceByRef(0).ToArray();

            // Get runner instance from the cache
            var result = false;
            LuaRunner scriptRunner = null;
            if (!sessionScriptCache.TryGet(digest, out scriptRunner))
            {
                if (storeWrapper.storeScriptCache.TryGetValue(digest, out var source))
                {
                    sessionScriptCache.TryLoad(source, digest, out scriptRunner);
                }

                if (scriptRunner == null)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_NO_SCRIPT, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    result = ExecuteScript(count - 1, scriptRunner);
                }
            }
            return result;
        }


        /// <summary>
        /// EVAL script numkeys [key [key ...]] [arg [arg ...]]
        /// </summary>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <returns></returns>
        private unsafe bool TryEVAL(int count, byte* ptr)
        {
            var script = parseState.GetArgSliceByRef(0).ReadOnlySpan;
            using (var luaRunner = new LuaRunner(script, this, logger))
            {
                var result = ExecuteScript(count - 1, luaRunner);
                return result;
            }
        }

        /// <summary>
        /// SCRIPT Commands (load, exists, flush, kills)
        /// </summary>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <returns></returns>
        private unsafe bool TryScript(int count, byte* ptr)
        {
            if (count >= 1)
            {
                var option = parseState.GetString(0).ToLowerInvariant();
                switch (option)
                {
                    case "load" when count == 2:
                        var source = parseState.GetArgSliceByRef(1).ReadOnlySpan;
                        sessionScriptCache.TryLoad(source, out var digest, out _);

                        // Add script to the store dictionary
                        storeWrapper.storeScriptCache.TryAdd(digest, source.ToArray());

                        while (!RespWriteUtils.WriteBulkString(digest, ref dcurr, dend))
                            SendAndReset();
                        break;
                    case "exists" when count == 2:
                        var sha1Exists = parseState.GetArgSliceByRef(0).ToArray();

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
                        break;
                    case "flush" when count == 1:
                        // Flush store script cache
                        storeWrapper.storeScriptCache.Clear();

                        // Flush session script cache
                        sessionScriptCache.Clear();

                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK.ToArray(), ref dcurr, dend))
                            SendAndReset();
                        break;
                    default:
                        break;
                }
            }
            else
            {
                // send error to output
                return AbortWithWrongNumberOfArguments("SCRIPT", count);
            }
            return true;
        }

        #region CommonMethods

        /// <summary>
        /// Invoke the execution of a server-side Lua script.
        /// </summary>
        /// <param name="count"></param>
        /// <param name="scriptRunner"></param>
        /// <returns></returns>
        private unsafe bool ExecuteScript(int count, LuaRunner scriptRunner)
        {
            int offset = 1;
            int nKeys = parseState.GetInt(offset++);
            count--;

            string[] keys = null;
            if (nKeys > 0)
            {
                keys = new string[nKeys];
                for (int i = 0; i < nKeys; i++)
                {
                    keys[i] = parseState.GetString(offset++);
                }
                count -= nKeys;

                //if (NetworkKeyArraySlotVerify(keys, true))
                //{
                //    return true;
                //}
            }

            string[] argv = null;
            if (count > 0)
            {
                argv = new string[count];
                for (int i = 0; i < count; i++)
                {
                    argv[i] = parseState.GetString(offset++);
                }
            }

            try
            {
                object luaTable = null;
                luaTable = scriptRunner.Run(keys, argv);
                if (luaTable != null)
                {
                    if ((luaTable as byte?) != null && (byte)luaTable == 36) //equals to $
                    {
                        while (!RespWriteUtils.WriteDirect((byte[])luaTable, ref dcurr, dend))
                            SendAndReset();
                    }
                    else if (luaTable as string != null)
                    {
                        while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes((string)luaTable), ref dcurr, dend))
                            SendAndReset();
                    }
                    else if (luaTable as Int64? != null)
                    {
                        while (!RespWriteUtils.WriteInteger((Int64)luaTable, ref dcurr, dend))
                            SendAndReset();
                    }
                    else if (luaTable as ArgSlice? != null)
                    {
                        while (!RespWriteUtils.WriteBulkString(((ArgSlice)luaTable).ToArray(), ref dcurr, dend))
                            SendAndReset();
                    }
                    else if (luaTable as Object[] != null)
                    {
                        // Two objects one boolean value and the result from the Lua Call
                        while (!RespWriteUtils.WriteAsciiBulkString((luaTable as Object[])[1].ToString().AsSpan(), ref dcurr, dend))
                            SendAndReset();
                    }
                }
                else
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error executing Lua script");
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_EMPTY, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            return true;
        }

        /// <summary>
        /// Parse key attributes in this format
        /// key[:storetype][:locktype]
        /// storetype >> 'raw' or 'obj'
        /// locktype >>  's' or 'x'
        /// </summary>
        /// <param name="keyName"></param>
        /// <returns></returns>
        private static (int nameEnd, bool raw, bool ex) ParseAttributes(byte[] keyName)
        {
            int[] bookmarks = new int[10];
            int totalBookmarks = 0;
            bool raw = true;
            bool ex = true;
            int nameEnd = 0;

            for (int j = keyName.Length - 1; j >= 0; j--)
            {
                if ((char)keyName[j] == ':')
                {
                    bookmarks[totalBookmarks] = j;
                    totalBookmarks++;
                }
            }

            if (totalBookmarks == 0)
                return (keyName.Length - 1, raw, ex);

            for (int k = 0; k < totalBookmarks; k++)
            {
                var i = bookmarks[k];

                //try read locktype 's' or 'x'
                //:x </EOF>
                if (i + 1 == keyName.Length - 1)
                {
                    if ((char)keyName[i + 1] == 'x' || (char)keyName[i + 1] == 'X')
                        ex = true;
                    else
                        ex = false;
                }

                //try read keytype
                //::
                if (i + 3 < keyName.Length)
                {
                    //make sure is raw
                    if (((char)keyName[i + 1] == 'r' || (char)keyName[i + 1] == 'R')
                        && ((char)keyName[i + 2] == 'a' || (char)keyName[i + 2] == 'A')
                        && ((char)keyName[i + 3] == 'w' || (char)keyName[i + 3] == 'W'))
                    {
                        raw = true;
                        nameEnd = i - 1;
                        break;
                    }
                    //make sure is obj
                    if (((char)keyName[i + 1] == 'o' || (char)keyName[i + 1] == 'O')
                        && ((char)keyName[i + 2] == 'b' || (char)keyName[i + 2] == 'B')
                        && ((char)keyName[i + 3] == 'j' || (char)keyName[i + 3] == 'J'))
                    {
                        raw = false;
                        nameEnd = i - 1;
                        break;
                    }
                }

                //keytype is not explicit but there's a placeholder ('::*')
                if (i + 1 < keyName.Length - 1 && (char)keyName[i + 1] == ':')
                {
                    raw = true;
                    nameEnd = i - 1;
                    break;
                }
            }

            return (nameEnd, raw, ex);
        }

        #endregion

    }
}
