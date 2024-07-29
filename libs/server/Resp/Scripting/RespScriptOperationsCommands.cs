// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Garnet.common;
using Tsavorite.core;

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
        private unsafe bool TryEvalSha(int count, byte* ptr)
        {
            var digest = parseState.GetArgSliceByRef(0).ToArray();
            int nKeys = parseState.GetInt(1);

            if (txnManager.state != TxnState.None)
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_SERVER_BUSY, ref dcurr, dend))
                    SendAndReset();
            }

            // Get runner instance from the cache
            var result = false;
            Runner scriptRunner = null;
            if (!sessionScriptCache.TryGet(digest, out scriptRunner))
            {
                if (storeWrapper.storeScriptCache.TryGetValue(digest, out var source))
                {
                    sessionScriptCache.TryLoad(source, digest, out scriptRunner);
                }

                if (scriptRunner == null)
                {
                    var errorScriptNotFound = Encoding.ASCII.GetBytes("-ERR NOSCRIPT No matching script. Please use EVAL.\r\n");
                    while (!RespWriteUtils.WriteDirect(errorScriptNotFound, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    // Variable count minus 2 (script hash and number of keys
                    result = ExecuteScript(count - 2, nKeys, ref ptr, scriptRunner.runnerMachine);
                }
                readHead = (int)(ptr - recvBufferPtr);
                return result;
            }


            /// <summary>
            /// EVAL script numkeys [key [key ...]] [arg [arg ...]]
            /// </summary>
            /// <param name="count"></param>
            /// <param name="ptr"></param>
            /// <returns></returns>
            private unsafe bool TryEval(int count, byte* ptr)
            {
                //ptr += 10;

                //read script
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] script, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                //read number of keys in the script
                if (!RespReadUtils.ReadIntWithLengthHeader(out var nKeys, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (txnManager.state != TxnState.None)
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_SERVER_BUSY, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                //minus script and nKeys which have been already read
                var left = count - 2;
                var garnetApi = new ScriptApi(this, true);
                var result = ExecuteScript(left, nKeys, ref ptr, new Runner(garnetApi, script));
                readHead = (int)(ptr - recvBufferPtr);
                return result;
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

                            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out byte[] source, ref ptr, recvBufferPtr + bytesRead))
                                return false;

                            sessionScriptCache.TryLoad(source, out var digest);

                            // Add script to the store dictionary
                            storeWrapper.storeScriptCache.TryAdd(digest, source);

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
                readHead = (int)(ptr - recvBufferPtr);
                return true;
            }

            #region CommonMethods

            /// <summary>
            /// EVAL script numkeys [key [key ...]] [arg [arg ...]]
            /// EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
            /// Invoke the execution of a server-side Lua script.
            /// </summary>
            /// <param name="count"></param>
            /// <param name="nKeys"></param>
            /// <param name="ptr"></param>
            /// <param name="scriptRunner"></param>
            /// <returns></returns>
            private unsafe bool ExecuteScript(int count, int nKeys, ref byte* ptr, Runner scriptRunner)
            {
                var left = count;

                // Key name and attributes. If ArgSlice is a key in a dictionary it must have GetHashCode and Equals methods
                Dictionary<ArgSlice, (int endKeyName, bool raw, bool ex)> keys = new();
                for (int i = 0; i < nKeys; i++)
                {
                    ArgSlice key = default;
                    if (!RespReadUtils.ReadPtrWithLengthHeader(ref key.ptr, ref key.length, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    var keyAttr = ParseAttributes(key.ToArray());
                    ArgSlice keyName = new(key.ptr, keyAttr.nameEnd + 1);
                    keys.TryAdd(keyName, keyAttr);
                    left -= 1;
                }

                // read args
                var nArgs = left;

                //using one-based index for lua compatibility
                var args = new string[nArgs + 1];

                // REVIEW
                args[0] = String.Empty;
                for (int j = 1; j <= nArgs; j++)
                {
                    ArgSlice arg = default;
                    if (!RespReadUtils.ReadStringWithLengthHeader(out args[j], ref ptr, recvBufferPtr + bytesRead))
                        return false;
                }

                var keysList = keys.Keys.ToArray();
                if (NetworkKeyArraySlotVerify(keysList, true))
                {
                    return true;
                }

                bool createTransaction = false;
                scratchBufferManager.Reset();

                var keysForScript = new (ArgSlice, bool)[keys.Count + 1];
                if (txnManager.state != TxnState.Running)
                {
                    Debug.Assert(txnManager.state == TxnState.None);
                    createTransaction = true;
                    //using one-based index for lua compatibility
                    int n = 1;
                    foreach (var keyInfo in keys)
                    {
                        var (_, raw, ex) = keyInfo.Value;
                        var isObj = !(bool)raw;
                        keysForScript[n++] = (keyInfo.Key, raw);
                        //TODO: how to indicate lock both stores?
                        txnManager.SaveKeyEntryToLock(keyInfo.Key, isObj, ex ? LockType.Exclusive : LockType.Shared);
                    }

                    // start transaction
                    if (!txnManager.Run())
                    {
                        txnManager.Reset(false);
                        readHead = (int)(ptr - recvBufferPtr);
                        return true;
                    }
                }

                try
                {
                    object luaTable = null;
                    luaTable = scriptRunner.RunScript(keysForScript, args);
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
                }
                catch (Exception ex)
                {
                    Debug.Print(ex.Message);
                    createTransaction = false;
                    txnManager.Reset(true);
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_EMPTY, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
                finally
                {
                    //reset buffer for the next txn
                    scratchBufferManager.Reset();
                }
                txnManager.Commit(createTransaction);
                return true;
            }

            /// <summary>
            /// Parse key attributes in this formar 
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
