// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.common;
using Microsoft.Extensions.Logging;
using NLua;

namespace Garnet.server
{
    /// <summary>
    /// Creates the instance to run Lua scripts
    /// </summary>
    internal sealed class LuaRunner : IDisposable
    {
        readonly string source;
        readonly ScratchBufferNetworkSender scratchBufferNetworkSender;
        readonly RespServerSession respServerSession;
        readonly ScratchBufferManager scratchBufferManager;
        readonly ILogger logger;
        readonly Lua state;
        readonly LuaTable sandbox_env;
        LuaFunction function;
        string[] keys, argv;
        readonly TxnKeyEntries txnKeyEntries;
        readonly bool txnMode;

        readonly LuaTable keyTable, argvTable;
        int keyLength, argvLength;

        /// <summary>
        /// Creates a new runner with the source of the script
        /// </summary>
        public LuaRunner(string source, bool txnMode = false, RespServerSession respServerSession = null, ScratchBufferNetworkSender scratchBufferNetworkSender = null, ILogger logger = null)
        {
            this.source = source;
            this.txnMode = txnMode;
            this.respServerSession = respServerSession;
            this.scratchBufferNetworkSender = scratchBufferNetworkSender;
            this.scratchBufferManager = respServerSession?.scratchBufferManager;
            this.logger = logger;

            state = new Lua();
            state.State.Encoding = Encoding.UTF8;
            if (txnMode)
            {
                this.txnKeyEntries = new TxnKeyEntries(16, respServerSession.storageSession.lockableContext, respServerSession.storageSession.objectStoreLockableContext);
                state.RegisterFunction("garnet_call", this, this.GetType().GetMethod(nameof(garnet_call_txn)));
            }
            else
            {
                state.RegisterFunction("garnet_call", this, this.GetType().GetMethod("garnet_call"));
            }
            state.DoString(@"
                import = function () end
                redis = {}
                function redis.call(cmd, ...)
                    return garnet_call(cmd, ...)
                end
                function redis.status_reply(text)
                    return text
                end
                function redis.error_reply(text)
                    return { err = text }
                end
                KEYS = {}
                ARGV = {}
                sandbox_env = {
                    tostring = tostring;
                    next = next;
                    assert = assert;
                    tonumber = tonumber;
                    rawequal = rawequal;
                    collectgarbage = collectgarbage;
                    coroutine = coroutine;
                    type = type;
                    select = select;
                    unpack = table.unpack;
                    gcinfo = gcinfo;
                    pairs = pairs;
                    loadstring = loadstring;
                    ipairs = ipairs;
                    error = error;
                    redis = redis;
                    math = math;
                    table = table;
                    string = string;
                    KEYS = KEYS;
                    ARGV = ARGV;
                }
                function load_sandboxed(source)
                    if (not source) then return nil end
                    return load(source, nil, nil, sandbox_env)
                end
            ");
            sandbox_env = (LuaTable)state["sandbox_env"];
            keyTable = (LuaTable)state["KEYS"];
            argvTable = (LuaTable)state["ARGV"];
        }

        /// <summary>
        /// Creates a new runner with the source of the script
        /// </summary>
        public LuaRunner(ReadOnlySpan<byte> source, bool txnMode, RespServerSession respServerSession, ScratchBufferNetworkSender scratchBufferNetworkSender, ILogger logger = null)
            : this(Encoding.UTF8.GetString(source), txnMode, respServerSession, scratchBufferNetworkSender, logger)
        {
        }

        /// <summary>
        /// Compile script
        /// </summary>
        public void Compile()
        {
            try
            {
                using var loader = (LuaFunction)state["load_sandboxed"];
                var result = loader.Call(source);
                if (result?.Length == 1)
                {
                    function = result[0] as LuaFunction;
                    return;
                }

                if (result?.Length == 2)
                {
                    throw new GarnetException($"Compilation error: {(string)result[1]}");
                }
                else
                {
                    throw new GarnetException($"Unable to load script");
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "CreateFunction threw an exception");
                throw;
            }
        }

        /// <summary>
        /// Dispose the runner
        /// </summary>
        public void Dispose()
        {
            function?.Dispose();
            state?.Dispose();
            sandbox_env?.Dispose();
        }

        /// <summary>
        /// Entry point for redis.call method from a Lua script (non-transactional mode)
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="args">Parameters</param>
        /// <returns></returns>
        public object garnet_call(string cmd, params object[] args)
            => respServerSession == null ? null : ProcessCommandFromScripting(respServerSession.basicGarnetApi, cmd, args);

        /// <summary>
        /// Entry point for redis.call method from a Lua script (transactional mode)
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="args">Parameters</param>
        /// <returns></returns>
        public object garnet_call_txn(string cmd, params object[] args)
            => respServerSession == null ? null : ProcessCommandFromScripting(respServerSession.lockableGarnetApi, cmd, args);

        /// <summary>
        /// Entry point method for executing commands from a Lua Script
        /// </summary>
        unsafe object ProcessCommandFromScripting<TGarnetApi>(TGarnetApi api, string cmd, params object[] args)
            where TGarnetApi : IGarnetApi
        {
            switch (cmd)
            {
                // We special-case a few performance-sensitive operations to directly invoke via the storage API
                case "SET":
                case "set":
                    {
                        if (!respServerSession.CheckACLPermissions(RespCommand.SET))
                            return Encoding.ASCII.GetString(CmdStrings.RESP_ERR_NOAUTH);
                        var key = scratchBufferManager.CreateArgSlice(Convert.ToString(args[0]));
                        var value = scratchBufferManager.CreateArgSlice(Convert.ToString(args[1]));
                        _ = api.SET(key, value);
                        return "OK";
                    }
                case "GET":
                case "get":
                    {
                        if (!respServerSession.CheckACLPermissions(RespCommand.GET))
                            throw new Exception(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_NOAUTH));
                        var key = scratchBufferManager.CreateArgSlice(Convert.ToString(args[0]));
                        var status = api.GET(key, out var value);
                        if (status == GarnetStatus.OK)
                            return value.ToString();
                        return null;
                    }
                // As fallback, we use RespServerSession with a RESP-formatted input. This could be optimized
                // in future to provide parse state directly.
                default:
                    {
                        var request = scratchBufferManager.FormatCommandAsResp(cmd, args, state);
                        _ = respServerSession.TryConsumeMessages(request.ptr, request.length);
                        var response = scratchBufferNetworkSender.GetResponse();
                        var result = ProcessResponse(response.ptr, response.length);
                        scratchBufferNetworkSender.Reset();
                        return result;
                    }
            }
        }

        /// <summary>
        /// Process a RESP-formatted response from the RespServerSession
        /// </summary>
        unsafe object ProcessResponse(byte* ptr, int length)
        {
            switch (*ptr)
            {
                case (byte)'+':
                    if (RespReadUtils.ReadSimpleString(out var resultStr, ref ptr, ptr + length))
                        return resultStr;
                    break;
                case (byte)':':
                    if (RespReadUtils.ReadIntegerAsString(out var resultInt, ref ptr, ptr + length))
                        return resultInt;
                    break;
                case (byte)'-':
                    if (RespReadUtils.ReadErrorAsString(out resultStr, ref ptr, ptr + length))
                        return resultStr;
                    break;

                case (byte)'$':
                    if (RespReadUtils.ReadStringResponseWithLengthHeader(out resultStr, ref ptr, ptr + length))
                        return resultStr;
                    break;

                case (byte)'*':
                    if (RespReadUtils.ReadStringArrayResponseWithLengthHeader(out var resultArray, ref ptr, ptr + length))
                        return resultArray;
                    break;

                default:
                    throw new Exception("Unexpected response: " + Encoding.UTF8.GetString(new Span<byte>(ptr, length)).Replace("\n", "|").Replace("\r", "") + "]");
            }
            return null;
        }

        /// <summary>
        /// Runs the precompiled Lua function with specified parse state
        /// </summary>
        public object Run(int count, SessionParseState parseState)
        {
            scratchBufferManager.Reset();

            int offset = 1;
            int nKeys = parseState.GetInt(offset++);
            count--;
            ResetParameters(nKeys, count - nKeys);

            if (nKeys > 0)
            {
                for (int i = 0; i < nKeys; i++)
                {
                    if (txnMode)
                    {
                        var key = parseState.GetArgSliceByRef(offset);
                        txnKeyEntries.AddKey(key, false, Tsavorite.core.LockType.Exclusive);
                        if (!respServerSession.storageSession.objectStoreLockableContext.IsNull)
                            txnKeyEntries.AddKey(key, true, Tsavorite.core.LockType.Exclusive);
                    }
                    keyTable[i + 1] = parseState.GetString(offset++);
                }
                count -= nKeys;

                //TODO: handle slot verification for Lua script keys
                //if (NetworkKeyArraySlotVerify(keys, true))
                //{
                //    return true;
                //}
            }

            if (count > 0)
            {
                for (int i = 0; i < count; i++)
                {
                    argvTable[i + 1] = parseState.GetString(offset++);
                }
            }

            if (txnMode && nKeys > 0)
            {
                return RunTransaction();
            }
            else
            {
                return Run();
            }
        }

        /// <summary>
        /// Runs the precompiled Lua function with specified (keys, argv) state
        /// </summary>
        public object Run(string[] keys = null, string[] argv = null)
        {
            scratchBufferManager?.Reset();
            LoadParameters(keys, argv);
            if (txnMode && keys?.Length > 0)
            {
                // Add keys to the transaction
                foreach (var key in keys)
                {
                    var _key = scratchBufferManager.CreateArgSlice(key);
                    txnKeyEntries.AddKey(_key, false, Tsavorite.core.LockType.Exclusive);
                    if (!respServerSession.storageSession.objectStoreLockableContext.IsNull)
                        txnKeyEntries.AddKey(_key, true, Tsavorite.core.LockType.Exclusive);
                }
                return RunTransaction();
            }
            else
            {
                return Run();
            }
        }

        object RunTransaction()
        {
            try
            {
                respServerSession.storageSession.lockableContext.BeginLockable();
                if (!respServerSession.storageSession.objectStoreLockableContext.IsNull)
                    respServerSession.storageSession.objectStoreLockableContext.BeginLockable();
                respServerSession.SetTransactionMode(true);
                txnKeyEntries.LockAllKeys();
                return Run();
            }
            finally
            {
                txnKeyEntries.UnlockAllKeys();
                respServerSession.SetTransactionMode(false);
                respServerSession.storageSession.lockableContext.EndLockable();
                if (!respServerSession.storageSession.objectStoreLockableContext.IsNull)
                    respServerSession.storageSession.objectStoreLockableContext.EndLockable();
            }
        }

        void ResetParameters(int nKeys, int nArgs)
        {
            if (keyLength > nKeys)
            {
                _ = state.DoString($"count = #KEYS for i={nKeys+1}, {keyLength} do KEYS[i]=nil end");
            }
            keyLength = nKeys;
            if (argvLength > nArgs)
            {
                _ = state.DoString($"count = #ARGV for i={nArgs + 1}, {argvLength} do ARGV[i]=nil end");
            }
            argvLength = nArgs;
        }

        void LoadParameters(string[] keys, string[] argv)
        {
            ResetParameters(keys?.Length ?? 0, argv?.Length ?? 0);
            if (keys != null)
            {
                for (int i = 0; i < keys.Length; i++)
                    keyTable[i + 1] = keys[i];
            }
            if (argv != null)
            {
                for (int i = 0; i < argv.Length; i++)
                    argvTable[i + 1] = argv[i];
            }
        }

        /// <summary>
        /// Runs the precompiled Lua function
        /// </summary>
        /// <returns></returns>
        object Run()
        {
            var result = function.Call();
            return result.Length > 0 ? result[0] : null;
        }

        /// <summary>
        /// Runs the precompiled Lua function
        /// </summary>
        /// <returns></returns>
        public void RunVoid()
            => function.Call();
    }
}