// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using Garnet.common;
using Microsoft.Extensions.Logging;
using NLua;
using Tsavorite.core;

namespace Garnet.server
{
    using BasicGarnetApi = GarnetApi<TransientKeyLocker, GarnetSafeEpochGuard>;
    using LockableGarnetApi = GarnetApi<TransactionalKeyLocker, GarnetSafeEpochGuard>;

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
        readonly TxnKeyEntries txnKeyEntries;
        readonly bool txnMode;
        readonly LuaFunction garnetCall;
        readonly LuaTable keyTable, argvTable;
        int keyLength, argvLength;
        Queue<IDisposable> disposeQueue;

        /// <summary>
        /// Creates a new runner with the source of the script
        /// </summary>
        public LuaRunner(string source, bool txnMode = false, RespServerSession respServerSession = null, ScratchBufferNetworkSender scratchBufferNetworkSender = null, ILogger logger = null)
        {
            this.source = source;
            this.txnMode = txnMode;
            this.respServerSession = respServerSession;
            this.scratchBufferNetworkSender = scratchBufferNetworkSender;
            scratchBufferManager = respServerSession?.scratchBufferManager;
            this.logger = logger;

            state = new Lua();
            state.State.Encoding = Encoding.UTF8;
            if (txnMode)
            {
                txnKeyEntries = new TxnKeyEntries(respServerSession.TsavoriteKernel, 16);
                garnetCall = state.RegisterFunction("garnet_call", this, GetType().GetMethod(nameof(garnet_call_txn)));
            }
            else
            {
                garnetCall = state.RegisterFunction("garnet_call", this, GetType().GetMethod("garnet_call"));
            }
            _ = state.DoString(@"
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
            garnetCall?.Dispose();
            keyTable?.Dispose();
            argvTable?.Dispose();
            sandbox_env?.Dispose();
            function?.Dispose();
            state?.Dispose();
        }

        /// <summary>
        /// Entry point for redis.call method from a Lua script (non-transactional mode)
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="args">Parameters</param>
        /// <returns></returns>
        public object garnet_call(string cmd, params object[] args)
            => respServerSession == null ? null : ProcessCommandFromScripting<TransientKeyLocker, GarnetSafeEpochGuard, BasicGarnetApi>(respServerSession.basicGarnetApi, cmd, args);

        /// <summary>
        /// Entry point for redis.call method from a Lua script (transactional mode)
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="args">Parameters</param>
        /// <returns></returns>
        public object garnet_call_txn(string cmd, params object[] args)
            => respServerSession == null ? null : ProcessCommandFromScripting<TransactionalKeyLocker, GarnetSafeEpochGuard, LockableGarnetApi>(respServerSession.lockableGarnetApi, cmd, args);

        /// <summary>
        /// Entry point method for executing commands from a Lua Script
        /// </summary>
        unsafe object ProcessCommandFromScripting<TKeyLocker, TEpochGuard, TGarnetApi>(TGarnetApi api, string cmd, params object[] args)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        {
            switch (cmd)
            {
                // We special-case a few performance-sensitive operations to directly invoke via the storage API
                case "SET" when args.Length == 2:
                case "set" when args.Length == 2:
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
                    if (RespReadUtils.Read64Int(out var number, ref ptr, ptr + length))
                        return number;
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
                    {
                        // Create return table
                        var returnValue = (LuaTable)state.DoString("return { }")[0];

                        // Queue up for disposal at the end of the script call
                        disposeQueue ??= new();
                        disposeQueue.Enqueue(returnValue);

                        // Populate the table
                        var i = 1;
                        foreach (var item in resultArray)
                            returnValue[i++] = item == null ? false : item;
                        return returnValue;
                    }
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

            var offset = 1;
            var nKeys = parseState.GetInt(offset++);
            count--;
            ResetParameters(nKeys, count - nKeys);

            if (nKeys > 0)
            {
                for (var i = 0; i < nKeys; i++)
                {
                    if (txnMode)
                    {
                        var key = parseState.GetArgSliceByRef(offset);
                        _ = txnKeyEntries.AddKey(respServerSession.storeWrapper, key, isObject: false, Tsavorite.core.LockType.Exclusive);
                        if (!respServerSession.storageSession.IsDual)
                            _ = txnKeyEntries.AddKey(respServerSession.storeWrapper, key, isObject: true, Tsavorite.core.LockType.Exclusive);
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
                for (var i = 0; i < count; i++)
                    argvTable[i + 1] = parseState.GetString(offset++);
            }

            return txnMode && nKeys > 0 ? RunTransaction() : Run();
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
                    _ = txnKeyEntries.AddKey(respServerSession.storeWrapper, _key, isObject: false, Tsavorite.core.LockType.Exclusive);
                    if (respServerSession.storageSession.IsDual)
                        _ = txnKeyEntries.AddKey(respServerSession.storeWrapper, _key, isObject: true, Tsavorite.core.LockType.Exclusive);
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
                respServerSession.storageSession.BeginTransaction();
                respServerSession.SetTransactionMode(true);
                txnKeyEntries.LockAllKeys(respServerSession.storageSession);
                return Run();
            }
            finally
            {
                txnKeyEntries.UnlockAllKeys(respServerSession.storageSession);
                respServerSession.SetTransactionMode(false);
                respServerSession.storageSession.EndTransaction();
            }
        }

        void ResetParameters(int nKeys, int nArgs)
        {
            if (keyLength > nKeys)
            {
                _ = state.DoString($"count = #KEYS for i={nKeys + 1}, {keyLength} do KEYS[i]=nil end");
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
                for (var i = 0; i < keys.Length; i++)
                    keyTable[i + 1] = keys[i];
            }
            if (argv != null)
            {
                for (var i = 0; i < argv.Length; i++)
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
            Cleanup();
            return result?.Length > 0 ? result[0] : null;
        }

        void Cleanup()
        {
            if (disposeQueue != null)
            {
                while (disposeQueue.Count > 0)
                {
                    var table = disposeQueue.Dequeue();
                    table.Dispose();
                }
            }
        }
    }
}