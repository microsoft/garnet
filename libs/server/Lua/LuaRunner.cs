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
                this.txnKeyEntries = new TxnKeyEntries(respServerSession.TsavoriteKernel, 16);
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
                    unpack = unpack;
                    gcinfo = gcinfo;
                    pairs = pairs;
                    loadstring = loadstring;
                    ipairs = ipairs;
                    error = error;
                    redis = redis;
                    math = math;
                }
                function load_sandboxed(source)
                    if (not source) then return nil end
                    return load(source, nil, nil, sandbox_env)
                end
            ");
            sandbox_env = (LuaTable)state["sandbox_env"];
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

            string[] keys = null;
            if (nKeys > 0)
            {
                // Lua uses 1-based indexing, so we allocate an extra entry in the array
                keys = new string[nKeys + 1];
                for (int i = 0; i < nKeys; i++)
                {
                    if (txnMode)
                    {
                        var key = parseState.GetArgSliceByRef(offset);
                        txnKeyEntries.AddKey(respServerSession.storeWrapper, key, false, Tsavorite.core.LockType.Exclusive);
                        if (!respServerSession.StorageSession.objectStoreLockableContext.IsNull)
                            txnKeyEntries.AddKey(respServerSession.storeWrapper, key, true, Tsavorite.core.LockType.Exclusive);
                    }
                    keys[i + 1] = parseState.GetString(offset++);
                }
                count -= nKeys;

                //TODO: handle slot verification for Lua script keys
                //if (NetworkKeyArraySlotVerify(keys, true))
                //{
                //    return true;
                //}
            }

            string[] argv = null;
            if (count > 0)
            {
                // Lua uses 1-based indexing, so we allocate an extra entry in the array
                argv = new string[count + 1];
                for (int i = 0; i < count; i++)
                {
                    argv[i + 1] = parseState.GetString(offset++);
                }
            }

            if (txnMode && nKeys > 0)
            {
                return RunTransactionInternal(keys, argv);
            }
            else
            {
                return RunInternal(keys, argv);
            }
        }

        /// <summary>
        /// Runs the precompiled Lua function with specified (keys, argv) state
        /// </summary>
        public object Run(string[] keys, string[] argv)
        {
            scratchBufferManager?.Reset();

            if (txnMode && keys?.Length > 0)
            {
                // Add keys to the transaction
                foreach (var key in keys)
                {
                    var _key = scratchBufferManager.CreateArgSlice(key);
                    txnKeyEntries.AddKey(respServerSession.storeWrapper, _key, false, Tsavorite.core.LockType.Exclusive);
                    if (!respServerSession.StorageSession.objectStoreLockableContext.IsNull)
                        txnKeyEntries.AddKey(respServerSession.storeWrapper, _key, true, Tsavorite.core.LockType.Exclusive);
                }
                return RunTransactionInternal(keys, argv);
            }
            else
            {
                return RunInternal(keys, argv);
            }
        }

        object RunTransactionInternal(string[] keys, string[] argv)
        {
            try
            {
                respServerSession.StorageSession.lockableContext.BeginTransaction();
                if (!respServerSession.StorageSession.objectStoreLockableContext.IsNull)
                    respServerSession.StorageSession.objectStoreLockableContext.EndTransaction();
                respServerSession.SetTransactionMode(true);
                txnKeyEntries.LockAllKeys(ref respServerSession.kernelSession);
                return RunInternal(keys, argv);
            }
            finally
            {
                txnKeyEntries.UnlockAllKeys(ref respServerSession.kernelSession);
                respServerSession.SetTransactionMode(false);
                respServerSession.StorageSession.lockableContext.EndTransaction();
                if (!respServerSession.StorageSession.objectStoreLockableContext.IsNull)
                    respServerSession.StorageSession.objectStoreLockableContext.EndTransaction();
            }
        }

        object RunInternal(string[] keys, string[] argv)
        {
            if (keys != this.keys)
            {
                if (keys == null)
                {
                    this.keys = null;
                    sandbox_env["KEYS"] = this.keys;
                }
                else
                {
                    if (this.keys != null && keys.Length == this.keys.Length)
                        Array.Copy(keys, this.keys, keys.Length);
                    else
                    {
                        this.keys = keys;
                        sandbox_env["KEYS"] = this.keys;
                    }
                }
            }
            if (argv != this.argv)
            {
                if (argv == null)
                {
                    this.argv = null;
                    sandbox_env["ARGV"] = this.argv;
                }
                else
                {
                    if (this.argv != null && argv.Length == this.argv.Length)
                        Array.Copy(argv, this.argv, argv.Length);
                    else
                    {
                        this.argv = argv;
                        sandbox_env["ARGV"] = this.argv;
                    }
                }
            }

            var result = function.Call();
            return result.Length > 0 ? result[0] : null;
        }

        /// <summary>
        /// Runs the precompiled Lua function
        /// </summary>
        /// <returns></returns>
        public object Run()
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