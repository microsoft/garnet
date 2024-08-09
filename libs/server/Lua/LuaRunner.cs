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

        /// <summary>
        /// Creates a new runner with the source of the script
        /// </summary>
        public LuaRunner(string source, StoreWrapper storeWrapper = null, ILogger logger = null)
        {
            this.source = source;
            if (storeWrapper != null)
            {
                this.scratchBufferNetworkSender = new ScratchBufferNetworkSender();
                this.respServerSession = new RespServerSession(scratchBufferNetworkSender, storeWrapper, null, null, false);
                this.scratchBufferManager = respServerSession.scratchBufferManager;
            }
            this.logger = logger;

            state = new Lua();
            state.State.Encoding = Encoding.UTF8;
            state.RegisterFunction("garnet_call", this, this.GetType().GetMethod("garnet_call"));
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
        public LuaRunner(ReadOnlySpan<byte> source, StoreWrapper storeWrapper = null, ILogger logger = null)
            : this(Encoding.UTF8.GetString(source), storeWrapper, logger)
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
            scratchBufferNetworkSender?.Dispose();
            respServerSession?.Dispose();
        }

        /// <summary>
        /// Entry point for redis.call method from a Lua script
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="args">Parameters</param>
        /// <returns></returns>
        public object garnet_call(string cmd, params object[] args)
            => respServerSession == null ? null : ProcessCommandFromScripting(cmd, args);

        /// <summary>
        /// Entry point method for executing commands from a Lua Script
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        unsafe object ProcessCommandFromScripting(string cmd, params object[] args)
        {
            scratchBufferManager.Reset();

            switch (cmd)
            {
                case "SET":
                case "set":
                    {
                        var key = scratchBufferManager.CreateArgSlice(Convert.ToString(args[0]));
                        var value = scratchBufferManager.CreateArgSlice(Convert.ToString(args[1]));
                        _ = respServerSession.basicGarnetApi.SET(key, value);
                        return "OK";
                    }
                case "GET":
                case "get":
                    {
                        var key = scratchBufferManager.CreateArgSlice(Convert.ToString(args[0]));
                        var status = respServerSession.basicGarnetApi.GET(key, out var value);
                        if (status == GarnetStatus.OK)
                            return value.ToString();
                        return null;
                    }
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
        /// Runs the precompiled Lua function with specified (keys, argv) state
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="argv"></param>
        /// <returns></returns>
        public object Run(string[] keys, string[] argv)
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
            if (result.Length > 0) return result[0]; else return null;
        }

        /// <summary>
        /// Runs the precompiled Lua function
        /// </summary>
        /// <returns></returns>
        public object Run()
        {
            var result = function.Call();
            if (result.Length > 0) return result[0]; else return null;
        }

        /// <summary>
        /// Runs the precompiled Lua function
        /// </summary>
        /// <returns></returns>
        public void RunVoid()
        {
            function.Call();
        }
    }
}