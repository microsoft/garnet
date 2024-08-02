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
        readonly RespServerSession respServerSession;
        readonly ILogger logger;
        readonly Lua state;
        readonly LuaTable sandbox_env;
        LuaFunction function;

        /// <summary>
        /// Creates a new runner with the source of the script
        /// </summary>
        public LuaRunner(string source, RespServerSession respServerSession = null, ILogger logger = null)
        {
            this.source = source;
            this.respServerSession = respServerSession;
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
                    print = print;
                    redis=redis;
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
        public LuaRunner(ReadOnlySpan<byte> source, RespServerSession respServerSession = null, ILogger logger = null)
            : this(Encoding.UTF8.GetString(source), respServerSession, logger)
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
        }

        /// <summary>
        /// Entry point for redis.call method from a Lua script
        /// </summary>
        /// <param name="args">Parameters</param>
        /// <returns></returns>
        public object garnet_call(string cmd, params object[] args)
        {
            return respServerSession.ProcessCommandFromScripting(state, cmd, args);
        }

        /// <summary>
        /// Runs the precompiled Lua function
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="argv"></param>
        /// <returns></returns>
        public object Run(string[] keys, string[] argv)
        {
            sandbox_env["KEYS"] = keys;
            sandbox_env["ARGV"] = argv;
            return function.Call()[0];
        }
    }
}