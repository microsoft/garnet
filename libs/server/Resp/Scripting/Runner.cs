// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Text;
using NLua;

namespace Garnet.server
{
    /// <summary>
    /// Creates the instance to run Lua scripts
    /// </summary>
    public class Runner
    {
        readonly ScriptApi _garnetApi;
        readonly Lua _luaBox;
        LuaFunction _luaFunction;
        private string _source;

        /// <summary>
        /// This constructor avoids the instantiation of the ScriptApi
        /// </summary>
        public Runner()
        {
            _luaBox = new Lua();
            _luaBox.State.Encoding = Encoding.UTF8;
            _luaBox.LoadCLRPackage();
        }

        /// <summary>
        /// Full initialization of the ScriptApi and Lua machine
        /// </summary>
        /// <param name="api"></param>
        public Runner(ScriptApi api) : this()
        {
            _garnetApi = api;
        }


        /// <summary>
        /// Creates a new runner with the source of the script
        /// </summary>
        /// <param name="garnetApi"></param>
        /// <param name="source"></param>
        public Runner(ScriptApi garnetApi, byte[] source) : this(garnetApi)
        {
            _source = Encoding.ASCII.GetString(source);
        }

        /// <summary>
        /// Creates the Lua Sandbox including the keys and args array values
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        private void CreateSandbox((ArgSlice, bool)[] keys, String[] args)
        {
            _luaBox["KEYS"] = keys;
            _luaBox["ARGV"] = args;
            _luaBox["garnetApi"] = _garnetApi;
            // load (ld [, source [, mode [, env]]])
            _luaBox.DoString(@"
                luanet.load_assembly('Garnet.server');
                GarnetAPI=luanet.import_type('Garnet.server.Garnet')
                local GarnetClass = {}
                GarnetClass.new = function()
                        local self = {}
                        function self.call(cmd, key, ...)
                            return garnetApi:call(cmd, key, ...)
                        end
                    return self
                end
                garnet = GarnetClass.new();

                local sandbox_env = {
                    tostring = tostring;
                    print = print;
                    KEYS = KEYS;
                    ARGV = ARGV;
                    GarnetType = GarnetType;
                    garnet=garnet;
                    garnetApi=garnetApi;
                    socket=socket;
                }               
                function load_sandboxed(source)
                    if (not source) then return nil end
                    return load(source, nil, nil , sandbox_env)
                end
                function run_sandboxed(source)
                    if (not source) then return nil end
                    sb_code, result_msg = load(source, nil, nil, sandbox_env)
                    return pcall(sb_code)
                end

                function execute_fc(sb_code, keys, args)
                    sandbox_env['KEYS'] = keys;
                    sandbox_env['ARGV'] = args;
                    return pcall(sb_code);
                end
            ");
        }

        /// <summary>
        /// Compiles the lua code inside the sandbox
        /// </summary>
        /// <param name="source"></param>
        /// <param name="errors"></param>
        /// <returns></returns>
        public int LoadScript(byte[] source, out string errors)
        {
            var error = 0;
            CreateSandbox(default, default);
            object[] res;
            errors = String.Empty;

            try
            {
                //overrides the import function to avoid any .net import
                //source: https://github.com/NLua/NLua#readme
                _luaBox.DoString(@"
                      import = function () end
                ");

                using var func = (LuaFunction)_luaBox["load_sandboxed"];
                res = func?.Call(Encoding.ASCII.GetString(source));

                if (res?.Length == 2)
                {
                    errors = $"Compilation error: {(string)res[1]}";
                    error = 1;
                }
                else if (res?.Length == 1)
                {
                    error = (_luaFunction = res[0] as LuaFunction) != null ? 0 : 1;
                }
            }
            catch (Exception ex)
            {
                //TODO: should we send ex.message to the log?
                Debug.Print(ex.Message);
            }
            return error;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        private (bool success, object returnValue) RunScriptWithSource((ArgSlice, bool)[] keys, String[] args)
        {
            object result = default;
            bool success = false;
            //update/create the sandbox 
            CreateSandbox(keys, args);

            //overrides the import function to avoid any .net import
            //source: https://github.com/NLua/NLua#readme
            _luaBox.DoString(@"
		            import = function () end
	            ");
            try
            {
                //TODO: add the error handlers in lua to customize the error messages
                using var func = (LuaFunction)_luaBox["run_sandboxed"];
                var res = func?.Call(_source);

                if (res != null)
                {
                    if (res.Length == 2)
                    {
                        success = (bool)res[0];
                        result = success ? res[1] : $"Compilation error: {res[1]}";
                    }
                    else
                    {
                        result = !res.ToString().Contains("err", StringComparison.OrdinalIgnoreCase)
                            ? res[0].ToString()
                            : (object)$"Compilation error: {res[0]}";
                    }
                }
                else
                {
                    // TODO: check if this is right or return null
                    result = string.Empty;
                }
            }
            catch (Exception ex)
            {
                //TODO: check if we need to log this error
                Debug.Print(ex.Message);
            }
            return (success, result);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="keys"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        public (bool success, object returnValue) RunScript(string source, (ArgSlice, bool)[] keys, String[] args)
        {
            _source = source;
            return RunScriptWithSource(keys, args);
        }


        /// <summary>
        /// Runs the precompiled luafunction
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        public object RunScript((ArgSlice, bool)[] keys, String[] args)
        {
            if (_source != default)
            {
                return RunScriptWithSource(keys, args).returnValue;
            }

            var cLua = (LuaFunction)_luaBox["execute_fc"];
            return cLua?.Call(_luaFunction, keys, args);
        }
    }
}
