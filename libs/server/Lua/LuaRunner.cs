// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using KeraLua;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    // hack hack hack
    internal sealed record ErrorResult(string Message);

    /// <summary>
    /// Creates the instance to run Lua scripts
    /// </summary>
    internal sealed class LuaRunner : IDisposable
    {
        // rooted to keep function pointer alive
        readonly LuaFunction garnetCall;

        // references into Registry on the Lua side
        readonly int sandboxEnvRegistryIndex;
        readonly int keysTableRegistryIndex;
        readonly int argvTableRegistryIndex;
        readonly int loadSandboxedRegistryIndex;
        int functionRegistryIndex;

        readonly string source;
        readonly ScratchBufferNetworkSender scratchBufferNetworkSender;
        readonly RespServerSession respServerSession;
        readonly ScratchBufferManager scratchBufferManager;
        readonly ILogger logger;
        readonly Lua state;
        readonly TxnKeyEntries txnKeyEntries;
        readonly bool txnMode;

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

            sandboxEnvRegistryIndex = -1;
            keysTableRegistryIndex = -1;
            argvTableRegistryIndex = -1;
            loadSandboxedRegistryIndex = -1;
            functionRegistryIndex = -1;

            // todo: custom allocator?
            state = new Lua();
            Debug.Assert(state.GetTop() == 0, "Stack should be empty at allocation");

            if (txnMode)
            {
                this.txnKeyEntries = new TxnKeyEntries(16, respServerSession.storageSession.lockableContext, respServerSession.storageSession.objectStoreLockableContext);

                garnetCall = garnet_call_txn;
            }
            else
            {
                garnetCall = garnet_call;
            }

            var sandboxRes = state.DoString(@"
                import = function () end
                redis = {}
                function redis.call(cmd, ...)
                    return garnet_call(cmd, ...)
                end
                function redis.status_reply(text)
                    return text
                end
                function redis.error_reply(text)
                    return { err = 'ERR ' .. text }
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
                    local rawFunc, err = load(source, nil, nil, sandbox_env)

                    -- compilation error is returned directly
                    if err then
                        return rawFunc, err
                    end

                    -- otherwise we wrap the compiled function in a helper
                    return function()
                        local rawRet = rawFunc()

                        -- handle ok response wrappers without crossing the pinvoke boundary
                        -- err response wrapper requires a bit more work, but is also rarer
                        if rawRet and type(rawRet) == ""table"" and rawRet.ok then
                            return rawRet.ok
                        end

                        return rawRet
                    end
                end
            ");
            if (sandboxRes)
            {
                throw new GarnetException("Could not initialize Lua sandbox state");
            }

            // register garnet_call in global namespace
            state.Register("garnet_call", garnetCall);

            var sandboxEnvType = state.GetGlobal("sandbox_env");
            Debug.Assert(sandboxEnvType == LuaType.Table, "Unexpected sandbox_env type");
            sandboxEnvRegistryIndex = state.Ref(LuaRegistry.Index);

            var keyTableType = state.GetGlobal("KEYS");
            Debug.Assert(keyTableType == LuaType.Table, "Unexpected KEYS type");
            keysTableRegistryIndex = state.Ref(LuaRegistry.Index);

            var argvTableType = state.GetGlobal("ARGV");
            Debug.Assert(argvTableType == LuaType.Table, "Unexpected ARGV type");
            argvTableRegistryIndex = state.Ref(LuaRegistry.Index);

            var loadSandboxedType = state.GetGlobal("load_sandboxed");
            Debug.Assert(loadSandboxedType == LuaType.Function, "Unexpected load_sandboxed type");
            loadSandboxedRegistryIndex = state.Ref(LuaRegistry.Index);

            Debug.Assert(state.GetTop() == 0, "Stack should be empty after initialization");
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
            Debug.Assert(functionRegistryIndex == -1, "Shouldn't compile multiple times");

            Debug.Assert(state.GetTop() == 0, "Stack should be empty at start of compilation");

            try
            {
                if (!state.CheckStack(2))
                {
                    throw new GarnetException("Insufficient stack space to compile function");
                }

                state.PushNumber(loadSandboxedRegistryIndex);
                var loadRes = state.GetTable(LuaRegistry.Index);
                Debug.Assert(loadRes == LuaType.Function, "Unexpected load_sandboxed type");

                state.PushString(source);
                state.Call(1, -1);  // multiple returns allowed

                var numRets = state.GetTop();
                if (numRets == 0)
                {
                    throw new GarnetException("Shouldn't happen, no returns from load_sandboxed");
                }
                else if (numRets == 1)
                {
                    var returnType = state.Type(1);
                    if (returnType != LuaType.Function)
                    {
                        throw new GarnetException($"Could not compile function, got back a {returnType}");
                    }

                    functionRegistryIndex = state.Ref(LuaRegistry.Index);
                }
                else if (numRets == 2)
                {
                    var error = state.CheckString(2);

                    throw new GarnetException($"Compilation error: {error}");

                }
                else
                {
                    throw new GarnetException($"Unexpected error compiling, got too many replies back: reply count = {numRets}");
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "CreateFunction threw an exception");
                throw;
            }
            finally
            {
                // force stack empty after compilation, no matter what happens
                state.SetTop(0);
            }
        }

        /// <summary>
        /// Dispose the runner
        /// </summary>
        public void Dispose()
        {
            state?.Dispose();
        }

        /// <summary>
        /// Entry point for redis.call method from a Lua script (non-transactional mode)
        /// </summary>
        public int garnet_call(IntPtr luaStatePtr)
        {
            Debug.Assert(state.Handle == luaStatePtr, "Unexpected state provided in call");

            if (respServerSession == null)
            {
                return NoSessionError();
            }

            return ProcessCommandFromScripting(respServerSession.basicGarnetApi);
        }

        /// <summary>
        /// Entry point for redis.call method from a Lua script (transactional mode)
        /// </summary>
        public int garnet_call_txn(IntPtr luaStatePtr)
        {
            Debug.Assert(state.Handle == luaStatePtr, "Unexpected state provided in call");

            if (respServerSession == null)
            {
                return NoSessionError();
            }

            return ProcessCommandFromScripting(respServerSession.lockableGarnetApi);
        }

        /// <summary>
        /// Call somehow came in with no valid resp server session.
        /// 
        /// Raise an error.
        /// </summary>
        /// <returns></returns>
        int NoSessionError()
        {
            logger?.LogError("Lua call came in without a valid resp session");

            state.PushString("No session available");

            // this will never return, but we can pretend it does
            return state.Error();
        }

        /// <summary>
        /// Entry point method for executing commands from a Lua Script
        /// </summary>
        unsafe int ProcessCommandFromScripting<TGarnetApi>(TGarnetApi api)
            where TGarnetApi : IGarnetApi
        {
            try
            {
                var argCount = state.GetTop();

                if (argCount == 0)
                {
                    return state.Error("Please specify at least one argument for this redis lib call script");
                }

                // todo: no alloc
                var cmd = state.CheckString(1).ToUpperInvariant();

                switch (cmd)
                {
                    // We special-case a few performance-sensitive operations to directly invoke via the storage API
                    case "SET" when argCount == 3:
                        {
                            if (!respServerSession.CheckACLPermissions(RespCommand.SET))
                            {
                                // todo: no alloc
                                return state.Error(Encoding.UTF8.GetString(CmdStrings.RESP_ERR_NOAUTH));
                            }

                            // todo: no alloc
                            var keyBuf = state.CheckBuffer(2);
                            var valBuf = state.CheckBuffer(3);

                            if (keyBuf == null || valBuf == null)
                            {
                                return ErrorInvalidArgumentType(state);
                            }

                            var key = scratchBufferManager.CreateArgSlice(keyBuf);
                            var value = scratchBufferManager.CreateArgSlice(valBuf);
                            _ = api.SET(key, value);

                            state.PushString("OK");
                            return 1;
                        }
                    case "GET" when argCount == 2:
                        {
                            if (!respServerSession.CheckACLPermissions(RespCommand.GET))
                            {
                                // todo: no alloc
                                return state.Error(Encoding.UTF8.GetString(CmdStrings.RESP_ERR_NOAUTH));
                            }

                            // todo: no alloc
                            var keyBuf = state.CheckBuffer(2);

                            if (keyBuf == null)
                            {
                                return ErrorInvalidArgumentType(state);
                            }

                            var key = scratchBufferManager.CreateArgSlice(keyBuf);
                            var status = api.GET(key, out var value);
                            if (status == GarnetStatus.OK)
                            {
                                // todo: no alloc
                                state.PushBuffer(value.ToArray());
                            }
                            else
                            {
                                state.PushNil();
                            }

                            return 1;
                        }

                    // As fallback, we use RespServerSession with a RESP-formatted input. This could be optimized
                    // in future to provide parse state directly.
                    default:
                        {
                            // todo: remove all these allocations
                            var stackArgs = ArrayPool<string>.Shared.Rent(argCount);

                            try
                            {
                                var top = state.GetTop();

                                // move backwards validating arguments
                                // and removing them from the stack
                                for (var i = argCount - 1; i >= 0; i--)
                                {
                                    var argType = state.Type(top);
                                    if (argType == LuaType.Nil)
                                    {
                                        stackArgs[i] = null;
                                    }
                                    else if (argType == LuaType.String)
                                    {
                                        stackArgs[i] = state.CheckString(top);
                                    }
                                    else if (argType == LuaType.Number)
                                    {
                                        var asNum = state.CheckNumber(top);
                                        stackArgs[i] = ((long)asNum).ToString();
                                    }
                                    else
                                    {
                                        state.Pop(1);

                                        return ErrorInvalidArgumentType(state);
                                    }

                                    state.Pop(1);
                                    top--;
                                }

                                Debug.Assert(state.GetTop() == 0, "Should have emptied the stack");

                                // command is handled specially, so trim it off
                                var cmdArgs = stackArgs.AsSpan().Slice(1, argCount - 1);

                                var request = scratchBufferManager.FormatCommandAsResp(cmd, cmdArgs);
                                _ = respServerSession.TryConsumeMessages(request.ptr, request.length);
                                var response = scratchBufferNetworkSender.GetResponse();
                                var result = ProcessResponse(response.ptr, response.length);
                                scratchBufferNetworkSender.Reset();
                                return result;
                            }
                            finally
                            {
                                ArrayPool<string>.Shared.Return(stackArgs);
                            }
                        }
                }
            }
            catch (Exception e)
            {
                logger?.LogError(e, "During Lua script execution");

                state.PushString(e.Message);
                return state.Error();
            }

            static int ErrorInvalidArgumentType(Lua state)
            {
                state.PushString("Lua redis lib command arguments must be strings or integers");
                return state.Error();
            }
        }

        /// <summary>
        /// Process a RESP-formatted response from the RespServerSession.
        /// 
        /// Pushes result onto state stack, and returns 1
        /// </summary>
        unsafe int ProcessResponse(byte* ptr, int length)
        {
            Debug.Assert(state.GetTop() == 0, "Stack should be empty before processing response");

            if (!state.CheckStack(3))
            {
                throw new GarnetException("Insufficent space on stack to prepare response");
            }

            switch (*ptr)
            {
                case (byte)'+':
                    // todo: remove alloc
                    if (RespReadUtils.ReadSimpleString(out var resultStr, ref ptr, ptr + length))
                    {
                        state.PushString(resultStr);
                        return 1;
                    }
                    goto default;

                case (byte)':':
                    if (RespReadUtils.Read64Int(out var number, ref ptr, ptr + length))
                    {
                        state.PushNumber(number);
                        return 1;
                    }
                    goto default;

                case (byte)'-':
                    // todo: remove alloc
                    if (RespReadUtils.ReadErrorAsString(out resultStr, ref ptr, ptr + length))
                    {
                        state.PushString(resultStr);
                        return state.Error();
                    }
                    goto default;

                case (byte)'$':
                    // todo: remove alloc
                    if (RespReadUtils.ReadStringResponseWithLengthHeader(out resultStr, ref ptr, ptr + length))
                    {
                        // bulk null strings are mapped to FALSE
                        // see: https://redis.io/docs/latest/develop/interact/programmability/lua-api/#lua-to-resp2-type-conversion
                        if (resultStr == null)
                        {
                            state.PushBoolean(false);
                        }
                        else
                        {
                            state.PushString(resultStr);
                        }
                        return 1;
                    }
                    goto default;

                case (byte)'*':
                    // todo: remove allocs
                    if (RespReadUtils.ReadRentedStringArrayResponseWithLengthHeader(ArrayPool<string>.Shared, out var resultArray, ref ptr, ptr + length))
                    {
                        try
                        {
                            // create the new table
                            state.NewTable();
                            Debug.Assert(state.GetTop() == 1, "New table should be at top of stack");

                            // Populate the table
                            var i = 1;
                            foreach (var item in resultArray.Span)
                            {
                                // null strings are mapped to false
                                // see: https://redis.io/docs/latest/develop/interact/programmability/lua-api/#lua-to-resp2-type-conversion
                                if (item == null)
                                {
                                    state.PushBoolean(false);
                                }
                                else
                                {
                                    state.PushString(item);
                                }

                                state.RawSetInteger(1, i);

                                i++;
                            }

                            return 1;
                        }
                        finally
                        {
                            if (!resultArray.IsEmpty)
                            {
                                if (MemoryMarshal.TryGetArray(resultArray, out ArraySegment<string> rented))
                                {
                                    ArrayPool<string>.Shared.Return(rented.Array);
                                }
                            }
                        }
                    }
                    goto default;

                default:
                    throw new Exception("Unexpected response: " + Encoding.UTF8.GetString(new Span<byte>(ptr, length)).Replace("\n", "|").Replace("\r", "") + "]");
            }
        }

        /// <summary>
        /// Runs the precompiled Lua function with specified parse state
        /// </summary>
        public object Run(int count, SessionParseState parseState)
        {
            Debug.Assert(state.GetTop() == 0, "Stack should be empty at invocation start");

            if (!state.CheckStack(3))
            {
                throw new GarnetException("Insufficient stack space to run script");
            }

            scratchBufferManager.Reset();

            int offset = 1;
            int nKeys = parseState.GetInt(offset++);
            count--;
            ResetParameters(nKeys, count - nKeys);

            if (nKeys > 0)
            {
                // get KEYS on the stack
                state.PushNumber(keysTableRegistryIndex);
                var loadedType = state.RawGet(LuaRegistry.Index);
                Debug.Assert(loadedType == LuaType.Table, "Unexpected type loaded when expecting KEYS");

                for (var i = 0; i < nKeys; i++)
                {
                    ref var key = ref parseState.GetArgSliceByRef(offset);

                    if (txnMode)
                    {
                        txnKeyEntries.AddKey(key, false, Tsavorite.core.LockType.Exclusive);
                        if (!respServerSession.storageSession.objectStoreLockableContext.IsNull)
                            txnKeyEntries.AddKey(key, true, Tsavorite.core.LockType.Exclusive);
                    }

                    // todo: no alloc
                    // todo: encoding is wrong here

                    // equivalent to KEYS[i+1] = key.ToString()
                    state.PushNumber(i + 1);
                    state.PushString(key.ToString());
                    state.RawSet(1);

                    offset++;
                }

                state.Pop(1);

                count -= nKeys;
            }

            if (count > 0)
            {
                // GET ARGV on the stack
                state.PushNumber(argvTableRegistryIndex);
                var loadedType = state.RawGet(LuaRegistry.Index);
                Debug.Assert(loadedType == LuaType.Table, "Unexpected type loaded when expecting ARGV");

                for (var i = 0; i < count; i++)
                {
                    // todo: no alloc
                    // todo encoding is wrong here

                    // equivalent to ARGV[i+1] = parseState.GetString(offset);
                    state.PushNumber(i + 1);
                    state.PushString(parseState.GetString(offset));
                    state.RawSet(1);

                    offset++;
                }

                state.Pop(1);
            }

            Debug.Assert(state.GetTop() == 0, "Stack should be empty before running function");

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
            Debug.Assert(state.GetTop() == 0, "Stack should be empty before resetting parameters");

            if (!state.CheckStack(2))
            {
                throw new GarnetException("Insufficient space on stack to reset parameters");
            }

            if (keyLength > nKeys)
            {
                // get KEYS on the stack
                state.PushNumber(keysTableRegistryIndex);
                var loadRes = state.GetTable(LuaRegistry.Index);
                Debug.Assert(loadRes == LuaType.Table, "Unexpected type for KEYS");

                // clear all the values in KEYS that we aren't going to set anyway
                for (var i = nKeys + 1; i <= keyLength; i++)
                {
                    state.PushNil();
                    state.RawSetInteger(1, i);
                }

                state.Pop(1);
            }

            keyLength = nKeys;

            if (argvLength > nArgs)
            {
                // get ARGV on the stack
                state.PushNumber(argvTableRegistryIndex);
                var loadRes = state.GetTable(LuaRegistry.Index);
                Debug.Assert(loadRes == LuaType.Table, "Unexpected type for ARGV");

                for (var i = nArgs + 1; i <= argvLength; i++)
                {
                    state.PushNil();
                    state.RawSetInteger(1, i);
                }

                state.Pop(1);
            }

            argvLength = nArgs;

            Debug.Assert(state.GetTop() == 0, "Stack should be empty after resetting parameters");
        }

        void LoadParameters(string[] keys, string[] argv)
        {
            Debug.Assert(state.GetTop() == 0, "Stack should be empty before invocation starts");

            if (!state.CheckStack(2))
            {
                throw new GarnetException("Insufficient stack space to call function");
            }

            ResetParameters(keys?.Length ?? 0, argv?.Length ?? 0);
            if (keys != null)
            {
                // get KEYS on the stack
                state.PushNumber(keysTableRegistryIndex);
                var loadRes = state.GetTable(LuaRegistry.Index);
                Debug.Assert(loadRes == LuaType.Table, "Unexpected type for KEYS");

                for (var i = 0; i < keys.Length; i++)
                {
                    // equivalent to KEYS[i+1] = keys[i]
                    state.PushString(keys[i]);
                    state.RawSetInteger(1, i + 1);
                }

                state.Pop(1);
            }

            if (argv != null)
            {
                // get ARGV on the stack
                state.PushNumber(argvTableRegistryIndex);
                var loadRes = state.GetTable(LuaRegistry.Index);
                Debug.Assert(loadRes == LuaType.Table, "Unexpected type for ARGV");

                for (var i = 0; i < argv.Length; i++)
                {
                    // equivalent to ARGV[i+1] = keys[i]
                    state.PushString(argv[i]);
                    state.RawSetInteger(1, i + 1);
                }

                state.Pop(1);
            }

            Debug.Assert(state.GetTop() == 0, "Stack should be empty when invocation ends");
        }

        /// <summary>
        /// Runs the precompiled Lua function
        /// </summary>
        object Run()
        {
            // todo: mapping is dependent on Resp2 vs Resp3 settings
            //       and that's not implemented at all

            // todo: this shouldn't read the result, it should write the response out
            Debug.Assert(state.GetTop() == 0, "Stack should be empty at start of invocation");

            if (!state.CheckStack(2))
            {
                throw new GarnetException("Insufficient stack space to run function");
            }

            try
            {
                state.PushNumber(functionRegistryIndex);
                var loadRes = state.GetTable(LuaRegistry.Index);
                Debug.Assert(loadRes == LuaType.Function, "Unexpected type for function to invoke");

                var callRes = state.PCall(0, 1, 0);
                if (callRes == LuaStatus.OK)
                {
                    // the actual call worked, handle the response

                    if (state.GetTop() == 0)
                    {
                        return null;
                    }

                    var retType = state.Type(1);
                    if (retType == LuaType.Nil)
                    {
                        return null;
                    }
                    else if (retType == LuaType.Number)
                    {
                        // Redis unconditionally converts all "number" replies to integer replies
                        // so we match that
                        // see: https://redis.io/docs/latest/develop/interact/programmability/lua-api/#lua-to-resp2-type-conversion
                        return (long)state.CheckNumber(1);
                    }
                    else if (retType == LuaType.String)
                    {
                        return state.CheckString(1);
                    }
                    else if (retType == LuaType.Boolean)
                    {
                        // Redis maps Lua false to null, and Lua true to 1
                        // this is strange, but documented
                        // see: https://redis.io/docs/latest/develop/interact/programmability/lua-api/#lua-to-resp2-type-conversion
                        if (state.ToBoolean(1))
                        {
                            return 1L;
                        }
                        else
                        {
                            return null;
                        }
                    }
                    else if (retType == LuaType.Table)
                    {
                        // todo: this is hacky, and doesn't support nested arrays or whatever
                        //       but is good enough for now
                        //       when refactored to avoid intermediate objects this should be fixed

                        // note: because we are dealing with a user provided type, we MUST respect
                        //       metatables - so we can't use any of the RawXXX methods

                        // if the key err is in there, we need to short circuit 
                        state.PushString("err");

                        var errType = state.GetTable(1);
                        if (errType == LuaType.String)
                        {
                            var errStr = state.CheckString(2);
                            // hack hack hack
                            // todo: all this goes away when we write results directly
                            return new ErrorResult(errStr);
                        }

                        state.Pop(1);

                        // otherwise, we need to convert the table to an array
                        var tableLength = state.Length(1);

                        var ret = new object[tableLength];
                        for (var i = 1; i <= tableLength; i++)
                        {
                            var type = state.GetInteger(1, i);
                            switch (type)
                            {
                                case LuaType.String:
                                    ret[i - 1] = state.CheckString(2);
                                    break;
                                case LuaType.Number:
                                    ret[i - 1] = (long)state.CheckNumber(2);
                                    break;
                                case LuaType.Boolean:
                                    ret[i - 1] = state.ToBoolean(2) ? 1L : null;
                                    break;
                                // Redis stops processesing the array when a nil is encountered
                                // see: https://redis.io/docs/latest/develop/interact/programmability/lua-api/#lua-to-resp2-type-conversion
                                case LuaType.Nil:
                                    return ret.Take(i - 1).ToArray();
                            }

                            state.Pop(1);
                        }

                        return ret;
                    }
                    else
                    {
                        // todo: implement
                        throw new NotImplementedException();
                    }
                }
                else
                {
                    // an error was raised

                    var stackTop = state.GetTop();
                    if (stackTop == 0)
                    {
                        // and we got nothing back
                        throw new GarnetException("An error occurred while invoking a Lua script");
                    }

                    // todo: we should just write this out, not throw
                    //       it's not exceptional
                    var msg = state.CheckString(stackTop);
                    throw new GarnetException(msg);
                }
            }
            finally
            {
                // FORCE the stack to be empty now
                state.SetTop(0);
            }
        }
    }
}