// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using KeraLua;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
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

            try
            {
                if (!state.CheckStack(2))
                {
                    throw new GarnetException("Insufficient stack space to compile function");
                }

                Debug.Assert(state.GetTop() == 0, "Stack should be empty before compilation");

                state.PushNumber(loadSandboxedRegistryIndex);
                state.GetTable(LuaRegistry.Index);
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
                Debug.Assert(state.GetTop() == 0, "Stack should be empty after compilation");
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
                            var args = ArrayPool<string>.Shared.Rent(argCount);

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
                                        args[i] = null;
                                    }
                                    else if (argType == LuaType.String)
                                    {
                                        args[i] = state.CheckString(top);
                                    }
                                    else if (argType == LuaType.Number)
                                    {
                                        var asNum = state.CheckNumber(top);
                                        args[i] = ((long)asNum).ToString();
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

                                var request = scratchBufferManager.FormatCommandAsResp(cmd, args.AsSpan()[..argCount]);
                                _ = respServerSession.TryConsumeMessages(request.ptr, request.length);
                                var response = scratchBufferNetworkSender.GetResponse();
                                var result = ProcessResponse(response.ptr, response.length);
                                scratchBufferNetworkSender.Reset();
                                return result;
                            }
                            finally
                            {
                                ArrayPool<string>.Shared.Return(args);
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
                        state.PushString(resultStr);
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
                                state.PushNumber(i);

                                if (item == null)
                                {
                                    state.PushNil();
                                }
                                else
                                {
                                    state.PushString(item);
                                }

                                state.RawSet(1);
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
                    state.PushString(parseState.GetString(offset));
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
            if (keyLength > nKeys)
            {
                var keyResetRes = state.DoString($"count = #KEYS for i={nKeys + 1}, {keyLength} do KEYS[i]=nil end");

                if (keyResetRes)
                {
                    throw new GarnetException("Couldn't reset KEYS to run script");
                }
            }

            keyLength = nKeys;

            if (argvLength > nArgs)
            {
                var argvResetRes = state.DoString($"count = #ARGV for i={nArgs + 1}, {argvLength} do ARGV[i]=nil end");

                if (argvResetRes)
                {
                    throw new GarnetException("Couldn't reset ARGV to run script");
                }
            }

            argvLength = nArgs;
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
                for (int i = 0; i < keys.Length; i++)
                {
                    // equivalent to KEYS[i+1] = keys[i]
                    state.PushNumber(i + 1);
                    state.PushString(keys[i]);
                    state.SetTable(keysTableRegistryIndex);
                }
            }
            if (argv != null)
            {
                for (int i = 0; i < argv.Length; i++)
                {
                    // equivalent to ARGV[i+1] = keys[i]
                    state.PushNumber(i + 1);
                    state.PushString(argv[i]);
                    state.SetTable(argvTableRegistryIndex);
                }
            }
        }

        /// <summary>
        /// Runs the precompiled Lua function
        /// </summary>
        object Run()
        {
            // todo: this shouldn't read the result, it should write the response out

            Debug.Assert(state.GetTop() == 0, "Stack should be empty at start of invocation");

            if (!state.CheckStack(1))
            {
                throw new GarnetException("Insufficient stack space to run function");
            }

            try
            {
                state.PushNumber(functionRegistryIndex);
                state.GetTable(LuaRegistry.Index);
                state.Call(0, 1);

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
                    return state.CheckNumber(1);
                }
                else if (retType == LuaType.String)
                {
                    return state.CheckString(1);
                }
                else
                {
                    // todo: implement
                    throw new NotImplementedException();
                }
            }
            finally
            {
                // FORCE the stack to be empty now
                state.SetTop(0);
            }
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