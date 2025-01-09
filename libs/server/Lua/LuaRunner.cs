// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
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
        /// <summary>
        /// Adapter to allow us to write directly to the network
        /// when in Garnet and still keep script runner work.
        /// </summary>
        private unsafe interface IResponseAdapter
        {
            /// <summary>
            /// Equivalent to the ref curr we pass into <see cref="RespWriteUtils"/> methods.
            /// </summary>
            ref byte* BufferCur { get; }

            /// <summary>
            /// Equivalent to the end we pass into <see cref="RespWriteUtils"/> methods.
            /// </summary>
            byte* BufferEnd { get; }

            /// <summary>
            /// Equivalent to <see cref="RespServerSession.SendAndReset()"/>.
            /// </summary>
            void SendAndReset();
        }

        /// <summary>
        /// Adapter <see cref="RespServerSession"/> so script results go directly
        /// to the network.
        /// </summary>
        private readonly struct RespResponseAdapter : IResponseAdapter
        {
            private readonly RespServerSession session;

            internal RespResponseAdapter(RespServerSession session)
            {
                this.session = session;
            }

            /// <inheritdoc />
            public unsafe ref byte* BufferCur
            => ref session.dcurr;

            /// <inheritdoc />
            public unsafe byte* BufferEnd
            => session.dend;

            /// <inheritdoc />
            public void SendAndReset()
            => session.SendAndReset();
        }

        /// <summary>
        /// For the runner, put output into an array.
        /// </summary>
        private unsafe struct RunnerAdapter : IResponseAdapter
        {
            private readonly ScratchBufferManager bufferManager;
            private byte* origin;
            private byte* curHead;

            internal RunnerAdapter(ScratchBufferManager bufferManager)
            {
                this.bufferManager = bufferManager;
                this.bufferManager.Reset();

                var scratchSpace = bufferManager.FullBuffer();

                origin = curHead = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(scratchSpace));
                BufferEnd = curHead + scratchSpace.Length;
            }

#pragma warning disable CS9084 // Struct member returns 'this' or other instance members by reference
            /// <inheritdoc />
            public unsafe ref byte* BufferCur
            => ref curHead;
#pragma warning restore CS9084

            /// <inheritdoc />
            public unsafe byte* BufferEnd { get; private set; }

            /// <summary>
            /// Gets a span that covers the responses as written so far.
            /// </summary>
            public readonly ReadOnlySpan<byte> Response
            {
                get
                {
                    var len = (int)(curHead - origin);

                    var full = bufferManager.FullBuffer();

                    return full[..len];
                }
            }

            /// <inheritdoc />
            public void SendAndReset()
            {
                var len = (int)(curHead - origin);

                // We don't actually send anywhere, we grow the backing array
                //
                // Since we're managing the start/end pointers outside of the buffer
                // we need to signal that the buffer has data to copy
                bufferManager.GrowBuffer(copyLengthOverride: len);

                var scratchSpace = bufferManager.FullBuffer();

                origin = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(scratchSpace));
                BufferEnd = origin + scratchSpace.Length;
                curHead = origin + len;
            }
        }

        const string LoaderBlock = @"
import = function () end
KEYS = {}
ARGV = {}
sandbox_env = {
    _G = _G;
    _VERSION = _VERSION;

    assert = assert;
    collectgarbage = collectgarbage;
    coroutine = coroutine;
    error = error;
    gcinfo = gcinfo;
    -- explicitly not allowing getfenv
    getmetatable = getmetatable;
    ipairs = ipairs;
    load = load;
    loadstring = loadstring;
    math = math;
    next = next;
    pairs = pairs;
    pcall = pcall;
    rawequal = rawequal;
    rawget = rawget;
    rawset = rawset;
    select = select;
    -- explicitly not allowing setfenv
    string = string;
    setmetatable = setmetatable;
    table = table;
    tonumber = tonumber;
    tostring = tostring;
    type = type;
    unpack = table.unpack;
    xpcall = xpcall;

    KEYS = KEYS;
    ARGV = ARGV;
}
-- do resets in the Lua side to minimize pinvokes
function reset_keys_and_argv(fromKey, fromArgv)
    local keyRef = KEYS
    local keyCount = #keyRef
    for i = fromKey, keyCount do
        table.remove(keyRef)
    end

    local argvRef = ARGV
    local argvCount = #argvRef
    for i = fromArgv, argvCount do
        table.remove(argvRef)
    end
end
-- responsible for sandboxing user provided code
function load_sandboxed(source)
    -- move into a local to avoid global lookup
    local garnetCallRef = garnet_call

    sandbox_env.redis = {
        status_reply = function(text)
            return text
        end,

        error_reply = function(text)
            return { err = 'ERR ' .. text }
        end,

        call = function(...)
            return garnetCallRef(...)
        end
    }

    local rawFunc, err = load(source, nil, nil, sandbox_env)

    return err, rawFunc
end
";

        private static readonly ReadOnlyMemory<byte> LoaderBlockBytes = Encoding.UTF8.GetBytes(LoaderBlock);

        // References into Registry on the Lua side
        //
        // These are mix of objects we regularly update,
        // constants we want to avoid copying from .NET to Lua,
        // and the compiled function definition.
        readonly int keysTableRegistryIndex;
        readonly int argvTableRegistryIndex;
        readonly int loadSandboxedRegistryIndex;
        readonly int resetKeysAndArgvRegistryIndex;
        readonly int okConstStringRegistryIndex;
        readonly int okLowerConstStringRegistryIndex;
        readonly int errConstStringRegistryIndex;
        readonly int noSessionAvailableConstStringRegistryIndex;
        readonly int pleaseSpecifyRedisCallConstStringRegistryIndex;
        readonly int errNoAuthConstStringRegistryIndex;
        readonly int errUnknownConstStringRegistryIndex;
        readonly int errBadArgConstStringRegistryIndex;

        readonly ReadOnlyMemory<byte> source;
        readonly ScratchBufferNetworkSender scratchBufferNetworkSender;
        readonly RespServerSession respServerSession;

        readonly ScratchBufferManager scratchBufferManager;
        readonly ILogger logger;
        readonly TxnKeyEntries txnKeyEntries;
        readonly bool txnMode;

        // The Lua registry index under which the user supplied function is stored post-compilation
        int functionRegistryIndex;

        // This cannot be readonly, as it is a mutable struct
        LuaStateWrapper state;

        // We need to temporarily store these for P/Invoke reasons
        // You shouldn't be touching them outside of the Compile and Run methods

        RunnerAdapter runnerAdapter;
        RespResponseAdapter sessionAdapter;
        RespServerSession preambleOuterSession;
        int preambleKeyAndArgvCount;
        int preambleNKeys;
        string[] preambleKeys;
        string[] preambleArgv;

        int keyLength, argvLength;

        /// <summary>
        /// Creates a new runner with the source of the script
        /// </summary>
        public unsafe LuaRunner(LuaMemoryManagementMode memMode, int? memLimitBytes, ReadOnlyMemory<byte> source, bool txnMode = false, RespServerSession respServerSession = null, ScratchBufferNetworkSender scratchBufferNetworkSender = null, ILogger logger = null)
        {
            this.source = source;
            this.txnMode = txnMode;
            this.respServerSession = respServerSession;
            this.scratchBufferNetworkSender = scratchBufferNetworkSender;
            this.logger = logger;

            scratchBufferManager = respServerSession?.scratchBufferManager ?? new();

            keysTableRegistryIndex = -1;
            argvTableRegistryIndex = -1;
            loadSandboxedRegistryIndex = -1;
            functionRegistryIndex = -1;

            state = new LuaStateWrapper(memMode, memLimitBytes, this.logger);

            delegate* unmanaged[Cdecl]<nint, int> garnetCall;
            if (txnMode)
            {
                txnKeyEntries = new TxnKeyEntries(16, respServerSession.storageSession.lockableContext, respServerSession.storageSession.objectStoreLockableContext);

                garnetCall = &LuaRunnerTrampolines.GarnetCallWithTransaction;
            }
            else
            {
                garnetCall = &LuaRunnerTrampolines.GarnetCallNoTransaction;
            }

            if (respServerSession == null)
            {
                // During benchmarking and testing this can happen, so just redirect once instead of on each redis.call
                garnetCall = &LuaRunnerTrampolines.GarnetCallNoSession;
            }

            var loadRes = state.LoadBuffer(LoaderBlockBytes.Span);
            if (loadRes != LuaStatus.OK)
            {
                throw new GarnetException("Could load loader into Lua");
            }

            var sandboxRes = state.PCall(0, -1);
            if (sandboxRes != LuaStatus.OK)
            {
                string errMsg;
                try
                {
                    if (state.StackTop >= 1)
                    {
                        // We control the definition of LoaderBlock, so we know this is a string
                        state.KnownStringToBuffer(1, out var errSpan);
                        errMsg = Encoding.UTF8.GetString(errSpan);
                    }
                    else
                    {
                        errMsg = "No error provided";
                    }
                }
                catch
                {
                    errMsg = "Error when fetching pcall error";
                }

                throw new GarnetException($"Could not initialize Lua sandbox state: {errMsg}");
            }

            // Register garnet_call in global namespace
            state.Register("garnet_call\0"u8, garnetCall);

            state.GetGlobal(LuaType.Table, "KEYS\0"u8);
            keysTableRegistryIndex = state.Ref();

            state.GetGlobal(LuaType.Table, "ARGV\0"u8);
            argvTableRegistryIndex = state.Ref();

            state.GetGlobal(LuaType.Function, "load_sandboxed\0"u8);
            loadSandboxedRegistryIndex = state.Ref();

            state.GetGlobal(LuaType.Function, "reset_keys_and_argv\0"u8);
            resetKeysAndArgvRegistryIndex = state.Ref();

            // Commonly used strings, register them once so we don't have to copy them over each time we need them
            okConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.LUA_OK);
            okLowerConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.LUA_ok);
            errConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.LUA_err);
            noSessionAvailableConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.LUA_No_session_available);
            pleaseSpecifyRedisCallConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.LUA_ERR_Please_specify_at_least_one_argument_for_this_redis_lib_call);
            errNoAuthConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.RESP_ERR_NOAUTH);
            errUnknownConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.LUA_ERR_Unknown_Redis_command_called_from_script);
            errBadArgConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.LUA_ERR_Lua_redis_lib_command_arguments_must_be_strings_or_integers);

            state.ExpectLuaStackEmpty();
        }

        /// <summary>
        /// Creates a new runner with the source of the script
        /// </summary>
        public LuaRunner(LuaOptions options, string source, bool txnMode = false, RespServerSession respServerSession = null, ScratchBufferNetworkSender scratchBufferNetworkSender = null, ILogger logger = null)
            : this(options.MemoryManagementMode, options.GetMemoryLimitBytes(), Encoding.UTF8.GetBytes(source), txnMode, respServerSession, scratchBufferNetworkSender, logger)
        {
        }

        /// <summary>
        /// Some strings we use a bunch, and copying them to Lua each time is wasteful
        ///
        /// So instead we stash them in the Registry and load them by index
        /// </summary>
        private int ConstantStringToRegistry(ReadOnlySpan<byte> str)
        {
            state.PushBuffer(str);
            return state.Ref();
        }

        /// <summary>
        /// Compile script for running in a .NET host.
        /// 
        /// Errors are raised as exceptions.
        /// </summary>
        public unsafe void CompileForRunner()
        {
            runnerAdapter = new RunnerAdapter(scratchBufferManager);
            try
            {
                LuaRunnerTrampolines.SetCallbackContext(this);
                state.PushCFunction(&LuaRunnerTrampolines.CompileForRunner);
                var res = state.PCall(0, 0);
                if (res == LuaStatus.OK)
                {
                    var resp = runnerAdapter.Response;
                    var respStart = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(resp));
                    var respEnd = respStart + resp.Length;
                    if (RespReadUtils.TryReadErrorAsSpan(out var errSpan, ref respStart, respEnd))
                    {
                        var errStr = Encoding.UTF8.GetString(errSpan);
                        throw new GarnetException(errStr);
                    }
                }
                else
                {
                    throw new GarnetException($"Internal Lua Error: {res}");
                }
            }
            finally
            {
                runnerAdapter = default;
                LuaRunnerTrampolines.ClearCallbackContext(this);
            }
        }

        /// <summary>
        /// Actually compiles for runner.
        /// 
        /// If you call this directly and Lua encounters an error, the process will crash.
        /// 
        /// Call <see cref="CompileForRunner"/> instead.
        /// </summary>
        internal int UnsafeCompileForRunner()
        => CompileCommon(ref runnerAdapter);

        /// <summary>
        /// Compile script for a <see cref="RespServerSession"/>.
        /// 
        /// Any errors encountered are written out as Resp errors.
        /// </summary>
        public unsafe bool CompileForSession(RespServerSession session)
        {
            sessionAdapter = new RespResponseAdapter(session);

            try
            {
                LuaRunnerTrampolines.SetCallbackContext(this);
                state.PushCFunction(&LuaRunnerTrampolines.CompileForSession);
                var res = state.PCall(0, 0);
                if (res != LuaStatus.OK)
                {
                    while (!RespWriteUtils.WriteError("Internal Lua Error"u8, ref session.dcurr, session.dend))
                        session.SendAndReset();

                    return false;
                }

                return true;
            }
            finally
            {
                sessionAdapter = default;
                LuaRunnerTrampolines.ClearCallbackContext(this);
            }
        }

        /// <summary>
        /// Actually compiles for runner.
        /// 
        /// If you call this directly and Lua encounters an error, the process will crash.
        /// 
        /// Call <see cref="CompileForSession"/> instead.
        /// </summary>
        internal int UnsafeCompileForSession()
        => CompileCommon(ref sessionAdapter);

        /// <summary>
        /// Drops compiled function, just for benchmarking purposes.
        /// </summary>
        public void ResetCompilation()
        {
            if (functionRegistryIndex != -1)
            {
                state.Unref(LuaRegistry.Index, functionRegistryIndex);
                functionRegistryIndex = -1;
            }
        }

        /// <summary>
        /// Compile script, writing errors out to given response.
        /// </summary>
        private unsafe int CompileCommon<TResponse>(ref TResponse resp)
            where TResponse : struct, IResponseAdapter
        {
            const int NeededStackSpace = 2;

            Debug.Assert(functionRegistryIndex == -1, "Shouldn't compile multiple times");

            state.ExpectLuaStackEmpty();

            state.ForceMinimumStackCapacity(NeededStackSpace);

            _ = state.RawGetInteger(LuaType.Function, (int)LuaRegistry.Index, loadSandboxedRegistryIndex);
            state.PushBuffer(source.Span);
            state.Call(1, 2);

            // Now the stack will have two things on it:
            //  1. The error (nil if not error)
            //  2. The function (nil if error)

            if (state.Type(1) == LuaType.Nil)
            {
                // No error, success!

                Debug.Assert(state.Type(2) == LuaType.Function, "Unexpected type returned from load_sandboxed");

                functionRegistryIndex = state.Ref();
            }
            else
            {
                // We control the definition of load_sandboxed, so we know this will be a string
                state.KnownStringToBuffer(1, out var errorBuf);

                var errStr = $"Compilation error: {Encoding.UTF8.GetString(errorBuf)}";
                while (!RespWriteUtils.WriteError(errStr, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();
            }

            return 0;
        }

        /// <summary>
        /// Dispose the runner
        /// </summary>
        public void Dispose()
        => state.Dispose();

        /// <summary>
        /// Entry point for redis.call method from a Lua script (non-transactional mode)
        /// </summary>
        public int GarnetCall(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            return ProcessCommandFromScripting(ref respServerSession.basicGarnetApi);
        }

        /// <summary>
        /// Entry point for redis.call method from a Lua script (transactional mode)
        /// </summary>
        public int GarnetCallWithTransaction(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            return ProcessCommandFromScripting(ref respServerSession.lockableGarnetApi);
        }

        /// <summary>
        /// Call somehow came in with no valid resp server session.
        /// 
        /// This is used in benchmarking.
        /// </summary>
        internal int NoSessionResponse(nint luaStatePtr)
        {
            const int NeededStackSpace = 1;

            state.CallFromLuaEntered(luaStatePtr);

            state.ForceMinimumStackCapacity(NeededStackSpace);

            state.PushNil();
            return 1;
        }

        /// <summary>
        /// Entry point method for executing commands from a Lua Script
        /// </summary>
        unsafe int ProcessCommandFromScripting<TGarnetApi>(ref TGarnetApi api)
            where TGarnetApi : IGarnetApi
        {
            const int AdditionalStackSpace = 1;

            try
            {
                var argCount = state.StackTop;

                if (argCount <= 0)
                {
                    return LuaStaticError(pleaseSpecifyRedisCallConstStringRegistryIndex);
                }

                state.ForceMinimumStackCapacity(AdditionalStackSpace);

                if (!state.CheckBuffer(1, out var cmdSpan))
                {
                    return LuaStaticError(errBadArgConstStringRegistryIndex);
                }

                // We special-case a few performance-sensitive operations to directly invoke via the storage API
                if (AsciiUtils.EqualsUpperCaseSpanIgnoringCase(cmdSpan, "SET"u8) && argCount == 3)
                {
                    if (!respServerSession.CheckACLPermissions(RespCommand.SET))
                    {
                        return LuaStaticError(errNoAuthConstStringRegistryIndex);
                    }

                    if (!state.CheckBuffer(2, out var keySpan) || !state.CheckBuffer(3, out var valSpan))
                    {
                        return LuaStaticError(errBadArgConstStringRegistryIndex);
                    }

                    // Note these spans are implicitly pinned, as they're actually on the Lua stack
                    var key = ArgSlice.FromPinnedSpan(keySpan);
                    var value = ArgSlice.FromPinnedSpan(valSpan);

                    _ = api.SET(key, value);

                    state.PushConstantString(okConstStringRegistryIndex);
                    return 1;
                }
                else if (AsciiUtils.EqualsUpperCaseSpanIgnoringCase(cmdSpan, "GET"u8) && argCount == 2)
                {
                    if (!respServerSession.CheckACLPermissions(RespCommand.GET))
                    {
                        return LuaStaticError(errNoAuthConstStringRegistryIndex);
                    }

                    if (!state.CheckBuffer(2, out var keySpan))
                    {
                        return LuaStaticError(errBadArgConstStringRegistryIndex);
                    }

                    // Span is (implicitly) pinned since it's actually on the Lua stack
                    var key = ArgSlice.FromPinnedSpan(keySpan);
                    var status = api.GET(key, out var value);
                    if (status == GarnetStatus.OK)
                    {
                        state.PushBuffer(value.ReadOnlySpan);
                    }
                    else
                    {
                        state.PushNil();
                    }

                    return 1;
                }

                // As fallback, we use RespServerSession with a RESP-formatted input. This could be optimized
                // in future to provide parse state directly.

                scratchBufferManager.Reset();
                scratchBufferManager.StartCommand(cmdSpan, argCount - 1);

                for (var i = 0; i < argCount - 1; i++)
                {
                    var argIx = 2 + i;

                    var argType = state.Type(argIx);
                    if (argType == LuaType.Nil)
                    {
                        scratchBufferManager.WriteNullArgument();
                    }
                    else if (argType is LuaType.String or LuaType.Number)
                    {
                        // KnownStringToBuffer will coerce a number into a string
                        //
                        // Redis nominally converts numbers to integers, but in this case just ToStrings things
                        state.KnownStringToBuffer(argIx, out var span);

                        // Span remains pinned so long as we don't pop the stack
                        scratchBufferManager.WriteArgument(span);
                    }
                    else
                    {
                        return LuaStaticError(errBadArgConstStringRegistryIndex);
                    }
                }

                var request = scratchBufferManager.ViewFullArgSlice();

                // Once the request is formatted, we can release all the args on the Lua stack
                //
                // This keeps the stack size down for processing the response
                state.Pop(argCount);

                _ = respServerSession.TryConsumeMessages(request.ptr, request.length);

                var response = scratchBufferNetworkSender.GetResponse();
                var result = ProcessResponse(response.ptr, response.length);
                scratchBufferNetworkSender.Reset();
                return result;
            }
            catch (Exception e)
            {
                logger?.LogError(e, "During Lua script execution");

                return state.RaiseError(e.Message);
            }
        }

        /// <summary>
        /// Cause a Lua error to be raised with a message previously registered.
        /// </summary>
        private int LuaStaticError(int constStringRegistryIndex)
        {
            const int NeededStackSize = 1;

            state.ForceMinimumStackCapacity(NeededStackSize);

            state.PushConstantString(constStringRegistryIndex);
            return state.RaiseErrorFromStack();
        }

        /// <summary>
        /// Process a RESP-formatted response from the RespServerSession.
        /// 
        /// Pushes result onto state stack and returns 1, or raises an error and never returns.
        /// </summary>
        private unsafe int ProcessResponse(byte* ptr, int length)
        {
            const int NeededStackSize = 3;

            state.ForceMinimumStackCapacity(NeededStackSize);

            switch (*ptr)
            {
                case (byte)'+':
                    ptr++;
                    length--;
                    if (RespReadUtils.ReadAsSpan(out var resultSpan, ref ptr, ptr + length))
                    {
                        state.PushBuffer(resultSpan);
                        return 1;
                    }
                    goto default;

                case (byte)':':
                    if (RespReadUtils.Read64Int(out var number, ref ptr, ptr + length))
                    {
                        state.PushInteger(number);
                        return 1;
                    }
                    goto default;

                case (byte)'-':
                    ptr++;
                    length--;
                    if (RespReadUtils.ReadAsSpan(out var errSpan, ref ptr, ptr + length))
                    {
                        if (errSpan.SequenceEqual(CmdStrings.RESP_ERR_GENERIC_UNK_CMD))
                        {
                            // Gets a special response
                            return LuaStaticError(errUnknownConstStringRegistryIndex);
                        }

                        state.PushBuffer(errSpan);
                        return state.RaiseErrorFromStack();

                    }
                    goto default;

                case (byte)'$':
                    if (length >= 5 && new ReadOnlySpan<byte>(ptr + 1, 4).SequenceEqual("-1\r\n"u8))
                    {
                        // Bulk null strings are mapped to FALSE
                        // See: https://redis.io/docs/latest/develop/interact/programmability/lua-api/#lua-to-resp2-type-conversion
                        state.PushBoolean(false);

                        return 1;
                    }
                    else if (RespReadUtils.ReadSpanWithLengthHeader(out var bulkSpan, ref ptr, ptr + length))
                    {
                        state.PushBuffer(bulkSpan);

                        return 1;
                    }
                    goto default;

                case (byte)'*':
                    if (RespReadUtils.ReadUnsignedArrayLength(out var itemCount, ref ptr, ptr + length))
                    {
                        // Create the new table
                        state.CreateTable(itemCount, 0);

                        for (var itemIx = 0; itemIx < itemCount; itemIx++)
                        {
                            if (*ptr == '$')
                            {
                                // Bulk String
                                if (length >= 4 && new ReadOnlySpan<byte>(ptr + 1, 4).SequenceEqual("-1\r\n"u8))
                                {
                                    // Null strings are mapped to false
                                    // See: https://redis.io/docs/latest/develop/interact/programmability/lua-api/#lua-to-resp2-type-conversion
                                    state.PushBoolean(false);
                                }
                                else if (RespReadUtils.ReadSpanWithLengthHeader(out var strSpan, ref ptr, ptr + length))
                                {
                                    state.PushBuffer(strSpan);
                                }
                                else
                                {
                                    // Error, drop the table we allocated
                                    state.Pop(1);
                                    goto default;
                                }
                            }
                            else
                            {
                                // In practice, we ONLY ever return bulk strings
                                // So just... not implementing the rest for now
                                throw new NotImplementedException($"Unexpected sigil: {(char)*ptr}");
                            }

                            // Stack now has table and value at itemIx on it
                            state.RawSetInteger(1, itemIx + 1);
                        }

                        return 1;
                    }
                    goto default;

                default:
                    throw new Exception("Unexpected response: " + Encoding.UTF8.GetString(new Span<byte>(ptr, length)).Replace("\n", "|").Replace("\r", "") + "]");
            }
        }

        /// <summary>
        /// Runs the precompiled Lua function with the given outer session.
        /// 
        /// Response is written directly into the <see cref="RespServerSession"/>.
        /// </summary>
        public unsafe void RunForSession(int count, RespServerSession outerSession)
        {
            const int NeededStackSize = 2;

            state.ForceMinimumStackCapacity(NeededStackSize);

            preambleOuterSession = outerSession;
            preambleKeyAndArgvCount = count;
            try
            {
                LuaRunnerTrampolines.SetCallbackContext(this);

                try
                {
                    state.PushCFunction(&LuaRunnerTrampolines.RunPreambleForSession);
                    var callRes = state.PCall(0, 0);
                    if (callRes != LuaStatus.OK)
                    {
                        while (!RespWriteUtils.WriteError("Internal Lua Error"u8, ref outerSession.dcurr, outerSession.dend))
                            outerSession.SendAndReset();

                        return;
                    }
                }
                finally
                {
                    preambleOuterSession = null;
                }

                var adapter = new RespResponseAdapter(outerSession);

                if (txnMode && preambleNKeys > 0)
                {
                    RunInTransaction(ref adapter);
                }
                else
                {
                    RunCommon(ref adapter);
                }
            }
            finally
            {
                LuaRunnerTrampolines.ClearCallbackContext(this);
            }
        }

        /// <summary>
        /// Setups a script to be run.
        /// 
        /// If you call this directly and Lua encounters an error, the process will crash.
        /// 
        /// Call <see cref="RunForSession"/> instead.
        /// </summary>
        internal int UnsafeRunPreambleForSession()
        {
            state.ExpectLuaStackEmpty();

            scratchBufferManager.Reset();

            ref var parseState = ref preambleOuterSession.parseState;

            var offset = 1;
            var nKeys = preambleNKeys = parseState.GetInt(offset++);
            preambleKeyAndArgvCount--;
            ResetParameters(nKeys, preambleKeyAndArgvCount - nKeys);

            if (nKeys > 0)
            {
                // Get KEYS on the stack
                _ = state.RawGetInteger(LuaType.Table, (int)LuaRegistry.Index, keysTableRegistryIndex);

                for (var i = 0; i < nKeys; i++)
                {
                    ref var key = ref parseState.GetArgSliceByRef(offset);

                    if (txnMode)
                    {
                        txnKeyEntries.AddKey(key, false, Tsavorite.core.LockType.Exclusive);
                        if (!respServerSession.storageSession.objectStoreLockableContext.IsNull)
                            txnKeyEntries.AddKey(key, true, Tsavorite.core.LockType.Exclusive);
                    }

                    // Equivalent to KEYS[i+1] = key
                    state.PushBuffer(key.ReadOnlySpan);
                    state.RawSetInteger(1, i + 1);

                    offset++;
                }

                // Remove KEYS from the stack
                state.Pop(1);

                preambleKeyAndArgvCount -= nKeys;
            }

            if (preambleKeyAndArgvCount > 0)
            {
                // Get ARGV on the stack
                _ = state.RawGetInteger(LuaType.Table, (int)LuaRegistry.Index, argvTableRegistryIndex);

                for (var i = 0; i < preambleKeyAndArgvCount; i++)
                {
                    ref var argv = ref parseState.GetArgSliceByRef(offset);

                    // Equivalent to ARGV[i+1] = argv
                    state.PushBuffer(argv.ReadOnlySpan);
                    state.RawSetInteger(1, i + 1);

                    offset++;
                }

                // Remove ARGV from the stack
                state.Pop(1);
            }

            return 0;
        }

        /// <summary>
        /// Runs the precompiled Lua function with specified (keys, argv) state.
        /// 
        /// Meant for use from a .NET host rather than in Garnet properly.
        /// </summary>
        public unsafe object RunForRunner(string[] keys = null, string[] argv = null)
        {
            const int NeededStackSize = 2;

            state.ForceMinimumStackCapacity(NeededStackSize);

            try
            {
                LuaRunnerTrampolines.SetCallbackContext(this);

                try
                {
                    preambleKeys = keys;
                    preambleArgv = argv;

                    state.PushCFunction(&LuaRunnerTrampolines.RunPreambleForRunner);
                    state.Call(0, 0);
                }
                finally
                {
                    preambleKeys = preambleArgv = null;
                }

                RunnerAdapter adapter;
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

                    adapter = new(scratchBufferManager);
                    RunInTransaction(ref adapter);
                }
                else
                {
                    adapter = new(scratchBufferManager);
                    RunCommon(ref adapter);
                }

                var resp = adapter.Response;
                var respCur = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(resp));
                var respEnd = respCur + resp.Length;

                if (RespReadUtils.TryReadErrorAsSpan(out var errSpan, ref respCur, respEnd))
                {
                    var errStr = Encoding.UTF8.GetString(errSpan);
                    throw new GarnetException(errStr);
                }

                var ret = MapRespToObject(ref respCur, respEnd);
                Debug.Assert(respCur == respEnd, "Should have fully consumed response");

                return ret;
            }
            finally
            {
                LuaRunnerTrampolines.ClearCallbackContext(this);
            }

            // Convert a RESP response into an object to return
            static object MapRespToObject(ref byte* cur, byte* end)
            {
                switch (*cur)
                {
                    case (byte)'+':
                        var simpleStrRes = RespReadUtils.ReadSimpleString(out var simpleStr, ref cur, end);
                        Debug.Assert(simpleStrRes, "Should never fail");

                        return simpleStr;

                    case (byte)':':
                        var readIntRes = RespReadUtils.Read64Int(out var int64, ref cur, end);
                        Debug.Assert(readIntRes, "Should never fail");

                        return int64;

                    // Error ('-') is handled before call to MapRespToObject

                    case (byte)'$':
                        var length = end - cur;

                        if (length >= 5 && new ReadOnlySpan<byte>(cur + 1, 4).SequenceEqual("-1\r\n"u8))
                        {
                            cur += 5;
                            return null;
                        }

                        var bulkStrRes = RespReadUtils.ReadStringResponseWithLengthHeader(out var bulkStr, ref cur, end);
                        Debug.Assert(bulkStrRes, "Should never fail");

                        return bulkStr;

                    case (byte)'*':
                        var arrayLengthRes = RespReadUtils.ReadUnsignedArrayLength(out var itemCount, ref cur, end);
                        Debug.Assert(arrayLengthRes, "Should never fail");

                        if (itemCount == 0)
                        {
                            return Array.Empty<object>();
                        }

                        var array = new object[itemCount];
                        for (var i = 0; i < array.Length; i++)
                        {
                            array[i] = MapRespToObject(ref cur, end);
                        }

                        return array;

                    default: throw new NotImplementedException($"Unexpected sigil {(char)*cur}");
                }
            }
        }

        /// <summary>
        /// Setups a script to be run.
        /// 
        /// If you call this directly and Lua encounters an error, the process will crash.
        /// 
        /// Call <see cref="RunForRunner"/> instead.
        /// </summary>
        internal int UnsafeRunPreambleForRunner()
        {
            state.ExpectLuaStackEmpty();

            scratchBufferManager?.Reset();

            return LoadParametersForRunner(preambleKeys, preambleArgv);
        }

        /// <summary>
        /// Calls <see cref="RunCommon"/> after setting up appropriate state for a transaction.
        /// </summary>
        private void RunInTransaction<TResponse>(ref TResponse response)
            where TResponse : struct, IResponseAdapter
        {
            try
            {
                respServerSession.storageSession.lockableContext.BeginLockable();
                if (!respServerSession.storageSession.objectStoreLockableContext.IsNull)
                    respServerSession.storageSession.objectStoreLockableContext.BeginLockable();
                respServerSession.SetTransactionMode(true);
                txnKeyEntries.LockAllKeys();

                RunCommon(ref response);
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

        /// <summary>
        /// Remove extra keys and args from KEYS and ARGV globals.
        /// </summary>
        internal void ResetParameters(int nKeys, int nArgs)
        {
            const int NeededStackSize = 3;

            state.ForceMinimumStackCapacity(NeededStackSize);

            if (keyLength > nKeys || argvLength > nArgs)
            {
                _ = state.RawGetInteger(LuaType.Function, (int)LuaRegistry.Index, resetKeysAndArgvRegistryIndex);

                state.PushInteger(nKeys + 1);
                state.PushInteger(nArgs + 1);

                state.Call(2, 0);
            }

            keyLength = nKeys;
            argvLength = nArgs;
        }

        /// <summary>
        /// Takes .NET strings for keys and args and pushes them into KEYS and ARGV globals.
        /// </summary>
        private int LoadParametersForRunner(string[] keys, string[] argv)
        {
            const int NeededStackSize = 2;

            state.ForceMinimumStackCapacity(NeededStackSize);

            ResetParameters(keys?.Length ?? 0, argv?.Length ?? 0);

            if (keys != null)
            {
                // get KEYS on the stack
                _ = state.RawGetInteger(LuaType.Table, (int)LuaRegistry.Index, keysTableRegistryIndex);

                for (var i = 0; i < keys.Length; i++)
                {
                    // equivalent to KEYS[i+1] = keys[i]
                    var key = keys[i];
                    PrepareString(key, scratchBufferManager, out var encoded);
                    state.PushBuffer(encoded);
                    state.RawSetInteger(1, i + 1);
                }

                state.Pop(1);
            }

            if (argv != null)
            {
                // get ARGV on the stack
                _ = state.RawGetInteger(LuaType.Table, (int)LuaRegistry.Index, argvTableRegistryIndex);

                for (var i = 0; i < argv.Length; i++)
                {
                    // equivalent to ARGV[i+1] = keys[i]
                    var arg = argv[i];
                    PrepareString(arg, scratchBufferManager, out var encoded);
                    state.PushBuffer(encoded);
                    state.RawSetInteger(1, i + 1);
                }

                state.Pop(1);
            }

            return 0;

            // Convert string into a span, using buffer for storage
            static void PrepareString(string raw, ScratchBufferManager buffer, out ReadOnlySpan<byte> strBytes)
            {
                var maxLen = Encoding.UTF8.GetMaxByteCount(raw.Length);

                buffer.Reset();
                var argSlice = buffer.CreateArgSlice(maxLen);
                var span = argSlice.Span;

                var written = Encoding.UTF8.GetBytes(raw, span);
                strBytes = span[..written];
            }
        }

        /// <summary>
        /// Runs the precompiled Lua function.
        /// </summary>
        private unsafe void RunCommon<TResponse>(ref TResponse resp)
            where TResponse : struct, IResponseAdapter
        {
            const int NeededStackSize = 2;

            // TODO: mapping is dependent on Resp2 vs Resp3 settings
            //       and that's not implemented at all

            try
            {
                state.ForceMinimumStackCapacity(NeededStackSize);

                _ = state.RawGetInteger(LuaType.Function, (int)LuaRegistry.Index, functionRegistryIndex);

                var callRes = state.PCall(0, 1);
                if (callRes == LuaStatus.OK)
                {
                    // The actual call worked, handle the response

                    if (state.StackTop == 0)
                    {
                        WriteNull(this, ref resp);
                        return;
                    }

                    var retType = state.Type(1);
                    var isNullish = retType is LuaType.Nil or LuaType.UserData or LuaType.Function or LuaType.Thread or LuaType.UserData;

                    if (isNullish)
                    {
                        WriteNull(this, ref resp);
                        return;
                    }
                    else if (retType == LuaType.Number)
                    {
                        WriteNumber(this, ref resp);
                        return;
                    }
                    else if (retType == LuaType.String)
                    {
                        WriteString(this, ref resp);
                        return;
                    }
                    else if (retType == LuaType.Boolean)
                    {
                        WriteBoolean(this, ref resp);
                        return;
                    }
                    else if (retType == LuaType.Table)
                    {
                        // Redis does not respect metatables, so RAW access is ok here

                        // If the key "ok" is in there, we need to short circuit
                        state.PushConstantString(okLowerConstStringRegistryIndex);
                        var okType = state.RawGet(null, 1);
                        if (okType == LuaType.String)
                        {
                            WriteString(this, ref resp);

                            // Remove table from stack
                            state.Pop(1);

                            return;
                        }

                        // Remove whatever we read from the table under the "ok" key
                        state.Pop(1);

                        // If the key "err" is in there, we need to short circuit 
                        state.PushConstantString(errConstStringRegistryIndex);

                        var errType = state.RawGet(null, 1);
                        if (errType == LuaType.String)
                        {
                            WriteError(this, ref resp);

                            // Remove table from stack
                            state.Pop(1);

                            return;
                        }

                        // Remove whatever we read from the table under the "err" key
                        state.Pop(1);

                        // Map this table to an array
                        WriteArray(this, ref resp);
                    }
                }
                else
                {
                    // An error was raised

                    if (state.StackTop == 0)
                    {
                        while (!RespWriteUtils.WriteError("ERR An error occurred while invoking a Lua script"u8, ref resp.BufferCur, resp.BufferEnd))
                            resp.SendAndReset();

                        return;
                    }
                    else if (state.StackTop == 1)
                    {
                        // PCall will put error in a string
                        state.KnownStringToBuffer(1, out var errBuf);

                        if (errBuf.Length >= 4 && MemoryMarshal.Read<int>("ERR "u8) == Unsafe.As<byte, int>(ref MemoryMarshal.GetReference(errBuf)))
                        {
                            // Response came back with a ERR, already - just pass it along
                            while (!RespWriteUtils.WriteError(errBuf, ref resp.BufferCur, resp.BufferEnd))
                                resp.SendAndReset();
                        }
                        else
                        {
                            // Otherwise, this is probably a Lua error - and those aren't very descriptive
                            // So slap some more information in

                            while (!RespWriteUtils.WriteDirect("-ERR Lua encountered an error: "u8, ref resp.BufferCur, resp.BufferEnd))
                                resp.SendAndReset();

                            while (!RespWriteUtils.WriteDirect(errBuf, ref resp.BufferCur, resp.BufferEnd))
                                resp.SendAndReset();

                            while (!RespWriteUtils.WriteDirect("\r\n"u8, ref resp.BufferCur, resp.BufferEnd))
                                resp.SendAndReset();
                        }

                        state.Pop(1);

                        return;
                    }
                    else
                    {
                        logger?.LogError("Got an unexpected number of values back from a pcall error {callRes}", callRes);

                        while (!RespWriteUtils.WriteError("ERR Unexpected error response"u8, ref resp.BufferCur, resp.BufferEnd))
                            resp.SendAndReset();

                        state.ClearStack();

                        return;
                    }
                }
            }
            finally
            {
                state.ExpectLuaStackEmpty();
            }

            // Write a null RESP value, remove the top value on the stack if there is one
            static void WriteNull(LuaRunner runner, ref TResponse resp)
            {
                while (!RespWriteUtils.WriteNull(ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                // The stack _could_ be empty if we're writing a null, so check before popping
                if (runner.state.StackTop != 0)
                {
                    runner.state.Pop(1);
                }
            }

            // Writes the number on the top of the stack, removes it from the stack
            static void WriteNumber(LuaRunner runner, ref TResponse resp)
            {
                Debug.Assert(runner.state.Type(runner.state.StackTop) == LuaType.Number, "Number was not on top of stack");

                // Redis unconditionally converts all "number" replies to integer replies so we match that
                // 
                // See: https://redis.io/docs/latest/develop/interact/programmability/lua-api/#lua-to-resp2-type-conversion
                var num = (long)runner.state.CheckNumber(runner.state.StackTop);

                while (!RespWriteUtils.WriteInteger(num, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                runner.state.Pop(1);
            }

            // Writes the string on the top of the stack, removes it from the stack
            static void WriteString(LuaRunner runner, ref TResponse resp)
            {
                runner.state.KnownStringToBuffer(runner.state.StackTop, out var buf);

                while (!RespWriteUtils.WriteBulkString(buf, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                runner.state.Pop(1);
            }

            // Writes the boolean on the top of the stack, removes it from the stack
            static void WriteBoolean(LuaRunner runner, ref TResponse resp)
            {
                Debug.Assert(runner.state.Type(runner.state.StackTop) == LuaType.Boolean, "Boolean was not on top of stack");

                // Redis maps Lua false to null, and Lua true to 1  this is strange, but documented
                //
                // See: https://redis.io/docs/latest/develop/interact/programmability/lua-api/#lua-to-resp2-type-conversion
                if (runner.state.ToBoolean(runner.state.StackTop))
                {
                    while (!RespWriteUtils.WriteInteger(1, ref resp.BufferCur, resp.BufferEnd))
                        resp.SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteNull(ref resp.BufferCur, resp.BufferEnd))
                        resp.SendAndReset();
                }

                runner.state.Pop(1);
            }

            // Writes the string on the top of the stack out as an error, removes the string from the stack
            static void WriteError(LuaRunner runner, ref TResponse resp)
            {
                runner.state.KnownStringToBuffer(runner.state.StackTop, out var errBuff);

                while (!RespWriteUtils.WriteError(errBuff, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                runner.state.Pop(1);
            }

            static void WriteArray(LuaRunner runner, ref TResponse resp)
            {
                // Redis does not respect metatables, so RAW access is ok here

                // 1 for the table, 1 for the pending value
                const int AdditonalNeededStackSize = 2;

                Debug.Assert(runner.state.Type(runner.state.StackTop) == LuaType.Table, "Table was not on top of stack");

                // Lua # operator - this MAY stop at nils, but isn't guaranteed to
                // See: https://www.lua.org/manual/5.3/manual.html#3.4.7
                var maxLen = runner.state.RawLen(runner.state.StackTop);

                // Find the TRUE length by scanning for nils
                var trueLen = 0;
                for (trueLen = 0; trueLen < maxLen; trueLen++)
                {
                    var type = runner.state.RawGetInteger(null, runner.state.StackTop, trueLen + 1);
                    runner.state.Pop(1);

                    if (type == LuaType.Nil)
                    {
                        break;
                    }
                }

                while (!RespWriteUtils.WriteArrayLength(trueLen, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                for (var i = 1; i <= trueLen; i++)
                {
                    // Push item at index i onto the stack
                    var type = runner.state.RawGetInteger(null, runner.state.StackTop, i);

                    switch (type)
                    {
                        case LuaType.String:
                            WriteString(runner, ref resp);
                            break;
                        case LuaType.Number:
                            WriteNumber(runner, ref resp);
                            break;
                        case LuaType.Boolean:
                            WriteBoolean(runner, ref resp);
                            break;
                        case LuaType.Table:
                            // For tables, we need to recurse - which means we need to check stack sizes again
                            runner.state.ForceMinimumStackCapacity(AdditonalNeededStackSize);
                            WriteArray(runner, ref resp);
                            break;

                        // All other Lua types map to nulls
                        default:
                            WriteNull(runner, ref resp);
                            break;
                    }
                }

                runner.state.Pop(1);
            }
        }
    }

    /// <summary>
    /// Holds static functions for Lua-to-.NET interop.
    /// 
    /// We annotate these as "unmanaged callers only" as a micro-optimization.
    /// See: https://devblogs.microsoft.com/dotnet/improvements-in-native-code-interop-in-net-5-0/#unmanagedcallersonly
    /// </summary>
    internal static class LuaRunnerTrampolines
    {
        [ThreadStatic]
        private static LuaRunner callbackContext;

        /// <summary>
        /// Set a <see cref="LuaRunner"/> that will be available in trampolines.
        /// 
        /// This assumes the same thread is used to call into Lua.
        /// 
        /// Call <see cref="ClearCallbackContext"/> when finished to avoid extending
        /// the lifetime of the <see cref="LuaRunner"/>.
        /// </summary>
        internal static void SetCallbackContext(LuaRunner context)
        {
            Debug.Assert(callbackContext == null, "Expected null context");
            callbackContext = context;
        }

        /// <summary>
        /// Clear a previously set 
        /// </summary>
        internal static void ClearCallbackContext(LuaRunner context)
        {
            Debug.Assert(ReferenceEquals(callbackContext, context), "Expected context to match");
            callbackContext = null;
        }

        /// <summary>
        /// Entry point for Lua PCall'ing into <see cref="LuaRunner.UnsafeCompileForRunner"/>.
        /// 
        /// We need this indirection to allow Lua to detect and report internal and memory errors
        /// without crashing the process.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int CompileForRunner(nint _)
        => callbackContext.UnsafeCompileForRunner();

        /// <summary>
        /// Entry point for Lua PCall'ing into <see cref="LuaRunner.UnsafeCompileForSession"/>.
        /// 
        /// We need this indirection to allow Lua to detect and report internal and memory errors
        /// without crashing the process.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int CompileForSession(nint _)
        => callbackContext.UnsafeCompileForSession();

        /// <summary>
        /// Entry point for Lua PCall'ing into <see cref="LuaRunner.UnsafeRunPreambleForRunner"/>.
        /// 
        /// We need this indirection to allow Lua to detect and report internal and memory errors
        /// without crashing the process.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int RunPreambleForRunner(nint _)
        => callbackContext.UnsafeRunPreambleForRunner();

        /// <summary>
        /// Entry point for Lua PCall'ing into <see cref="LuaRunner.UnsafeRunPreambleForSession"/>.
        /// 
        /// We need this indirection to allow Lua to detect and report internal and memory errors
        /// without crashing the process.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int RunPreambleForSession(nint _)
        => callbackContext.UnsafeRunPreambleForSession();

        /// <summary>
        /// Entry point for Lua calling back into Garnet via redis.call(...).
        /// 
        /// This entry point is for when there isn't an active <see cref="RespServerSession"/>.
        /// This should only happen during testing.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int GarnetCallNoSession(nint luaState)
        => callbackContext.NoSessionResponse(luaState);

        /// <summary>
        /// Entry point for Lua calling back into Garnet via redis.call(...).
        /// 
        /// This entry point is for when a transaction is in effect.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int GarnetCallWithTransaction(nint luaState)
        => callbackContext.GarnetCallWithTransaction(luaState);

        /// <summary>
        /// Entry point for Lua calling back into Garnet via redis.call(...).
        /// 
        /// This entry point is for when a transaction is not necessary.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int GarnetCallNoTransaction(nint luaState)
        => callbackContext.GarnetCall(luaState);
    }
}