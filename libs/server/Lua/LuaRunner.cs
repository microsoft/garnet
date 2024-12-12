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
            private byte* curEnd;

            internal RunnerAdapter(ScratchBufferManager bufferManager)
            {
                this.bufferManager = bufferManager;
                this.bufferManager.Reset();

                var scratchSpace = bufferManager.FullBuffer();

                origin = curHead = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(scratchSpace));
                curEnd = curHead + scratchSpace.Length;
            }

#pragma warning disable CS9084 // Struct member returns 'this' or other instance members by reference
            /// <inheritdoc />
            public unsafe ref byte* BufferCur
            => ref curHead;
#pragma warning restore CS9084

            /// <inheritdoc />
            public unsafe byte* BufferEnd
            => curEnd;

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
                bufferManager.GrowBuffer();

                var scratchSpace = bufferManager.FullBuffer();

                origin = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(scratchSpace));
                curEnd = origin + scratchSpace.Length;
                curHead = origin + len;
            }
        }

        // Rooted to keep function pointer alive
        readonly LuaFunction garnetCall;

        // References into Registry on the Lua side
        //
        // These are mix of objects we regularly update,
        // constants we want to avoid copying from .NET to Lua,
        // and the compiled function definition.
        readonly int sandboxEnvRegistryIndex;
        readonly int keysTableRegistryIndex;
        readonly int argvTableRegistryIndex;
        readonly int loadSandboxedRegistryIndex;
        readonly int resetKeysAndArgvRegistryIndex;
        readonly int okConstStringRegisteryIndex;
        readonly int errConstStringRegistryIndex;
        readonly int noSessionAvailableConstStringRegisteryIndex;
        readonly int pleaseSpecifyRedisCallConstStringRegistryIndex;
        readonly int errNoAuthConstStringRegistryIndex;
        readonly int errUnknownConstStringRegistryIndex;
        readonly int errBadArgConstStringRegistryIndex;
        int functionRegistryIndex;

        readonly ReadOnlyMemory<byte> source;
        readonly ScratchBufferNetworkSender scratchBufferNetworkSender;
        readonly RespServerSession respServerSession;

        readonly ScratchBufferManager scratchBufferManager;
        readonly ILogger logger;
        readonly Lua state;
        readonly TxnKeyEntries txnKeyEntries;
        readonly bool txnMode;

        int keyLength, argvLength;

        int curStackSize, curStackTop;

        /// <summary>
        /// Creates a new runner with the source of the script
        /// </summary>
        public LuaRunner(ReadOnlyMemory<byte> source, bool txnMode = false, RespServerSession respServerSession = null, ScratchBufferNetworkSender scratchBufferNetworkSender = null, ILogger logger = null)
        {
            const int NeededStackSize = 1;

            this.source = source;
            this.txnMode = txnMode;
            this.respServerSession = respServerSession;
            this.scratchBufferNetworkSender = scratchBufferNetworkSender;
            this.scratchBufferManager = respServerSession?.scratchBufferManager ?? new();
            this.logger = logger;

            sandboxEnvRegistryIndex = -1;
            keysTableRegistryIndex = -1;
            argvTableRegistryIndex = -1;
            loadSandboxedRegistryIndex = -1;
            functionRegistryIndex = -1;

            // TODO: custom allocator?
            state = new Lua();
            AssertLuaStackEmpty();
            curStackTop = 0;

            ForceGrowLuaStack(NeededStackSize);

            if (txnMode)
            {
                txnKeyEntries = new TxnKeyEntries(16, respServerSession.storageSession.lockableContext, respServerSession.storageSession.objectStoreLockableContext);

                garnetCall = garnet_call_txn;
            }
            else
            {
                garnetCall = garnet_call;
            }

            var sandboxRes = state.DoString(@"
                import = function () end
                redis = {}
                function redis.call(...)
                    return garnet_call(...)
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
                -- do resets in the Lua side to minimize pinvokes
                function reset_keys_and_argv(fromKey, fromArgv)
                    local keyCount = #KEYS
                    for i = fromKey, keyCount do
                        KEYS[i] = nil
                    end

                    local argvCount = #ARGV
                    for i = fromArgv, argvCount do
                        ARGV[i] = nil
                    end
                end
                -- responsible for sandboxing user provided code
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

            // Register garnet_call in global namespace
            state.Register("garnet_call", garnetCall);

            CheckedGetGlobal(LuaType.Table, "sandbox_env");
            sandboxEnvRegistryIndex = CheckedRef();

            CheckedGetGlobal(LuaType.Table, "KEYS");
            keysTableRegistryIndex = CheckedRef();

            CheckedGetGlobal(LuaType.Table, "ARGV");
            argvTableRegistryIndex = CheckedRef();

            CheckedGetGlobal(LuaType.Function, "load_sandboxed");
            loadSandboxedRegistryIndex = CheckedRef();

            CheckedGetGlobal(LuaType.Function, "reset_keys_and_argv");
            resetKeysAndArgvRegistryIndex = CheckedRef();

            // Commonly used strings, register them once so we don't have to copy them over each time we need them
            okConstStringRegisteryIndex = ConstantStringToRegistery(NeededStackSize, CmdStrings.LUA_OK);
            errConstStringRegistryIndex = ConstantStringToRegistery(NeededStackSize, CmdStrings.LUA_err);
            noSessionAvailableConstStringRegisteryIndex = ConstantStringToRegistery(NeededStackSize, CmdStrings.LUA_No_session_available);
            pleaseSpecifyRedisCallConstStringRegistryIndex = ConstantStringToRegistery(NeededStackSize, CmdStrings.LUA_ERR_Please_specify_at_least_one_argument_for_this_redis_lib_call);
            errNoAuthConstStringRegistryIndex = ConstantStringToRegistery(NeededStackSize, CmdStrings.RESP_ERR_NOAUTH);
            errUnknownConstStringRegistryIndex = ConstantStringToRegistery(NeededStackSize, CmdStrings.LUA_ERR_Unknown_Redis_command_called_from_script);
            errBadArgConstStringRegistryIndex = ConstantStringToRegistery(NeededStackSize, CmdStrings.LUA_ERR_Lua_redis_lib_command_arguments_must_be_strings_or_integers);

            AssertLuaStackEmpty();
        }

        /// <summary>
        /// Creates a new runner with the source of the script
        /// </summary>
        public LuaRunner(string source, bool txnMode = false, RespServerSession respServerSession = null, ScratchBufferNetworkSender scratchBufferNetworkSender = null, ILogger logger = null)
            : this(Encoding.UTF8.GetBytes(source), txnMode, respServerSession, scratchBufferNetworkSender, logger)
        {
        }

        /// <summary>
        /// Some strings we use a bunch, and copying them to Lua each time is wasteful
        ///
        /// So instead we stash them in the Registry and load them by index
        /// </summary>
        int ConstantStringToRegistery(int top, ReadOnlySpan<byte> str)
        {
            AssertLuaStackEmpty();

            CheckedPushBuffer(top, str);
            return CheckedRef();
        }

        /// <summary>
        /// Compile script for running in a .NET host.
        /// 
        /// Errors are raised as exceptions.
        /// </summary>
        public unsafe void CompileForRunner()
        {
            var adapter = new RunnerAdapter(scratchBufferManager);
            CompileCommon(ref adapter);

            var resp = adapter.Response;
            var respStart = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(resp));
            var respEnd = respStart + resp.Length;
            if (RespReadUtils.TryReadErrorAsSpan(out var errSpan, ref respStart, respEnd))
            {
                var errStr = Encoding.UTF8.GetString(errSpan);
                throw new GarnetException(errStr);
            }
        }

        /// <summary>
        /// Compile script for a <see cref="RespServerSession"/>.
        /// 
        /// Any errors encountered are written out as Resp errors.
        /// </summary>
        public void CompileForSession(RespServerSession session)
        {
            var adapter = new RespResponseAdapter(session);
            CompileCommon(ref adapter);
        }

        /// <summary>
        /// Compile script, writing errors out to given response.
        /// </summary>
        unsafe void CompileCommon<TResponse>(ref TResponse resp)
            where TResponse : struct, IResponseAdapter
        {
            const int NeededStackSpace = 2;

            Debug.Assert(functionRegistryIndex == -1, "Shouldn't compile multiple times");

            AssertLuaStackEmpty();
            curStackTop = 0;

            try
            {
                ForceGrowLuaStack(NeededStackSpace);

                CheckedPushNumber(NeededStackSpace, loadSandboxedRegistryIndex);
                CheckedGetTable(LuaType.Function, (int)LuaRegistry.Index);

                CheckedPushBuffer(NeededStackSpace, source.Span);
                CheckedCall(1, -1); // Multiple returns allowed

                var numRets = state.GetTop();
                curStackTop = numRets;

                if (numRets == 0)
                {
                    while (!RespWriteUtils.WriteError("Shouldn't happen, no returns from load_sandboxed"u8, ref resp.BufferCur, resp.BufferEnd))
                        resp.SendAndReset();

                    return;
                }
                else if (numRets == 1)
                {
                    var returnType = state.Type(1);
                    if (returnType != LuaType.Function)
                    {
                        var errStr = $"Could not compile function, got back a {returnType}";
                        while (!RespWriteUtils.WriteError(errStr, ref resp.BufferCur, resp.BufferEnd))
                            resp.SendAndReset();

                        return;
                    }

                    functionRegistryIndex = CheckedRef();
                }
                else if (numRets == 2)
                {
                    NativeMethods.CheckBuffer(state.Handle, 2, out var errorBuf);

                    var errStr = $"Compilation error: {Encoding.UTF8.GetString(errorBuf)}";
                    while (!RespWriteUtils.WriteError(errStr, ref resp.BufferCur, resp.BufferEnd))
                        resp.SendAndReset();

                    CheckedPop(2);

                    return;
                }
                else
                {
                    CheckedPop(numRets);

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
                AssertLuaStackEmpty();
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
                return NoSessionResponse();
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
                return NoSessionResponse();
            }

            return ProcessCommandFromScripting(respServerSession.lockableGarnetApi);
        }

        /// <summary>
        /// Call somehow came in with no valid resp server session.
        /// 
        /// This is used in benchmarking.
        /// </summary>
        int NoSessionResponse()
        {
            const int NeededStackSpace = 1;

            ForceGrowLuaStack(NeededStackSpace);

            CheckedPushNil(NeededStackSpace);
            return 1;
        }

        /// <summary>
        /// Entry point method for executing commands from a Lua Script
        /// </summary>
        unsafe int ProcessCommandFromScripting<TGarnetApi>(TGarnetApi api)
            where TGarnetApi : IGarnetApi
        {
            const int AdditionalStackSpace = 1;

            // This is LUA_MINSTACK, which is 20
            curStackSize = 20;
            curStackTop = state.GetTop();

            try
            {
                var argCount = curStackTop;

                if (argCount == 0)
                {
                    return LuaStaticError(argCount, pleaseSpecifyRedisCallConstStringRegistryIndex);
                }

                ForceGrowLuaStack(AdditionalStackSpace);
                var neededStackSpace = argCount + AdditionalStackSpace;

                if (!NativeMethods.CheckBuffer(state.Handle, 1, out var cmdSpan))
                {
                    return LuaStaticError(neededStackSpace, errBadArgConstStringRegistryIndex);
                }

                // We special-case a few performance-sensitive operations to directly invoke via the storage API
                if (AsciiUtils.EqualsUpperCaseSpanIgnoringCase(cmdSpan, "SET"u8) && argCount == 3)
                {
                    if (!respServerSession.CheckACLPermissions(RespCommand.SET))
                    {
                        return LuaStaticError(neededStackSpace, errNoAuthConstStringRegistryIndex);
                    }

                    if (!NativeMethods.CheckBuffer(state.Handle, 2, out var keySpan) || !NativeMethods.CheckBuffer(state.Handle, 3, out var valSpan))
                    {
                        return LuaStaticError(neededStackSpace, errBadArgConstStringRegistryIndex);
                    }

                    // Note these spans are implicitly pinned, as they're actually on the Lua stack
                    var key = ArgSlice.FromPinnedSpan(keySpan);
                    var value = ArgSlice.FromPinnedSpan(valSpan);

                    _ = api.SET(key, value);

                    CheckedPushConstantString(neededStackSpace, okConstStringRegisteryIndex);
                    return 1;
                }
                else if (AsciiUtils.EqualsUpperCaseSpanIgnoringCase(cmdSpan, "GET"u8) && argCount == 2)
                {
                    if (!respServerSession.CheckACLPermissions(RespCommand.GET))
                    {
                        return LuaStaticError(neededStackSpace, errNoAuthConstStringRegistryIndex);
                    }

                    if (!NativeMethods.CheckBuffer(state.Handle, 2, out var keySpan))
                    {
                        return LuaStaticError(neededStackSpace, errBadArgConstStringRegistryIndex);
                    }

                    // Span is (implicitly) pinned since it's actually on the Lua stack
                    var key = ArgSlice.FromPinnedSpan(keySpan);
                    var status = api.GET(key, out var value);
                    if (status == GarnetStatus.OK)
                    {
                        CheckedPushBuffer(neededStackSpace, value.ReadOnlySpan);
                    }
                    else
                    {
                        CheckedPushNil(neededStackSpace);
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
                        NativeMethods.KnownStringToBuffer(state.Handle, argIx, out var span);

                        // Span remains pinned so long as we don't pop the stack
                        scratchBufferManager.WriteArgument(span);
                    }
                    else
                    {
                        return LuaStaticError(neededStackSpace, errBadArgConstStringRegistryIndex);
                    }
                }

                var request = scratchBufferManager.ViewFullArgSlice();

                // Once the request is formatted, we can release all the args on the Lua stack
                //
                // This keeps the stack size down for processing the response
                CheckedPop(argCount);

                _ = respServerSession.TryConsumeMessages(request.ptr, request.length);

                var response = scratchBufferNetworkSender.GetResponse();
                var result = ProcessResponse(response.ptr, response.length);
                scratchBufferNetworkSender.Reset();
                return result;
            }
            catch (Exception e)
            {
                logger?.LogError(e, "During Lua script execution");

                // Clear the stack
                state.SetTop(0);
                curStackTop = 0;

                ForceGrowLuaStack(1);

                // TODO: Remove alloc
                var b = Encoding.UTF8.GetBytes(e.Message);
                CheckedPushBuffer(AdditionalStackSpace, b);
                return state.Error();
            }
        }

        /// <summary>
        /// Cause a Lua error to be raised with a message previously registered.
        /// </summary>
        int LuaStaticError(int top, int constStringRegistryIndex)
        {
            const int NeededStackSize = 1;

            ForceGrowLuaStack(NeededStackSize);

            CheckedPushConstantString(top + NeededStackSize, constStringRegistryIndex);
            return state.Error();
        }

        /// <summary>
        /// Process a RESP-formatted response from the RespServerSession.
        /// 
        /// Pushes result onto state stack and returns 1, or raises an error and never returns.
        /// </summary>
        unsafe int ProcessResponse(byte* ptr, int length)
        {
            const int NeededStackSize = 3;

            AssertLuaStackEmpty();

            ForceGrowLuaStack(NeededStackSize);

            switch (*ptr)
            {
                case (byte)'+':
                    ptr++;
                    length--;
                    if (RespReadUtils.ReadAsSpan(out var resultSpan, ref ptr, ptr + length))
                    {
                        CheckedPushBuffer(NeededStackSize, resultSpan);
                        return 1;
                    }
                    goto default;

                case (byte)':':
                    if (RespReadUtils.Read64Int(out var number, ref ptr, ptr + length))
                    {
                        CheckedPushNumber(NeededStackSize, number);
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
                            return LuaStaticError(NeededStackSize, errUnknownConstStringRegistryIndex);
                        }

                        CheckedPushBuffer(NeededStackSize, errSpan);
                        return state.Error();

                    }
                    goto default;

                case (byte)'$':
                    if (length >= 5 && new ReadOnlySpan<byte>(ptr + 1, 4).SequenceEqual("-1\r\n"u8))
                    {
                        // Bulk null strings are mapped to FALSE
                        // See: https://redis.io/docs/latest/develop/interact/programmability/lua-api/#lua-to-resp2-type-conversion
                        CheckedPushBoolean(NeededStackSize, false);

                        return 1;
                    }
                    else if (RespReadUtils.ReadSpanWithLengthHeader(out var bulkSpan, ref ptr, ptr + length))
                    {
                        CheckedPushBuffer(NeededStackSize, bulkSpan);

                        return 1;
                    }
                    goto default;

                case (byte)'*':
                    if (RespReadUtils.ReadUnsignedArrayLength(out var itemCount, ref ptr, ptr + length))
                    {
                        // Create the new table
                        CheckedCreateTable(itemCount, 0);

                        for (var itemIx = 0; itemIx < itemCount; itemIx++)
                        {
                            if (*ptr == '$')
                            {
                                // Bulk String
                                if (length >= 4 && new ReadOnlySpan<byte>(ptr + 1, 4).SequenceEqual("-1\r\n"u8))
                                {
                                    // Null strings are mapped to false
                                    // See: https://redis.io/docs/latest/develop/interact/programmability/lua-api/#lua-to-resp2-type-conversion
                                    CheckedPushBoolean(NeededStackSize, false);
                                }
                                else if (RespReadUtils.ReadSpanWithLengthHeader(out var strSpan, ref ptr, ptr + length))
                                {
                                    CheckedPushBuffer(NeededStackSize, strSpan);
                                }
                                else
                                {
                                    // Error, drop the table we allocated
                                    CheckedPop(1);
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
                            CheckedRawSetInteger(1, itemIx + 1);
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
        public void RunForSession(int count, RespServerSession outerSession)
        {
            const int NeededStackSize = 3;

            AssertLuaStackEmpty();

            ForceGrowLuaStack(NeededStackSize);

            scratchBufferManager.Reset();

            var parseState = outerSession.parseState;

            var offset = 1;
            var nKeys = parseState.GetInt(offset++);
            count--;
            ResetParameters(nKeys, count - nKeys);

            if (nKeys > 0)
            {
                // Get KEYS on the stack
                CheckedPushNumber(NeededStackSize, keysTableRegistryIndex);
                CheckedRawGet(LuaType.Table, (int)LuaRegistry.Index);

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
                    CheckedPushNumber(NeededStackSize, i + 1);
                    CheckedPushBuffer(NeededStackSize, key.ReadOnlySpan);
                    CheckedRawSet(1);

                    offset++;
                }

                // Remove KEYS from the stack
                CheckedPop(1);

                count -= nKeys;
            }

            if (count > 0)
            {
                // Get ARGV on the stack
                CheckedPushNumber(NeededStackSize, argvTableRegistryIndex);
                CheckedRawGet(LuaType.Table, (int)LuaRegistry.Index);

                for (var i = 0; i < count; i++)
                {
                    ref var argv = ref parseState.GetArgSliceByRef(offset);

                    // Equivalent to ARGV[i+1] = argv
                    CheckedPushNumber(NeededStackSize, i + 1);
                    CheckedPushBuffer(NeededStackSize, argv.ReadOnlySpan);
                    CheckedRawSet(1);

                    offset++;
                }

                // Remove ARGV from the stack
                CheckedPop(1);
            }

            AssertLuaStackEmpty();

            var adapter = new RespResponseAdapter(outerSession);

            if (txnMode && nKeys > 0)
            {
                RunInTransaction(ref adapter);
            }
            else
            {
                RunCommon(ref adapter);
            }
        }

        /// <summary>
        /// Runs the precompiled Lua function with specified (keys, argv) state.
        /// 
        /// Meant for use from a .NET host rather than in Garnet properly.
        /// </summary>
        public unsafe object RunForRunner(string[] keys = null, string[] argv = null)
        {
            scratchBufferManager?.Reset();
            LoadParametersForRunner(keys, argv);

            var adapter = new RunnerAdapter(scratchBufferManager);

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

                RunInTransaction(ref adapter);
            }
            else
            {
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
        /// Calls <see cref="RunCommon"/> after setting up appropriate state for a transaction.
        /// </summary>
        void RunInTransaction<TResponse>(ref TResponse response)
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

            AssertLuaStackEmpty();

            ForceGrowLuaStack(NeededStackSize);

            if (keyLength > nKeys || argvLength > nArgs)
            {
                CheckedRawGetInteger(LuaType.Function, (int)LuaRegistry.Index, resetKeysAndArgvRegistryIndex);

                CheckedPushNumber(NeededStackSize, nKeys + 1);
                CheckedPushNumber(NeededStackSize, nArgs + 1);

                var resetRes = CheckedPCall(2, 0);
                Debug.Assert(resetRes == LuaStatus.OK, "Resetting should never fail");
            }

            keyLength = nKeys;
            argvLength = nArgs;

            AssertLuaStackEmpty();
        }

        /// <summary>
        /// Takes .NET strings for keys and args and pushes them into KEYS and ARGV globals.
        /// </summary>
        void LoadParametersForRunner(string[] keys, string[] argv)
        {
            const int NeededStackSize = 2;

            AssertLuaStackEmpty();

            ForceGrowLuaStack(NeededStackSize);

            ResetParameters(keys?.Length ?? 0, argv?.Length ?? 0);

            if (keys != null)
            {
                // get KEYS on the stack
                CheckedPushNumber(NeededStackSize, keysTableRegistryIndex);
                CheckedGetTable(LuaType.Table, (int)LuaRegistry.Index);

                for (var i = 0; i < keys.Length; i++)
                {
                    // equivalent to KEYS[i+1] = keys[i]
                    var key = keys[i];
                    PrepareString(key, scratchBufferManager, out var encoded);
                    CheckedPushBuffer(NeededStackSize, encoded);
                    CheckedRawSetInteger(1, i + 1);
                }

                CheckedPop(1);
            }

            if (argv != null)
            {
                // get ARGV on the stack
                CheckedPushNumber(NeededStackSize, argvTableRegistryIndex);
                CheckedGetTable(LuaType.Table, (int)LuaRegistry.Index);

                for (var i = 0; i < argv.Length; i++)
                {
                    // equivalent to ARGV[i+1] = keys[i]
                    var arg = argv[i];
                    PrepareString(arg, scratchBufferManager, out var encoded);
                    CheckedPushBuffer(NeededStackSize, encoded);
                    CheckedRawSetInteger(1, i + 1);
                }

                CheckedPop(1);
            }

            AssertLuaStackEmpty();

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
        unsafe void RunCommon<TResponse>(ref TResponse resp)
            where TResponse : struct, IResponseAdapter
        {
            const int NeededStackSize = 2;

            // TODO: mapping is dependent on Resp2 vs Resp3 settings
            //       and that's not implemented at all

            AssertLuaStackEmpty();

            try
            {
                ForceGrowLuaStack(NeededStackSize);

                CheckedPushNumber(NeededStackSize, functionRegistryIndex);
                CheckedGetTable(LuaType.Function, (int)LuaRegistry.Index);

                var callRes = CheckedPCall(0, 1);
                if (callRes == LuaStatus.OK)
                {
                    // The actual call worked, handle the response

                    if (curStackTop == 0)
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
                        // TODO: because we are dealing with a user provided type, we MUST respect
                        //       metatables - so we can't use any of the RawXXX methods
                        //       so we need a test that use metatables (and compare to how Redis does this)

                        // If the key err is in there, we need to short circuit 
                        CheckedPushConstantString(NeededStackSize, errConstStringRegistryIndex);

                        var errType = CheckedGetTable(null, 1);
                        if (errType == LuaType.String)
                        {
                            WriteError(this, ref resp);

                            // Remove table from stack
                            CheckedPop(1);

                            return;
                        }

                        // Remove whatever we read from the table under the "err" key
                        CheckedPop(1);

                        // Map this table to an array
                        WriteArray(this, ref resp);
                    }
                }
                else
                {
                    // An error was raised

                    if (curStackTop == 0)
                    {
                        while (!RespWriteUtils.WriteError("ERR An error occurred while invoking a Lua script"u8, ref resp.BufferCur, resp.BufferEnd))
                            resp.SendAndReset();

                        return;
                    }
                    else if (curStackTop == 1)
                    {
                        if (NativeMethods.CheckBuffer(state.Handle, 1, out var errBuf))
                        {
                            while (!RespWriteUtils.WriteError(errBuf, ref resp.BufferCur, resp.BufferEnd))
                                resp.SendAndReset();
                        }

                        CheckedPop(1);

                        return;
                    }
                    else
                    {
                        logger?.LogError("Got an unexpected number of values back from a pcall error {callRes}", callRes);

                        while (!RespWriteUtils.WriteError("ERR Unexpected error response"u8, ref resp.BufferCur, resp.BufferEnd))
                            resp.SendAndReset();

                        state.SetTop(0);
                        curStackTop = 0;

                        return;
                    }
                }
            }
            finally
            {
                AssertLuaStackEmpty();
            }

            // Write a null RESP value, remove the top value on the stack if there is one
            static void WriteNull(LuaRunner runner, ref TResponse resp)
            {
                while (!RespWriteUtils.WriteNull(ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                // The stack _could_ be empty if we're writing a null, so check before popping
                if (runner.curStackTop != 0)
                {
                    runner.CheckedPop(1);
                }
            }

            // Writes the number on the top of the stack, removes it from the stack
            static void WriteNumber(LuaRunner runner, ref TResponse resp)
            {
                Debug.Assert(runner.state.Type(runner.curStackTop) == LuaType.Number, "Number was not on top of stack");

                // Redis unconditionally converts all "number" replies to integer replies so we match that
                // 
                // See: https://redis.io/docs/latest/develop/interact/programmability/lua-api/#lua-to-resp2-type-conversion
                var num = (long)runner.state.CheckNumber(runner.curStackTop);

                while (!RespWriteUtils.WriteInteger(num, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                runner.CheckedPop(1);
            }

            // Writes the string on the top of the stack, removes it from the stack
            static void WriteString(LuaRunner runner, ref TResponse resp)
            {
                NativeMethods.KnownStringToBuffer(runner.state.Handle, runner.curStackTop, out var buf);

                while (!RespWriteUtils.WriteBulkString(buf, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                runner.CheckedPop(1);
            }

            // Writes the boolean on the top of the stack, removes it from the stack
            static void WriteBoolean(LuaRunner runner, ref TResponse resp)
            {
                Debug.Assert(runner.state.Type(runner.curStackTop) == LuaType.Boolean, "Boolean was not on top of stack");

                // Redis maps Lua false to null, and Lua true to 1  this is strange, but documented
                //
                // See: https://redis.io/docs/latest/develop/interact/programmability/lua-api/#lua-to-resp2-type-conversion
                if (runner.state.ToBoolean(runner.curStackTop))
                {
                    while (!RespWriteUtils.WriteInteger(1, ref resp.BufferCur, resp.BufferEnd))
                        resp.SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteNull(ref resp.BufferCur, resp.BufferEnd))
                        resp.SendAndReset();
                }

                runner.CheckedPop(1);
            }

            // Writes the string on the top of the stack out as an error, removes the string from the stack
            static void WriteError(LuaRunner runner, ref TResponse resp)
            {
                NativeMethods.KnownStringToBuffer(runner.state.Handle, runner.curStackTop, out var errBuff);

                while (!RespWriteUtils.WriteError(errBuff, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                runner.CheckedPop(1);
            }

            static void WriteArray(LuaRunner runner, ref TResponse resp)
            {
                // 1 for the table, 1 for the pending value
                const int AdditonalNeededStackSize = 2;

                Debug.Assert(runner.state.Type(runner.curStackTop) == LuaType.Table, "Table was not on top of stack");

                // Lua # operator - this MAY stop at nils, but isn't guaranteed to
                // See: https://www.lua.org/manual/5.3/manual.html#3.4.7
                var maxLen = runner.state.Length(runner.curStackTop);

                // TODO: is it faster to punch a function in for this?
                // Find the TRUE length by scanning for nils
                var trueLen = 0;
                for (trueLen = 0; trueLen < maxLen; trueLen++)
                {
                    var type = runner.CheckedGetInteger(null, runner.curStackTop, trueLen + 1);
                    runner.CheckedPop(1);

                    if (type == LuaType.Nil)
                    {
                        break;
                    }
                }

                while (!RespWriteUtils.WriteArrayLength((int)trueLen, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                for (var i = 1; i <= trueLen; i++)
                {
                    // Push item at index i onto the stack
                    var type = runner.CheckedGetInteger(null, runner.curStackTop, i);

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
                            if (runner.curStackSize < runner.curStackTop + AdditonalNeededStackSize)
                            {
                                try
                                {
                                    runner.ForceGrowLuaStack(AdditonalNeededStackSize);
                                }
                                catch
                                {
                                    // This is the only place we can raise an exception, cull the Stack
                                    runner.state.SetTop(0);
                                    runner.curStackTop = 0;

                                    throw;
                                }
                            }

                            WriteArray(runner, ref resp);

                            break;

                        // All other Lua types map to nulls
                        default:
                            WriteNull(runner, ref resp);
                            break;
                    }
                }

                runner.CheckedPop(1);
            }
        }

        // TODO: I think we'd prefer all these helpers factor into their own file

        /// <summary>
        /// Ensure there's enough space on the Lua stack for <paramref name="additionalCapacity"/> more items.
        /// 
        /// Throws if there is not.
        /// 
        /// Maintains <see cref="curStackTop"/> to avoid unnecessary p/invokes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ForceGrowLuaStack(int additionalCapacity)
        {
            var availableSpace = curStackSize - curStackTop;

            if (availableSpace >= additionalCapacity)
            {
                return;
            }

            var needed = additionalCapacity - availableSpace;
            if (!state.CheckStack(needed))
            {
                throw new GarnetException("Could not reserve additional capacity on the Lua stack");
            }

            curStackSize += additionalCapacity;
        }

        /// <summary>
        /// This should be used for all PushBuffer calls into Lua.
        /// 
        /// If the string is a constant, consider registering it in the constructor and using <see cref="CheckedPushConstantString"/> instead.
        /// </summary>
        /// <seealso cref="AssertLuaStackBelow"/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckedPushBuffer(int reservedCapacity, ReadOnlySpan<byte> buffer, [CallerFilePath] string file = null, [CallerMemberName] string method = null, [CallerLineNumber] int line = -1)
        {
            AssertLuaStackBelow(reservedCapacity, file, method, line);

            NativeMethods.PushBuffer(state.Handle, buffer);
            curStackTop++;
        }

        /// <summary>
        /// This should be used for all PushNil calls into Lua.
        /// </summary>
        /// <seealso cref="AssertLuaStackBelow"/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckedPushNil(int reservedCapacity, [CallerFilePath] string file = null, [CallerMemberName] string method = null, [CallerLineNumber] int line = -1)
        {
            AssertLuaStackBelow(reservedCapacity, file, method, line);

            state.PushNil();
            curStackTop++;
        }

        /// <summary>
        /// This should be used for all PushNumber calls into Lua.
        /// </summary>
        /// <seealso cref="AssertLuaStackBelow"/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckedPushNumber(int reservedCapacity, double number, [CallerFilePath] string file = null, [CallerMemberName] string method = null, [CallerLineNumber] int line = -1)
        {
            AssertLuaStackBelow(reservedCapacity, file, method, line);

            state.PushNumber(number);
            curStackTop++;
        }

        /// <summary>
        /// This should be used for all PushBoolean calls into Lua.
        /// </summary>
        /// <seealso cref="AssertLuaStackBelow"/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckedPushBoolean(int reservedCapacity, bool b, [CallerFilePath] string file = null, [CallerMemberName] string method = null, [CallerLineNumber] int line = -1)
        {
            AssertLuaStackBelow(reservedCapacity, file, method, line);

            state.PushBoolean(b);
            curStackTop++;
        }

        /// <summary>
        /// This should be used for all Pop calls into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckedPop(int num)
        {
            state.Pop(num);
            curStackTop -= num;

            AssertLuaStackExpected();
        }

        /// <summary>
        /// This should be used for all Calls into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="curStackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckedCall(int args, int rets)
        {
            var oldStackTop = curStackTop;
            state.Call(args, rets);

            if (rets < 0)
            {
                curStackTop = state.GetTop();
            }
            else
            {
                curStackTop = oldStackTop - (args + 1) + rets;
            }

            AssertLuaStackExpected();
        }

        /// <summary>
        /// This should be used for all PCalls into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="curStackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private LuaStatus CheckedPCall(int args, int rets)
        {
            var oldStack = curStackTop;
            var res = state.PCall(args, rets, 0);

            if (res != LuaStatus.OK || rets < 0)
            {
                curStackTop = state.GetTop();
            }
            else
            {
                curStackTop = oldStack - (args + 1) + rets;
            }

            AssertLuaStackExpected();

            return res;
        }

        /// <summary>
        /// This should be used for all RawSetIntegers into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="curStackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckedRawSetInteger(int stackIndex, int tableIndex)
        {
            state.RawSetInteger(stackIndex, tableIndex);
            curStackTop--;

            AssertLuaStackExpected();
        }

        /// This should be used for all RawSets into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="curStackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckedRawSet(int stackIndex)
        {
            state.RawSet(stackIndex);
            curStackTop -= 2;

            AssertLuaStackExpected();
        }

        /// <summary>
        /// This should be used for all RawGetIntegers into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="curStackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckedRawGetInteger(LuaType expectedType, int stackIndex, int tableIndex)
        {
            var actual = state.RawGetInteger(stackIndex, tableIndex);
            Debug.Assert(actual == expectedType, "Unexpected type received");
            curStackTop++;

            AssertLuaStackExpected();
        }

        /// <summary>
        /// This should be used for all GetIntegers into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="curStackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private LuaType CheckedGetInteger(LuaType? expectedType, int stackIndex, int tableIndex)
        {
            var actual = state.GetInteger(stackIndex, tableIndex);
            Debug.Assert(expectedType == null || actual == expectedType, "Unexpected type received");
            curStackTop++;

            AssertLuaStackExpected();

            return actual;
        }

        /// <summary>
        /// This should be used for all RawGets into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="curStackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckedRawGet(LuaType expectedType, int stackIndex)
        {
            var actual = state.RawGet(stackIndex);
            Debug.Assert(actual == expectedType, "Unexpected type received");

            AssertLuaStackExpected();
        }

        /// <summary>
        /// This should be used for all GetTables into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="curStackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private LuaType CheckedGetTable(LuaType? expectedType, int stackIndex)
        {
            var actual = state.GetTable(stackIndex);
            Debug.Assert(expectedType == null || actual == expectedType, "Unexpected type received");

            AssertLuaStackExpected();

            return actual;
        }

        /// <summary>
        /// This should be used for all Refs into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="curStackTop"/> to minimize p/invoke calls.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int CheckedRef()
        {
            var ret = state.Ref(LuaRegistry.Index);
            curStackTop--;

            AssertLuaStackExpected();

            return ret;
        }

        /// <summary>
        /// This should be used for all CreateTables into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="curStackTop"/> to minimize p/invoke calls.
        /// </summary>
        private void CheckedCreateTable(int numArr, int numRec)
        {
            state.CreateTable(numArr, numRec);
            curStackTop++;

            AssertLuaStackExpected();
        }

        /// <summary>
        /// This should be used for all GetGlobals into Lua.
        /// 
        /// Maintains <see cref="curStackSize"/> and <see cref="curStackTop"/> to minimize p/invoke calls.
        /// </summary>
        private void CheckedGetGlobal(LuaType expectedType, string globalName)
        {
            var type = state.GetGlobal(globalName);
            Debug.Assert(type == expectedType, "Unexpected type received");

            curStackTop++;

            AssertLuaStackExpected();
        }

        /// <summary>
        /// This should be used to push all known constants strings (registered in constructor with <see cref="ConstantStringToRegistery(int, ReadOnlySpan{byte})"/>)
        /// into Lua.
        /// 
        /// This avoids extra copying of data between .NET and Lua.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckedPushConstantString(int reservedCapacity, int constStringRegistryIndex, [CallerFilePath] string file = null, [CallerMemberName] string method = null, [CallerLineNumber] int line = -1)
        {
            AssertLuaStackBelow(reservedCapacity, file, method, line);
            Debug.Assert(IsConstantStringRegistryIndex(constStringRegistryIndex), "Can't use this with unknown string");

            CheckedRawGetInteger(LuaType.String, (int)LuaRegistry.Index, constStringRegistryIndex);

            // Check if index corresponds to value registered in constructor
            bool IsConstantStringRegistryIndex(int index)
            => index == okConstStringRegisteryIndex ||
               index == errConstStringRegistryIndex ||
               index == noSessionAvailableConstStringRegisteryIndex ||
               index == pleaseSpecifyRedisCallConstStringRegistryIndex ||
               index == errNoAuthConstStringRegistryIndex ||
               index == errUnknownConstStringRegistryIndex ||
               index == errBadArgConstStringRegistryIndex;
        }

        /// <summary>
        /// Check that the Lua stack is empty in DEBUG builds.
        /// 
        /// This is never necessary for correctness, but is often useful to find logical bugs.
        /// </summary>
        [Conditional("DEBUG")]
        [MethodImpl(MethodImplOptions.NoInlining)]
        private void AssertLuaStackEmpty([CallerFilePath] string file = null, [CallerMemberName] string method = null, [CallerLineNumber] int line = -1)
        {
            Debug.Assert(state.GetTop() == 0, $"Lua stack not empty when expected ({method}:{line} in {file})");
        }

        /// <summary>
        /// Check that the Lua stack top is where expected in DEBUG builds.
        /// </summary>
        [Conditional("DEBUG")]
        [MethodImpl(MethodImplOptions.NoInlining)]
        private void AssertLuaStackExpected([CallerFilePath] string file = null, [CallerMemberName] string method = null, [CallerLineNumber] int line = -1)
        {
            Debug.Assert(state.GetTop() == curStackTop, $"Lua stack not where expected ({method}:{line} in {file})");
        }

        /// <summary>
        /// Check the Lua stack has not grown beyond the capacity we initially reserved.
        /// 
        /// This asserts (in DEBUG) that the next .PushXXX will succeed.
        /// 
        /// In practice, Lua almost always gives us enough space (default is ~20 slots) but that's not guaranteed and can be false
        /// for complicated redis.call invocations.
        /// </summary>
        [Conditional("DEBUG")]
        [MethodImpl(MethodImplOptions.NoInlining)]
        private void AssertLuaStackBelow(int reservedCapacity, [CallerFilePath] string file = null, [CallerMemberName] string method = null, [CallerLineNumber] int line = -1)
        {
            Debug.Assert(state.GetTop() < reservedCapacity, $"About to push to Lua stack without having reserved sufficient capacity.");
        }
    }
}