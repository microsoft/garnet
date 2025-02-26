// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
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
    -- rawset is proxied to implement readonly tables
    select = select;
    -- explicitly not allowing setfenv
    -- setmetatable is proxied to implement readonly tables
    string = string;
    table = table;
    tonumber = tonumber;
    tostring = tostring;
    type = type;
    unpack = table.unpack;
    xpcall = xpcall;

    KEYS = KEYS;
    ARGV = ARGV;
}
-- no reference to outermost set of globals (_G) should survive sandboxing
sandbox_env._G = sandbox_env
-- lock down a table, recursively doing the same to all table members
local rawGetRef = rawget
local readonly_metatable = {
    __index = function(onTable, key)
        return rawGetRef(onTable, key)
    end,
    __newindex = function(onTable, key, value)
        error('Attempt to modify a readonly table', 0)
    end
}
function recursively_readonly_table(table)
    if table.__readonly then
        return table
    end

    table.__readonly = true

    for key, value in pairs(table) do
        if type(value) == 'table' then
            recursively_readonly_table(value)
        end
    end

    setmetatable(table, readonly_metatable)
end
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
    local pCallRef = pcall
    local sha1hexRef = garnet_sha1hex
    local logRef = garnet_log
    local aclCheckCmdRef = garnet_acl_check_cmd
    local setRespRef = garnet_setresp
    local setMetatableRef = setmetatable
    local rawsetRaw = rawset

    sandbox_env.redis = {
        status_reply = function(text)
            return text
        end,

        error_reply = function(text)
            return { err = 'ERR ' .. text }
        end,

        call = garnetCallRef,

        pcall = function(...)
            local success, errOrRes = pCallRef(garnetCallRef, ...)
            if success then
                return errOrRes
            end

            return { err = errOrRes }
        end,

        sha1hex = sha1hexRef,

        LOG_DEBUG = 0,
        LOG_VERBOSE = 1,
        LOG_NOTICE = 2,
        LOG_WARNING = 3,

        log = logRef,

        REPL_ALL = 3,
        REPL_AOF = 1,
        REPL_REPLICA = 2,
        REPL_SLAVE = 2,
        REPL_NONE = 0,

        set_repl = function(...)
            -- this is a giant footgun, straight up not implementing it
            error('ERR redis.set_repl is not supported in Garnet', 0)
        end,

        replicate_commands = function(...)
            return true
        end,

        breakpoint = function(...)
            -- this is giant and weird, not implementing
            error('ERR redis.breakpoint is not supported in Garnet', 0)
        end,

        debug = function(...)
            -- this is giant and weird, not implementing
            error('ERR redis.debug is not supported in Garnet', 0)
        end,

        acl_check_cmd = aclCheckCmdRef,
        setresp = setRespRef,

        REDIS_VERSION = garnet_REDIS_VERSION,
        REDIS_VERSION_NUM = garnet_REDIS_VERSION_NUM
    }

    -- prevent modification to metatables for readonly tables
    -- Redis accomplishes this by patching Lua, we'd rather ship
    -- vanilla Lua and do it in code
    sandbox_env.setmetatable = function(table, metatable)
        if table and table.__readonly then
            error('Attempt to modify a readonly table', 0)
        end

        return setMetatableRef(table, metatable)
    end

    -- prevent bypassing metatables to update readonly tables
    -- as above, Redis prevents this with a patch to Lua
    sandbox_env.rawset = function(table, key, value)
        if table and table.__readonly then
            error('Attempt to modify a readonly table', 0)
        end

        return rawsetRef(table, key, value)
    end

    recursively_readonly_table(sandbox_env)

    local rawFunc, err = load(source, nil, nil, sandbox_env)

    return err, rawFunc
end
";

        private static readonly ReadOnlyMemory<byte> LoaderBlockBytes = Encoding.UTF8.GetBytes(LoaderBlock);

        private static (int Start, ulong[] ByteMask) NoScriptDetails = InitializeNoScriptDetails();

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
        readonly int errWrongNumberOfArgumentsConstStringRegistryIndex;
        readonly int errRedisLogRequiredTwoArgumentsOrMoreConstStringRegistryIndex;
        readonly int errFirstArgumentMustBeANumberConstStringRegistryIndex;
        readonly int errInvalidDebugLevelConstStringRegistryIndex;
        readonly int errInvalidCommandPassedToRedis_acl_check_cmdConstStringRegistryIndex;
        readonly int errRedisSetrespRequiresOneArgumentConstStringRegistryIndex;
        readonly int errRespVersionMustBe2Or3ConstStringRegistryIndex;
        readonly int errLoggingDisabledConstStringRegistryIndex;

        readonly LuaLoggingMode logMode;
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
        public unsafe LuaRunner(
            LuaMemoryManagementMode memMode,
            int? memLimitBytes,
            LuaLoggingMode logMode,
            ReadOnlyMemory<byte> source,
            bool txnMode = false,
            RespServerSession respServerSession = null,
            ScratchBufferNetworkSender scratchBufferNetworkSender = null,
            string redisVersion = "0.0.0.0",
            ILogger logger = null
        )
        {
            this.source = source;
            this.txnMode = txnMode;
            this.respServerSession = respServerSession;
            this.scratchBufferNetworkSender = scratchBufferNetworkSender;
            this.logMode = logMode;
            this.logger = logger;

            scratchBufferManager = respServerSession?.scratchBufferManager ?? new();

            // Explicitly force to RESP2 for now
            if (respServerSession != null)
            {
                respServerSession.respProtocolVersion = 2;

                // The act of setting these fields causes NoScript checks to be performed
                (respServerSession.noScriptStart, respServerSession.noScriptBitmap) = NoScriptDetails;
            }

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
                throw new GarnetException("Couldn't load loader into Lua");
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

            // Register functions provided by .NET in global namespace
            state.Register("garnet_call\0"u8, garnetCall);
            state.Register("garnet_sha1hex\0"u8, &LuaRunnerTrampolines.SHA1Hex);
            state.Register("garnet_log\0"u8, &LuaRunnerTrampolines.Log);
            state.Register("garnet_acl_check_cmd\0"u8, &LuaRunnerTrampolines.AclCheckCommand);
            state.Register("garnet_setresp\0"u8, &LuaRunnerTrampolines.SetResp);

            var redisVersionBytes = Encoding.UTF8.GetBytes(redisVersion);
            state.PushBuffer(redisVersionBytes);
            state.SetGlobal("garnet_REDIS_VERSION\0"u8);

            var redisVersionParsed = Version.Parse(redisVersion);
            var redisVersionNum =
                ((byte)redisVersionParsed.Major << 16) |
                ((byte)redisVersionParsed.Minor << 8) |
                ((byte)redisVersionParsed.Build << 0);
            state.PushInteger(redisVersionNum);
            state.SetGlobal("garnet_REDIS_VERSION_NUM\0"u8);

            state.GetGlobal(LuaType.Table, "KEYS\0"u8);
            keysTableRegistryIndex = state.Ref();

            state.GetGlobal(LuaType.Table, "ARGV\0"u8);
            argvTableRegistryIndex = state.Ref();

            state.GetGlobal(LuaType.Function, "load_sandboxed\0"u8);
            loadSandboxedRegistryIndex = state.Ref();

            state.GetGlobal(LuaType.Function, "reset_keys_and_argv\0"u8);
            resetKeysAndArgvRegistryIndex = state.Ref();

            // Commonly used strings, register them once so we don't have to copy them over each time we need them
            //
            // As a side benefit, we don't have to worry about reserving memory for them either during normal operation
            okConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.LUA_OK);
            okLowerConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.LUA_ok);
            errConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.LUA_err);
            noSessionAvailableConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.LUA_No_session_available);
            pleaseSpecifyRedisCallConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.LUA_ERR_Please_specify_at_least_one_argument_for_this_redis_lib_call);
            errNoAuthConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.RESP_ERR_NOAUTH);
            errUnknownConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.LUA_ERR_Unknown_Redis_command_called_from_script);
            errBadArgConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.LUA_ERR_Lua_redis_lib_command_arguments_must_be_strings_or_integers);
            errWrongNumberOfArgumentsConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.Lua_ERR_wrong_number_of_arguments);
            errRedisLogRequiredTwoArgumentsOrMoreConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.Lua_ERR_redis_log_requires_two_arguments_or_more);
            errFirstArgumentMustBeANumberConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.Lua_ERR_First_argument_must_be_a_number_log_level);
            errInvalidDebugLevelConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.Lua_ERR_Invalid_debug_level);
            errInvalidCommandPassedToRedis_acl_check_cmdConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.Lua_ERR_Invalid_command_passed_to_redis_acl_check_cmd);
            errRedisSetrespRequiresOneArgumentConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.Lua_ERR_redis_setresp_requires_one_argument);
            errRespVersionMustBe2Or3ConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.Lua_ERR_RESP_version_must_be_2_or_3);
            errLoggingDisabledConstStringRegistryIndex = ConstantStringToRegistry(CmdStrings.Lua_ERR_redis_log_disabled);

            state.ExpectLuaStackEmpty();
        }

        /// <summary>
        /// Creates a new runner with the source of the script
        /// </summary>
        public LuaRunner(LuaOptions options, string source, bool txnMode = false, RespServerSession respServerSession = null, ScratchBufferNetworkSender scratchBufferNetworkSender = null, string redisVersion = "0.0.0.0", ILogger logger = null)
            : this(options.MemoryManagementMode, options.GetMemoryLimitBytes(), options.LogMode, Encoding.UTF8.GetBytes(source), txnMode, respServerSession, scratchBufferNetworkSender, redisVersion, logger)
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
                    while (!RespWriteUtils.TryWriteError("Internal Lua Error"u8, ref session.dcurr, session.dend))
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
                while (!RespWriteUtils.TryWriteError(errStr, ref resp.BufferCur, resp.BufferEnd))
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
        /// Entry point for redis.sha1hex method from a Lua script.
        /// </summary>
        public int SHA1Hex(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var argCount = state.StackTop;
            if (argCount != 1)
            {
                state.PushConstantString(errWrongNumberOfArgumentsConstStringRegistryIndex);
                return state.RaiseErrorFromStack();
            }

            if (!state.CheckBuffer(1, out var bytes))
            {
                bytes = default;
            }

            Span<byte> hashBytes = stackalloc byte[SessionScriptCache.SHA1Len / 2];
            Span<byte> hexRes = stackalloc byte[SessionScriptCache.SHA1Len];

            SessionScriptCache.GetScriptDigest(bytes, hashBytes, hexRes);

            state.PushBuffer(hexRes);
            return 1;
        }

        /// <summary>
        /// Entry point for redis.log(...) from a Lua script.
        /// </summary>
        public int Log(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var argCount = state.StackTop;
            if (argCount < 2)
            {
                return LuaStaticError(errRedisLogRequiredTwoArgumentsOrMoreConstStringRegistryIndex);
            }

            if (state.Type(1) != LuaType.Number)
            {
                return LuaStaticError(errFirstArgumentMustBeANumberConstStringRegistryIndex);
            }

            var rawLevel = state.CheckNumber(1);
            if (rawLevel is not (0 or 1 or 2 or 3))
            {
                return LuaStaticError(errInvalidDebugLevelConstStringRegistryIndex);
            }

            if (logMode == LuaLoggingMode.Disable)
            {
                return LuaStaticError(errLoggingDisabledConstStringRegistryIndex);
            }

            // When shipped as a service, allowing arbitrary writes to logs is dangerous
            // so we support disabling it (while not breaking existing scripts)
            if (logMode == LuaLoggingMode.Silent)
            {
                return 0;
            }

            // Even if enabled, if no logger was provided we can just bail
            if (logger == null)
            {
                return 0;
            }

            // Construct and log the equivalent message
            string logMessage;
            if (argCount == 2)
            {
                if (state.CheckBuffer(2, out var buff))
                {
                    logMessage = Encoding.UTF8.GetString(buff);
                }
                else
                {
                    logMessage = "";
                }
            }
            else
            {
                var sb = new StringBuilder();

                for (var argIx = 2; argIx <= argCount; argIx++)
                {
                    if (state.CheckBuffer(argIx, out var buff))
                    {
                        if (sb.Length != 0)
                        {
                            _ = sb.Append(' ');
                        }

                        _ = sb.Append(Encoding.UTF8.GetString(buff));
                    }
                }

                logMessage = sb.ToString();
            }

            var logLevel =
                rawLevel switch
                {
                    0 => LogLevel.Debug,
                    1 => LogLevel.Information,
                    2 => LogLevel.Warning,
                    // We validated this above, so really it's just 3 but the switch needs to be exhaustive
                    _ => LogLevel.Error,
                };

            logger.Log(logLevel, "redis.log: {message}", logMessage.ToString());

            return 0;
        }

        /// <summary>
        /// Entry point for redis.setresp(...) from a Lua script.
        /// </summary>
        public int SetResp(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1)
            {
                return LuaStaticError(errRedisSetrespRequiresOneArgumentConstStringRegistryIndex);
            }

            double num;
            if (state.Type(1) != LuaType.Number || (num = state.CheckNumber(1)) is not 2 or 3)
            {
                return LuaStaticError(errRespVersionMustBe2Or3ConstStringRegistryIndex);
            }

            // TODO: actually implement RESP 3 support here (requires changes to RunCommon as well)
            if (num != 2)
            {
                state.PushBuffer("ERR Garnet Lua script only supports RESP2"u8);
                return state.RaiseErrorFromStack();
            }

            return 0;
        }

        /// <summary>
        /// Entry point for redis.acl_check_cmd(...) from a Lua script.
        /// </summary>
        public int AclCheckCommand(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount == 0)
            {
                return LuaStaticError(pleaseSpecifyRedisCallConstStringRegistryIndex);
            }

            if (!state.CheckBuffer(1, out var cmdSpan))
            {
                return LuaStaticError(errBadArgConstStringRegistryIndex);
            }

            // It's most accurate to use our existing parsing code
            // But it requires correct argument counts, and redis.acl_check_cmd doesn't.
            //
            // So we need to determine the expected minimum and maximum counts and truncate or add
            // any arguments

            var cmdStr = Encoding.UTF8.GetString(cmdSpan);
            if (!RespCommandsInfo.TryGetRespCommandInfo(cmdStr, out var info, externalOnly: false, includeSubCommands: true))
            {
                return LuaStaticError(errInvalidCommandPassedToRedis_acl_check_cmdConstStringRegistryIndex);
            }

            var providedRespArgCount = luaArgCount - 1;

            var isBitOpParent = info.Command == RespCommand.BITOP && providedRespArgCount == 0;
            var hasSubCommands = (info.SubCommands?.Length ?? 0) > 0;
            var providesSubCommand = hasSubCommands && providedRespArgCount >= 1;

            bool success;
            if (isBitOpParent)
            {
                // BITOP is _weird_

                // Going to push AND, OR, etc. onto the stack, so reserve a slot
                state.ForceMinimumStackCapacity(1);

                success = true;
                foreach (var subCommand in RespCommand.BITOP.ExpandForACLs())
                {
                    switch (subCommand)
                    {
                        case RespCommand.BITOP_AND: state.PushBuffer("AND"u8); break;
                        case RespCommand.BITOP_OR: state.PushBuffer("OR"u8); break;
                        case RespCommand.BITOP_XOR: state.PushBuffer("XOR"u8); break;
                        case RespCommand.BITOP_NOT: state.PushBuffer("NOT"u8); break;

                        default: throw new InvalidOperationException($"Unexpected BITOP sub command: {subCommand}");
                    }


                    var (parsedCmd, badArg) = PrepareAndCheckRespRequest(ref state, respServerSession, scratchBufferManager, info, cmdSpan, luaArgCount: 2);

                    // Remove the BITOP sub command
                    state.Pop(1);

                    if (badArg)
                    {
                        return LuaStaticError(errBadArgConstStringRegistryIndex);
                    }

                    if (parsedCmd == RespCommand.INVALID)
                    {
                        return LuaStaticError(errInvalidCommandPassedToRedis_acl_check_cmdConstStringRegistryIndex);
                    }

                    if (!respServerSession.CheckACLPermissions(parsedCmd))
                    {
                        success = false;
                        break;
                    }
                }

            }
            else if (hasSubCommands && !providesSubCommand)
            {
                // Complicated case here:
                //   - Caller has provided a command which has subcommands...
                //   - But they haven't provided the subcommand!
                //   - So any ACL check will fail, because the ACL covers the actual (ie. sub) command
                //
                // So what we do is check ALL of the subcommands, and if-and-only-if the current user
                // can run all of them do we return true.
                //
                // This matches intention behind redis.acl_check_cmd calls, in that a subsequent call
                // with that parent command will always succeed if we return true here.

                // Going to push the subcommand text onto the stack, so reserve some space
                state.ForceMinimumStackCapacity(1);

                success = true;

                byte[] subCommandScratchArr = null;
                Span<byte> subCommandScratch = stackalloc byte[64];
                try
                {
                    foreach (var subCommand in info.SubCommands)
                    {
                        var subCommandStr = subCommand.Name.AsSpan()[(subCommand.Name.IndexOf('|') + 1)..];

                        if (subCommandScratch.Length < subCommandStr.Length)
                        {
                            if (subCommandScratchArr != null)
                            {
                                ArrayPool<byte>.Shared.Return(subCommandScratchArr);
                            }

                            subCommandScratchArr = ArrayPool<byte>.Shared.Rent(subCommandStr.Length);
                            subCommandScratch = subCommandScratchArr;
                        }

                        if (!Encoding.UTF8.TryGetBytes(subCommandStr, subCommandScratch, out var written))
                        {
                            // If len(chars) != len(bytes) we're going to fail (no commands are non-ASCII)
                            // so just bail

                            success = false;
                            break;
                        }

                        var subCommandBuf = subCommandScratch[..written];

                        state.PushBuffer(subCommandBuf);
                        var (parsedCmd, badArg) = PrepareAndCheckRespRequest(ref state, respServerSession, scratchBufferManager, subCommand, cmdSpan, luaArgCount: 2);

                        // Remove the extra sub-command
                        state.Pop(1);

                        if (badArg)
                        {
                            return LuaStaticError(errBadArgConstStringRegistryIndex);
                        }

                        if (parsedCmd == RespCommand.INVALID)
                        {
                            return LuaStaticError(errInvalidCommandPassedToRedis_acl_check_cmdConstStringRegistryIndex);
                        }

                        if (!respServerSession.CheckACLPermissions(parsedCmd))
                        {
                            success = false;
                            break;
                        }
                    }
                }
                finally
                {
                    if (subCommandScratchArr != null)
                    {
                        ArrayPool<byte>.Shared.Return(subCommandScratchArr);
                    }
                }

                // We're done with these, so free up the space
                state.Pop(luaArgCount);

                state.PushBoolean(success);
            }
            else
            {
                var (parsedCommand, badArg) = PrepareAndCheckRespRequest(ref state, respServerSession, scratchBufferManager, info, cmdSpan, luaArgCount);

                if (badArg)
                {
                    return LuaStaticError(errBadArgConstStringRegistryIndex);
                }

                if (parsedCommand == RespCommand.INVALID)
                {
                    return LuaStaticError(errInvalidCommandPassedToRedis_acl_check_cmdConstStringRegistryIndex);
                }

                success = respServerSession.CheckACLPermissions(parsedCommand);
            }

            // We're done with these, so free up the space
            state.Pop(luaArgCount);

            state.PushBoolean(success);
            return 1;

            // Prepare a dummy RESP command with the given command and the current args on the Lua stack
            // and have the RespServerSession parse it
            static (RespCommand Parsed, bool BadArg) PrepareAndCheckRespRequest(
                ref LuaStateWrapper state,
                RespServerSession respServerSession,
                ScratchBufferManager scratchBufferManager,
                RespCommandsInfo cmdInfo,
                ReadOnlySpan<byte> cmdSpan,
                int luaArgCount
            )
            {
                var providedRespArgCount = luaArgCount - 1;

                // Figure out what the RESP command array should look like
                var minRespArgCount = Math.Abs(cmdInfo.Arity) - 1;
                var maxRespArgCount = cmdInfo.Arity < 0 ? int.MaxValue : (cmdInfo.Arity - 1);
                var actualRespArgCount = Math.Min(Math.Max(providedRespArgCount, minRespArgCount), maxRespArgCount);

                // RESP format the args so we can parse the command (and sub-command, and maybe keys down the line?)

                scratchBufferManager.Reset();
                scratchBufferManager.StartCommand(cmdSpan, actualRespArgCount);

                for (var i = 0; i < actualRespArgCount; i++)
                {
                    if (i < providedRespArgCount)
                    {
                        // Fill in the args we actually have
                        var stackIx = 2 + i;

                        var argType = state.Type(stackIx);
                        if (argType == LuaType.Nil)
                        {
                            scratchBufferManager.WriteNullArgument();
                        }
                        else if (argType is LuaType.String or LuaType.Number)
                        {
                            // KnownStringToBuffer will coerce a number into a string
                            //
                            // Redis nominally converts numbers to integers, but in this case just ToStrings things
                            state.KnownStringToBuffer(stackIx, out var span);

                            // Span remains pinned so long as we don't pop the stack
                            scratchBufferManager.WriteArgument(span);
                        }
                        else
                        {
                            return (RespCommand.INVALID, true);
                        }
                    }
                    else
                    {
                        // For args we don't have, shove in an empty string
                        scratchBufferManager.WriteArgument(default);
                    }
                }

                var request = scratchBufferManager.ViewFullArgSlice();
                var parsedCommand = respServerSession.ParseRespCommandBuffer(request.ReadOnlySpan);

                return (parsedCommand, false);
            }
        }

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
                        // Redis is weird, but false instead of Nil is correct here
                        state.PushBoolean(false);
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
                // We cannot let exceptions propogate back to Lua, that is not something .NET promises will work

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
                    if (RespReadUtils.TryReadAsSpan(out var resultSpan, ref ptr, ptr + length))
                    {
                        // Construct a table = { 'ok': value }
                        state.CreateTable(0, 1);
                        state.PushConstantString(okLowerConstStringRegistryIndex);
                        state.PushBuffer(resultSpan);
                        state.RawSet(1);

                        return 1;
                    }
                    goto default;

                case (byte)':':
                    if (RespReadUtils.TryReadInt64(out var number, ref ptr, ptr + length))
                    {
                        state.PushInteger(number);
                        return 1;
                    }
                    goto default;

                case (byte)'-':
                    ptr++;
                    length--;
                    if (RespReadUtils.TryReadAsSpan(out var errSpan, ref ptr, ptr + length))
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
                    else if (RespReadUtils.TryReadSpanWithLengthHeader(out var bulkSpan, ref ptr, ptr + length))
                    {
                        state.PushBuffer(bulkSpan);

                        return 1;
                    }
                    goto default;

                case (byte)'*':
                    if (RespReadUtils.TryReadSignedArrayLength(out var itemCount, ref ptr, ptr + length))
                    {
                        if (itemCount == -1)
                        {
                            // Null multi-bulk -> maps to false
                            state.PushBoolean(false);
                        }
                        else
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
                                    else if (RespReadUtils.TryReadSpanWithLengthHeader(out var strSpan, ref ptr, ptr + length))
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
                ResetTimeout();

                try
                {
                    state.PushCFunction(&LuaRunnerTrampolines.RunPreambleForSession);
                    var callRes = state.PCall(0, 0);
                    if (callRes != LuaStatus.OK)
                    {
                        while (!RespWriteUtils.TryWriteError("Internal Lua Error"u8, ref outerSession.dcurr, outerSession.dend))
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
                ResetTimeout();

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
                        var simpleStrRes = RespReadUtils.TryReadSimpleString(out var simpleStr, ref cur, end);
                        Debug.Assert(simpleStrRes, "Should never fail");

                        return simpleStr;

                    case (byte)':':
                        var readIntRes = RespReadUtils.TryReadInt64(out var int64, ref cur, end);
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

                        var bulkStrRes = RespReadUtils.TryReadStringResponseWithLengthHeader(out var bulkStr, ref cur, end);
                        Debug.Assert(bulkStrRes, "Should never fail");

                        return bulkStr;

                    case (byte)'*':
                        var arrayLengthRes = RespReadUtils.TryReadUnsignedArrayLength(out var itemCount, ref cur, end);
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
        /// Clear timeout state before running.
        /// </summary>
        private unsafe void ResetTimeout()
        => state.TrySetHook(null, 0, 0);

        /// <summary>
        /// Request that the current execution of this <see cref="LuaRunner"/> timeout.
        /// </summary>
        internal unsafe void RequestTimeout()
        => state.TrySetHook(&LuaRunnerTrampolines.ForceTimeout, LuaHookMask.Count, 1);

        /// <summary>
        /// Raises a Lua error reporting that the script has timed out.
        /// 
        /// If you call this outside of PCALL context, the process will crash.
        /// </summary>
        internal void UnsafeForceTimeout()
        => state.RaiseError("ERR Lua script exceeded configured timeout");

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
                        while (!RespWriteUtils.TryWriteError("ERR An error occurred while invoking a Lua script"u8, ref resp.BufferCur, resp.BufferEnd))
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
                            while (!RespWriteUtils.TryWriteError(errBuf, ref resp.BufferCur, resp.BufferEnd))
                                resp.SendAndReset();
                        }
                        else
                        {
                            // Otherwise, this is probably a Lua error - and those aren't very descriptive
                            // So slap some more information in

                            while (!RespWriteUtils.TryWriteDirect("-ERR Lua encountered an error: "u8, ref resp.BufferCur, resp.BufferEnd))
                                resp.SendAndReset();

                            while (!RespWriteUtils.TryWriteDirect(errBuf, ref resp.BufferCur, resp.BufferEnd))
                                resp.SendAndReset();

                            while (!RespWriteUtils.TryWriteDirect("\r\n"u8, ref resp.BufferCur, resp.BufferEnd))
                                resp.SendAndReset();
                        }

                        state.Pop(1);

                        return;
                    }
                    else
                    {
                        logger?.LogError("Got an unexpected number of values back from a pcall error {callRes}", callRes);

                        while (!RespWriteUtils.TryWriteError("ERR Unexpected error response"u8, ref resp.BufferCur, resp.BufferEnd))
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
                while (!RespWriteUtils.TryWriteNull(ref resp.BufferCur, resp.BufferEnd))
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

                while (!RespWriteUtils.TryWriteInt64(num, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                runner.state.Pop(1);
            }

            // Writes the string on the top of the stack, removes it from the stack
            static void WriteString(LuaRunner runner, ref TResponse resp)
            {
                runner.state.KnownStringToBuffer(runner.state.StackTop, out var buf);

                while (!RespWriteUtils.TryWriteBulkString(buf, ref resp.BufferCur, resp.BufferEnd))
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
                    while (!RespWriteUtils.TryWriteInt32(1, ref resp.BufferCur, resp.BufferEnd))
                        resp.SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.TryWriteNull(ref resp.BufferCur, resp.BufferEnd))
                        resp.SendAndReset();
                }

                runner.state.Pop(1);
            }

            // Writes the string on the top of the stack out as an error, removes the string from the stack
            static void WriteError(LuaRunner runner, ref TResponse resp)
            {
                runner.state.KnownStringToBuffer(runner.state.StackTop, out var errBuff);

                while (!RespWriteUtils.TryWriteError(errBuff, ref resp.BufferCur, resp.BufferEnd))
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

                while (!RespWriteUtils.TryWriteArrayLength(trueLen, ref resp.BufferCur, resp.BufferEnd))
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

        /// <summary>
        /// Construct a bitmap we can quickly check for NoScript commands in.
        /// </summary>
        /// <returns></returns>
        private static (int Start, ulong[] Bitmap) InitializeNoScriptDetails()
        {
            if (!RespCommandsInfo.TryGetRespCommandsInfo(out var allCommands, externalOnly: true))
            {
                throw new InvalidOperationException("Could not build NoScript bitmap");
            }

            var noScript =
                allCommands
                    .Where(static kv => kv.Value.Flags.HasFlag(RespCommandFlags.NoScript))
                    .Select(static kv => kv.Value.Command)
                    .OrderBy(static x => x)
                    .ToList();

            var start = (int)noScript[0];
            var end = (int)noScript[^1];
            var size = end - start + 1;
            var numULongs = size / sizeof(ulong);
            if ((size % numULongs) != 0)
            {
                numULongs++;
            }

            var bitmap = new ulong[numULongs];
            foreach (var member in noScript)
            {
                var asInt = (int)member;
                var stepped = asInt - start;
                var ulongIndex = stepped / sizeof(ulong);
                var bitIndex = stepped % sizeof(ulong);

                bitmap[ulongIndex] |= 1UL << bitIndex;
            }

            return (start, bitmap);
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
        private static LuaRunner CallbackContext;

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
            Debug.Assert(CallbackContext == null, "Expected null context");
            CallbackContext = context;
        }

        /// <summary>
        /// Clear a previously set 
        /// </summary>
        internal static void ClearCallbackContext(LuaRunner context)
        {
            Debug.Assert(ReferenceEquals(CallbackContext, context), "Expected context to match");
            CallbackContext = null;
        }

        /// <summary>
        /// Entry point for Lua PCall'ing into <see cref="LuaRunner.UnsafeCompileForRunner"/>.
        /// 
        /// We need this indirection to allow Lua to detect and report internal and memory errors
        /// without crashing the process.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int CompileForRunner(nint _)
        => CallbackContext.UnsafeCompileForRunner();

        /// <summary>
        /// Entry point for Lua PCall'ing into <see cref="LuaRunner.UnsafeCompileForSession"/>.
        /// 
        /// We need this indirection to allow Lua to detect and report internal and memory errors
        /// without crashing the process.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int CompileForSession(nint _)
        => CallbackContext.UnsafeCompileForSession();

        /// <summary>
        /// Entry point for Lua PCall'ing into <see cref="LuaRunner.UnsafeRunPreambleForRunner"/>.
        /// 
        /// We need this indirection to allow Lua to detect and report internal and memory errors
        /// without crashing the process.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int RunPreambleForRunner(nint _)
        => CallbackContext.UnsafeRunPreambleForRunner();

        /// <summary>
        /// Entry point for Lua PCall'ing into <see cref="LuaRunner.UnsafeRunPreambleForSession"/>.
        /// 
        /// We need this indirection to allow Lua to detect and report internal and memory errors
        /// without crashing the process.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int RunPreambleForSession(nint _)
        => CallbackContext.UnsafeRunPreambleForSession();

        /// <summary>
        /// Entry point for Lua calling back into Garnet via redis.call(...).
        /// 
        /// This entry point is for when there isn't an active <see cref="RespServerSession"/>.
        /// This should only happen during testing.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int GarnetCallNoSession(nint luaState)
        => CallbackContext.NoSessionResponse(luaState);

        /// <summary>
        /// Entry point for Lua calling back into Garnet via redis.call(...).
        /// 
        /// This entry point is for when a transaction is in effect.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int GarnetCallWithTransaction(nint luaState)
        => CallbackContext.GarnetCallWithTransaction(luaState);

        /// <summary>
        /// Entry point for Lua calling back into Garnet via redis.call(...).
        /// 
        /// This entry point is for when a transaction is not necessary.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int GarnetCallNoTransaction(nint luaState)
        => CallbackContext.GarnetCall(luaState);

        /// <summary>
        /// Entry point for checking timeouts, called periodically from Lua.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "Callback must take these parameters")]
        internal static void ForceTimeout(nint luaState, nint debugState)
        => CallbackContext?.UnsafeForceTimeout();

        /// <summary>
        /// Entry point for calls to redis.sha1hex.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int SHA1Hex(nint luaState)
        => CallbackContext.SHA1Hex(luaState);

        /// <summary>
        /// Entry point for calls to redis.log.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Log(nint luaState)
        => CallbackContext.Log(luaState);

        /// <summary>
        /// Entry point for calls to redis.acl_check_cmd.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int AclCheckCommand(nint luaState)
        => CallbackContext.AclCheckCommand(luaState);

        /// <summary>
        /// Entry point for calls to redis.setresp.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int SetResp(nint luaState)
        => CallbackContext.SetResp(luaState);
    }
}