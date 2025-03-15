// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
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
            /// What version of the RESP protocol to write responses out as.
            /// </summary>
            byte RespProtocolVersion { get; }

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
            public byte RespProtocolVersion
            => session.respProtocolVersion;

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

            /// <inheritdoc />
            public byte RespProtocolVersion { get; } = 2;

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

        /// <summary>
        /// Just to DRY it up some, a holding type for all the constant strings we pre-load into
        /// the Lua VM.
        /// </summary>
        private readonly struct ConstantStringRegistryIndexes
        {
            /// <see cref="CmdStrings.LUA_OK"/>
            internal int Ok { get; }
            /// <see cref="CmdStrings.LUA_ok"/>
            internal int OkLower { get; }
            /// <see cref="CmdStrings.LUA_err"/>
            internal int Err { get; }
            /// <see cref="CmdStrings.LUA_No_session_available"/>
            internal int NoSessionAvailable { get; }
            /// <see cref="CmdStrings.LUA_ERR_Please_specify_at_least_one_argument_for_this_redis_lib_call"/>
            internal int PleaseSpecifyRedisCall { get; }
            /// <see cref="CmdStrings.RESP_ERR_NOAUTH"/>
            internal int ErrNoAuth { get; }
            /// <see cref="CmdStrings.LUA_ERR_Unknown_Redis_command_called_from_script"/>
            internal int ErrUnknown { get; }
            /// <see cref="CmdStrings.LUA_ERR_Lua_redis_lib_command_arguments_must_be_strings_or_integers"/>
            internal int ErrBadArg { get; }
            /// <see cref="CmdStrings.Lua_ERR_wrong_number_of_arguments"/>
            internal int ErrWrongNumberOfArgs { get; }
            /// <see cref="CmdStrings.Lua_ERR_redis_log_requires_two_arguments_or_more"/>
            internal int ErrRedisLogRequired { get; }
            /// <see cref="CmdStrings.Lua_ERR_First_argument_must_be_a_number_log_level"/>
            internal int ErrFirstArgMustBeNumber { get; }
            /// <see cref="CmdStrings.Lua_ERR_Invalid_debug_level"/>
            internal int ErrInvalidDebugLevel { get; }
            /// <see cref="CmdStrings.Lua_ERR_Invalid_command_passed_to_redis_acl_check_cmd"/>
            internal int ErrInvalidCommand { get; }
            /// <see cref="CmdStrings.Lua_ERR_redis_setresp_requires_one_argument"/>
            internal int ErrRedisSetRespArg { get; }
            /// <see cref="CmdStrings.Lua_ERR_RESP_version_must_be_2_or_3"/>
            internal int ErrRespVersion { get; }
            /// <see cref="CmdStrings.Lua_ERR_redis_log_disabled"/>
            internal int ErrLoggingDisabled { get; }
            /// <see cref="CmdStrings.Lua_double"/>
            internal int Double { get; }
            /// <see cref="CmdStrings.Lua_map"/>
            internal int Map { get; }
            /// <see cref="CmdStrings.Lua_set"/>
            internal int Set { get; }
            /// <see cref="CmdStrings.Lua_big_number"/>
            internal int BigNumber { get; }
            /// <see cref="CmdStrings.Lua_format"/>
            internal int Format { get; }
            /// <see cref="CmdStrings.Lua_string"/>
            internal int String { get; }

            internal ConstantStringRegistryIndexes(ref LuaStateWrapper state)
            {
                // Commonly used strings, register them once so we don't have to copy them over each time we need them
                //
                // As a side benefit, we don't have to worry about reserving memory for them either during normal operation
                Ok = ConstantStringToRegistry(ref state, CmdStrings.LUA_OK);
                OkLower = ConstantStringToRegistry(ref state, CmdStrings.LUA_ok);
                Err = ConstantStringToRegistry(ref state, CmdStrings.LUA_err);
                NoSessionAvailable = ConstantStringToRegistry(ref state, CmdStrings.LUA_No_session_available);
                PleaseSpecifyRedisCall = ConstantStringToRegistry(ref state, CmdStrings.LUA_ERR_Please_specify_at_least_one_argument_for_this_redis_lib_call);
                ErrNoAuth = ConstantStringToRegistry(ref state, CmdStrings.RESP_ERR_NOAUTH);
                ErrUnknown = ConstantStringToRegistry(ref state, CmdStrings.LUA_ERR_Unknown_Redis_command_called_from_script);
                ErrBadArg = ConstantStringToRegistry(ref state, CmdStrings.LUA_ERR_Lua_redis_lib_command_arguments_must_be_strings_or_integers);
                ErrWrongNumberOfArgs = ConstantStringToRegistry(ref state, CmdStrings.Lua_ERR_wrong_number_of_arguments);
                ErrRedisLogRequired = ConstantStringToRegistry(ref state, CmdStrings.Lua_ERR_redis_log_requires_two_arguments_or_more);
                ErrFirstArgMustBeNumber = ConstantStringToRegistry(ref state, CmdStrings.Lua_ERR_First_argument_must_be_a_number_log_level);
                ErrInvalidDebugLevel = ConstantStringToRegistry(ref state, CmdStrings.Lua_ERR_Invalid_debug_level);
                ErrInvalidCommand = ConstantStringToRegistry(ref state, CmdStrings.Lua_ERR_Invalid_command_passed_to_redis_acl_check_cmd);
                ErrRedisSetRespArg = ConstantStringToRegistry(ref state, CmdStrings.Lua_ERR_redis_setresp_requires_one_argument);
                ErrRespVersion = ConstantStringToRegistry(ref state, CmdStrings.Lua_ERR_RESP_version_must_be_2_or_3);
                ErrLoggingDisabled = ConstantStringToRegistry(ref state, CmdStrings.Lua_ERR_redis_log_disabled);
                Double = ConstantStringToRegistry(ref state, CmdStrings.Lua_double);
                Map = ConstantStringToRegistry(ref state, CmdStrings.Lua_map);
                Set = ConstantStringToRegistry(ref state, CmdStrings.Lua_set);
                BigNumber = ConstantStringToRegistry(ref state, CmdStrings.Lua_big_number);
                Format = ConstantStringToRegistry(ref state, CmdStrings.Lua_format);
                String = ConstantStringToRegistry(ref state, CmdStrings.Lua_string);
            }

            /// <summary>
            /// Some strings we use a bunch, and copying them to Lua each time is wasteful
            ///
            /// So instead we stash them in the Registry and load them by index
            /// </summary>
            private int ConstantStringToRegistry(ref LuaStateWrapper state, ReadOnlySpan<byte> str)
            {
                state.PushBuffer(str);
                return state.Ref();
            }
        }

        /// <summary>
        /// Simple cache of allowed functions to loader block.
        /// </summary>
        private sealed record LoaderBlockCache(HashSet<string> AllowedFunctions, ReadOnlyMemory<byte> LoaderBlockBytes);

        private const string LoaderBlock = @"
-- globals to fill in on each invocation
KEYS = {}
ARGV = {}

-- disable for sandboxing purposes
import = function () end

-- cutdown os for sandboxing purposes
local osClockRef = os.clock
os = {
    clock = osClockRef
}

-- define cjson for (optional) inclusion into sandbox_env
local cjson = {
    encode = garnet_cjson_encode;
    decode = garnet_cjson_decode;
}

-- define bit for (optional) inclusion into sandbox_env
local bit = {
    tobit = garnet_bit_tobit;
    tohex = garnet_bit_tohex;
    bnot = function(...) return garnet_bitop(0, ...); end;
    bor = function(...) return garnet_bitop(1, ...); end;
    band = function(...) return garnet_bitop(2, ...); end;
    bxor = function(...) return garnet_bitop(3, ...); end;
    lshift = function(...) return garnet_bitop(4, ...); end;
    rshift = function(...) return garnet_bitop(5, ...); end;
    arshift = function(...) return garnet_bitop(6, ...); end;
    rol = function(...) return garnet_bitop(7, ...); end;
    ror = function(...) return garnet_bitop(8, ...); end;
    bswap = garnet_bit_bswap;
}

-- define cmsgpack for (optional) inclusion into sandbox_env
local cmsgpack = {
    pack = garnet_cmsgpack_pack;
    unpack = garnet_cmsgpack_unpack;
}

-- define struct for (optional) inclusion into sandbox_env
local struct = {
    pack = string.pack;
    unpack = string.unpack;
    size = string.packsize;
}

-- define redis for (optional, but almost always) inclusion into sandbox_env
local garnetCallRef = garnet_call
local pCallRef = pcall
local redis = {
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

    sha1hex = garnet_sha1hex,

    LOG_DEBUG = 0,
    LOG_VERBOSE = 1,
    LOG_NOTICE = 2,
    LOG_WARNING = 3,

    log = garnet_log,

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

    acl_check_cmd = garnet_acl_check_cmd,
    setresp = garnet_setresp,

    REDIS_VERSION = garnet_REDIS_VERSION,
    REDIS_VERSION_NUM = garnet_REDIS_VERSION_NUM
}

-- unpack moved after Lua 5.1, this provides Redis compat
local unpack = table.unpack

-- added after Lua 5.1, removing to maintain Redis compat
string.pack = nil
string.unpack = nil
string.packsize = nil
math.maxinteger = nil
math.type = nil
math.mininteger = nil
math.tointeger = nil
math.ult = nil
table.pack = nil
table.unpack = nil
table.move = nil

-- in Lua 5.1 but not 5.4, so implemented on the .NET side
local loadstring = garnet_loadstring
math.atan2 = garnet_atan2
math.cosh = garnet_cosh
math.frexp = garnet_frexp
math.ldexp = garnet_ldexp
math.log10 = garnet_log10
math.pow = garnet_pow
math.sinh = garnet_sinh
math.tanh = garnet_tanh
table.maxn = garnet_maxn

local collectgarbageRef = collectgarbage
local setMetatableRef = setmetatable
local rawsetRef = rawset

-- prevent modification to metatables for readonly tables
-- Redis accomplishes this by patching Lua, we'd rather ship
-- vanilla Lua and do it in code
local setmetatable = function(table, metatable)
    if table and table.__readonly then
        error('Attempt to modify a readonly table', 0)
    end

    return setMetatableRef(table, metatable)
end

-- prevent bypassing metatables to update readonly tables
-- as above, Redis prevents this with a patch to Lua
local rawset = function(table, key, value)
    if table and table.__readonly then
        error('Attempt to modify a readonly table', 0)
    end

    return rawsetRef(table, key, value)
end

-- technically deprecated in 5.1, but available in Redis
-- this is only 'sort of' correct as 5.4 doesn't expose the same
-- gc primitives
local gcinfo = function()
    return collectgarbageRef('count'), 0
end

-- global object used for the sandbox environment
--
-- replacements are performed before VM initialization
-- to allow configuring available functions
sandbox_env = {
    _VERSION = _VERSION;

    KEYS = KEYS;
    ARGV = ARGV;

!!SANDBOX_ENV REPLACEMENT TARGET!!
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

    setMetatableRef(table, readonly_metatable)
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
    recursively_readonly_table(sandbox_env)

    local rawFunc, err = load(source, nil, nil, sandbox_env)

    return err, rawFunc
end
";
        private static readonly HashSet<string> DefaultAllowedFunctions = [
            // Built ins
            "assert",
            "collectgarbage",
            "coroutine",
            "error",
            "gcinfo",
            // Intentionally not supporting getfenv, as it's too weird to backport to Lua 5.4
            "getmetatable",
            "ipairs",
            "load",
            "loadstring",
            "math",
            "next",
            "pairs",
            "pcall",
            "rawequal",
            "rawget",
            // Note rawset is proxied to implement readonly tables
            "rawset",
            "select",
            // Intentionally not supporting setfenv, as it's too weird to backport to Lua 5.4
            // Note setmetatable is proxied to implement readonly tables
            "setmetatable",
            "string",
            "table",
            "tonumber",
            "tostring",
            "type",
            // Note unpack is actually table.unpack, and defined in the loader block
            "unpack",
            "xpcall",

            // Runtime libs
            "bit",
            "cjson",
            "cmsgpack",
            // Note os only contains clock due to definition in the loader block
            "os",
            // Note struct is actually implemented by Lua 5.4's string.pack/unpack/size
            "struct",

            // Interface force communicating back with Garnet
            "redis",
        ];

        private static LoaderBlockCache CachedLoaderBlock;

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
        readonly ConstantStringRegistryIndexes constStrs;

        readonly LuaLoggingMode logMode;
        readonly HashSet<string> allowedFunctions;
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
            HashSet<string> allowedFunctions,
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
            this.allowedFunctions = allowedFunctions;
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

            // Lua 5.4 does not provide these functions, but 5.1 does - so implement tehm
            state.Register("garnet_atan2\0"u8, &LuaRunnerTrampolines.Atan2);
            state.Register("garnet_cosh\0"u8, &LuaRunnerTrampolines.Cosh);
            state.Register("garnet_frexp\0"u8, &LuaRunnerTrampolines.Frexp);
            state.Register("garnet_ldexp\0"u8, &LuaRunnerTrampolines.Ldexp);
            state.Register("garnet_log10\0"u8, &LuaRunnerTrampolines.Log10);
            state.Register("garnet_pow\0"u8, &LuaRunnerTrampolines.Pow);
            state.Register("garnet_sinh\0"u8, &LuaRunnerTrampolines.Sinh);
            state.Register("garnet_tanh\0"u8, &LuaRunnerTrampolines.Tanh);
            state.Register("garnet_maxn\0"u8, &LuaRunnerTrampolines.Maxn);
            state.Register("garnet_loadstring\0"u8, &LuaRunnerTrampolines.LoadString);

            // Things provided as Lua libraries, which we actually implement in .NET
            state.Register("garnet_cjson_encode\0"u8, &LuaRunnerTrampolines.CJsonEncode);
            state.Register("garnet_cjson_decode\0"u8, &LuaRunnerTrampolines.CJsonDecode);
            state.Register("garnet_bit_tobit\0"u8, &LuaRunnerTrampolines.BitToBit);
            state.Register("garnet_bit_tohex\0"u8, &LuaRunnerTrampolines.BitToHex);
            // garnet_bitop implements bnot, bor, band, xor, etc. but isn't directly exposed
            state.Register("garnet_bitop\0"u8, &LuaRunnerTrampolines.Bitop);
            state.Register("garnet_bit_bswap\0"u8, &LuaRunnerTrampolines.BitBswap);
            state.Register("garnet_cmsgpack_pack\0"u8, &LuaRunnerTrampolines.CMsgPackPack);
            state.Register("garnet_cmsgpack_unpack\0"u8, &LuaRunnerTrampolines.CMsgPackUnpack);
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

            var loadRes = state.LoadBuffer(PrepareLoaderBlockBytes(allowedFunctions).Span);
            if (loadRes != LuaStatus.OK)
            {
                if (state.StackTop == 1 && state.CheckBuffer(1, out var buff))
                {
                    var innerError = Encoding.UTF8.GetString(buff);
                    throw new GarnetException($"Could initialize Lua VM: {innerError}");
                }

                throw new GarnetException("Could initialize Lua VM");
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

            state.GetGlobal(LuaType.Table, "KEYS\0"u8);
            keysTableRegistryIndex = state.Ref();

            state.GetGlobal(LuaType.Table, "ARGV\0"u8);
            argvTableRegistryIndex = state.Ref();

            state.GetGlobal(LuaType.Function, "load_sandboxed\0"u8);
            loadSandboxedRegistryIndex = state.Ref();

            state.GetGlobal(LuaType.Function, "reset_keys_and_argv\0"u8);
            resetKeysAndArgvRegistryIndex = state.Ref();

            // Load all the constant strings into the VM
            constStrs = new(ref state);

            state.ExpectLuaStackEmpty();
        }

        /// <summary>
        /// Creates a new runner with the source of the script
        /// </summary>
        public LuaRunner(LuaOptions options, string source, bool txnMode = false, RespServerSession respServerSession = null, ScratchBufferNetworkSender scratchBufferNetworkSender = null, string redisVersion = "0.0.0.0", ILogger logger = null)
            : this(options.MemoryManagementMode, options.GetMemoryLimitBytes(), options.LogMode, options.AllowedFunctions, Encoding.UTF8.GetBytes(source), txnMode, respServerSession, scratchBufferNetworkSender, redisVersion, logger)
        {
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

                return functionRegistryIndex != -1;
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
                state.PushConstantString(constStrs.ErrWrongNumberOfArgs);
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
                return LuaStaticError(constStrs.ErrRedisLogRequired);
            }

            if (state.Type(1) != LuaType.Number)
            {
                return LuaStaticError(constStrs.ErrFirstArgMustBeNumber);
            }

            var rawLevel = state.CheckNumber(1);
            if (rawLevel is not (0 or 1 or 2 or 3))
            {
                return LuaStaticError(constStrs.ErrInvalidDebugLevel);
            }

            if (logMode == LuaLoggingMode.Disable)
            {
                return LuaStaticError(constStrs.ErrLoggingDisabled);
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
        /// Entry point for math.atan2 from a Lua script.
        /// </summary>
        public int Atan2(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 2 || state.Type(1) != LuaType.Number || state.Type(2) != LuaType.Number)
            {
                return state.RaiseError("bad argument to atan2");
            }

            var x = state.CheckNumber(1);
            var y = state.CheckNumber(2);

            var res = Math.Atan2(x, y);
            state.Pop(2);
            state.PushNumber(res);
            return 1;
        }

        /// <summary>
        /// Entry point for math.cosh from a Lua script.
        /// </summary>
        public int Cosh(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1 || state.Type(1) != LuaType.Number)
            {
                return state.RaiseError("bad argument to cosh");
            }

            var value = state.CheckNumber(1);

            var res = Math.Cosh(value);
            state.Pop(1);
            state.PushNumber(res);
            return 1;
        }

        /// <summary>
        /// Entry point for math.frexp from a Lua script.
        /// </summary>
        public int Frexp(nint luaStatePtr)
        {
            // Based on: https://github.com/MachineCognitis/C.math.NET/ (MIT License)

            const long DBL_EXP_MASK = 0x7FF0000000000000L;
            const int DBL_MANT_BITS = 52;
            const long DBL_SGN_MASK = -1 - 0x7FFFFFFFFFFFFFFFL;
            const long DBL_MANT_MASK = 0x000FFFFFFFFFFFFFL;
            const long DBL_EXP_CLR_MASK = DBL_SGN_MASK | DBL_MANT_MASK;

            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1 || state.Type(1) != LuaType.Number)
            {
                return state.RaiseError("bad argument to frexp");
            }

            var number = state.CheckNumber(1);

            var bits = BitConverter.DoubleToInt64Bits(number);
            var exp = (int)((bits & DBL_EXP_MASK) >> DBL_MANT_BITS);
            var exponent = 0;

            if (exp == 0x7FF || number == 0D)
            {
                number += number;
            }
            else
            {
                // Not zero and finite.
                exponent = exp - 1022;
                if (exp == 0)
                {
                    // Subnormal, scale number so that it is in [1, 2).
                    number *= BitConverter.Int64BitsToDouble(0x4350000000000000L); // 2^54
                    bits = BitConverter.DoubleToInt64Bits(number);
                    exp = (int)((bits & DBL_EXP_MASK) >> DBL_MANT_BITS);
                    exponent = exp - 1022 - 54;
                }
                // Set exponent to -1 so that number is in [0.5, 1).
                number = BitConverter.Int64BitsToDouble((bits & DBL_EXP_CLR_MASK) | 0x3FE0000000000000L);
            }

            state.ForceMinimumStackCapacity(2);
            state.Pop(1);

            var numberAsFloat = (float)number;

            if ((long)numberAsFloat == numberAsFloat)
            {
                state.PushInteger((long)numberAsFloat);
            }
            else
            {
                state.PushNumber(number);
            }

            state.PushInteger(exponent);

            return 2;
        }

        /// <summary>
        /// Entry point for math.ldexp from a Lua script.
        /// </summary>
        public int Ldexp(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 2 || state.Type(1) != LuaType.Number || state.Type(2) != LuaType.Number)
            {
                return state.RaiseError("bad argument to ldexp");
            }

            var m = state.CheckNumber(1);
            var e = (int)state.CheckNumber(2);

            var res = m * Math.Pow(2, e);

            state.Pop(2);

            if ((long)res == res)
            {
                state.PushInteger((long)res);
            }
            else
            {
                state.PushNumber(res);
            }

            return 1;
        }

        /// <summary>
        /// Entry point for math.log10 from a Lua script.
        /// </summary>
        public int Log10(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1 || state.Type(1) != LuaType.Number)
            {
                return state.RaiseError("bad argument to log10");
            }

            var val = state.CheckNumber(1);

            var res = Math.Log10(val);

            state.Pop(1);
            state.PushNumber(res);
            return 1;
        }

        /// <summary>
        /// Entry point for math.pow from a Lua script.
        /// </summary>
        public int Pow(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 2 || state.Type(1) != LuaType.Number || state.Type(2) != LuaType.Number)
            {
                return state.RaiseError("bad argument to pow");
            }

            var x = state.CheckNumber(1);
            var y = state.CheckNumber(2);

            var res = Math.Pow(x, y);

            state.Pop(2);
            state.PushNumber(res);
            return 1;
        }

        /// <summary>
        /// Entry point for math.sinh from a Lua script.
        /// </summary>
        public int Sinh(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1 || state.Type(1) != LuaType.Number)
            {
                return state.RaiseError("bad argument to sinh");
            }

            var val = state.CheckNumber(1);

            var res = Math.Sinh(val);

            state.Pop(1);
            state.PushNumber(res);
            return 1;
        }

        /// <summary>
        /// Entry point for math.sinh from a Lua script.
        /// </summary>
        public int Tanh(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1 || state.Type(1) != LuaType.Number)
            {
                return state.RaiseError("bad argument to tanh");
            }

            var val = state.CheckNumber(1);

            var res = Math.Tanh(val);

            state.Pop(1);
            state.PushNumber(res);
            return 1;
        }

        /// <summary>
        /// Entry point for table.maxn from a Lua script.
        /// </summary>
        public int Maxn(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1 || state.Type(1) != LuaType.Table)
            {
                return state.RaiseError("bad argument to maxn");
            }

            state.ForceMinimumStackCapacity(2);

            double res = 0;

            // Initial key value onto stack
            state.PushNil();
            while (state.Next(1) != 0)
            {
                // Remove value
                state.Pop(1);

                double keyVal;
                if (state.Type(2) == LuaType.Number && (keyVal = state.CheckNumber(2)) > res)
                {
                    res = keyVal;
                }
            }

            // Remove table, and push largest number
            state.Pop(1);
            state.PushNumber(res);
            return 1;
        }

        /// <summary>
        /// Entry point for loadstring from a Lua script.
        /// </summary>
        public int LoadString(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (
                (luaArgCount == 1 && state.Type(1) != LuaType.String) ||
                (luaArgCount == 2 && (state.Type(1) != LuaType.String || state.Type(2) != LuaType.String)) ||
                (luaArgCount > 2)
              )
            {
                return state.RaiseError("bad argument to loadstring");
            }

            // Ignore chunk name
            if (luaArgCount == 2)
            {
                state.Pop(1);
            }

            _ = state.CheckBuffer(1, out var buff);
            if (buff.Contains((byte)0))
            {
                return state.RaiseError("bad argument to loadstring, interior null byte");
            }

            state.ForceMinimumStackCapacity(1);

            var res = state.LoadString(buff);
            if (res != LuaStatus.OK)
            {
                state.ClearStack();
                state.PushNil();
                state.PushBuffer("load_string encountered error"u8);
                return 2;
            }

            return 1;
        }

        /// <summary>
        /// Converts a Lua number (ie. a double) into the expected 32-bit integer for
        /// bit operations.
        /// </summary>
        private static int LuaNumberToBitValue(double value)
        {
            var scaled = value + 6_755_399_441_055_744.0;
            var asULong = BitConverter.DoubleToUInt64Bits(scaled);
            var asUInt = (uint)asULong;

            return (int)asUInt;
        }

        /// <summary>
        /// Entry point for bit.tobit from a Lua script.
        /// </summary>
        public int BitToBit(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount < 1 || state.Type(1) != LuaType.Number)
            {
                return state.RaiseError("bad argument to tobit");
            }

            var rawValue = state.CheckNumber(1);

            // Make space on the stack
            state.Pop(1);

            state.PushNumber(LuaNumberToBitValue(rawValue));

            return 1;
        }

        /// <summary>
        /// Entry point for bit.tohex from a Lua script.
        /// </summary>
        public int BitToHex(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount == 0 || state.Type(1) != LuaType.Number)
            {
                return state.RaiseError("bad argument to tohex");
            }

            var numDigits = 8;

            if (luaArgCount == 2)
            {
                if (state.Type(2) != LuaType.Number)
                {
                    return state.RaiseError("bad argument to tohex");
                }

                numDigits = (int)state.CheckNumber(2);
            }

            var value = LuaNumberToBitValue(state.CheckNumber(1));

            ReadOnlySpan<byte> hexBytes;
            if (numDigits == int.MinValue)
            {
                numDigits = 8;
                hexBytes = "0123456789ABCDEF"u8;
            }
            else if (numDigits < 0)
            {
                numDigits = -numDigits;
                hexBytes = "0123456789ABCDEF"u8;
            }
            else
            {
                hexBytes = "0123456789abcdef"u8;
            }

            if (numDigits > 8)
            {
                numDigits = 8;
            }

            Span<byte> buff = stackalloc byte[numDigits];
            for (var i = buff.Length - 1; i >= 0; i--)
            {
                buff[i] = hexBytes[value & 0xF];
                value >>= 4;
            }

            // Free up space on stack
            state.Pop(luaArgCount);

            state.PushBuffer(buff);
            return 1;
        }

        /// <summary>
        /// Entry point for bit.bswap from a Lua script.
        /// </summary>
        public int BitBswap(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1 || state.Type(1) != LuaType.Number)
            {
                return state.RaiseError("bad argument to bswap");
            }

            var value = LuaNumberToBitValue(state.CheckNumber(1));

            // Free up space on stack
            state.Pop(1);

            var swapped = BinaryPrimitives.ReverseEndianness(value);
            state.PushNumber(swapped);
            return 1;
        }

        /// <summary>
        /// Entry point for garnet_bitop from a Lua script.
        /// 
        /// Used to implement bit.bnot, bit.bor, bit.band, etc.
        /// </summary>
        public int Bitop(nint luaStatePtr)
        {
            const int BNot = 0;
            const int BOr = 1;
            const int BAnd = 2;
            const int BXor = 3;
            const int LShift = 4;
            const int RShift = 5;
            const int ARShift = 6;
            const int Rol = 7;
            const int Ror = 8;

            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount == 0 || state.Type(1) != LuaType.Number)
            {
                return state.RaiseError("bitop was not indicated, should never happen");
            }

            var bitop = (int)state.CheckNumber(1);
            if (bitop < BNot || bitop > Ror)
            {
                return state.RaiseError($"invalid bitop {bitop} was indicated, should never happen");
            }

            // Handle bnot specially
            if (bitop == BNot)
            {
                if (luaArgCount < 2 || state.Type(2) != LuaType.Number)
                {
                    return state.RaiseError("bad argument to bnot");
                }

                var val = LuaNumberToBitValue(state.CheckNumber(2));
                var res = ~val;
                state.Pop(2);

                state.PushNumber(res);
                return 1;
            }

            var binOpName =
                bitop switch
                {
                    BOr => "bor",
                    BAnd => "band",
                    BXor => "bxor",
                    LShift => "lshift",
                    RShift => "rshift",
                    ARShift => "arshift",
                    Rol => "rol",
                    _ => "ror",
                };

            if (luaArgCount < 2)
            {
                return state.RaiseError($"bad argument to {binOpName}");
            }

            if (bitop is BOr or BAnd or BXor)
            {
                var ret =
                    bitop switch
                    {
                        BOr => 0,
                        BXor => 0,
                        _ => -1,
                    };

                for (var argIx = 2; argIx <= luaArgCount; argIx++)
                {
                    if (state.Type(argIx) != LuaType.Number)
                    {
                        return state.RaiseError($"bad argument to {binOpName}");
                    }

                    var nextValue = LuaNumberToBitValue(state.CheckNumber(argIx));

                    ret =
                        bitop switch
                        {
                            BOr => ret | nextValue,
                            BXor => ret ^ nextValue,
                            _ => ret & nextValue,
                        };
                }

                state.Pop(luaArgCount);
                state.PushNumber(ret);

                return 1;
            }

            if (luaArgCount < 3 || state.Type(2) != LuaType.Number || state.Type(3) != LuaType.Number)
            {
                return state.RaiseError($"bad argument to {binOpName}");
            }

            var x = LuaNumberToBitValue(state.CheckNumber(2));
            var n = ((int)state.CheckNumber(3)) & 0b1111;

            var shiftRes =
                bitop switch
                {
                    LShift => x << n,
                    RShift => (int)((uint)x >> n),
                    ARShift => x >> n,
                    Rol => (int)BitOperations.RotateLeft((uint)x, n),
                    _ => (int)BitOperations.RotateRight((uint)x, n),
                };

            state.Pop(luaArgCount);
            state.PushNumber(shiftRes);
            return 1;
        }

        /// <summary>
        /// Entry point for cjson.encode from a Lua script.
        /// </summary>
        public int CJsonEncode(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1)
            {
                return state.RaiseError("bad argument to encode");
            }

            Encode(this, 0);

            // Encoding should leave nothing on the stack
            state.ExpectLuaStackEmpty();

            // Push the encoded string
            var result = scratchBufferManager.ViewFullArgSlice().ReadOnlySpan;
            state.PushBuffer(result);

            return 1;

            // Encode the unknown type on the top of the stack
            static void Encode(LuaRunner self, int depth)
            {
                if (depth > 1000)
                {
                    // Match Redis max decoding depth
                    _ = self.state.RaiseError("Cannot serialise, excessive nesting (1001)");
                }

                var argType = self.state.Type(self.state.StackTop);

                switch (argType)
                {
                    case LuaType.Boolean:
                        EncodeBool(self);
                        break;
                    case LuaType.Nil:
                        EncodeNull(self);
                        break;
                    case LuaType.Number:
                        EncodeNumber(self);
                        break;
                    case LuaType.String:
                        EncodeString(self);
                        break;
                    case LuaType.Table:
                        EncodeTable(self, depth);
                        break;
                    case LuaType.Function:
                    case LuaType.LightUserData:
                    case LuaType.None:
                    case LuaType.Thread:
                    case LuaType.UserData:
                    default:
                        _ = self.state.RaiseError($"Cannot serialise {argType} to JSON");
                        break;
                }
            }

            // Encode the boolean on the top of the stack and remove it
            static void EncodeBool(LuaRunner self)
            {
                Debug.Assert(self.state.Type(self.state.StackTop) == LuaType.Boolean, "Expected boolean on top of stack");

                var data = self.state.ToBoolean(self.state.StackTop) ? "true"u8 : "false"u8;

                var into = self.scratchBufferManager.ViewRemainingArgSlice(data.Length).Span;
                data.CopyTo(into);
                self.scratchBufferManager.MoveOffset(data.Length);

                self.state.Pop(1);
            }

            // Encode the nil on the top of the stack and remove it
            static void EncodeNull(LuaRunner self)
            {
                Debug.Assert(self.state.Type(self.state.StackTop) == LuaType.Nil, "Expected nil on top of stack");

                var into = self.scratchBufferManager.ViewRemainingArgSlice(4).Span;
                "null"u8.CopyTo(into);
                self.scratchBufferManager.MoveOffset(4);

                self.state.Pop(1);
            }

            // Encode the number on the top of the stack and remove it
            static void EncodeNumber(LuaRunner self)
            {
                Debug.Assert(self.state.Type(self.state.StackTop) == LuaType.Number, "Expected number on top of stack");

                var number = self.state.CheckNumber(self.state.StackTop);

                Span<byte> space = stackalloc byte[64];

                if (!number.TryFormat(space, out var written, "G", CultureInfo.InvariantCulture))
                {
                    _ = self.state.RaiseError("Unable to format number");
                }

                var into = self.scratchBufferManager.ViewRemainingArgSlice(written).Span;
                space[..written].CopyTo(into);
                self.scratchBufferManager.MoveOffset(written);

                self.state.Pop(1);
            }

            // Encode the string on the top of the stack and remove it
            static void EncodeString(LuaRunner self)
            {
                Debug.Assert(self.state.Type(self.state.StackTop) == LuaType.String, "Expected string on top of stack");

                _ = self.state.CheckBuffer(self.state.StackTop, out var buff);

                self.scratchBufferManager.ViewRemainingArgSlice(1).Span[0] = (byte)'"';
                self.scratchBufferManager.MoveOffset(1);

                var escapeIx = buff.IndexOfAny((byte)'"', (byte)'\\');
                while (escapeIx != -1)
                {
                    var into = self.scratchBufferManager.ViewRemainingArgSlice(escapeIx + 2).Span;
                    buff[..escapeIx].CopyTo(into);

                    into[escapeIx] = (byte)'\\';

                    var toEscape = buff[escapeIx];
                    if (toEscape == (byte)'"')
                    {
                        into[escapeIx + 1] = (byte)'"';
                    }
                    else
                    {
                        into[escapeIx + 1] = (byte)'\\';
                    }

                    self.scratchBufferManager.MoveOffset(escapeIx + 2);

                    buff = buff[(escapeIx + 1)..];
                    escapeIx = buff.IndexOfAny((byte)'"', (byte)'\\');
                }

                var tailInto = self.scratchBufferManager.ViewRemainingArgSlice(buff.Length + 1).Span;
                buff.CopyTo(tailInto);
                tailInto[buff.Length] = (byte)'"';
                self.scratchBufferManager.MoveOffset(buff.Length + 1);

                self.state.Pop(1);
            }

            // Encode the table on the top of the stack and remove it
            static void EncodeTable(LuaRunner self, int depth)
            {
                Debug.Assert(self.state.Type(self.state.StackTop) == LuaType.Table, "Expected table on top of stack");

                // Space for key & value
                self.state.ForceMinimumStackCapacity(2);

                var tableIndex = self.state.StackTop;

                var isArray = false;
                var arrayLength = 0;

                self.state.PushNil();
                while (self.state.Next(tableIndex) != 0)
                {
                    // Pop value
                    self.state.Pop(1);

                    double keyAsNumber;
                    if (self.state.Type(tableIndex + 1) == LuaType.Number && (keyAsNumber = self.state.CheckNumber(tableIndex + 1)) >= 1 && keyAsNumber == (int)keyAsNumber)
                    {
                        if (keyAsNumber > arrayLength)
                        {
                            // Need at least one integer key >= 1 to consider this an array
                            isArray = true;
                            arrayLength = (int)keyAsNumber;
                        }
                    }
                    else
                    {
                        // Non-integer key, or integer <= 0, so it's not an array
                        isArray = false;

                        // Remove key
                        self.state.Pop(1);

                        break;
                    }
                }

                if (isArray)
                {
                    EncodeArray(self, arrayLength, depth);
                }
                else
                {
                    EncodeObject(self, depth);
                }
            }

            // Encode the table on the top of the stack as an array and remove it
            static void EncodeArray(LuaRunner self, int length, int depth)
            {
                Debug.Assert(self.state.Type(self.state.StackTop) == LuaType.Table, "Expected table on top of stack");

                // Space for value
                self.state.ForceMinimumStackCapacity(1);

                var tableIndex = self.state.StackTop;

                self.scratchBufferManager.ViewRemainingArgSlice(1).Span[0] = (byte)'[';
                self.scratchBufferManager.MoveOffset(1);

                for (var ix = 1; ix <= length; ix++)
                {
                    if (ix != 1)
                    {
                        self.scratchBufferManager.ViewRemainingArgSlice(1).Span[0] = (byte)',';
                        self.scratchBufferManager.MoveOffset(1);
                    }

                    _ = self.state.RawGetInteger(null, tableIndex, ix);
                    Encode(self, depth + 1);
                }

                self.scratchBufferManager.ViewRemainingArgSlice(1).Span[0] = (byte)']';
                self.scratchBufferManager.MoveOffset(1);

                // Remove table
                self.state.Pop(1);
            }

            // Encode the table on the top of the stack as an object and remove it
            static void EncodeObject(LuaRunner self, int depth)
            {
                Debug.Assert(self.state.Type(self.state.StackTop) == LuaType.Table, "Expected table on top of stack");

                // Space for key and value and a copy of key
                self.state.ForceMinimumStackCapacity(3);

                var tableIndex = self.state.StackTop;

                self.scratchBufferManager.ViewRemainingArgSlice(1).Span[0] = (byte)'{';
                self.scratchBufferManager.MoveOffset(1);

                var firstValue = true;

                self.state.PushNil();
                while (self.state.Next(tableIndex) != 0)
                {
                    LuaType keyType;
                    if ((keyType = self.state.Type(tableIndex + 1)) is not (LuaType.String or LuaType.Number))
                    {
                        // Ignore non-string-ify-abile keys

                        // Remove value
                        self.state.Pop(1);

                        continue;
                    }

                    if (!firstValue)
                    {
                        self.scratchBufferManager.ViewRemainingArgSlice(1).Span[0] = (byte)',';
                        self.scratchBufferManager.MoveOffset(1);
                    }

                    // Copy key to top of stack
                    self.state.PushValue(tableIndex + 1);

                    // Force the _copy_ of the key to be a string
                    // if it is not already one.
                    //
                    // We don't modify the original key value, so we
                    // can continue using it with Next(...)
                    if (keyType == LuaType.Number)
                    {
                        _ = self.state.CheckBuffer(tableIndex + 3, out _);
                    }

                    // Encode key
                    Encode(self, depth + 1);

                    self.scratchBufferManager.ViewRemainingArgSlice(1).Span[0] = (byte)':';
                    self.scratchBufferManager.MoveOffset(1);

                    // Encode value
                    Encode(self, depth + 1);

                    firstValue = false;
                }

                self.scratchBufferManager.ViewRemainingArgSlice(1).Span[0] = (byte)'}';
                self.scratchBufferManager.MoveOffset(1);

                // Remove table
                self.state.Pop(1);
            }
        }

        /// <summary>
        /// Entry point for cjson.decode from a Lua script.
        /// </summary>
        public int CJsonDecode(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1)
            {
                return state.RaiseError("bad argument to decode");
            }

            var argType = state.Type(1);
            if (argType == LuaType.Number)
            {
                // We'd coerce this to a string, and then decode it, so just pass it back as is
                //
                // There are some cases where this wouldn't work, potentially, but they are super implementation
                // specific so we can just pretend we made them work
                return 1;
            }

            if (argType != LuaType.String)
            {
                return state.RaiseError("bad argument to decode");
            }

            _ = state.CheckBuffer(1, out var buff);

            try
            {
                var parsed = JsonNode.Parse(buff, documentOptions: new JsonDocumentOptions { MaxDepth = 1000 });
                Decode(this, parsed);

                return 1;
            }
            catch (Exception e)
            {
                if (e.Message.Contains("maximum configured depth of 1000"))
                {
                    // Maximum depth exceeded, munge to a compatible Redis error
                    return state.RaiseError("Found too many nested data structures (1001)");
                }

                // Invalid token is implied (and matches Redis error replies)
                //
                // Additinal error details can be gleaned from messages
                return state.RaiseError($"Expected value but found invalid token.  Inner Message = {e.Message}");
            }

            // Convert the JsonNode into a Lua value on the stack
            static void Decode(LuaRunner self, JsonNode node)
            {
                if (node is JsonValue v)
                {
                    DecodeValue(self, v);
                }
                else if (node is JsonArray a)
                {
                    DecodeArray(self, a);
                }
                else if (node is JsonObject o)
                {
                    DecodeObject(self, o);
                }
                else
                {
                    _ = self.state.RaiseError($"Unexpected json node type: {node.GetType().Name}");
                }
            }

            // Convert the JsonValue int to a Lua string, nil, or number on the stack
            static void DecodeValue(LuaRunner self, JsonValue value)
            {
                // Reserve space for the value
                self.state.ForceMinimumStackCapacity(1);

                switch (value.GetValueKind())
                {
                    case JsonValueKind.Null: self.state.PushNil(); break;
                    case JsonValueKind.True: self.state.PushBoolean(true); break;
                    case JsonValueKind.False: self.state.PushBoolean(false); break;
                    case JsonValueKind.Number: self.state.PushNumber(value.GetValue<double>()); break;
                    case JsonValueKind.String:
                        var str = value.GetValue<string>();

                        self.scratchBufferManager.Reset();
                        var buf = self.scratchBufferManager.UTF8EncodeString(str);

                        self.state.PushBuffer(buf);
                        break;
                    case JsonValueKind.Undefined:
                    case JsonValueKind.Object:
                    case JsonValueKind.Array:
                    default:
                        _ = self.state.RaiseError($"Unexpected json value kind: {value.GetValueKind()}");
                        break;
                }
            }

            // Convert the JsonArray into a Lua table on the stack
            static void DecodeArray(LuaRunner self, JsonArray arr)
            {
                // Reserve space for the table
                self.state.ForceMinimumStackCapacity(1);

                self.state.CreateTable(arr.Count, 0);

                var tableIndex = self.state.StackTop;

                var storeAtIx = 1;
                foreach (var item in arr)
                {
                    // Places item on the stack
                    Decode(self, item);

                    // Save into the table
                    self.state.RawSetInteger(tableIndex, storeAtIx);
                    storeAtIx++;
                }
            }

            // Convert the JsonObject into a Lua table on the stack
            static void DecodeObject(LuaRunner self, JsonObject obj)
            {
                // Reserve space for table and key
                self.state.ForceMinimumStackCapacity(2);

                self.state.CreateTable(0, obj.Count);

                var tableIndex = self.state.StackTop;

                foreach (var (key, value) in obj)
                {
                    // Decode key to string
                    self.scratchBufferManager.Reset();
                    var buf = self.scratchBufferManager.UTF8EncodeString(key);
                    self.state.PushBuffer(buf);

                    // Decode value
                    Decode(self, value);

                    self.state.RawSet(tableIndex);
                }
            }
        }

        /// <summary>
        /// Entry point for cmsgpack.pack from a Lua script.
        /// </summary>
        public int CMsgPackPack(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var numLuaArgs = state.StackTop;

            if (numLuaArgs == 0)
            {
                return state.RaiseError("bad argument to pack");
            }

            // Redis concatenates all the message packs together if there are multiple
            //
            // Somewhat odd, but we match that behavior

            scratchBufferManager.Reset();

            for (var argIx = 1; argIx <= numLuaArgs; argIx++)
            {
                // Because each encode removes the encoded value
                // we always encode the argument at position 1
                Encode(this, 1, 0);
            }

            // After all encoding, stack should be empty
            state.ExpectLuaStackEmpty();

            var ret = scratchBufferManager.ViewFullArgSlice().ReadOnlySpan;
            state.PushBuffer(ret);

            return 1;

            // Encode a single item at the top of the stack, and remove it
            static void Encode(LuaRunner self, int stackIndex, int depth)
            {
                var type = self.state.Type(stackIndex);
                switch (type)
                {
                    case LuaType.Boolean: EncodeBool(self, stackIndex); break;
                    case LuaType.Number: EncodeNumber(self, stackIndex); break;
                    case LuaType.String: EncodeBytes(self, stackIndex); break;
                    case LuaType.Table:

                        if (depth == 16)
                        {
                            // Redis treats a too deeply nested table as a null
                            //
                            // This is weird, but we match it
                            self.scratchBufferManager.ViewRemainingArgSlice(1).Span[0] = 0xC0;
                            self.scratchBufferManager.MoveOffset(1);

                            self.state.Remove(stackIndex);

                            return;
                        }

                        EncodeTable(self, stackIndex, depth);
                        break;

                    // Everything else maps to null, NOT an error
                    case LuaType.Function:
                    case LuaType.LightUserData:
                    case LuaType.Nil:
                    case LuaType.None:
                    case LuaType.Thread:
                    case LuaType.UserData:
                    default: EncodeNull(self, stackIndex); break;
                }
            }

            // Encode a null-ish value at stackIndex, and remove it
            static void EncodeNull(LuaRunner self, int stackIndex)
            {
                Debug.Assert(self.state.Type(stackIndex) is not (LuaType.Boolean or LuaType.Number or LuaType.String or LuaType.Table), "Expected null-ish type");

                self.scratchBufferManager.ViewRemainingArgSlice(1).Span[0] = 0xC0;
                self.scratchBufferManager.MoveOffset(1);

                self.state.Remove(stackIndex);
            }

            // Encode a boolean at stackIndex, and remove it
            static void EncodeBool(LuaRunner self, int stackIndex)
            {
                Debug.Assert(self.state.Type(stackIndex) == LuaType.Boolean, "Expected boolean");

                var value = (byte)(self.state.ToBoolean(stackIndex) ? 0xC3 : 0xC2);

                self.scratchBufferManager.ViewRemainingArgSlice(1).Span[0] = value;
                self.scratchBufferManager.MoveOffset(1);

                self.state.Remove(stackIndex);
            }

            // Encode a number at stackIndex, and remove it
            static void EncodeNumber(LuaRunner self, int stackIndex)
            {
                Debug.Assert(self.state.Type(stackIndex) == LuaType.Number, "Expected number");

                var numRaw = self.state.CheckNumber(stackIndex);
                var isInt = numRaw == (long)numRaw;

                if (isInt)
                {
                    EncodeInteger(self, (long)numRaw);
                }
                else
                {
                    EncodeFloatingPoint(self, numRaw);
                }

                self.state.Remove(stackIndex);
            }

            // Encode an integer
            static void EncodeInteger(LuaRunner self, long value)
            {
                // positive 7-bit fixint
                if ((byte)(value & 0b0111_1111) == value)
                {
                    self.scratchBufferManager.ViewRemainingArgSlice(1).Span[0] = (byte)value;
                    self.scratchBufferManager.MoveOffset(1);

                    return;
                }

                // negative 5-bit fixint
                if ((sbyte)(value | 0b1110_0000) == value)
                {
                    self.scratchBufferManager.ViewRemainingArgSlice(1).Span[0] = (byte)value;
                    self.scratchBufferManager.MoveOffset(1);
                    return;
                }

                // 8-bit int
                if (value is >= sbyte.MinValue and <= sbyte.MaxValue)
                {
                    var into = self.scratchBufferManager.ViewRemainingArgSlice(2).Span;

                    into[0] = 0xD0;
                    into[1] = (byte)value;
                    self.scratchBufferManager.MoveOffset(2);
                    return;
                }

                // 8-bit uint
                if (value is >= byte.MinValue and <= byte.MaxValue)
                {
                    var into = self.scratchBufferManager.ViewRemainingArgSlice(2).Span;

                    into[0] = 0xCC;
                    into[1] = (byte)value;
                    self.scratchBufferManager.MoveOffset(2);
                    return;
                }

                // 16-bit int
                if (value is >= short.MinValue and <= short.MaxValue)
                {
                    var into = self.scratchBufferManager.ViewRemainingArgSlice(3).Span;

                    into[0] = 0xD1;
                    BinaryPrimitives.WriteInt16BigEndian(into[1..], (short)value);
                    self.scratchBufferManager.MoveOffset(3);
                    return;
                }

                // 16-bit uint
                if (value is >= ushort.MinValue and <= ushort.MaxValue)
                {
                    var into = self.scratchBufferManager.ViewRemainingArgSlice(3).Span;

                    into[0] = 0xCD;
                    BinaryPrimitives.WriteUInt16BigEndian(into[1..], (ushort)value);
                    self.scratchBufferManager.MoveOffset(3);
                    return;
                }

                // 32-bit int
                if (value is >= int.MinValue and <= int.MaxValue)
                {
                    var into = self.scratchBufferManager.ViewRemainingArgSlice(5).Span;

                    into[0] = 0xD2;
                    BinaryPrimitives.WriteInt32BigEndian(into[1..], (int)value);
                    self.scratchBufferManager.MoveOffset(5);
                    return;
                }

                // 32-bit uint
                if (value is >= uint.MinValue and <= uint.MaxValue)
                {
                    var into = self.scratchBufferManager.ViewRemainingArgSlice(5).Span;

                    into[0] = 0xCE;
                    BinaryPrimitives.WriteUInt32BigEndian(into[1..], (uint)value);
                    self.scratchBufferManager.MoveOffset(5);
                    return;
                }

                // 64-bit uint
                if (value > uint.MaxValue)
                {
                    var into = self.scratchBufferManager.ViewRemainingArgSlice(9).Span;

                    into[0] = 0xCF;
                    BinaryPrimitives.WriteUInt64BigEndian(into[1..], (ulong)value);
                    self.scratchBufferManager.MoveOffset(9);
                    return;
                }

                // 64-bit int
                {
                    var into = self.scratchBufferManager.ViewRemainingArgSlice(9).Span;

                    into[0] = 0xD3;
                    BinaryPrimitives.WriteInt64BigEndian(into[1..], value);
                    self.scratchBufferManager.MoveOffset(9);
                }
            }

            // Encode a floating point value
            static void EncodeFloatingPoint(LuaRunner self, double value)
            {
                // While Redis has code that attempts to pack doubles into floats
                // it doesn't appear to do anything, so we just always write a double

                var into = self.scratchBufferManager.ViewRemainingArgSlice(9).Span;

                into[0] = 0xCB;
                BinaryPrimitives.WriteDoubleBigEndian(into[1..], value);
                self.scratchBufferManager.MoveOffset(9);
            }

            // Encodes a string as at stackIndex, and remove it
            static void EncodeBytes(LuaRunner self, int stackIndex)
            {
                Debug.Assert(self.state.Type(stackIndex) == LuaType.String, "Expected string");

                _ = self.state.CheckBuffer(stackIndex, out var data);

                if (data.Length < 32)
                {
                    var into = self.scratchBufferManager.ViewRemainingArgSlice(1 + data.Length).Span;

                    into[0] = (byte)(0xA0 | data.Length);
                    data.CopyTo(into[1..]);
                    self.scratchBufferManager.MoveOffset(1 + data.Length);
                }
                else if (data.Length <= byte.MaxValue)
                {
                    var into = self.scratchBufferManager.ViewRemainingArgSlice(2 + data.Length).Span;

                    into[0] = 0xD9;
                    into[1] = (byte)data.Length;
                    data.CopyTo(into[2..]);
                    self.scratchBufferManager.MoveOffset(2 + data.Length);
                }
                else if (data.Length <= ushort.MaxValue)
                {
                    var into = self.scratchBufferManager.ViewRemainingArgSlice(3 + data.Length).Span;

                    into[0] = 0xDA;
                    BinaryPrimitives.WriteUInt16BigEndian(into[1..], (ushort)data.Length);
                    data.CopyTo(into[3..]);
                    self.scratchBufferManager.MoveOffset(3 + data.Length);
                }
                else
                {
                    var into = self.scratchBufferManager.ViewRemainingArgSlice(5 + data.Length).Span;

                    into[0] = 0xDB;
                    BinaryPrimitives.WriteUInt32BigEndian(into[1..], (uint)data.Length);
                    data.CopyTo(into[5..]);
                    self.scratchBufferManager.MoveOffset(5 + data.Length);
                }

                self.state.Remove(stackIndex);
            }

            // Encode a table at stackIndex, and remove it
            static void EncodeTable(LuaRunner self, int stackIndex, int depth)
            {
                Debug.Assert(self.state.Type(stackIndex) == LuaType.Table, "Expected table");

                // Space for key and value
                self.state.ForceMinimumStackCapacity(2);

                var tableIndex = stackIndex;

                // A zero-length table is serialized as an array
                var isArray = true;
                var count = 0;
                var max = 0;

                var keyIndex = self.state.StackTop + 1;

                // Measure the table and figure out if we're creating a map or an array
                self.state.PushNil();
                while (self.state.Next(tableIndex) != 0)
                {
                    count++;

                    // Remove value
                    self.state.Pop(1);

                    double keyAsNum;
                    if (self.state.Type(keyIndex) != LuaType.Number || (keyAsNum = self.state.CheckNumber(keyIndex)) <= 0 || keyAsNum != (int)keyAsNum)
                    {
                        isArray = false;
                    }
                    else
                    {
                        if (keyAsNum > max)
                        {
                            max = (int)keyAsNum;
                        }
                    }
                }

                if (isArray && count == max)
                {
                    EncodeArray(self, stackIndex, depth, count);
                }
                else
                {
                    EncodeMap(self, stackIndex, depth, count);
                }
            }

            // Encode a table at stackIndex into an array, and remove it
            static void EncodeArray(LuaRunner self, int stackIndex, int depth, int count)
            {
                Debug.Assert(self.state.Type(stackIndex) == LuaType.Table, "Expected table");
                Debug.Assert(count >= 0, "Array should have positive length");

                // Reserve space for value
                self.state.ForceMinimumStackCapacity(1);

                var tableIndex = stackIndex;
                var valueIndex = tableIndex + 1;

                // Encode length
                if (count <= 15)
                {
                    var into = self.scratchBufferManager.ViewRemainingArgSlice(1).Span;
                    into[0] = (byte)(0b1001_0000 | count);
                    self.scratchBufferManager.MoveOffset(1);
                }
                else if (count <= ushort.MaxValue)
                {
                    var into = self.scratchBufferManager.ViewRemainingArgSlice(3).Span;

                    into[0] = 0xDC;
                    BinaryPrimitives.WriteUInt16BigEndian(into[1..], (ushort)count);
                    self.scratchBufferManager.MoveOffset(3);
                }
                else
                {
                    var into = self.scratchBufferManager.ViewRemainingArgSlice(5).Span;

                    into[0] = 0xDD;
                    BinaryPrimitives.WriteUInt32BigEndian(into[1..], (uint)count);
                    self.scratchBufferManager.MoveOffset(5);
                }

                // Write each element out
                for (var ix = 1; ix <= count; ix++)
                {
                    _ = self.state.RawGetInteger(null, tableIndex, ix);
                    Encode(self, valueIndex, depth + 1);
                }

                self.state.Remove(tableIndex);
            }

            // Encode a table at stackIndex into a map, and remove it
            static void EncodeMap(LuaRunner self, int stackIndex, int depth, int count)
            {
                Debug.Assert(self.state.Type(stackIndex) == LuaType.Table, "Expected table");
                Debug.Assert(count >= 0, "Map should have positive length");

                // Reserve space for key, value, and copy of key
                self.state.ForceMinimumStackCapacity(2);

                var tableIndex = stackIndex;
                var keyIndex = self.state.StackTop + 1;
                var valueIndex = keyIndex + 1;
                var keyCopyIndex = valueIndex + 1;

                // Encode length
                if (count <= 15)
                {
                    var into = self.scratchBufferManager.ViewRemainingArgSlice(1).Span;

                    into[0] = (byte)(0b1000_0000 | count);
                    self.scratchBufferManager.MoveOffset(1);
                }
                else if (count <= ushort.MaxValue)
                {
                    var into = self.scratchBufferManager.ViewRemainingArgSlice(3).Span;

                    into[0] = 0xDE;
                    BinaryPrimitives.WriteUInt16BigEndian(into[1..], (ushort)count);
                    self.scratchBufferManager.MoveOffset(3);
                }
                else
                {
                    var into = self.scratchBufferManager.ViewRemainingArgSlice(5).Span;

                    into[0] = 0xDF;
                    BinaryPrimitives.WriteUInt32BigEndian(into[1..], (uint)count);
                    self.scratchBufferManager.MoveOffset(5);
                }

                self.state.PushNil();
                while (self.state.Next(tableIndex) != 0)
                {
                    // Make a copy of the key
                    self.state.PushValue(keyIndex);

                    // Write the key
                    Encode(self, keyCopyIndex, depth + 1);

                    // Write the value
                    Encode(self, valueIndex, depth + 1);
                }

                self.state.Remove(tableIndex);
            }
        }

        /// <summary>
        /// Entry point for cmsgpack.unpack from a Lua script.
        /// </summary>
        public int CMsgPackUnpack(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var numLuaArgs = state.StackTop;

            if (numLuaArgs == 0)
            {
                return state.RaiseError("bad argument to unpack");
            }

            _ = state.CheckBuffer(1, out var data);

            var decodedCount = 0;
            while (!data.IsEmpty)
            {
                // Reserve space for the result
                state.ForceMinimumStackCapacity(1);

                try
                {
                    Decode(ref data, ref state);
                    decodedCount++;
                }
                catch (Exception e)
                {
                    // Best effort at matching Redis behavior
                    return state.RaiseError($"Missing bytes in input. {e.Message}");
                }
            }

            return decodedCount;

            // Decode a msg pack
            static void Decode(ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                var sigil = data[0];
                data = data[1..];

                switch (sigil)
                {
                    case 0xC0: DecodeNull(ref data, ref state); return;
                    case 0xC2: DecodeBoolean(false, ref data, ref state); return;
                    case 0xC3: DecodeBoolean(true, ref data, ref state); return;
                    // 7-bit positive integers handled below
                    // 5-bit negative integers handled below
                    case 0xCC: DecodeUInt8(ref data, ref state); return;
                    case 0xCD: DecodeUInt16(ref data, ref state); return;
                    case 0xCE: DecodeUInt32(ref data, ref state); return;
                    case 0xCF: DecodeUInt64(ref data, ref state); return;
                    case 0xD0: DecodeInt8(ref data, ref state); return;
                    case 0xD1: DecodeInt16(ref data, ref state); return;
                    case 0xD2: DecodeInt32(ref data, ref state); return;
                    case 0xD3: DecodeInt64(ref data, ref state); return;
                    case 0xCA: DecodeSingle(ref data, ref state); return;
                    case 0xCB: DecodeDouble(ref data, ref state); return;
                    // <= 31 byte strings handled below
                    case 0xD9: DecodeSmallString(ref data, ref state); return;
                    case 0xDA: DecodeMidString(ref data, ref state); return;
                    case 0xDB: DecodeLargeString(ref data, ref state); return;
                    // We treat bins as strings
                    case 0xC4: goto case 0xD9;
                    case 0xC5: goto case 0xDA;
                    case 0xC6: goto case 0xDB;
                    // <= 15 element arrays are handled below
                    case 0xDC: DecodeMidArray(ref data, ref state); return;
                    case 0xDD: DecodeLargeArray(ref data, ref state); return;
                    // <= 15 pair maps are handled below
                    case 0xDE: DecodeMidMap(ref data, ref state); return;
                    case 0xDF: DecodeLargeMap(ref data, ref state); return;

                    default:
                        if ((sigil & 0b1000_0000) == 0)
                        {
                            DecodeTinyUInt(sigil, ref state);
                            return;
                        }
                        else if ((sigil & 0b1110_0000) == 0b1110_0000)
                        {
                            DecodeTinyInt(sigil, ref state);
                            return;
                        }
                        else if ((sigil & 0b1110_0000) == 0b1010_0000)
                        {
                            DecodeTinyString(sigil, ref data, ref state);
                            return;
                        }
                        else if ((sigil & 0b1111_0000) == 0b1001_0000)
                        {
                            DecodeSmallArray(sigil, ref data, ref state);
                            return;
                        }
                        else if ((sigil & 0b1111_0000) == 0b1000_0000)
                        {
                            DecodeSmallMap(sigil, ref data, ref state);
                            return;
                        }

                        _ = state.RaiseError($"Unexpected MsgPack sigil {sigil}/x{sigil:X2}/b{sigil:B8}");
                        return;
                }
            }

            // Decode a null push it to the stack
            static void DecodeNull(ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                state.PushNil();
            }

            // Decode a boolean and push it to the stack
            static void DecodeBoolean(bool b, ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                state.PushBoolean(b);
            }

            // Decode a byte, moving past it in data and pushing it to the stack
            static void DecodeUInt8(ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                state.PushNumber(data[0]);
                data = data[1..];
            }

            // Decode a positive 7-bit value, pushing it to the stack
            static void DecodeTinyUInt(byte sigil, ref LuaStateWrapper state)
            {
                state.PushNumber(sigil);
            }

            // Decode a ushort, moving past it in data and pushing it to the stack
            static void DecodeUInt16(ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                state.PushNumber(BinaryPrimitives.ReadUInt16BigEndian(data));
                data = data[2..];
            }

            // Decode a uint, moving past it in data and pushing it to the stack
            static void DecodeUInt32(ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                state.PushNumber(BinaryPrimitives.ReadUInt32BigEndian(data));
                data = data[4..];
            }

            // Decode a ulong, moving past it in data and pushing it to the stack
            static void DecodeUInt64(ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                state.PushNumber(BinaryPrimitives.ReadUInt64BigEndian(data));
                data = data[8..];
            }

            // Decode a negative 5-bit value, pushing it to the stack
            static void DecodeTinyInt(byte sigil, ref LuaStateWrapper state)
            {
                var signExtended = (int)(0xFFFF_FF00 | sigil);
                state.PushNumber(signExtended);
            }

            // Decode a sbyte, moving past it in data and pushing it to the stack
            static void DecodeInt8(ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                state.PushNumber((sbyte)data[0]);
                data = data[1..];
            }

            // Decode a short, moving past it in data and pushing it to the stack
            static void DecodeInt16(ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                state.PushNumber(BinaryPrimitives.ReadInt16BigEndian(data));
                data = data[2..];
            }

            // Decode a int, moving past it in data and pushing it to the stack
            static void DecodeInt32(ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                state.PushNumber(BinaryPrimitives.ReadInt32BigEndian(data));
                data = data[4..];
            }

            // Decode a long, moving past it in data and pushing it to the stack
            static void DecodeInt64(ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                state.PushNumber(BinaryPrimitives.ReadInt64BigEndian(data));
                data = data[8..];
            }

            // Decode a float, moving past it in data and pushing it to the stack
            static void DecodeSingle(ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                state.PushNumber(BinaryPrimitives.ReadSingleBigEndian(data));
                data = data[4..];
            }

            // Decode a double, moving past it in data and pushing it to the stack
            static void DecodeDouble(ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                state.PushNumber(BinaryPrimitives.ReadDoubleBigEndian(data));
                data = data[8..];
            }

            // Decode a string size <= 31, moving past it in data and pushing it to the stack
            static void DecodeTinyString(byte sigil, ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                var len = sigil & 0b0001_1111;
                var str = data[..len];

                state.PushBuffer(str);
                data = data[len..];
            }

            // Decode a string size <= 255, moving past it in data and pushing it to the stack
            static void DecodeSmallString(ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                var len = data[0];
                data = data[1..];

                var str = data[..len];

                state.PushBuffer(str);

                data = data[str.Length..];
            }

            // Decode a string size <= 65,535, moving past it in data and pushing it to the stack
            static void DecodeMidString(ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                var len = BinaryPrimitives.ReadUInt16BigEndian(data);
                data = data[2..];

                var str = data[..(int)len];

                state.PushBuffer(str);

                data = data[str.Length..];
            }

            // Decode a string size <= 4,294,967,295, moving past it in data and pushing it to the stack
            static void DecodeLargeString(ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                var len = BinaryPrimitives.ReadUInt32BigEndian(data);
                data = data[4..];

                if ((int)len < 0)
                {
                    _ = state.RaiseError($"String length is too long: {len}");
                    return;
                }

                var str = data[..(int)len];

                state.PushBuffer(str);

                data = data[str.Length..];
            }

            // Decode an array with <= 15 items, moving past it in data and pushing it to the stack
            static void DecodeSmallArray(byte sigil, ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                // Reserve extra space for the temporary item
                state.ForceMinimumStackCapacity(1);

                var len = sigil & 0b0000_1111;

                state.CreateTable(len, 0);
                var arrayIndex = state.StackTop;

                for (var i = 1; i <= len; i++)
                {
                    // Push the element onto the stack
                    Decode(ref data, ref state);
                    state.RawSetInteger(arrayIndex, i);
                }
            }

            // Decode an array with <= 65,535 items, moving past it in data and pushing it to the stack
            static void DecodeMidArray(ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                // Reserve extra space for the temporary item
                state.ForceMinimumStackCapacity(1);

                var len = BinaryPrimitives.ReadUInt16BigEndian(data);
                data = data[2..];

                state.CreateTable(len, 0);
                var arrayIndex = state.StackTop;

                for (var i = 1; i <= len; i++)
                {
                    // Push the element onto the stack
                    Decode(ref data, ref state);
                    state.RawSetInteger(arrayIndex, i);
                }
            }

            // Decode an array with <= 4,294,967,295 items, moving past it in data and pushing it to the stack
            static void DecodeLargeArray(ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                // Reserve extra space for the temporary item
                state.ForceMinimumStackCapacity(1);

                var len = BinaryPrimitives.ReadUInt32BigEndian(data);
                data = data[4..];

                if ((int)len < 0)
                {
                    _ = state.RaiseError($"Array length is too long: {len}");
                    return;
                }

                state.CreateTable((int)len, 0);
                var arrayIndex = state.StackTop;

                for (var i = 1; i <= len; i++)
                {
                    // Push the element onto the stack
                    Decode(ref data, ref state);
                    state.RawSetInteger(arrayIndex, i);
                }
            }

            // Decode an map with <= 15 key-value pairs, moving past it in data and pushing it to the stack
            static void DecodeSmallMap(byte sigil, ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                // Reserve extra space for the temporary key & value
                state.ForceMinimumStackCapacity(2);

                var len = sigil & 0b0000_1111;

                state.CreateTable(0, len);
                var mapIndex = state.StackTop;

                for (var i = 1; i <= len; i++)
                {
                    // Push the key onto the stack
                    Decode(ref data, ref state);

                    // Push the value onto the stack
                    Decode(ref data, ref state);

                    state.RawSet(mapIndex);
                }
            }

            // Decode a map with <= 65,535 key-value pairs, moving past it in data and pushing it to the stack
            static void DecodeMidMap(ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                // Reserve extra space for the temporary key & value
                state.ForceMinimumStackCapacity(2);

                var len = BinaryPrimitives.ReadUInt16BigEndian(data);
                data = data[2..];

                state.CreateTable(0, len);
                var mapIndex = state.StackTop;

                for (var i = 1; i <= len; i++)
                {
                    // Push the key onto the stack
                    Decode(ref data, ref state);

                    // Push the value onto the stack
                    Decode(ref data, ref state);

                    state.RawSet(mapIndex);
                }
            }

            // Decode a map with <= 4,294,967,295 key-value pairs, moving past it in data and pushing it to the stack
            static void DecodeLargeMap(ref ReadOnlySpan<byte> data, ref LuaStateWrapper state)
            {
                // Reserve extra space for the temporary key & value
                state.ForceMinimumStackCapacity(2);

                var len = BinaryPrimitives.ReadUInt32BigEndian(data);
                data = data[4..];

                if ((int)len < 0)
                {
                    _ = state.RaiseError($"Map length is too long: {len}");
                    return;
                }

                state.CreateTable(0, (int)len);
                var mapIndex = state.StackTop;

                for (var i = 1; i <= len; i++)
                {
                    // Push the key onto the stack
                    Decode(ref data, ref state);

                    // Push the value onto the stack
                    Decode(ref data, ref state);

                    state.RawSet(mapIndex);
                }
            }
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
                return LuaStaticError(constStrs.ErrRedisSetRespArg);
            }

            double num;
            if (state.Type(1) != LuaType.Number || (num = state.CheckNumber(1)) is not (2 or 3))
            {
                return LuaStaticError(constStrs.ErrRespVersion);
            }

            respServerSession.respProtocolVersion = (byte)num;

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
                return LuaStaticError(constStrs.PleaseSpecifyRedisCall);
            }

            if (!state.CheckBuffer(1, out var cmdSpan))
            {
                return LuaStaticError(constStrs.ErrBadArg);
            }

            // It's most accurate to use our existing parsing code
            // But it requires correct argument counts, and redis.acl_check_cmd doesn't.
            //
            // So we need to determine the expected minimum and maximum counts and truncate or add
            // any arguments

            var cmdStr = Encoding.UTF8.GetString(cmdSpan);
            if (!RespCommandsInfo.TryGetRespCommandInfo(cmdStr, out var info, externalOnly: false, includeSubCommands: true))
            {
                return LuaStaticError(constStrs.ErrInvalidCommand);
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
                        return LuaStaticError(constStrs.ErrBadArg);
                    }

                    if (parsedCmd == RespCommand.INVALID)
                    {
                        return LuaStaticError(constStrs.ErrInvalidCommand);
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
                            return LuaStaticError(constStrs.ErrBadArg);
                        }

                        if (parsedCmd == RespCommand.INVALID)
                        {
                            return LuaStaticError(constStrs.ErrInvalidCommand);
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
                    return LuaStaticError(constStrs.ErrBadArg);
                }

                if (parsedCommand == RespCommand.INVALID)
                {
                    return LuaStaticError(constStrs.ErrInvalidCommand);
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
                    return LuaStaticError(constStrs.PleaseSpecifyRedisCall);
                }

                state.ForceMinimumStackCapacity(AdditionalStackSpace);

                if (!state.CheckBuffer(1, out var cmdSpan))
                {
                    return LuaStaticError(constStrs.ErrBadArg);
                }

                // We special-case a few performance-sensitive operations to directly invoke via the storage API
                if (AsciiUtils.EqualsUpperCaseSpanIgnoringCase(cmdSpan, "SET"u8) && argCount == 3)
                {
                    if (!respServerSession.CheckACLPermissions(RespCommand.SET))
                    {
                        return LuaStaticError(constStrs.ErrNoAuth);
                    }

                    if (!state.CheckBuffer(2, out var keySpan) || !state.CheckBuffer(3, out var valSpan))
                    {
                        return LuaStaticError(constStrs.ErrBadArg);
                    }

                    // Note these spans are implicitly pinned, as they're actually on the Lua stack
                    var key = ArgSlice.FromPinnedSpan(keySpan);
                    var value = ArgSlice.FromPinnedSpan(valSpan);

                    _ = api.SET(key, value);

                    state.PushConstantString(constStrs.Ok);
                    return 1;
                }
                else if (AsciiUtils.EqualsUpperCaseSpanIgnoringCase(cmdSpan, "GET"u8) && argCount == 2)
                {
                    if (!respServerSession.CheckACLPermissions(RespCommand.GET))
                    {
                        return LuaStaticError(constStrs.ErrNoAuth);
                    }

                    if (!state.CheckBuffer(2, out var keySpan))
                    {
                        return LuaStaticError(constStrs.ErrBadArg);
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
                        return LuaStaticError(constStrs.ErrBadArg);
                    }
                }

                var request = scratchBufferManager.ViewFullArgSlice();

                // Once the request is formatted, we can release all the args on the Lua stack
                //
                // This keeps the stack size down for processing the response
                state.Pop(argCount);

                _ = respServerSession.TryConsumeMessages(request.ptr, request.length);

                var response = scratchBufferNetworkSender.GetResponse();

                var result = ProcessRespResponse(respServerSession.respProtocolVersion, response.ptr, response.length);

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
        /// Process a RESP(2|3)-formatted response.
        /// 
        /// Pushes result onto Lua stack and returns 1, or raises an error and never returns.
        /// </summary>
        private unsafe int ProcessRespResponse(byte respProtocolVersion, byte* respPtr, int respLen)
        {
            var respEnd = respPtr + respLen;

            var ret = ProcessSingleRespTerm(respProtocolVersion, ref respPtr, respEnd);

            if (respPtr != respEnd)
            {
                throw new InvalidOperationException("RESP3 Response not fully consumed, this should never happen");
            }

            return ret;
        }

        private unsafe int ProcessSingleRespTerm(byte respProtocolVersion, ref byte* respPtr, byte* respEnd)
        {
            var indicator = (char)*respPtr;

            var curTop = state.StackTop;

            switch (indicator)
            {
                // Simple reply (Common)
                case '+':
                    respPtr++;
                    if (RespReadUtils.TryReadAsSpan(out var resultSpan, ref respPtr, respEnd))
                    {
                        state.ForceMinimumStackCapacity(3);

                        // Construct a table = { 'ok': value }
                        state.CreateTable(0, 1);
                        state.PushConstantString(constStrs.OkLower);
                        state.PushBuffer(resultSpan);
                        state.RawSet(curTop + 1);

                        return 1;
                    }
                    goto default;

                // Integer (Common)
                case ':':
                    if (RespReadUtils.TryReadInt64(out var number, ref respPtr, respEnd))
                    {
                        state.ForceMinimumStackCapacity(1);

                        state.PushInteger(number);
                        return 1;
                    }
                    goto default;

                // Error (Common)
                case '-':
                    respPtr++;
                    if (RespReadUtils.TryReadAsSpan(out var errSpan, ref respPtr, respEnd))
                    {
                        if (errSpan.SequenceEqual(CmdStrings.RESP_ERR_GENERIC_UNK_CMD))
                        {
                            // Gets a special response
                            return LuaStaticError(constStrs.ErrUnknown);
                        }

                        state.ForceMinimumStackCapacity(1);

                        state.PushBuffer(errSpan);
                        return state.RaiseErrorFromStack();

                    }
                    goto default;

                // Bulk string or null bulk string (Common)
                case '$':
                    var remainingLength = respEnd - respPtr;

                    if (remainingLength >= 5 && new ReadOnlySpan<byte>(respPtr + 1, 4).SequenceEqual("-1\r\n"u8))
                    {
                        state.ForceMinimumStackCapacity(1);

                        // Bulk null strings are mapped to FALSE
                        // See: https://redis.io/docs/latest/develop/interact/programmability/lua-api/#lua-to-resp2-type-conversion
                        state.PushBoolean(false);

                        respPtr += 5;

                        return 1;
                    }
                    else if (RespReadUtils.TryReadSpanWithLengthHeader(out var bulkSpan, ref respPtr, respEnd))
                    {
                        state.ForceMinimumStackCapacity(1);

                        state.PushBuffer(bulkSpan);

                        return 1;
                    }
                    goto default;

                // Array (Common)
                case '*':
                    if (RespReadUtils.TryReadSignedArrayLength(out var arrayItemCount, ref respPtr, respEnd))
                    {
                        if (arrayItemCount == -1)
                        {
                            // Null multi-bulk -> maps to false
                            state.ForceMinimumStackCapacity(1);

                            state.PushBoolean(false);
                        }
                        else
                        {
                            // Create the new table
                            state.ForceMinimumStackCapacity(1);

                            state.CreateTable(arrayItemCount, 0);

                            for (var itemIx = 0; itemIx < arrayItemCount; itemIx++)
                            {
                                // Pushes the item to the top of the stack
                                _ = ProcessSingleRespTerm(respProtocolVersion, ref respPtr, respEnd);

                                // Store the item into the table
                                state.RawSetInteger(curTop + 1, itemIx + 1);
                            }
                        }

                        return 1;
                    }
                    goto default;

                // Map (RESP3 only)
                case '%':
                    if (respProtocolVersion != 3)
                    {
                        goto default;
                    }

                    if (RespReadUtils.TryReadSignedMapLength(out var mapPairCount, ref respPtr, respEnd) && mapPairCount >= 0)
                    {
                        state.ForceMinimumStackCapacity(3);

                        // Response is a two level table, where { map = { ... } }
                        state.CreateTable(1, 0);
                        state.PushConstantString(constStrs.Map);
                        state.CreateTable(mapPairCount, 0);

                        for (var pair = 0; pair < mapPairCount; pair++)
                        {
                            // Read key
                            _ = ProcessSingleRespTerm(respProtocolVersion, ref respPtr, respEnd);

                            // Read value
                            _ = ProcessSingleRespTerm(respProtocolVersion, ref respPtr, respEnd);

                            // Set t[k] = v
                            state.RawSet(curTop + 3);
                        }

                        // Store the sub-table into the parent table
                        state.RawSet(curTop + 1);

                        return 1;
                    }
                    goto default;

                // Null (RESP3 only)
                case '_':
                    {
                        if (respProtocolVersion != 3)
                        {
                            goto default;
                        }

                        var remaining = respEnd - respPtr;
                        if (remaining >= 3 && *(ushort*)(respPtr + 1) == MemoryMarshal.Read<ushort>("\r\n"u8))
                        {
                            respPtr += 3;

                            state.ForceMinimumStackCapacity(1);
                            state.PushNil();

                            return 1;
                        }
                    }
                    goto default;

                // Set (RESP3 only)
                case '~':
                    if (respProtocolVersion != 3)
                    {
                        goto default;
                    }

                    if (RespReadUtils.TryReadSignedSetLength(out var setItemCount, ref respPtr, respEnd) && setItemCount >= 0)
                    {
                        state.ForceMinimumStackCapacity(4);

                        // Response is a two level table, where { set = { ... } }
                        state.CreateTable(1, 0);
                        state.PushConstantString(constStrs.Set);
                        state.CreateTable(setItemCount, 0);

                        for (var pair = 0; pair < setItemCount; pair++)
                        {
                            // Read value, which we use as a key
                            _ = ProcessSingleRespTerm(respProtocolVersion, ref respPtr, respEnd);

                            // Unconditionally the value under the key is true
                            state.PushBoolean(true);

                            // Set t[value] = true
                            state.RawSet(curTop + 3);
                        }

                        // Store the sub-table into the parent table
                        state.RawSet(curTop + 1);

                        return 1;
                    }
                    goto default;

                // Boolean (RESP3 only)
                case '#':
                    if (respProtocolVersion != 3)
                    {
                        goto default;
                    }

                    if ((respEnd - respPtr) >= 4)
                    {
                        state.ForceMinimumStackCapacity(1);

                        var asInt = *(int*)respPtr;
                        respPtr += 4;

                        if (asInt == MemoryMarshal.Read<uint>("#t\r\n"u8))
                        {
                            state.PushBoolean(true);
                            return 1;
                        }
                        else if (asInt == MemoryMarshal.Read<uint>("#f\r\n"u8))
                        {
                            state.PushBoolean(false);
                            return 1;
                        }

                        // Undo advance in (unlikely) error state
                        respPtr -= 4;
                    }
                    goto default;

                // Double (RESP3 only)
                case ',':
                    {
                        if (respProtocolVersion != 3)
                        {
                            goto default;
                        }

                        var fullRemainingSpan = new ReadOnlySpan<byte>(respPtr, (int)(respEnd - respPtr));
                        var endOfDoubleIx = fullRemainingSpan.IndexOf("\r\n"u8);
                        if (endOfDoubleIx != -1)
                        {
                            // ,<some data>\r\n
                            var doubleSpan = fullRemainingSpan[..(endOfDoubleIx + 2)];

                            double parsed;
                            if (doubleSpan.Length >= 4 && *(int*)respPtr == MemoryMarshal.Read<int>(",inf"u8))
                            {
                                parsed = double.PositiveInfinity;
                                respPtr += doubleSpan.Length;
                            }
                            else if (doubleSpan.Length >= 4 && *(int*)respPtr == MemoryMarshal.Read<int>(",nan"u8))
                            {
                                parsed = double.NaN;
                                respPtr += doubleSpan.Length;
                            }
                            else if (doubleSpan.Length >= 5 && *(int*)(respPtr + 1) == MemoryMarshal.Read<int>("-inf"u8))
                            {
                                parsed = double.NegativeInfinity;
                                respPtr += doubleSpan.Length;
                            }
                            else if (doubleSpan.Length >= 5 && *(int*)(respPtr + 1) == MemoryMarshal.Read<int>("-nan"u8))
                            {
                                // No distinction between positive and negative NaNs, by design
                                parsed = double.NaN;
                                respPtr += doubleSpan.Length;
                            }
                            else if (doubleSpan.Length >= 4 && double.TryParse(doubleSpan[1..^2], provider: CultureInfo.InvariantCulture, out parsed))
                            {
                                respPtr += doubleSpan.Length;
                            }
                            else
                            {
                                goto default;
                            }

                            state.ForceMinimumStackCapacity(3);

                            // Create table like { double = <parsed> }
                            state.CreateTable(1, 0);
                            state.PushConstantString(constStrs.Double);
                            state.PushNumber(parsed);

                            state.RawSet(curTop + 1);

                            return 1;
                        }
                    }
                    goto default;

                // Big number (RESP3 only)
                case '(':
                    {
                        if (respProtocolVersion != 3)
                        {
                            goto default;
                        }

                        var fullRemainingSpan = new ReadOnlySpan<byte>(respPtr, (int)(respEnd - respPtr));
                        var endOfBigNum = fullRemainingSpan.IndexOf("\r\n"u8);
                        if (endOfBigNum != -1)
                        {
                            var bigNumSpan = fullRemainingSpan[..(endOfBigNum + 2)];
                            if (bigNumSpan.Length >= 4)
                            {
                                var bigNumBuf = bigNumSpan[1..^2];
                                if (bigNumBuf.ContainsAnyExceptInRange((byte)'0', (byte)'9'))
                                {
                                    goto default;
                                }

                                respPtr += bigNumSpan.Length;

                                state.ForceMinimumStackCapacity(3);

                                // Create table like { big_number = <bigNumBuf> }
                                state.CreateTable(1, 0);
                                state.PushConstantString(constStrs.BigNumber);
                                state.PushBuffer(bigNumSpan);

                                state.RawSet(curTop + 1);

                                return 1;
                            }
                        }
                    }
                    goto default;

                // Verbatim strings (RESP3 only)
                case '=':
                    if (respProtocolVersion != 3)
                    {
                        goto default;
                    }

                    if (RespReadUtils.TryReadVerbatimStringLength(out var verbatimStringLength, ref respPtr, respEnd) && verbatimStringLength >= 0)
                    {
                        var remainingLen = respEnd - respPtr;
                        if (remainingLen >= verbatimStringLength + 2)
                        {
                            var format = new ReadOnlySpan<byte>(respPtr, 3);
                            var data = new ReadOnlySpan<byte>(respPtr + 4, verbatimStringLength - 4);

                            respPtr += verbatimStringLength;
                            if (*(ushort*)respPtr != MemoryMarshal.Read<ushort>("\r\n"u8))
                            {
                                respPtr -= verbatimStringLength;
                                goto default;
                            }

                            respPtr += 2;

                            // create table like { format = <format>, string = {data} }
                            state.ForceMinimumStackCapacity(3);
                            state.CreateTable(2, 0);

                            state.PushConstantString(constStrs.Format);
                            state.PushBuffer(format);
                            state.RawSet(curTop + 1);

                            state.PushConstantString(constStrs.String);
                            state.PushBuffer(data);
                            state.RawSet(curTop + 1);

                            return 1;
                        }
                    }
                    goto default;


                default:
                    throw new Exception("Unexpected response: " + Encoding.UTF8.GetString(new Span<byte>(respPtr, (int)(respEnd - respPtr))).Replace("\n", "|").Replace("\r", "") + "]");
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
            var txnVersion = respServerSession.storageSession.stateMachineDriver.AcquireTransactionVersion();
            try
            {
                respServerSession.storageSession.lockableContext.BeginLockable();
                if (!respServerSession.storageSession.objectStoreLockableContext.IsNull)
                    respServerSession.storageSession.objectStoreLockableContext.BeginLockable();
                respServerSession.SetTransactionMode(true);
                txnKeyEntries.LockAllKeys();

                txnVersion = respServerSession.storageSession.stateMachineDriver.VerifyTransactionVersion(txnVersion);
                respServerSession.storageSession.lockableContext.LocksAcquired(txnVersion);
                if (!respServerSession.storageSession.objectStoreLockableContext.IsNull)
                    respServerSession.storageSession.objectStoreLockableContext.LocksAcquired(txnVersion);
                RunCommon(ref response);
            }
            finally
            {
                txnKeyEntries.UnlockAllKeys();
                respServerSession.SetTransactionMode(false);
                respServerSession.storageSession.lockableContext.EndLockable();
                if (!respServerSession.storageSession.objectStoreLockableContext.IsNull)
                    respServerSession.storageSession.objectStoreLockableContext.EndLockable();
                respServerSession.storageSession.stateMachineDriver.EndTransaction(txnVersion);
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

            try
            {
                state.ForceMinimumStackCapacity(NeededStackSize);

                // Every invocation starts in RESP2
                if (respServerSession != null)
                {
                    respServerSession.respProtocolVersion = 2;
                }

                _ = state.RawGetInteger(LuaType.Function, (int)LuaRegistry.Index, functionRegistryIndex);

                var callRes = state.PCall(0, 1);
                if (callRes == LuaStatus.OK)
                {
                    // The actual call worked, handle the response
                    WriteResponse(ref resp);
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
        }

        /// <summary>
        /// Convert value on top of stack (if any) into a RESP# reply
        /// and write it out to <paramref name="resp" />.
        /// </summary>
        private void WriteResponse<TResponse>(ref TResponse resp)
            where TResponse : struct, IResponseAdapter
        {
            if (state.StackTop == 0)
            {
                if (resp.RespProtocolVersion == 3)
                {
                    WriteResp3Null(this, pop: false, ref resp);
                }
                else
                {
                    WriteResp2Null(this, pop: false, ref resp);
                }

                return;
            }

            WriteSingleItem(this, ref resp);

            static void WriteSingleItem(LuaRunner runner, ref TResponse resp)
            {
                var curTop = runner.state.StackTop;
                var retType = runner.state.Type(curTop);
                var isNullish = retType is LuaType.Nil or LuaType.UserData or LuaType.Function or LuaType.Thread or LuaType.UserData;

                if (isNullish)
                {
                    if (resp.RespProtocolVersion == 3)
                    {
                        WriteResp3Null(runner, pop: true, ref resp);
                    }
                    else
                    {
                        WriteResp2Null(runner, pop: true, ref resp);
                    }

                    return;
                }
                else if (retType == LuaType.Number)
                {
                    WriteNumber(runner, ref resp);
                    return;
                }
                else if (retType == LuaType.String)
                {
                    WriteString(runner, ref resp);
                    return;
                }
                else if (retType == LuaType.Boolean)
                {
                    if (runner.respServerSession?.respProtocolVersion == 3 && resp.RespProtocolVersion == 2)
                    {
                        // This is not in spec, but is how Redis actually behaves
                        var toPush = runner.state.ToBoolean(curTop) ? 1 : 0;
                        runner.state.Pop(1);
                        runner.state.PushInteger(toPush);

                        WriteNumber(runner, ref resp);
                        return;
                    }
                    else if (runner.respServerSession?.respProtocolVersion == 2 && resp.RespProtocolVersion == 3)
                    {
                        // Likewise, this is how Redis actuallys behaves
                        if (runner.state.ToBoolean(curTop))
                        {
                            runner.state.Pop(1);
                            runner.state.PushInteger(1);

                            WriteNumber(runner, ref resp);
                            return;
                        }
                        else
                        {
                            WriteResp3Null(runner, pop: true, ref resp);
                            return;
                        }
                    }
                    else if (runner.respServerSession?.respProtocolVersion == 3)
                    {
                        // RESP3 has a proper boolean type
                        WriteResp3Boolean(runner, ref resp);
                        return;
                    }
                    else
                    {
                        // RESP2 booleans are weird
                        // false = null (the bulk nil)
                        // true = 1 (the integer)

                        if (runner.state.ToBoolean(curTop))
                        {
                            runner.state.Pop(1);
                            runner.state.PushInteger(1);

                            WriteNumber(runner, ref resp);
                            return;
                        }
                        else
                        {
                            WriteResp2Null(runner, pop: true, ref resp);
                            return;
                        }
                    }
                }
                else if (retType == LuaType.Table)
                {
                    // Redis does not respect metatables, so RAW access is ok here

                    // Need space for the lookup keys ("ok", "err", etc.) and their values
                    runner.state.ForceMinimumStackCapacity(1);

                    runner.state.PushConstantString(runner.constStrs.Double);
                    var doubleType = runner.state.RawGet(null, curTop);
                    if (doubleType == LuaType.Number)
                    {
                        if (resp.RespProtocolVersion == 3)
                        {
                            WriteDouble(runner, ref resp);
                        }
                        else
                        {
                            // Force double to string for RESP2
                            _ = runner.state.CheckBuffer(curTop + 1, out _);
                            WriteString(runner, ref resp);
                        }

                        // Remove table from stack
                        runner.state.Pop(1);

                        return;
                    }

                    // Remove whatever we read from the table under the "double" key
                    runner.state.Pop(1);

                    runner.state.PushConstantString(runner.constStrs.Map);
                    var mapType = runner.state.RawGet(null, curTop);
                    if (mapType == LuaType.Table)
                    {
                        if (resp.RespProtocolVersion == 3)
                        {
                            WriteMap(runner, ref resp);
                        }
                        else
                        {
                            WriteMapToArray(runner, ref resp);
                        }

                        // remove table from stack
                        runner.state.Pop(1);

                        return;
                    }

                    // Remove whatever we read from the table under the "map" key
                    runner.state.Pop(1);

                    runner.state.PushConstantString(runner.constStrs.Set);
                    var setType = runner.state.RawGet(null, curTop);
                    if (setType == LuaType.Table)
                    {
                        if (resp.RespProtocolVersion == 3)
                        {
                            WriteSet(runner, ref resp);
                        }
                        else
                        {
                            WriteSetToArray(runner, ref resp);
                        }

                        // remove table from stack
                        runner.state.Pop(1);

                        return;
                    }

                    // Remove whatever we read from the table under the "set" key
                    runner.state.Pop(1);

                    // If the key "ok" is in there, we need to short circuit
                    runner.state.PushConstantString(runner.constStrs.OkLower);
                    var okType = runner.state.RawGet(null, curTop);
                    if (okType == LuaType.String)
                    {
                        WriteString(runner, ref resp);

                        // Remove table from stack
                        runner.state.Pop(1);

                        return;
                    }

                    // Remove whatever we read from the table under the "ok" key
                    runner.state.Pop(1);

                    // If the key "err" is in there, we need to short circuit 
                    runner.state.PushConstantString(runner.constStrs.Err);

                    var errType = runner.state.RawGet(null, curTop);
                    if (errType == LuaType.String)
                    {
                        WriteError(runner, ref resp);

                        // Remove table from stack
                        runner.state.Pop(1);

                        return;
                    }

                    // Remove whatever we read from the table under the "err" key
                    runner.state.Pop(1);

                    // Map this table to an array
                    WriteArray(runner, ref resp);
                    return;
                }
            }

            // Write out $-1\r\n (the RESP2 null) and (optionally) pop the null value off the stack
            static unsafe void WriteResp2Null(LuaRunner runner, bool pop, ref TResponse resp)
            {
                while (!RespWriteUtils.TryWriteNull(ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                if (pop)
                {
                    runner.state.Pop(1);
                }
            }

            // Write out _\r\n (the RESP3 null) and (optionally) pop the null value off the stack
            static unsafe void WriteResp3Null(LuaRunner runner, bool pop, ref TResponse resp)
            {
                while (!RespWriteUtils.TryWriteResp3Null(ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                if (pop)
                {
                    runner.state.Pop(1);
                }
            }

            // Writes the number on the top of the stack, removes it from the stack
            static unsafe void WriteNumber(LuaRunner runner, ref TResponse resp)
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
            static unsafe void WriteString(LuaRunner runner, ref TResponse resp)
            {
                runner.state.KnownStringToBuffer(runner.state.StackTop, out var buf);

                // Strings can be veeeerrrrry large, so we can't use the short helpers
                // Thus we write the full string directly
                while (!RespWriteUtils.TryWriteBulkStringLength(buf, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                // Repeat while we have bytes left to write
                while (!buf.IsEmpty)
                {
                    // Copy bytes over
                    var destSpace = resp.BufferEnd - resp.BufferCur;
                    var copyLen = (int)(destSpace < buf.Length ? destSpace : buf.Length);
                    buf.Slice(0, copyLen).CopyTo(new Span<byte>(resp.BufferCur, copyLen));

                    // Advance
                    resp.BufferCur += copyLen;

                    // Flush if we filled the buffer
                    if (destSpace == copyLen)
                    {
                        resp.SendAndReset();
                    }

                    // Move past the data we wrote out
                    buf = buf.Slice(copyLen);
                }

                // End the string
                while (!RespWriteUtils.TryWriteNewLine(ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                runner.state.Pop(1);
            }

            // Writes the boolean on the top of the stack, removes it from the stack
            static unsafe void WriteResp3Boolean(LuaRunner runner, ref TResponse resp)
            {
                Debug.Assert(runner.state.Type(runner.state.StackTop) == LuaType.Boolean, "Boolean was not on top of stack");

                // In RESP3 there is a dedicated boolean type
                if (runner.state.ToBoolean(runner.state.StackTop))
                {
                    while (!RespWriteUtils.TryWriteTrue(ref resp.BufferCur, resp.BufferEnd))
                        resp.SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.TryWriteFalse(ref resp.BufferCur, resp.BufferEnd))
                        resp.SendAndReset();
                }

                runner.state.Pop(1);
            }

            // Writes the number on the top of the stack as a double, removes it from the stack
            static unsafe void WriteDouble(LuaRunner runner, ref TResponse resp)
            {
                Debug.Assert(runner.state.Type(runner.state.StackTop) == LuaType.Number, "Number was not on top of stack");

                var num = runner.state.CheckNumber(runner.state.StackTop);
                while (!RespWriteUtils.TryWriteDoubleNumeric(num, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                runner.state.Pop(1);
            }

            // Write a table on the top of the stack as a map, removes it from the stack
            static unsafe void WriteMap(LuaRunner runner, ref TResponse resp)
            {
                // 2 for the returned key and value from Next, 1 for the temp copy of the returned key
                const int AdditonalNeededStackSize = 3;

                Debug.Assert(runner.state.Type(runner.state.StackTop) == LuaType.Table, "Table was not on top of stack");

                runner.state.ForceMinimumStackCapacity(AdditonalNeededStackSize);

                var mapSize = 0;

                var tableIx = runner.state.StackTop;

                // Push nil key as "first key"
                runner.state.PushNil();
                while (runner.state.Next(tableIx) != 0)
                {
                    // Now we have value at top of stack, and key one below it

                    mapSize++;

                    // Remove value, we don't need it
                    runner.state.Pop(1);
                }

                // Write the map header
                while (!RespWriteUtils.TryWriteMapLength(mapSize, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                // Write the values out by traversing the table again
                runner.state.PushNil();
                while (runner.state.Next(tableIx) != 0)
                {
                    // Copy key to top of stack
                    runner.state.PushValue(tableIx + 1);

                    // Write (and remove) key out
                    WriteSingleItem(runner, ref resp);

                    // Write (and remove) value out
                    WriteSingleItem(runner, ref resp);

                    // Now we have the original key value on the stack
                }

                // Remove the table
                runner.state.Pop(1);
            }

            // Convert a table to an array, where each key-value pair is converted to 2 entries
            static unsafe void WriteMapToArray(LuaRunner runner, ref TResponse resp)
            {
                // 2 for the returned key and value from Next, 1 for the temp copy of the returned key
                const int AdditonalNeededStackSize = 3;

                Debug.Assert(runner.state.Type(runner.state.StackTop) == LuaType.Table, "Table was not on top of stack");

                runner.state.ForceMinimumStackCapacity(AdditonalNeededStackSize);

                var mapSize = 0;

                var tableIx = runner.state.StackTop;

                // Push nil key as "first key"
                runner.state.PushNil();
                while (runner.state.Next(tableIx) != 0)
                {
                    // Now we have value at top of stack, and key one below it

                    mapSize++;

                    // Remove value, we don't need it
                    runner.state.Pop(1);
                }

                var arraySize = mapSize * 2;

                // Write the array header
                while (!RespWriteUtils.TryWriteArrayLength(arraySize, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                // Write the values out by traversing the table again
                runner.state.PushNil();
                while (runner.state.Next(tableIx) != 0)
                {
                    // Copy key to top of stack
                    runner.state.PushValue(tableIx + 1);

                    // Write (and remove) key out
                    WriteSingleItem(runner, ref resp);

                    // Write (and remove) value out
                    WriteSingleItem(runner, ref resp);

                    // Now we have the original key value on the stack
                }

                // Remove the table
                runner.state.Pop(1);
            }

            // Write a table on the top of the stack as a set, removes it from the stack
            static unsafe void WriteSet(LuaRunner runner, ref TResponse resp)
            {
                // 2 for the returned key and value from Next
                const int AdditonalNeededStackSize = 2;

                Debug.Assert(runner.state.Type(runner.state.StackTop) == LuaType.Table, "Table was not on top of stack");

                runner.state.ForceMinimumStackCapacity(AdditonalNeededStackSize);

                var setSize = 0;

                var tableIx = runner.state.StackTop;

                // Push nil key as "first key"
                runner.state.PushNil();
                while (runner.state.Next(tableIx) != 0)
                {
                    // Now we have value at top of stack, and key one below it

                    setSize++;

                    // Remove value, we don't need it
                    runner.state.Pop(1);
                }

                // Write the set header
                while (!RespWriteUtils.TryWriteSetLength(setSize, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                // Write the values out by traversing the table again
                runner.state.PushNil();
                while (runner.state.Next(tableIx) != 0)
                {
                    // Remove the value, it's ignored
                    runner.state.Pop(1);

                    // Make a copy of the key
                    runner.state.PushValue(tableIx + 1);

                    // Write (and remove) key copy out
                    WriteSingleItem(runner, ref resp);

                    // Now we have the original key value on the stack
                }

                // Remove the table
                runner.state.Pop(1);
            }

            // Write a table on the top of the stack as an array that contains only the keys of the
            // table, then remove the table from the stack
            static unsafe void WriteSetToArray(LuaRunner runner, ref TResponse resp)
            {
                // 2 for the returned key and value from Next
                const int AdditonalNeededStackSize = 2;

                Debug.Assert(runner.state.Type(runner.state.StackTop) == LuaType.Table, "Table was not on top of stack");

                runner.state.ForceMinimumStackCapacity(AdditonalNeededStackSize);

                var setSize = 0;

                var tableIx = runner.state.StackTop;

                // Push nil key as "first key"
                runner.state.PushNil();
                while (runner.state.Next(tableIx) != 0)
                {
                    // Now we have value at top of stack, and key one below it

                    setSize++;

                    // Remove value, we don't need it
                    runner.state.Pop(1);
                }

                var arraySize = setSize;

                // Write the array header
                while (!RespWriteUtils.TryWriteArrayLength(arraySize, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                // Write the values out by traversing the table again
                runner.state.PushNil();
                while (runner.state.Next(tableIx) != 0)
                {
                    // Remove the value, it's ignored
                    runner.state.Pop(1);

                    // Make a copy of the key
                    runner.state.PushValue(tableIx + 1);

                    // Write (and remove) key copy out
                    WriteSingleItem(runner, ref resp);

                    // Now we have the original key value on the stack
                }

                // Remove the table
                runner.state.Pop(1);
            }

            // Writes the string on the top of the stack out as an error, removes the string from the stack
            static unsafe void WriteError(LuaRunner runner, ref TResponse resp)
            {
                runner.state.KnownStringToBuffer(runner.state.StackTop, out var errBuff);

                while (!RespWriteUtils.TryWriteError(errBuff, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                runner.state.Pop(1);
            }

            // Writes the table on the top of the stack out as an array, removed table from the stack
            static unsafe void WriteArray(LuaRunner runner, ref TResponse resp)
            {
                // Redis does not respect metatables, so RAW access is ok here

                // 1 for the pending value
                const int AdditonalNeededStackSize = 1;

                Debug.Assert(runner.state.Type(runner.state.StackTop) == LuaType.Table, "Table was not on top of stack");

                runner.state.ForceMinimumStackCapacity(AdditonalNeededStackSize);

                // Lua # operator - this MAY stop at nils, but isn't guaranteed to
                // See: https://www.lua.org/manual/5.3/manual.html#3.4.7
                var maxLen = runner.state.RawLen(runner.state.StackTop);

                // Find the TRUE length by scanning for nils
                int trueLen;
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
                    _ = runner.state.RawGetInteger(null, runner.state.StackTop, i);

                    // Write the item out, removing it from teh stack
                    WriteSingleItem(runner, ref resp);
                }

                // Remove the table
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

        /// <summary>
        /// Modifies <see cref="LoaderBlock"/> to account for <paramref name="allowedFunctions"/>, and converts to bytes.
        /// 
        /// Provided as an optimization, as often this can be memoized.
        /// </summary>
        private static ReadOnlyMemory<byte> PrepareLoaderBlockBytes(HashSet<string> allowedFunctions)
        {
            // If nothing is explicitly allowed, fallback to our defaults
            if (allowedFunctions.Count == 0)
            {
                allowedFunctions = DefaultAllowedFunctions;
            }

            // Most of the time this list never changes, so reuse the work
            var cache = CachedLoaderBlock;
            if (cache != null && ReferenceEquals(cache.AllowedFunctions, allowedFunctions))
            {
                return cache.LoaderBlockBytes;
            }

            // Build the subset of a Lua table where we export all these functions
            var wholeIncludes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var replacement = new StringBuilder();
            foreach (var wholeRef in allowedFunctions.Where(static x => !x.Contains('.')))
            {
                if (!DefaultAllowedFunctions.Contains(wholeRef))
                {
                    // Skip functions not intentionally exported
                    continue;
                }

                _ = replacement.AppendLine($"    {wholeRef}={wholeRef};");
                _ = wholeIncludes.Add(wholeRef);
            }

            // Partial includes (ie. os.clock) need special handling
            var partialIncludes = allowedFunctions.Where(static x => x.Contains('.')).Select(static x => (Leading: x[..x.IndexOf('.')], Trailing: x[(x.IndexOf('.') + 1)..]));
            foreach (var grouped in partialIncludes.GroupBy(static t => t.Leading, StringComparer.OrdinalIgnoreCase))
            {
                if (wholeIncludes.Contains(grouped.Key))
                {
                    // Including a subset of something included in whole doesn't affect things
                    continue;
                }

                if (!DefaultAllowedFunctions.Contains(grouped.Key))
                {
                    // Skip functions not intentionally exported
                    continue;
                }

                _ = replacement.AppendLine($"    {grouped.Key}={{");
                foreach (var part in grouped.Select(static t => t.Trailing).Distinct().OrderBy(static t => t))
                {
                    _ = replacement.AppendLine($"        {part}={grouped.Key}.{part};");
                }
                _ = replacement.AppendLine("    };");
            }

            var decl = replacement.ToString();
            var finalLoaderBlock = LoaderBlock.Replace("!!SANDBOX_ENV REPLACEMENT TARGET!!", decl);

            // Save off for next caller
            //
            // Inherently race-y, but that's fine - worst case we do a little extra work
            var newCache = new LoaderBlockCache(allowedFunctions, Encoding.UTF8.GetBytes(finalLoaderBlock));
            CachedLoaderBlock = newCache;

            return newCache.LoaderBlockBytes;
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

        /// <summary>
        /// Entry point for calls to math.atan2.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Atan2(nint luaState)
        => CallbackContext.Atan2(luaState);

        /// <summary>
        /// Entry point for calls to math.cosh.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Cosh(nint luaState)
        => CallbackContext.Cosh(luaState);

        /// <summary>
        /// Entry point for calls to math.frexp.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Frexp(nint luaState)
        => CallbackContext.Frexp(luaState);

        /// <summary>
        /// Entry point for calls to math.ldexp.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Ldexp(nint luaState)
        => CallbackContext.Ldexp(luaState);

        /// <summary>
        /// Entry point for calls to math.log10.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Log10(nint luaState)
        => CallbackContext.Log10(luaState);

        /// <summary>
        /// Entry point for calls to math.pow.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Pow(nint luaState)
        => CallbackContext.Pow(luaState);

        /// <summary>
        /// Entry point for calls to math.sinh.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Sinh(nint luaState)
        => CallbackContext.Sinh(luaState);

        /// <summary>
        /// Entry point for calls to math.tanh.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Tanh(nint luaState)
        => CallbackContext.Tanh(luaState);

        /// <summary>
        /// Entry point for calls to table.maxn.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Maxn(nint luaState)
        => CallbackContext.Maxn(luaState);

        /// <summary>
        /// Entry point for calls to loadstring.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int LoadString(nint luaState)
        => CallbackContext.LoadString(luaState);

        /// <summary>
        /// Entry point for calls to cjson.encode.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int CJsonEncode(nint luaState)
        => CallbackContext.CJsonEncode(luaState);

        /// <summary>
        /// Entry point for calls to cjson.decode.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int CJsonDecode(nint luaState)
        => CallbackContext.CJsonDecode(luaState);

        /// <summary>
        /// Entry point for calls to bit.tobit.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int BitToBit(nint luaState)
        => CallbackContext.BitToBit(luaState);

        /// <summary>
        /// Entry point for calls to bit.tohex.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int BitToHex(nint luaState)
        => CallbackContext.BitToHex(luaState);

        /// <summary>
        /// Entry point for calls to garnet_bitop, which backs
        /// bit.bnot, bit.bor, etc.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Bitop(nint luaState)
        => CallbackContext.Bitop(luaState);

        /// <summary>
        /// Entry point for calls to bit.bswap.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int BitBswap(nint luaState)
        => CallbackContext.BitBswap(luaState);

        /// <summary>
        /// Entry point for calls to cmsgpack.pack.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int CMsgPackPack(nint luaState)
        => CallbackContext.CMsgPackPack(luaState);

        /// <summary>
        /// Entry point for calls to cmsgpack.unpack.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int CMsgPackUnpack(nint luaState)
        => CallbackContext.CMsgPackUnpack(luaState);
    }
}