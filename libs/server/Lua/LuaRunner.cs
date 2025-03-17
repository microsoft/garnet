// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
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
    internal sealed partial class LuaRunner : IDisposable
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
            /// <see cref="CmdStrings.LUA_ERR_wrong_number_of_arguments"/>
            internal int ErrWrongNumberOfArgs { get; }
            /// <see cref="CmdStrings.LUA_ERR_redis_log_requires_two_arguments_or_more"/>
            internal int ErrRedisLogRequired { get; }
            /// <see cref="CmdStrings.LUA_ERR_First_argument_must_be_a_number_log_level"/>
            internal int ErrFirstArgMustBeNumber { get; }
            /// <see cref="CmdStrings.LUA_ERR_Invalid_debug_level"/>
            internal int ErrInvalidDebugLevel { get; }
            /// <see cref="CmdStrings.LUA_ERR_Invalid_command_passed_to_redis_acl_check_cmd"/>
            internal int ErrInvalidCommand { get; }
            /// <see cref="CmdStrings.LUA_ERR_redis_setresp_requires_one_argument"/>
            internal int ErrRedisSetRespArg { get; }
            /// <see cref="CmdStrings.LUA_ERR_RESP_version_must_be_2_or_3"/>
            internal int ErrRespVersion { get; }
            /// <see cref="CmdStrings.LUA_ERR_redis_log_disabled"/>
            internal int ErrLoggingDisabled { get; }
            /// <see cref="CmdStrings.LUA_double"/>
            internal int Double { get; }
            /// <see cref="CmdStrings.LUA_map"/>
            internal int Map { get; }
            /// <see cref="CmdStrings.Lua_set"/>
            internal int Set { get; }
            /// <see cref="CmdStrings.LUA_big_number"/>
            internal int BigNumber { get; }
            /// <see cref="CmdStrings.LUA_format"/>
            internal int Format { get; }
            /// <see cref="CmdStrings.LUA_string"/>
            internal int String { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_atan2"/>
            internal int BadArgATan2 { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_cosh"/>
            internal int BadArgCosh { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_frexp"/>
            internal int BadArgFrexp { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_ldexp"/>
            internal int BadArgLdexp { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_log10"/>
            internal int BadArgLog10 { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_pow"/>
            internal int BadArgPow { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_sinh"/>
            internal int BadArgSinh { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_tanh"/>
            internal int BadArgTanh { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_maxn"/>
            internal int BadArgMaxn { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_loadstring"/>
            internal int BadArgLoadString { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_loadstring_null_byte"/>
            internal int BadArgLoadStringNullByte { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_tobit"/>
            internal int BadArgToBit { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_tohex"/>
            internal int BadArgToHex { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_bswap"/>
            internal int BadArgBSwap { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_bnot"/>
            internal int BadArgBNot { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_encode"/>
            internal int BadArgEncode { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_decode"/>
            internal int BadArgDecode { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_pack"/>
            internal int BadArgPack { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_unpack"/>
            internal int BadArgUnpack { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_bor"/>
            internal int BadArgBOr { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_band"/>
            internal int BadArgBAnd { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_bxor" />
            internal int BadArgBXor { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_lshift"/>
            internal int BadArgLShift { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_rshift"/>
            internal int BadArgRShift { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_arshift"/>
            internal int BadArgARShift { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_rol"/>
            internal int BadArgRol { get; }
            /// <see cref="CmdStrings.LUA_bad_arg_ror"/>
            internal int BadArgRor { get; }
            /// <see cref="CmdStrings.LUA_unexpected_json_value_kind"/>
            internal int UnexpectedJsonValueKind { get; }
            /// <see cref="CmdStrings.LUA_cannot_serialise_to_json"/>
            internal int CannotSerialiseToJson { get; }
            /// <see cref="CmdStrings.LUA_unexpected_error"/>
            internal int UnexpectedError { get; }
            /// <see cref="CmdStrings.LUA_cannot_serialise_excessive_nesting"/>
            internal int CannotSerializeNesting { get; }
            /// <see cref="CmdStrings.LUA_unable_to_format_number"/>
            internal int UnableToFormatNumber { get; }
            /// <see cref="CmdStrings.LUA_found_too_many_nested"/>
            internal int FoundTooManyNested { get; }
            /// <see cref="CmdStrings.LUA_expected_value_but_found_invalid"/>
            internal int ExpectedValueButFound { get; }
            /// <see cref="CmdStrings.LUA_missing_bytes_in_input"/>
            internal int MissingBytesInInput { get; }
            /// <see cref="CmdStrings.LUA_unexpected_msgpack_sigil"/>
            internal int UnexpectedMsgPackSigil { get; }
            /// <see cref="CmdStrings.LUA_msgpack_string_too_long"/>
            internal int MsgPackStringTooLong { get; }
            /// <see cref="CmdStrings.LUA_msgpack_array_too_long"/>
            internal int MsgPackArrayTooLong { get; }
            /// <see cref="CmdStrings.LUA_msgpack_map_too_long"/>
            internal int MsgPackMapTooLong { get; }
            /// <see cref="CmdStrings.LUA_insufficient_lua_stack_space"/>
            internal int InsufficientLuaStackSpace { get; }
            /// <see cref="CmdStrings.LUA_parameter_reset_failed_memory"/>
            internal int ParameterResetFailedMemory { get; }
            /// <see cref="CmdStrings.LUA_parameter_reset_failed_syntax"/>
            internal int ParameterResetFailedSyntax { get; }
            /// <see cref="CmdStrings.LUA_parameter_reset_failed_runtime"/>
            internal int ParameterResetFailedRuntime { get; }
            /// <see cref="CmdStrings.LUA_parameter_reset_failed_other"/>
            internal int ParameterResetFailedOther { get; }
            /// <see cref="CmdStrings.LUA_out_of_memory"/>
            internal int OutOfMemory { get; }
            /// <see cref="CmdStrings.LUA_load_string_error"/>
            internal int LoadStringError { get; }
            /// <see cref="CmdStrings.LUA_AND"/>
            internal int AND { get; }
            /// <see cref="CmdStrings.LUA_OR"/>
            internal int OR { get; }
            /// <see cref="CmdStrings.LUA_XOR"/>
            internal int XOR { get; }
            /// <see cref="CmdStrings.LUA_NOT"/>
            internal int NOT { get; }

            internal ConstantStringRegistryIndexes(ref LuaStateWrapper state)
            {
                // Commonly used strings, register them once so we don't have to copy them over each time we need them
                //
                // As an additional benefit, we don't have to worry about reserving memory for them either during normal operation
                //
                // This makes OOM probes unnecessary in many cases
                Ok = ConstantStringToRegistry(ref state, CmdStrings.LUA_OK);
                OkLower = ConstantStringToRegistry(ref state, CmdStrings.LUA_ok);
                Err = ConstantStringToRegistry(ref state, CmdStrings.LUA_err);
                NoSessionAvailable = ConstantStringToRegistry(ref state, CmdStrings.LUA_No_session_available);
                PleaseSpecifyRedisCall = ConstantStringToRegistry(ref state, CmdStrings.LUA_ERR_Please_specify_at_least_one_argument_for_this_redis_lib_call);
                ErrNoAuth = ConstantStringToRegistry(ref state, CmdStrings.RESP_ERR_NOAUTH);
                ErrUnknown = ConstantStringToRegistry(ref state, CmdStrings.LUA_ERR_Unknown_Redis_command_called_from_script);
                ErrBadArg = ConstantStringToRegistry(ref state, CmdStrings.LUA_ERR_Lua_redis_lib_command_arguments_must_be_strings_or_integers);
                ErrWrongNumberOfArgs = ConstantStringToRegistry(ref state, CmdStrings.LUA_ERR_wrong_number_of_arguments);
                ErrRedisLogRequired = ConstantStringToRegistry(ref state, CmdStrings.LUA_ERR_redis_log_requires_two_arguments_or_more);
                ErrFirstArgMustBeNumber = ConstantStringToRegistry(ref state, CmdStrings.LUA_ERR_First_argument_must_be_a_number_log_level);
                ErrInvalidDebugLevel = ConstantStringToRegistry(ref state, CmdStrings.LUA_ERR_Invalid_debug_level);
                ErrInvalidCommand = ConstantStringToRegistry(ref state, CmdStrings.LUA_ERR_Invalid_command_passed_to_redis_acl_check_cmd);
                ErrRedisSetRespArg = ConstantStringToRegistry(ref state, CmdStrings.LUA_ERR_redis_setresp_requires_one_argument);
                ErrRespVersion = ConstantStringToRegistry(ref state, CmdStrings.LUA_ERR_RESP_version_must_be_2_or_3);
                ErrLoggingDisabled = ConstantStringToRegistry(ref state, CmdStrings.LUA_ERR_redis_log_disabled);
                Double = ConstantStringToRegistry(ref state, CmdStrings.LUA_double);
                Map = ConstantStringToRegistry(ref state, CmdStrings.LUA_map);
                Set = ConstantStringToRegistry(ref state, CmdStrings.Lua_set);
                BigNumber = ConstantStringToRegistry(ref state, CmdStrings.LUA_big_number);
                Format = ConstantStringToRegistry(ref state, CmdStrings.LUA_format);
                String = ConstantStringToRegistry(ref state, CmdStrings.LUA_string);
                BadArgATan2 = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_atan2);
                BadArgCosh = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_cosh);
                BadArgFrexp = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_frexp);
                BadArgLdexp = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_ldexp);
                BadArgLog10 = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_log10);
                BadArgPow = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_pow);
                BadArgSinh = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_sinh);
                BadArgTanh = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_tanh);
                BadArgMaxn = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_maxn);
                BadArgLoadString = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_loadstring);
                BadArgLoadStringNullByte = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_loadstring_null_byte);
                BadArgToBit = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_tobit);
                BadArgToHex = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_tohex);
                BadArgBSwap = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_bswap);
                BadArgBNot = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_bnot);
                BadArgEncode = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_encode);
                BadArgDecode = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_decode);
                BadArgPack = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_pack);
                BadArgUnpack = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_unpack);
                BadArgBOr = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_bor);
                BadArgBAnd = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_band);
                BadArgBXor = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_bxor);
                BadArgLShift = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_lshift);
                BadArgARShift = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_arshift);
                BadArgRShift = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_rshift);
                BadArgRol = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_rol);
                BadArgRor = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_ror);
                UnexpectedJsonValueKind = ConstantStringToRegistry(ref state, CmdStrings.LUA_unexpected_json_value_kind);
                CannotSerialiseToJson = ConstantStringToRegistry(ref state, CmdStrings.LUA_cannot_serialise_to_json);
                UnexpectedError = ConstantStringToRegistry(ref state, CmdStrings.LUA_unexpected_error);
                CannotSerializeNesting = ConstantStringToRegistry(ref state, CmdStrings.LUA_cannot_serialise_excessive_nesting);
                UnableToFormatNumber = ConstantStringToRegistry(ref state, CmdStrings.LUA_unable_to_format_number);
                FoundTooManyNested = ConstantStringToRegistry(ref state, CmdStrings.LUA_found_too_many_nested);
                ExpectedValueButFound = ConstantStringToRegistry(ref state, CmdStrings.LUA_expected_value_but_found_invalid);
                MissingBytesInInput = ConstantStringToRegistry(ref state, CmdStrings.LUA_missing_bytes_in_input);
                UnexpectedMsgPackSigil = ConstantStringToRegistry(ref state, CmdStrings.LUA_unexpected_msgpack_sigil);
                MsgPackStringTooLong = ConstantStringToRegistry(ref state, CmdStrings.LUA_msgpack_string_too_long);
                MsgPackArrayTooLong = ConstantStringToRegistry(ref state, CmdStrings.LUA_msgpack_array_too_long);
                MsgPackMapTooLong = ConstantStringToRegistry(ref state, CmdStrings.LUA_msgpack_map_too_long);
                InsufficientLuaStackSpace = ConstantStringToRegistry(ref state, CmdStrings.LUA_insufficient_lua_stack_space);
                ParameterResetFailedMemory = ConstantStringToRegistry(ref state, CmdStrings.LUA_parameter_reset_failed_memory);
                ParameterResetFailedSyntax = ConstantStringToRegistry(ref state, CmdStrings.LUA_parameter_reset_failed_syntax);
                ParameterResetFailedRuntime = ConstantStringToRegistry(ref state, CmdStrings.LUA_parameter_reset_failed_runtime);
                ParameterResetFailedOther = ConstantStringToRegistry(ref state, CmdStrings.LUA_parameter_reset_failed_other);
                OutOfMemory = ConstantStringToRegistry(ref state, CmdStrings.LUA_out_of_memory);
                LoadStringError = ConstantStringToRegistry(ref state, CmdStrings.LUA_load_string_error);
                AND = ConstantStringToRegistry(ref state, CmdStrings.LUA_AND);
                OR = ConstantStringToRegistry(ref state, CmdStrings.LUA_OR);
                XOR = ConstantStringToRegistry(ref state, CmdStrings.LUA_XOR);
                NOT = ConstantStringToRegistry(ref state, CmdStrings.LUA_NOT);
            }

            /// <summary>
            /// Some strings we use a bunch, and copying them to Lua each time is wasteful
            ///
            /// So instead we stash them in the Registry and load them by index
            /// </summary>
            private static int ConstantStringToRegistry(ref LuaStateWrapper state, ReadOnlySpan<byte> str)
            {
                if (!state.TryPushBuffer(str))
                {
                    throw new GarnetException("Insufficient space in Lua VM for constant string");
                }

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

-- unpack moved after Lua 5.1, this provides Redis compat
local unpack = table.unpack

-- common functions for handling error replies from Garnet
local chain_func = function(f1, f2)
    return function(...)
        return f1(f2, ...)
    end
end

local error_wrapper_r0 = function(rawFunc, ...) 
    local err = rawFunc(...)
    if err then
        error(err, 0)
    end
end

local error_wrapper_r1 = function(rawFunc, ...) 
    local r1, err = rawFunc(...)
    if err then
        error(err, 0)
    end

    return r1
end

local error_wrapper_r2 = function(rawFunc, ...) 
    local r1, r2, err = rawFunc(...)
    if err then
        error(err, 0)
    end

    return r1, r2
end

local unpackTrampolineRef = garnet_unpack_trampoline
local error_wrapper_rvar_check = function(err, ...)
    if err then
        error(err, 0)
    end

    return ...
end

local error_wrapper_rvar = function(rawFunc, ...)
    -- variable numbers of returns require extra work
    -- and requires that the error token is FIRST, not last
    -- and a count of expected returns is around to handle
    -- trailing nils
    local rets = {rawFunc(...)}
    local err = rets[1]
    local count = rets[2]
    if err then
        error(err, 0)
    end

    return error_wrapper_rvar_check(unpackTrampolineRef(rets, count))
end

-- cutdown os for sandboxing purposes
local osClockRef = os.clock
os = {
    clock = osClockRef
}

-- define cjson for (optional) inclusion into sandbox_env
local cjson = {
    encode = chain_func(error_wrapper_r1, garnet_cjson_encode);
    decode = chain_func(error_wrapper_r1, garnet_cjson_decode);
}

-- define bit for (optional) inclusion into sandbox_env
local garnetBitopRef = chain_func(error_wrapper_r1, garnet_bitop)
local bit = {
    tobit = chain_func(error_wrapper_r1, garnet_bit_tobit);
    tohex = chain_func(error_wrapper_r1, garnet_bit_tohex);
    bnot = function(...) return garnetBitopRef(0, ...); end;
    bor = function(...) return garnetBitopRef(1, ...); end;
    band = function(...) return garnetBitopRef(2, ...); end;
    bxor = function(...) return garnetBitopRef(3, ...); end;
    lshift = function(...) return garnetBitopRef(4, ...); end;
    rshift = function(...) return garnetBitopRef(5, ...); end;
    arshift = function(...) return garnetBitopRef(6, ...); end;
    rol = function(...) return garnetBitopRef(7, ...); end;
    ror = function(...) return garnetBitopRef(8, ...); end;
    bswap = chain_func(error_wrapper_r1, garnet_bit_bswap);
}

-- define cmsgpack for (optional) inclusion into sandbox_env
local cmsgpack = {
    pack = chain_func(error_wrapper_r1, garnet_cmsgpack_pack);
    unpack = chain_func(error_wrapper_rvar, garnet_cmsgpack_unpack);
}

-- define struct for (optional) inclusion into sandbox_env
local struct = {
    pack = string.pack;
    unpack = string.unpack;
    size = string.packsize;
}

-- define redis for (optional, but almost always) inclusion into sandbox_env
local garnetCallRef = chain_func(error_wrapper_r1, garnet_call)
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

    sha1hex = chain_func(error_wrapper_r1, garnet_sha1hex),

    LOG_DEBUG = 0,
    LOG_VERBOSE = 1,
    LOG_NOTICE = 2,
    LOG_WARNING = 3,

    log = chain_func(error_wrapper_r0, garnet_log),

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

    acl_check_cmd = chain_func(error_wrapper_r1, garnet_acl_check_cmd),
    setresp = chain_func(error_wrapper_r0, garnet_setresp),

    REDIS_VERSION = garnet_REDIS_VERSION,
    REDIS_VERSION_NUM = garnet_REDIS_VERSION_NUM
}

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
local loadstring = chain_func(error_wrapper_r2, garnet_loadstring)
math.atan2 = chain_func(error_wrapper_r1, garnet_atan2)
math.cosh = chain_func(error_wrapper_r1, garnet_cosh)
math.frexp = chain_func(error_wrapper_r2, garnet_frexp)
math.ldexp = chain_func(error_wrapper_r1, garnet_ldexp)
math.log10 = chain_func(error_wrapper_r1, garnet_log10)
math.pow = chain_func(error_wrapper_r1, garnet_pow)
math.sinh = chain_func(error_wrapper_r1, garnet_sinh)
math.tanh = chain_func(error_wrapper_r1, garnet_tanh)
table.maxn = chain_func(error_wrapper_r1, garnet_maxn)

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
-- timeout error must be raised on Lua
local debugRef = debug
local force_timeout = function()
    error('ERR Lua script exceeded configured timeout', 0)
end
function request_timeout()
    debugRef.sethook(force_timeout, '', 1)
end
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
-- force new 'global' environment to be readonly
recursively_readonly_table(sandbox_env)
-- responsible for sandboxing user provided code
function load_sandboxed(source)
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
        readonly int requestTimeoutRegsitryIndex;
        readonly ConstantStringRegistryIndexes constStrs;

        readonly LuaLoggingMode logMode;
        readonly HashSet<string> allowedFunctions;
        readonly ReadOnlyMemory<byte> source;
        readonly ScratchBufferNetworkSender scratchBufferNetworkSender;
        readonly RespServerSession respServerSession;

        readonly ScratchBufferManager scratchBufferManager;
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

        internal readonly ILogger logger;

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
            state.Register("garnet_unpack_trampoline\0"u8, &LuaRunnerTrampolines.UnpackTrampoline);

            var redisVersionBytes = Encoding.UTF8.GetBytes(redisVersion);
            if (!state.TryPushBuffer(redisVersionBytes))
            {
                throw new GarnetException("Insufficient space in Lua VM for redis version global");
            }

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
                if (state.StackTop == 1 && state.Type(1) == LuaType.String)
                {
                    state.KnownStringToBuffer(1, out var buff);
                    var innerError = Encoding.UTF8.GetString(buff);
                    throw new GarnetException($"Could not initialize Lua VM: {innerError}");
                }

                throw new GarnetException("Could not initialize Lua VM");
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

            state.GetGlobal(LuaType.Function, "request_timeout\0"u8);
            requestTimeoutRegsitryIndex = state.Ref();

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
            state.ExpectLuaStackEmpty();

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
        /// Compile script for a <see cref="RespServerSession"/>.
        /// 
        /// Any errors encountered are written out as Resp errors.
        /// </summary>
        public unsafe bool CompileForSession(RespServerSession session)
        {
            state.ExpectLuaStackEmpty();

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
        /// Dispose the runner
        /// </summary>
        public void Dispose()
        => state.Dispose();

        /// <summary>
        /// We can't use Lua's normal error handling functions on Linux, so instead we go through wrappers.
        /// 
        /// The last slot on the stack is used for an error message, the rest are filled with nils.
        /// 
        /// Raising the error is handled (on the Lua side) with the error_wrapper_r# functions.
        /// </summary>
        private int LuaWrappedError([ConstantExpected] int nonErrorReturns, ReadOnlySpan<byte> errorMsg)
        {
            Debug.Assert(nonErrorReturns <= LuaStateWrapper.LUA_MINSTACK - 1, "Cannot safely return this many returns in an error path");

            state.ClearStack();

            var stackRes = state.TryEnsureMinimumStackCapacity(nonErrorReturns + 1);
            Debug.Assert(stackRes, "Bounds check above should mean this never fails");

            for (var i = 0; i < nonErrorReturns; i++)
            {
                state.PushNil();
            }

            if (!state.TryPushBuffer(errorMsg))
            {
                // Don't have enough space to provide the actual error, which is itself an OOM error
                return LuaWrappedError(nonErrorReturns, constStrs.OutOfMemory);
            }

            return nonErrorReturns + 1;
        }

        /// <summary>
        /// We can't use Lua's normal error handling functions on Linux, so instead we go through wrappers.
        /// 
        /// The last slot on the stack is used for an error message, the rest are filled with nils.
        /// 
        /// Raising the error is handled (on the Lua side) with the error_wrapper_r# functions.
        /// </summary>
        private int LuaWrappedError([ConstantExpected] int nonErrorReturns, int constStringRegistryIndex)
        {
            Debug.Assert(nonErrorReturns <= LuaStateWrapper.LUA_MINSTACK - 1, "Cannot safely return this many returns in an error path");

            state.ClearStack();

            var stackRes = state.TryEnsureMinimumStackCapacity(nonErrorReturns + 1);
            Debug.Assert(stackRes, "Should never fail, as nonErrorReturns+1 <= LUA_MINSTACK");

            for (var i = 0; i < nonErrorReturns; i++)
            {
                state.PushNil();
            }

            state.PushConstantString(constStringRegistryIndex);
            return nonErrorReturns + 1;
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
                logger?.LogError("RESP3 Response not fully consumed, this should never happen");

                return LuaWrappedError(1, constStrs.UnexpectedError);
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
                        // Space for table, 'ok', and value
                        const int NeededStackSize = 3;

                        if (!state.TryEnsureMinimumStackCapacity(NeededStackSize))
                        {
                            respPtr = respEnd;
                            return LuaWrappedError(1, constStrs.InsufficientLuaStackSpace);
                        }

                        // Construct a table = { 'ok': value }
                        state.CreateTable(0, 1);
                        state.PushConstantString(constStrs.OkLower);

                        if (!state.TryPushBuffer(resultSpan))
                        {
                            return LuaWrappedError(1, constStrs.OutOfMemory);
                        }

                        state.RawSet(curTop + 1);

                        return 1;
                    }
                    goto default;

                // Integer (Common)
                case ':':
                    if (RespReadUtils.TryReadInt64(out var number, ref respPtr, respEnd))
                    {
                        // Space for integer
                        const int NeededStackSize = 1;

                        if (!state.TryEnsureMinimumStackCapacity(NeededStackSize))
                        {
                            respPtr = respEnd;
                            return LuaWrappedError(1, constStrs.InsufficientLuaStackSpace);
                        }

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
                            return LuaWrappedError(1, constStrs.ErrUnknown);
                        }

                        return LuaWrappedError(1, errSpan);
                    }
                    goto default;

                // Bulk string or null bulk string (Common)
                case '$':
                    var remainingLength = respEnd - respPtr;

                    if (remainingLength >= 5 && new ReadOnlySpan<byte>(respPtr + 1, 4).SequenceEqual("-1\r\n"u8))
                    {
                        // Space for boolean
                        const int NeededStackSize = 1;

                        if (!state.TryEnsureMinimumStackCapacity(NeededStackSize))
                        {
                            respPtr = respEnd;
                            return LuaWrappedError(1, constStrs.InsufficientLuaStackSpace);
                        }

                        // Bulk null strings are mapped to FALSE
                        // See: https://redis.io/docs/latest/develop/interact/programmability/lua-api/#lua-to-resp2-type-conversion
                        state.PushBoolean(false);

                        respPtr += 5;

                        return 1;
                    }
                    else if (RespReadUtils.TryReadSpanWithLengthHeader(out var bulkSpan, ref respPtr, respEnd))
                    {
                        // Space for string
                        const int NeededStackSize = 3;

                        if (!state.TryEnsureMinimumStackCapacity(NeededStackSize))
                        {
                            respPtr = respEnd;
                            return LuaWrappedError(1, constStrs.InsufficientLuaStackSpace);
                        }

                        if (!state.TryPushBuffer(bulkSpan))
                        {
                            return LuaWrappedError(1, constStrs.OutOfMemory);
                        }

                        return 1;
                    }
                    goto default;

                // Array (Common)
                case '*':
                    if (RespReadUtils.TryReadSignedArrayLength(out var arrayItemCount, ref respPtr, respEnd))
                    {
                        if (arrayItemCount == -1)
                        {
                            // Space for boolean
                            const int NeededStackSize = 3;

                            if (!state.TryEnsureMinimumStackCapacity(NeededStackSize))
                            {
                                respPtr = respEnd;
                                return LuaWrappedError(1, constStrs.InsufficientLuaStackSpace);
                            }

                            state.PushBoolean(false);
                        }
                        else
                        {
                            // Space for table
                            const int NeededStackSize = 1;

                            if (!state.TryEnsureMinimumStackCapacity(NeededStackSize))
                            {
                                respPtr = respEnd;
                                return LuaWrappedError(1, constStrs.InsufficientLuaStackSpace);
                            }

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
                        // Space for table, 'map', and sub-table
                        const int NeededStackSize = 3;

                        if (!state.TryEnsureMinimumStackCapacity(NeededStackSize))
                        {
                            respPtr = respEnd;
                            return LuaWrappedError(1, constStrs.InsufficientLuaStackSpace);
                        }

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

                            // Space for nil
                            const int NeededStackSize = 1;

                            if (!state.TryEnsureMinimumStackCapacity(NeededStackSize))
                            {
                                respPtr = respEnd;
                                return LuaWrappedError(1, constStrs.InsufficientLuaStackSpace);
                            }

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
                        // Space for table, 'set', sub-table, key, boolean
                        const int NeededStackSize = 5;

                        if (!state.TryEnsureMinimumStackCapacity(NeededStackSize))
                        {
                            respPtr = respEnd;
                            return LuaWrappedError(1, constStrs.InsufficientLuaStackSpace);
                        }

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
                        // Space for boolean
                        const int NeededStackSize = 1;

                        if (!state.TryEnsureMinimumStackCapacity(NeededStackSize))
                        {
                            respPtr = respEnd;
                            return LuaWrappedError(1, constStrs.InsufficientLuaStackSpace);
                        }

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

                            // Space for table, 'double', and number
                            const int NeededStackSize = 3;

                            if (!state.TryEnsureMinimumStackCapacity(NeededStackSize))
                            {
                                respPtr = respEnd;
                                return LuaWrappedError(1, constStrs.InsufficientLuaStackSpace);
                            }

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

                                // Space for table, 'big_number', and string
                                const int NeededStackSize = 3;

                                if (!state.TryEnsureMinimumStackCapacity(NeededStackSize))
                                {
                                    respPtr = respEnd;
                                    return LuaWrappedError(1, constStrs.InsufficientLuaStackSpace);
                                }

                                // Create table like { big_number = <bigNumBuf> }
                                state.CreateTable(1, 0);
                                state.PushConstantString(constStrs.BigNumber);

                                if (!state.TryPushBuffer(bigNumSpan))
                                {
                                    return LuaWrappedError(1, constStrs.OutOfMemory);
                                }

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

                            // Space for table, 'format'|'string', and string
                            const int NeededStackSize = 3;

                            if (!state.TryEnsureMinimumStackCapacity(NeededStackSize))
                            {
                                respPtr = respEnd;
                                return LuaWrappedError(1, constStrs.InsufficientLuaStackSpace);
                            }

                            state.CreateTable(2, 0);

                            state.PushConstantString(constStrs.Format);

                            if (!state.TryPushBuffer(format))
                            {
                                return LuaWrappedError(1, constStrs.OutOfMemory);
                            }

                            state.RawSet(curTop + 1);

                            state.PushConstantString(constStrs.String);

                            if (!state.TryPushBuffer(data))
                            {
                                return LuaWrappedError(1, constStrs.OutOfMemory);
                            }

                            state.RawSet(curTop + 1);

                            return 1;
                        }
                    }
                    goto default;

                default:
                    logger?.LogError("Unexpected response, this should never happen");
                    return LuaWrappedError(1, constStrs.UnexpectedError);
            }
        }

        /// <summary>
        /// Runs the precompiled Lua function with the given outer session.
        /// 
        /// Response is written directly into the <see cref="RespServerSession"/>.
        /// </summary>
        public unsafe void RunForSession(int count, RespServerSession outerSession)
        {
            // Space for function
            const int NeededStackSize = 1;

            var stackRes = state.TryEnsureMinimumStackCapacity(NeededStackSize);
            Debug.Assert(stackRes, "LUA_MINSTACK should be large enough that this never fails");

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
                    if (callRes != LuaStatus.OK || state.StackTop != 0)
                    {
                        ReadOnlySpan<byte> err;
                        if (state.StackTop >= 1 && state.Type(1) == LuaType.String)
                        {
                            state.KnownStringToBuffer(1, out err);
                        }
                        else
                        {
                            err = "Internal Lua Error"u8;
                        }

                        while (!RespWriteUtils.TryWriteError(err, ref outerSession.dcurr, outerSession.dend))
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
        /// Runs the precompiled Lua function with specified (keys, argv) state.
        /// 
        /// Meant for use from a .NET host rather than in Garnet properly.
        /// </summary>
        public unsafe object RunForRunner(string[] keys = null, string[] argv = null)
        {
            // Space for function
            const int NeededStackSize = 1;

            var stackRes = state.TryEnsureMinimumStackCapacity(NeededStackSize);
            Debug.Assert(stackRes, "LUA_MINSTACK should be large enough that this never fails");

            try
            {
                LuaRunnerTrampolines.SetCallbackContext(this);
                ResetTimeout();

                try
                {
                    preambleKeys = keys;
                    preambleArgv = argv;

                    state.PushCFunction(&LuaRunnerTrampolines.RunPreambleForRunner);
                    var preambleRes = state.PCall(0, 0);
                    if (preambleRes != LuaStatus.OK || state.StackTop != 0)
                    {
                        string errMsg;
                        if (state.StackTop >= 1 && state.Type(1) == LuaType.String)
                        {
                            // We control the definition of LoaderBlock, so we know this is a string
                            state.KnownStringToBuffer(1, out var preambleErrSpan);
                            errMsg = Encoding.UTF8.GetString(preambleErrSpan);
                        }
                        else
                        {
                            errMsg = "No error provided";
                        }

                        throw new GarnetException($"Could not run function preamble: {errMsg}");
                    }
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
        => state.TrySetHook(&LuaRunnerTrampolines.RequestTimeout, LuaHookMask.Count, 1);

        /// <summary>
        /// Remove extra keys and args from KEYS and ARGV globals.
        /// </summary>
        internal bool TryResetParameters(int nKeys, int nArgs, out LuaStatus failingStatus)
        {
            // Space for key count, value count, and function
            const int NeededStackSize = 3;

            var stackRes = state.TryEnsureMinimumStackCapacity(NeededStackSize);
            Debug.Assert(stackRes, "LUA_MINSTACK should be large enough that this never fails");

            if (keyLength > nKeys || argvLength > nArgs)
            {
                _ = state.RawGetInteger(LuaType.Function, (int)LuaRegistry.Index, resetKeysAndArgvRegistryIndex);

                state.PushInteger(nKeys + 1);
                state.PushInteger(nArgs + 1);

                var callRes = state.PCall(2, 0);
                if (callRes != LuaStatus.OK)
                {
                    failingStatus = callRes;
                    return false;
                }
            }

            keyLength = nKeys;
            argvLength = nArgs;

            failingStatus = LuaStatus.OK;
            return true;
        }

        /// <summary>
        /// Takes .NET strings for keys and args and pushes them into KEYS and ARGV globals.
        /// </summary>
        private int LoadParametersForRunner(string[] keys, string[] argv)
        {
            // Space for table and value
            const int NeededStackSize = 2;

            var stackRes = state.TryEnsureMinimumStackCapacity(NeededStackSize);
            Debug.Assert(stackRes, "LUA_MINSTACK should be large enough that this never fails");

            if (!TryResetParameters(keys?.Length ?? 0, argv?.Length ?? 0, out var failingStatus))
            {
                var constStrId =
                    failingStatus switch
                    {
                        LuaStatus.ErrSyntax => constStrs.ParameterResetFailedSyntax,
                        LuaStatus.ErrMem => constStrs.ParameterResetFailedMemory,
                        LuaStatus.ErrRun => constStrs.ParameterResetFailedRuntime,
                        LuaStatus.ErrErr or LuaStatus.Yield or LuaStatus.OK or _ => constStrs.ParameterResetFailedOther,
                    };
                return LuaWrappedError(0, constStrId);
            }

            if (keys != null)
            {
                // get KEYS on the stack
                _ = state.RawGetInteger(LuaType.Table, (int)LuaRegistry.Index, keysTableRegistryIndex);

                for (var i = 0; i < keys.Length; i++)
                {
                    // equivalent to KEYS[i+1] = keys[i]
                    var key = keys[i];
                    PrepareString(key, scratchBufferManager, out var encoded);

                    if (!state.TryPushBuffer(encoded))
                    {
                        return LuaWrappedError(0, constStrs.OutOfMemory);
                    }

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

                    if (!state.TryPushBuffer(encoded))
                    {
                        return LuaWrappedError(0, constStrs.OutOfMemory);
                    }

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
            // Space for function
            const int NeededStackSize = 1;

            var stackRes = state.TryEnsureMinimumStackCapacity(NeededStackSize);
            Debug.Assert(stackRes, "LUA_MINSTACK should be large enough that this never fails");

            try
            {
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
        private unsafe void WriteResponse<TResponse>(ref TResponse resp)
            where TResponse : struct, IResponseAdapter
        {
            // Copy the response object before a trial serialization
            const int TopLevelNeededStackSpace = 1;

            // Need space for the lookup keys ("ok", "err", etc.) and their values
            const int KeyNeededStackSpace = 1;

            // 2 for the returned key and value from Next, 1 for the temp copy of the returned key
            const int MapNeededStackSpace = 3;

            // 2 for the returned key and value from Next, 1 for the temp copy of the returned key
            const int ArrayNeededStackSize = 3;

            // 2 for the returned key and value from Next
            const int SetNeededStackSize = 2;

            // 1 for the pending value
            const int TableNeededStackSize = 1;

            // This is a bit tricky since we don't know in advance how deep
            // the stack could get during serialization.
            //
            // Typically we'll have plenty of space, so we don't want to do a
            // double pass unless we have to.
            //
            // So what we do is serialize the response, dynamically expanding the
            // stack.  IF we have to send, we remember that and DON'T but instead
            // keep going to see if we'll run out of stack space.
            //
            // At the end, if we didn't fill the buffer (that is, we never SendAndReset)
            // and didn't run out of stack space - we just return.  If we ran out of stack space,
            // we reset resp.BufferCur and write an error out.  If we filled the buffer but
            // didn't run out of stack space, we serialize AGAIN this time know we'll succeed
            // so we can sent as we go like normal.
            //
            // Ideally we do everything in a single pass.

            if (state.StackTop == 0)
            {
                if (resp.RespProtocolVersion == 3)
                {
                    _ = TryWriteResp3Null(this, canSend: true, pop: false, ref resp, out var sendErr);
                    Debug.Assert(sendErr == -1, "Sending a top level null should always suceed since no stack space is needed");
                }
                else
                {
                    _ = TryWriteResp2Null(this, canSend: true, pop: false, ref resp, out var sendErr);
                    Debug.Assert(sendErr == -1, "Sending a top level null should always suceed since no stack space is needed");
                }

                return;
            }

            var oldCur = resp.BufferCur;

            var stackRes = state.TryEnsureMinimumStackCapacity(TopLevelNeededStackSpace);
            Debug.Assert(stackRes, "Caller should have ensured we're < LUA_MINSTACK");

            // Copy the value in case we need a second pass
            // 
            // Note that this is just copying a reference in the case it's a table, string, etc.
            state.PushValue(1);

            var wholeResponseFitInBuffer = TryWriteSingleItem(this, canSend: false, ref resp, out var err);
            if (wholeResponseFitInBuffer)
            {
                // Success in a single pass, we're done

                // Remove the extra value copy we pushed
                state.Pop(1);
                return;
            }

            // Either an error occurred, or we need a second pass
            // Regardless we need to roll BufferCur back

            resp.BufferCur = oldCur;

            if (err != -1)
            {
                // An error was encountered, so write it out

                state.ClearStack();
                state.PushConstantString(err);
                state.KnownStringToBuffer(1, out var errBuff);

                while (!RespWriteUtils.TryWriteError(errBuff, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                return;
            }

            // Second pass is required, but now we KNOW it will succeed
            var secondPassRes = TryWriteSingleItem(this, canSend: true, ref resp, out var secondPassErr);
            Debug.Assert(!secondPassRes, "Should have required a send in this path");
            Debug.Assert(secondPassErr == -1, "No error should be possible on the second pass");

            // Write out a single RESP item and pop it off the stack, returning true if all fit in the current send buffer
            static bool TryWriteSingleItem(LuaRunner runner, bool canSend, ref TResponse resp, out int errConstStrIndex)
            {
                var curTop = runner.state.StackTop;
                var retType = runner.state.Type(curTop);
                var isNullish = retType is LuaType.Nil or LuaType.UserData or LuaType.Function or LuaType.Thread or LuaType.UserData;

                if (isNullish)
                {
                    var fitInBuffer = true;

                    if (resp.RespProtocolVersion == 3)
                    {
                        if (!TryWriteResp3Null(runner, canSend, pop: true, ref resp, out errConstStrIndex))
                        {
                            fitInBuffer = false;
                        }
                    }
                    else
                    {
                        if (!TryWriteResp2Null(runner, canSend, pop: true, ref resp, out errConstStrIndex))
                        {
                            fitInBuffer = false;
                        }
                    }

                    return fitInBuffer;
                }
                else if (retType == LuaType.Number)
                {
                    return TryWriteNumber(runner, canSend, ref resp, out errConstStrIndex);
                }
                else if (retType == LuaType.String)
                {
                    return TryWriteString(runner, canSend, ref resp, out errConstStrIndex);
                }
                else if (retType == LuaType.Boolean)
                {
                    if (runner.respServerSession?.respProtocolVersion == 3 && resp.RespProtocolVersion == 2)
                    {
                        // This is not in spec, but is how Redis actually behaves
                        var toPush = runner.state.ToBoolean(curTop) ? 1 : 0;
                        runner.state.Pop(1);
                        runner.state.PushInteger(toPush);

                        return TryWriteNumber(runner, canSend, ref resp, out errConstStrIndex);
                    }
                    else if (runner.respServerSession?.respProtocolVersion == 2 && resp.RespProtocolVersion == 3)
                    {
                        // Likewise, this is how Redis actually behaves
                        if (runner.state.ToBoolean(curTop))
                        {
                            runner.state.Pop(1);
                            runner.state.PushInteger(1);

                            return TryWriteNumber(runner, canSend, ref resp, out errConstStrIndex);
                        }
                        else
                        {
                            return TryWriteResp3Null(runner, canSend, pop: true, ref resp, out errConstStrIndex);
                        }
                    }
                    else if (runner.respServerSession?.respProtocolVersion == 3)
                    {
                        // RESP3 has a proper boolean type
                        return TryWriteResp3Boolean(runner, canSend, ref resp, out errConstStrIndex);
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

                            return TryWriteNumber(runner, canSend, ref resp, out errConstStrIndex);
                        }
                        else
                        {
                            return TryWriteResp2Null(runner, canSend, pop: true, ref resp, out errConstStrIndex);
                        }
                    }
                }
                else if (retType == LuaType.Table)
                {
                    // Redis does not respect metatables, so RAW access is ok here

                    var stackRes = runner.state.TryEnsureMinimumStackCapacity(KeyNeededStackSpace);
                    Debug.Assert(stackRes, "Space should have already been reserved");

                    runner.state.PushConstantString(runner.constStrs.Double);
                    var doubleType = runner.state.RawGet(null, curTop);
                    if (doubleType == LuaType.Number)
                    {
                        var fitInBuffer = true;

                        if (resp.RespProtocolVersion == 3)
                        {
                            if (!TryWriteDouble(runner, canSend, ref resp, out errConstStrIndex))
                            {
                                fitInBuffer = false;
                                if (errConstStrIndex != -1)
                                {
                                    // Fail the whole serialization
                                    return false;
                                }
                            }
                        }
                        else
                        {
                            // Force double to string for RESP2
                            runner.state.NumberToString(curTop + 1, out _);
                            if (!TryWriteString(runner, canSend, ref resp, out errConstStrIndex))
                            {
                                fitInBuffer = false;
                                if (errConstStrIndex != -1)
                                {
                                    // Fail the whole serialization
                                    return false;
                                }
                            }
                        }

                        // Remove table from stack
                        runner.state.Pop(1);

                        return fitInBuffer;
                    }

                    // Remove whatever we read from the table under the "double" key
                    runner.state.Pop(1);

                    runner.state.PushConstantString(runner.constStrs.Map);
                    var mapType = runner.state.RawGet(null, curTop);
                    if (mapType == LuaType.Table)
                    {
                        var fitInBuffer = true;

                        if (resp.RespProtocolVersion == 3)
                        {
                            if (!TryWriteMap(runner, canSend, ref resp, out errConstStrIndex))
                            {
                                fitInBuffer = false;
                                if (errConstStrIndex != -1)
                                {
                                    // Fail the whole serialization
                                    return false;
                                }
                            }
                        }
                        else
                        {
                            if (!TryWriteMapToArray(runner, canSend, ref resp, out errConstStrIndex))
                            {
                                fitInBuffer = false;
                                if (errConstStrIndex != -1)
                                {
                                    // Fail the whole serialization
                                    return false;
                                }
                            }
                        }

                        // remove table from stack
                        runner.state.Pop(1);

                        return fitInBuffer;
                    }

                    // Remove whatever we read from the table under the "map" key
                    runner.state.Pop(1);

                    runner.state.PushConstantString(runner.constStrs.Set);
                    var setType = runner.state.RawGet(null, curTop);
                    if (setType == LuaType.Table)
                    {
                        var fitInBuffer = false;

                        if (resp.RespProtocolVersion == 3)
                        {
                            if (!TryWriteSet(runner, canSend, ref resp, out errConstStrIndex))
                            {
                                fitInBuffer = false;
                                if (errConstStrIndex != -1)
                                {
                                    // Fail the whole serialization
                                    return false;
                                }
                            }
                        }
                        else
                        {
                            if (!TryWriteSetToArray(runner, canSend, ref resp, out errConstStrIndex))
                            {
                                fitInBuffer = false;
                                if (errConstStrIndex != -1)
                                {
                                    // Fail the whole serialization
                                    return false;
                                }
                            }
                        }

                        // remove table from stack
                        runner.state.Pop(1);

                        return fitInBuffer;
                    }

                    // Remove whatever we read from the table under the "set" key
                    runner.state.Pop(1);

                    // If the key "ok" is in there, we need to short circuit
                    runner.state.PushConstantString(runner.constStrs.OkLower);
                    var okType = runner.state.RawGet(null, curTop);
                    if (okType == LuaType.String)
                    {
                        var fitInBuffer = true;
                        if (!TryWriteString(runner, canSend, ref resp, out errConstStrIndex))
                        {
                            fitInBuffer = false;
                            if (errConstStrIndex != -1)
                            {
                                // Fail the whole serialization
                                return false;
                            }
                        }

                        // Remove table from stack
                        runner.state.Pop(1);

                        return fitInBuffer;
                    }

                    // Remove whatever we read from the table under the "ok" key
                    runner.state.Pop(1);

                    // If the key "err" is in there, we need to short circuit 
                    runner.state.PushConstantString(runner.constStrs.Err);

                    var errType = runner.state.RawGet(null, curTop);
                    if (errType == LuaType.String)
                    {
                        var fitInBuffer = false;

                        if (!TryWriteError(runner, canSend, ref resp, out errConstStrIndex))
                        {
                            fitInBuffer = false;
                            if (errConstStrIndex != -1)
                            {
                                // Fail the whole serialization
                                return false;
                            }
                        }

                        // Remove table from stack
                        runner.state.Pop(1);

                        return fitInBuffer;
                    }

                    // Remove whatever we read from the table under the "err" key
                    runner.state.Pop(1);

                    // Map this table to an array
                    return TryWriteTableToArray(runner, canSend, ref resp, out errConstStrIndex);
                }

                Debug.Fail($"All types should have been handled, found {retType}");
                errConstStrIndex = -1;
                return true;
            }

            // Write out $-1\r\n (the RESP2 null) and (optionally) pop the null value off the stack, returning true if all fit in the current send buffer
            static unsafe bool TryWriteResp2Null(LuaRunner runner, bool canSend, bool pop, ref TResponse resp, out int errConstStrIndex)
            {
                var fitInBuffer = true;

                while (!RespWriteUtils.TryWriteNull(ref resp.BufferCur, resp.BufferEnd))
                {
                    fitInBuffer = false;

                    if (canSend)
                    {
                        resp.SendAndReset();
                    }
                    else
                    {
                        break;
                    }
                }

                if (pop)
                {
                    runner.state.Pop(1);
                }

                errConstStrIndex = -1;
                return fitInBuffer;
            }

            // Write out _\r\n (the RESP3 null) and (optionally) pop the null value off the stack, returning true if all fit in the current send buffer
            static unsafe bool TryWriteResp3Null(LuaRunner runner, bool canSend, bool pop, ref TResponse resp, out int errConstStrIndex)
            {
                var fitInBuffer = true;

                while (!RespWriteUtils.TryWriteResp3Null(ref resp.BufferCur, resp.BufferEnd))
                {
                    fitInBuffer = false;

                    if (canSend)
                    {
                        resp.SendAndReset();
                    }
                    else
                    {
                        break;
                    }
                }

                if (pop)
                {
                    runner.state.Pop(1);
                }

                errConstStrIndex = -1;
                return fitInBuffer;
            }

            // Writes the number on the top of the stack, removes it from the stack, returning true if all fit in the current send buffer
            static unsafe bool TryWriteNumber(LuaRunner runner, bool canSend, ref TResponse resp, out int errConstStrIndex)
            {
                Debug.Assert(runner.state.Type(runner.state.StackTop) == LuaType.Number, "Number was not on top of stack");

                // Redis unconditionally converts all "number" replies to integer replies so we match that
                // 
                // See: https://redis.io/docs/latest/develop/interact/programmability/lua-api/#lua-to-resp2-type-conversion
                var num = (long)runner.state.CheckNumber(runner.state.StackTop);

                var fitInBuffer = true;

                while (!RespWriteUtils.TryWriteInt64(num, ref resp.BufferCur, resp.BufferEnd))
                {
                    fitInBuffer = false;

                    if (canSend)
                    {
                        resp.SendAndReset();
                    }
                    else
                    {
                        break;
                    }
                }

                runner.state.Pop(1);

                errConstStrIndex = -1;
                return fitInBuffer;
            }

            // Writes the string on the top of the stack, removes it from the stack, returning true if all fit in the current send buffer
            static unsafe bool TryWriteString(LuaRunner runner, bool canSend, ref TResponse resp, out int errConstStrIndex)
            {
                runner.state.KnownStringToBuffer(runner.state.StackTop, out var buf);

                var fitInBuffer = true;

                // Strings can be veeeerrrrry large, so we can't use the short helpers
                // Thus we write the full string directly
                while (!RespWriteUtils.TryWriteBulkStringLength(buf, ref resp.BufferCur, resp.BufferEnd))
                {
                    fitInBuffer = false;

                    if (canSend)
                    {
                        resp.SendAndReset();
                    }
                    else
                    {
                        break;
                    }
                }

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
                        fitInBuffer = false;

                        if (canSend)
                        {
                            resp.SendAndReset();
                        }
                        else
                        {
                            break;
                        }
                    }

                    // Move past the data we wrote out
                    buf = buf.Slice(copyLen);
                }

                // End the string
                while (!RespWriteUtils.TryWriteNewLine(ref resp.BufferCur, resp.BufferEnd))
                {
                    fitInBuffer = false;

                    if (canSend)
                    {
                        resp.SendAndReset();
                    }
                    else
                    {
                        break;
                    }
                }

                runner.state.Pop(1);

                errConstStrIndex = -1;
                return fitInBuffer;
            }

            // Writes the boolean on the top of the stack, removes it from the stack, returning true if all fit in the current send buffer
            static unsafe bool TryWriteResp3Boolean(LuaRunner runner, bool canSend, ref TResponse resp, out int errConstStrIndex)
            {
                Debug.Assert(runner.state.Type(runner.state.StackTop) == LuaType.Boolean, "Boolean was not on top of stack");

                var fitInBuffer = true;

                // In RESP3 there is a dedicated boolean type
                if (runner.state.ToBoolean(runner.state.StackTop))
                {
                    while (!RespWriteUtils.TryWriteTrue(ref resp.BufferCur, resp.BufferEnd))
                    {
                        fitInBuffer = false;

                        if (canSend)
                        {
                            resp.SendAndReset();
                        }
                        else
                        {
                            break;
                        }
                    }
                }
                else
                {
                    while (!RespWriteUtils.TryWriteFalse(ref resp.BufferCur, resp.BufferEnd))
                    {
                        fitInBuffer = false;

                        if (canSend)
                        {
                            resp.SendAndReset();
                        }
                        else
                        {
                            break;
                        }
                    }
                }

                runner.state.Pop(1);

                errConstStrIndex = -1;
                return fitInBuffer;
            }

            // Writes the number on the top of the stack as a double, removes it from the stack, returning true if all fit in the current send buffer
            static unsafe bool TryWriteDouble(LuaRunner runner, bool canSend, ref TResponse resp, out int errConstStrIndex)
            {
                Debug.Assert(runner.state.Type(runner.state.StackTop) == LuaType.Number, "Number was not on top of stack");

                var fitInBuffer = true;

                var num = runner.state.CheckNumber(runner.state.StackTop);

                while (!RespWriteUtils.TryWriteDoubleNumeric(num, ref resp.BufferCur, resp.BufferEnd))
                {
                    fitInBuffer = false;

                    if (canSend)
                    {
                        resp.SendAndReset();
                    }
                    else
                    {
                        break;
                    }
                }

                runner.state.Pop(1);

                errConstStrIndex = -1;
                return fitInBuffer;
            }

            // Write a table on the top of the stack as a map, removes it from the stack, returning true if all fit in the current send buffer
            static unsafe bool TryWriteMap(LuaRunner runner, bool canSend, ref TResponse resp, out int errConstStrIndex)
            {
                Debug.Assert(runner.state.Type(runner.state.StackTop) == LuaType.Table, "Table was not on top of stack");

                if (!runner.state.TryEnsureMinimumStackCapacity(MapNeededStackSpace))
                {
                    errConstStrIndex = runner.constStrs.InsufficientLuaStackSpace;
                    return false;
                }

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

                var fitInBuffer = true;

                // Write the map header
                while (!RespWriteUtils.TryWriteMapLength(mapSize, ref resp.BufferCur, resp.BufferEnd))
                {
                    fitInBuffer = false;

                    if (fitInBuffer)
                    {
                        resp.SendAndReset();
                    }
                    else
                    {
                        break;
                    }
                }

                // Write the values out by traversing the table again
                runner.state.PushNil();
                while (runner.state.Next(tableIx) != 0)
                {
                    // Copy key to top of stack
                    runner.state.PushValue(tableIx + 1);

                    // Write (and remove) key out
                    if (!TryWriteSingleItem(runner, canSend, ref resp, out errConstStrIndex))
                    {
                        fitInBuffer = false;

                        if (errConstStrIndex != -1)
                        {
                            // Fail the whole serialization
                            return false;
                        }
                    }

                    // Write (and remove) value out
                    if (!TryWriteSingleItem(runner, canSend, ref resp, out errConstStrIndex))
                    {
                        fitInBuffer = false;

                        if (errConstStrIndex != -1)
                        {
                            // Fail the whole serialization
                            return false;
                        }
                    }

                    // Now we have the original key value on the stack
                }

                // Remove the table
                runner.state.Pop(1);

                errConstStrIndex = -1;
                return fitInBuffer;
            }

            // Convert a table to an array, where each key-value pair is converted to 2 entries, returning true if all fit in the current send buffer
            static unsafe bool TryWriteMapToArray(LuaRunner runner, bool canSend, ref TResponse resp, out int errConstStrIndex)
            {
                Debug.Assert(runner.state.Type(runner.state.StackTop) == LuaType.Table, "Table was not on top of stack");

                if (!runner.state.TryEnsureMinimumStackCapacity(ArrayNeededStackSize))
                {
                    errConstStrIndex = runner.constStrs.InsufficientLuaStackSpace;
                    return false;
                }

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

                var fitInBuffer = true;

                // Write the array header
                while (!RespWriteUtils.TryWriteArrayLength(arraySize, ref resp.BufferCur, resp.BufferEnd))
                {
                    fitInBuffer = false;

                    if (canSend)
                    {
                        resp.SendAndReset();
                    }
                    else
                    {
                        break;
                    }
                }

                // Write the values out by traversing the table again
                runner.state.PushNil();
                while (runner.state.Next(tableIx) != 0)
                {
                    // Copy key to top of stack
                    runner.state.PushValue(tableIx + 1);

                    // Write (and remove) key out
                    if (!TryWriteSingleItem(runner, canSend, ref resp, out errConstStrIndex))
                    {
                        fitInBuffer = false;
                        if (errConstStrIndex != -1)
                        {
                            // Fail the whole serialization
                            return false;
                        }
                    }

                    // Write (and remove) value out
                    if (!TryWriteSingleItem(runner, canSend, ref resp, out errConstStrIndex))
                    {
                        fitInBuffer = false;
                        if (errConstStrIndex != -1)
                        {
                            // Fail the whole serialization
                            return false;
                        }
                    }

                    // Now we have the original key value on the stack
                }

                // Remove the table
                runner.state.Pop(1);
                errConstStrIndex = -1;
                return fitInBuffer;
            }

            // Write a table on the top of the stack as a set, removes it from the stack, returning true if all fit in the current send buffer
            static unsafe bool TryWriteSet(LuaRunner runner, bool canSend, ref TResponse resp, out int errConstStrIndex)
            {
                Debug.Assert(runner.state.Type(runner.state.StackTop) == LuaType.Table, "Table was not on top of stack");

                if (!runner.state.TryEnsureMinimumStackCapacity(SetNeededStackSize))
                {
                    errConstStrIndex = runner.constStrs.InsufficientLuaStackSpace;
                    return false;
                }

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

                var fitInBuffer = true;

                // Write the set header
                while (!RespWriteUtils.TryWriteSetLength(setSize, ref resp.BufferCur, resp.BufferEnd))
                {
                    fitInBuffer = false;

                    if (canSend)
                    {
                        resp.SendAndReset();
                    }
                    else
                    {
                        break;
                    }
                }

                // Write the values out by traversing the table again
                runner.state.PushNil();
                while (runner.state.Next(tableIx) != 0)
                {
                    // Remove the value, it's ignored
                    runner.state.Pop(1);

                    // Make a copy of the key
                    runner.state.PushValue(tableIx + 1);

                    // Write (and remove) key copy out
                    if (!TryWriteSingleItem(runner, canSend, ref resp, out errConstStrIndex))
                    {
                        fitInBuffer = false;
                        if (errConstStrIndex != -1)
                        {
                            // Fail the whole serialization
                            return false;
                        }
                    }

                    // Now we have the original key value on the stack
                }

                // Remove the table
                runner.state.Pop(1);

                errConstStrIndex = -1;
                return fitInBuffer;
            }

            // Write a table on the top of the stack as an array that contains only the keys of the
            // table, then remove the table from the stack, returning true if all fit in the current send buffer
            static unsafe bool TryWriteSetToArray(LuaRunner runner, bool canSend, ref TResponse resp, out int errConstStrIndex)
            {
                Debug.Assert(runner.state.Type(runner.state.StackTop) == LuaType.Table, "Table was not on top of stack");

                if (!runner.state.TryEnsureMinimumStackCapacity(SetNeededStackSize))
                {
                    errConstStrIndex = runner.constStrs.InsufficientLuaStackSpace;
                    return false;
                }

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

                var fitInBuffer = true;

                // Write the array header
                while (!RespWriteUtils.TryWriteArrayLength(arraySize, ref resp.BufferCur, resp.BufferEnd))
                {
                    fitInBuffer = false;

                    if (canSend)
                    {
                        resp.SendAndReset();
                    }
                    else
                    {
                        break;
                    }
                }

                // Write the values out by traversing the table again
                runner.state.PushNil();
                while (runner.state.Next(tableIx) != 0)
                {
                    // Remove the value, it's ignored
                    runner.state.Pop(1);

                    // Make a copy of the key
                    runner.state.PushValue(tableIx + 1);

                    // Write (and remove) key copy out
                    if (!TryWriteSingleItem(runner, canSend, ref resp, out errConstStrIndex))
                    {
                        fitInBuffer = false;
                        if (errConstStrIndex != -1)
                        {
                            // Fail the whole serialization
                            return false;
                        }
                    }

                    // Now we have the original key value on the stack
                }

                // Remove the table
                runner.state.Pop(1);

                errConstStrIndex = -1;
                return fitInBuffer;
            }

            // Writes the string on the top of the stack out as an error, removes the string from the stack, returning true if all fit in the current send buffer
            static unsafe bool TryWriteError(LuaRunner runner, bool canSend, ref TResponse resp, out int errConstStrIndex)
            {
                runner.state.KnownStringToBuffer(runner.state.StackTop, out var errBuff);

                var fitInBuffer = true;

                while (!RespWriteUtils.TryWriteError(errBuff, ref resp.BufferCur, resp.BufferEnd))
                {
                    fitInBuffer = false;

                    if (canSend)
                    {
                        resp.SendAndReset();
                    }
                    else
                    {
                        break;
                    }
                }

                runner.state.Pop(1);

                errConstStrIndex = -1;
                return fitInBuffer;
            }

            // Writes the table on the top of the stack out as an array, removed table from the stack, returning true if all fit in the current send buffer
            static unsafe bool TryWriteTableToArray(LuaRunner runner, bool canSend, ref TResponse resp, out int errConstStrIndex)
            {
                // Redis does not respect metatables, so RAW access is ok here
                Debug.Assert(runner.state.Type(runner.state.StackTop) == LuaType.Table, "Table was not on top of stack");

                if (!runner.state.TryEnsureMinimumStackCapacity(TableNeededStackSize))
                {
                    errConstStrIndex = runner.constStrs.InsufficientLuaStackSpace;
                    return false;
                }

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

                var fitInBuffer = true;

                while (!RespWriteUtils.TryWriteArrayLength(trueLen, ref resp.BufferCur, resp.BufferEnd))
                {
                    fitInBuffer = false;

                    if (canSend)
                    {
                        resp.SendAndReset();
                    }
                    else
                    {
                        break;
                    }
                }

                for (var i = 1; i <= trueLen; i++)
                {
                    // Push item at index i onto the stack
                    _ = runner.state.RawGetInteger(null, runner.state.StackTop, i);

                    // Write the item out, removing it from teh stack
                    if (!TryWriteSingleItem(runner, canSend, ref resp, out errConstStrIndex))
                    {
                        fitInBuffer = false;
                        if (errConstStrIndex != -1)
                        {
                            // Fail the whole serialization
                            return false;
                        }
                    }
                }

                // Remove the table
                runner.state.Pop(1);

                errConstStrIndex = -1;
                return fitInBuffer;
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
}