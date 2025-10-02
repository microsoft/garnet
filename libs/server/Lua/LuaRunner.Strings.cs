// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;

namespace Garnet.server
{
    // Constant string infrastructure goes in here.
    internal sealed partial class LuaRunner
    {
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
            /// <see cref="CmdStrings.LUA_bad_arg_format"/>
            internal int BadArgFormat { get; }
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
            /// <see cref="CmdStrings.LUA_DIFF"/>
            internal int DIFF { get; }
            /// <see cref="CmdStrings.LUA_KEYS"/>
            internal int KEYS { get; }
            /// <see cref="CmdStrings.LUA_ARGV"/>
            internal int ARGV { get; }

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
                BadArgFormat = ConstantStringToRegistry(ref state, CmdStrings.LUA_bad_arg_format);
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
                DIFF = ConstantStringToRegistry(ref state, CmdStrings.LUA_DIFF);
                KEYS = ConstantStringToRegistry(ref state, CmdStrings.LUA_KEYS);
                ARGV = ConstantStringToRegistry(ref state, CmdStrings.LUA_ARGV);
            }

            /// <summary>
            /// Some strings we use a bunch, and copying them to Lua each time is wasteful
            ///
            /// So instead we stash them in the Registry and load them by index
            /// </summary>
            private static int ConstantStringToRegistry(ref LuaStateWrapper state, ReadOnlySpan<byte> str)
            {
                if (!state.TryPushBuffer(str) || !state.TryRef(out var ret))
                {
                    throw new GarnetException("Insufficient space in Lua VM for constant string");
                }

                return ret;
            }
        }
    }
}