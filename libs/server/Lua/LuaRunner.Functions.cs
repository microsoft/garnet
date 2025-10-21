// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
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
using Tsavorite.core;

namespace Garnet.server
{
    // All "called from Lua"-functions and details go here
    //
    // One import thing is NO exceptions can bubble out of these functions
    // or .NET is going to crash horribly.
    internal sealed partial class LuaRunner
    {
        // Entry calls from Lua (via LuaRunnerTrampolines)

        /// <summary>
        /// Actually compiles for runner.
        /// 
        /// If you call this directly and Lua encounters an error, the process will crash.
        /// 
        /// Call <see cref="CompileForRunner"/> instead.
        /// </summary>
        internal int UnsafeCompileForRunner(nint luaStatePtr)
        => CompileCommon(luaStatePtr, ref runnerAdapter);

        /// <summary>
        /// Actually compiles for runner.
        /// 
        /// If you call this directly and Lua encounters an error, the process will crash.
        /// 
        /// Call <see cref="CompileForSession"/> instead.
        /// </summary>
        internal int UnsafeCompileForSession(nint luaStatePtr)
        => CompileCommon(luaStatePtr, ref sessionAdapter);

        /// <summary>
        /// Setups a script to be run.
        /// 
        /// If you call this directly and Lua encounters an error, the process will crash.
        /// 
        /// Call <see cref="RunForRunner"/> instead.
        /// </summary>
        internal int UnsafeRunPreambleForRunner(nint luaStatePtr)
        {
            // Space for table and key
            const int NeededStackSize = 2;

            state.KnownCallFromLuaEntered(luaStatePtr, 0);

            var keys = preambleKeys;
            var argv = preambleArgv;

            Debug.Assert(state.TryEnsureMinimumStackCapacity(NeededStackSize), "LUA_MINSTACK should be large enough that this never fails");

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
                if (keysArrCapacity < keys.Length)
                {
                    if (!TryRecreateKEYS(keys.Length))
                    {
                        return LuaWrappedError(0, constStrs.OutOfMemory);
                    }
                }

                // get KEYS on the stack
                _ = state.RawGetInteger(LuaType.Table, (int)LuaRegistry.Index, sandboxEnvRegistryIndex);
                state.PushConstantString(constStrs.KEYS);
                _ = state.RawGet(LuaType.Table, 1);
                state.Remove(1);

                for (var i = 0; i < keys.Length; i++)
                {
                    // equivalent to KEYS[i+1] = keys[i]
                    var key = keys[i];
                    PrepareString(key, scratchBufferBuilder, out var encoded);

                    if (!state.TryPushBuffer(encoded))
                    {
                        return LuaWrappedError(0, constStrs.OutOfMemory);
                    }

                    state.RawSetInteger(keysArrCapacity, 1, i + 1);
                }

                state.Pop(1);
            }

            if (argv != null)
            {
                if (argvArrCapacity < argv.Length)
                {
                    if (!TryRecreateARGV(argv.Length))
                    {
                        return LuaWrappedError(0, constStrs.OutOfMemory);
                    }
                }

                // get ARGV on the stack
                _ = state.RawGetInteger(LuaType.Table, (int)LuaRegistry.Index, sandboxEnvRegistryIndex);
                state.PushConstantString(constStrs.ARGV);
                _ = state.RawGet(LuaType.Table, 1);
                state.Remove(1);

                for (var i = 0; i < argv.Length; i++)
                {
                    // equivalent to ARGV[i+1] = keys[i]
                    var arg = argv[i];
                    PrepareString(arg, scratchBufferBuilder, out var encoded);

                    if (!state.TryPushBuffer(encoded))
                    {
                        return LuaWrappedError(0, constStrs.OutOfMemory);
                    }

                    state.RawSetInteger(argvArrCapacity, 1, i + 1);
                }

                state.Pop(1);
            }

            return 0;

            // Convert string into a span, using buffer for storage
            static void PrepareString(string raw, ScratchBufferBuilder buffer, out ReadOnlySpan<byte> strBytes)
            {
                // Try to fit in the existing buffer
                var into = buffer.FullBuffer();
                if (Encoding.UTF8.TryGetBytes(raw, into, out var written))
                {
                    strBytes = into[..written];
                    return;
                }

                var maxLen = Encoding.UTF8.GetMaxByteCount(raw.Length);

                buffer.Reset();
                var argSlice = buffer.CreateArgSlice(maxLen);
                var span = argSlice.Span;

                written = Encoding.UTF8.GetBytes(raw, span);
                strBytes = span[..written];
            }
        }

        /// <summary>
        /// Setups a script to be run.
        /// 
        /// If you call this directly and Lua encounters an error, the process will crash.
        /// 
        /// Call <see cref="RunForSession"/> instead.
        /// </summary>
        internal int UnsafeRunPreambleForSession(nint luaStatePtr)
        {
            state.KnownCallFromLuaEntered(luaStatePtr, 0);

            ref var parseState = ref preambleOuterSession.parseState;

            var offset = 1;
            var nKeys = preambleNKeys = parseState.GetInt(offset++);
            preambleKeyAndArgvCount--;
            if (!TryResetParameters(nKeys, preambleKeyAndArgvCount - nKeys, out var failingStatus))
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

            if (nKeys > 0)
            {
                if (keysArrCapacity < nKeys)
                {
                    if (!TryRecreateKEYS(nKeys))
                    {
                        return LuaWrappedError(0, constStrs.InsufficientLuaStackSpace);
                    }
                }

                // One for KEYS, one for the key being written;
                const int NeededStackSpace = 2;

                Debug.Assert(state.TryEnsureMinimumStackCapacity(NeededStackSpace), "LUA_MINSTACK should ensure this always succeeds");

                // Get KEYS on the stack
                _ = state.RawGetInteger(LuaType.Table, (int)LuaRegistry.Index, sandboxEnvRegistryIndex);
                state.PushConstantString(constStrs.KEYS);
                _ = state.RawGet(LuaType.Table, 1);
                state.Remove(1);

                for (var i = 0; i < nKeys; i++)
                {
                    ref var key = ref parseState.GetArgSliceByRef(offset);

                    if (txnMode)
                    {
                        txnKeyEntries.AddKey(key, LockType.Exclusive);
                    }

                    // Equivalent to KEYS[i+1] = key
                    if (!state.TryPushBuffer(key.ReadOnlySpan))
                    {
                        return LuaWrappedError(0, constStrs.OutOfMemory);
                    }

                    state.RawSetInteger(keysArrCapacity, 1, i + 1);

                    offset++;
                }

                // Remove KEYS from the stack
                state.Pop(1);

                preambleKeyAndArgvCount -= nKeys;
            }

            if (preambleKeyAndArgvCount > 0)
            {
                if (argvArrCapacity < preambleKeyAndArgvCount)
                {
                    if (!TryRecreateARGV(preambleKeyAndArgvCount))
                    {
                        return LuaWrappedError(0, constStrs.OutOfMemory);
                    }
                }

                // One for ARGV, one for the arg being written
                const int NeededStackSpace = 2;

                Debug.Assert(state.TryEnsureMinimumStackCapacity(NeededStackSpace), "LUA_MINSTACK should ensure this always succeeds");

                // Get ARGV on the stack
                _ = state.RawGetInteger(LuaType.Table, (int)LuaRegistry.Index, sandboxEnvRegistryIndex);
                state.PushConstantString(constStrs.ARGV);
                _ = state.RawGet(LuaType.Table, 1);
                state.Remove(1);

                for (var i = 0; i < preambleKeyAndArgvCount; i++)
                {
                    ref var argv = ref parseState.GetArgSliceByRef(offset);

                    // Equivalent to ARGV[i+1] = argv
                    if (!state.TryPushBuffer(argv.ReadOnlySpan))
                    {
                        return LuaWrappedError(0, constStrs.OutOfMemory);
                    }

                    state.RawSetInteger(argvArrCapacity, 1, i + 1);

                    offset++;
                }

                // Remove ARGV from the stack
                state.Pop(1);
            }

            return 0;
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

            if (state.StackTop > 0)
            {
                state.Pop(1);
            }

            Debug.Assert(state.TryEnsureMinimumStackCapacity(NeededStackSpace), "LUA_MIN_STACK is large enough this should never fail");

            state.PushNil();
            return 1;
        }

        /// <summary>
        /// Entry point for redis.call method from a Lua script (transactional mode)
        /// </summary>
        public int GarnetCallWithTransaction(nint luaStatePtr)
        => ProcessCommandFromScripting(luaStatePtr, ref respServerSession.transactionalGarnetApi);

        /// <summary>
        /// Entry point for redis.call method from a Lua script (non-transactional mode)
        /// </summary>
        public int GarnetCall(nint luaStatePtr)
        => ProcessCommandFromScripting(luaStatePtr, ref respServerSession.basicGarnetApi);

        /// <summary>
        /// Registers a debug hook which forces a timeout.
        /// </summary>
        internal void RequestTimeout(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);
            _ = state.RawGetInteger(LuaType.Function, (int)LuaRegistry.Index, requestTimeoutRegsitryIndex);
            var res = state.PCall(0, 0);
            if (res != LuaStatus.OK)
            {
                // We can't do anything else here, raising an error some other will will crash the process
                logger?.LogCritical("Request timeout failed with {res}", res);
            }
        }

        /// <summary>
        /// Entry point for garnet_unpack_trampoline from a Lua script.
        /// 
        /// This is an odd function, but is used to handle variable returns
        /// after error checking has occurred.
        /// </summary>
        internal int UnpackTrampoline(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            Debug.Assert(state.StackTop == 2, "Expected exactly 2 arguments");
            Debug.Assert(state.Type(1) == LuaType.Table, "Expected table as first argument");
            Debug.Assert(state.Type(2) == LuaType.Number, "Expected count as second argument");

            // As an internal call, we know these are fine
            var count = (int)state.CheckNumber(2);
            state.Pop(1);

            // We're going to return count items, and need 1 slot for the error
            if (!state.TryEnsureMinimumStackCapacity(count + 1))
            {
                return LuaWrappedError(0, constStrs.InsufficientLuaStackSpace);
            }

            // Error slot, which is empty after the stack check
            state.PushNil();

            for (var ix = 1; ix <= count; ix++)
            {
                // + 2 to skip err and count
                _ = state.RawGetInteger(null, 1, ix + 2);
            }

            return count + 1;
        }

        /// <summary>
        /// Entry point for redis.sha1hex method from a Lua script.
        /// </summary>
        internal int SHA1Hex(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var argCount = state.StackTop;
            if (argCount != 1)
            {
                return LuaWrappedError(1, constStrs.ErrWrongNumberOfArgs);
            }

            ReadOnlySpan<byte> bytes;
            var argType = state.Type(1);
            if (argType == LuaType.String)
            {
                state.KnownStringToBuffer(1, out bytes);
            }
            else if (argType == LuaType.Number)
            {
                if (!state.TryNumberToString(1, out bytes))
                {
                    return LuaWrappedError(1, constStrs.OutOfMemory);
                }
            }
            else
            {
                bytes = [];
            }

            Span<byte> hashBytes = stackalloc byte[SessionScriptCache.SHA1Len / 2];
            Span<byte> hexRes = stackalloc byte[SessionScriptCache.SHA1Len];

            SessionScriptCache.GetScriptDigest(bytes, hashBytes, hexRes);

            if (!state.TryPushBuffer(hexRes))
            {
                return LuaWrappedError(1, constStrs.OutOfMemory);
            }

            return 1;
        }

        /// <summary>
        /// Entry point for redis.log(...) from a Lua script.
        /// </summary>
        internal int Log(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var argCount = state.StackTop;
            if (argCount < 2)
            {
                return LuaWrappedError(0, constStrs.ErrRedisLogRequired);
            }

            if (state.Type(1) != LuaType.Number)
            {
                return LuaWrappedError(0, constStrs.ErrFirstArgMustBeNumber);
            }

            var rawLevel = state.CheckNumber(1);
            if (rawLevel is not (0 or 1 or 2 or 3))
            {
                return LuaWrappedError(0, constStrs.ErrInvalidDebugLevel);
            }

            if (logMode == LuaLoggingMode.Disable)
            {
                return LuaWrappedError(0, constStrs.ErrLoggingDisabled);
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
                var type = state.Type(2);
                if (type == LuaType.String)
                {
                    state.KnownStringToBuffer(2, out var buff);
                    logMessage = Encoding.UTF8.GetString(buff);
                }
                else if (type == LuaType.Number)
                {
                    if (!state.TryNumberToString(2, out var buff))
                    {
                        return LuaWrappedError(0, constStrs.OutOfMemory);
                    }
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
                    ReadOnlySpan<byte> buff;
                    var type = state.Type(argIx);

                    if (type == LuaType.String)
                    {
                        state.KnownStringToBuffer(argIx, out buff);
                    }
                    else if (type == LuaType.Number)
                    {
                        if (!state.TryNumberToString(argIx, out buff))
                        {
                            return LuaWrappedError(0, constStrs.OutOfMemory);
                        }
                    }
                    else
                    {
                        continue;
                    }

                    if (sb.Length != 0)
                    {
                        _ = sb.Append(' ');
                    }

                    _ = sb.Append(Encoding.UTF8.GetString(buff));
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
        internal int Atan2(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 2 || state.Type(1) != LuaType.Number || state.Type(2) != LuaType.Number)
            {
                return LuaWrappedError(1, constStrs.BadArgATan2);
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
        internal int Cosh(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1 || state.Type(1) != LuaType.Number)
            {
                return LuaWrappedError(1, constStrs.BadArgCosh);
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
        internal int Frexp(nint luaStatePtr)
        {
            // Based on: https://github.com/MachineCognitis/C.math.NET/ (MIT License)

            const long DBL_EXP_MASK = 0x7FF0000000000000L;
            const int DBL_MANT_BITS = 52;
            const long DBL_SGN_MASK = -1 - 0x7FFFFFFFFFFFFFFFL;
            const long DBL_MANT_MASK = 0x000FFFFFFFFFFFFFL;
            const long DBL_EXP_CLR_MASK = DBL_SGN_MASK | DBL_MANT_MASK;

            // 1 for number [-0.5, 0.5] and 1 for exponent >= 0
            const int NeededStackSpace = 2;

            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1 || state.Type(1) != LuaType.Number)
            {
                return LuaWrappedError(2, constStrs.BadArgFrexp);
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

            state.Pop(1);

            if (!state.TryEnsureMinimumStackCapacity(NeededStackSpace))
            {
                return LuaWrappedError(2, constStrs.InsufficientLuaStackSpace);
            }

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
        internal int Ldexp(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 2 || state.Type(1) != LuaType.Number || state.Type(2) != LuaType.Number)
            {
                return LuaWrappedError(1, constStrs.BadArgLdexp);
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
        internal int Log10(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1 || state.Type(1) != LuaType.Number)
            {
                return LuaWrappedError(1, constStrs.BadArgLog10);
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
        internal int Pow(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 2 || state.Type(1) != LuaType.Number || state.Type(2) != LuaType.Number)
            {
                return LuaWrappedError(1, constStrs.BadArgPow);
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
        internal int Sinh(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1 || state.Type(1) != LuaType.Number)
            {
                return LuaWrappedError(1, constStrs.BadArgSinh);
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
        internal int Tanh(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1 || state.Type(1) != LuaType.Number)
            {
                return LuaWrappedError(1, constStrs.BadArgTanh);
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
        internal int Maxn(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            // 1 for key, 1 for value
            const int NeededStackSpace = 2;

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1 || state.Type(1) != LuaType.Table)
            {
                return LuaWrappedError(1, constStrs.BadArgMaxn);
            }

            if (!state.TryEnsureMinimumStackCapacity(NeededStackSpace))
            {
                return LuaWrappedError(1, constStrs.InsufficientLuaStackSpace);
            }

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
        internal int LoadString(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            // 1 for either the compiled chunk, or the error message
            const int NeededStackSpace = 1;

            var luaArgCount = state.StackTop;
            if (
                (luaArgCount == 1 && state.Type(1) != LuaType.String) ||
                (luaArgCount == 2 && (state.Type(1) != LuaType.String || state.Type(2) != LuaType.String)) ||
                (luaArgCount > 2)
              )
            {
                return LuaWrappedError(2, constStrs.BadArgLoadString);
            }

            // Ignore chunk name
            if (luaArgCount == 2)
            {
                state.Pop(1);
            }

            state.KnownStringToBuffer(1, out var buff);
            if (buff.Contains((byte)0))
            {
                return LuaWrappedError(2, constStrs.BadArgLoadStringNullByte);
            }

            if (!state.TryEnsureMinimumStackCapacity(NeededStackSpace))
            {
                return LuaWrappedError(1, constStrs.InsufficientLuaStackSpace);
            }

            var res = state.LoadString(buff);
            if (res != LuaStatus.OK)
            {
                state.ClearStack();
                state.PushNil();
                state.PushConstantString(constStrs.LoadStringError);
                return 2;
            }

            return 1;
        }


        /// <summary>
        /// Entry point for bit.tobit from a Lua script.
        /// </summary>
        internal int BitToBit(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount < 1 || state.Type(1) != LuaType.Number)
            {
                return LuaWrappedError(1, constStrs.BadArgToBit);
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
        internal int BitToHex(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount == 0 || state.Type(1) != LuaType.Number)
            {
                return LuaWrappedError(1, constStrs.BadArgToHex);
            }

            var numDigits = 8;

            if (luaArgCount == 2)
            {
                if (state.Type(2) != LuaType.Number)
                {
                    return LuaWrappedError(1, constStrs.BadArgToHex);
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

            if (!state.TryPushBuffer(buff))
            {
                return LuaWrappedError(1, constStrs.OutOfMemory);
            }

            return 1;
        }

        /// <summary>
        /// Entry point for bit.bswap from a Lua script.
        /// </summary>
        internal int BitBswap(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1 || state.Type(1) != LuaType.Number)
            {
                return LuaWrappedError(1, constStrs.BadArgBSwap);
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
        internal int Bitop(nint luaStatePtr)
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
                logger?.LogError("bitop was not indicated, should never happen");
                return LuaWrappedError(1, constStrs.UnexpectedError);
            }

            var bitop = (int)state.CheckNumber(1);
            if (bitop is < BNot or > Ror)
            {
                logger?.LogError("invalid bitop was passed, should never happen");
                return LuaWrappedError(1, constStrs.UnexpectedError);
            }

            // Handle bnot specially
            if (bitop == BNot)
            {
                if (luaArgCount < 2 || state.Type(2) != LuaType.Number)
                {
                    return LuaWrappedError(1, constStrs.BadArgBNot);
                }

                var val = LuaNumberToBitValue(state.CheckNumber(2));
                var res = ~val;
                state.Pop(2);

                state.PushNumber(res);
                return 1;
            }

            var binOpErr =
                bitop switch
                {
                    BOr => constStrs.BadArgBOr,
                    BAnd => constStrs.BadArgBAnd,
                    BXor => constStrs.BadArgBXor,
                    LShift => constStrs.BadArgLShift,
                    RShift => constStrs.BadArgRShift,
                    ARShift => constStrs.BadArgARShift,
                    Rol => constStrs.BadArgRol,
                    _ => constStrs.BadArgRor,
                };

            if (luaArgCount < 2)
            {
                return LuaWrappedError(1, binOpErr);
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
                        return LuaWrappedError(1, binOpErr);
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
                return LuaWrappedError(1, binOpErr);
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
        internal int CJsonEncode(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1)
            {
                return LuaWrappedError(1, constStrs.BadArgEncode);
            }

            scratchBufferBuilder.Reset();
            var ret = Encode(this, 0);

            if (ret == 1)
            {
                // Encoding should leave nothing on the stack
                state.ExpectLuaStackEmpty();

                // Push the encoded string
                var result = scratchBufferBuilder.ViewFullArgSlice().ReadOnlySpan;
                if (!state.TryPushBuffer(result))
                {
                    return LuaWrappedError(1, constStrs.OutOfMemory);
                }
            }

            return ret;

            // Encode the unknown type on the top of the stack
            static int Encode(LuaRunner self, int depth)
            {
                if (depth > 1000)
                {
                    // Match Redis max decoding depth
                    return self.LuaWrappedError(1, self.constStrs.CannotSerializeNesting);
                }

                var argType = self.state.Type(self.state.StackTop);

                switch (argType)
                {
                    case LuaType.Boolean:
                        return EncodeBool(self);
                    case LuaType.Nil:
                        return EncodeNull(self);
                    case LuaType.Number:
                        return EncodeNumber(self);
                    case LuaType.String:
                        return EncodeString(self);
                    case LuaType.Table:
                        return EncodeTable(self, depth);
                    case LuaType.Function:
                    case LuaType.LightUserData:
                    case LuaType.None:
                    case LuaType.Thread:
                    case LuaType.UserData:
                    default:
                        self.logger?.LogError("Cannot serialize {luaType} to JSON", argType);

                        return self.LuaWrappedError(1, self.constStrs.CannotSerialiseToJson);
                }
            }

            // Encode the boolean on the top of the stack and remove it
            static int EncodeBool(LuaRunner self)
            {
                Debug.Assert(self.state.Type(self.state.StackTop) == LuaType.Boolean, "Expected boolean on top of stack");

                var data = self.state.ToBoolean(self.state.StackTop) ? "true"u8 : "false"u8;

                var into = self.scratchBufferBuilder.ViewRemainingArgSlice(data.Length).Span;
                data.CopyTo(into);
                self.scratchBufferBuilder.MoveOffset(data.Length);

                self.state.Pop(1);

                return 1;
            }

            // Encode the nil on the top of the stack and remove it
            static int EncodeNull(LuaRunner self)
            {
                Debug.Assert(self.state.Type(self.state.StackTop) == LuaType.Nil, "Expected nil on top of stack");

                var into = self.scratchBufferBuilder.ViewRemainingArgSlice(4).Span;
                "null"u8.CopyTo(into);
                self.scratchBufferBuilder.MoveOffset(4);

                self.state.Pop(1);

                return 1;
            }

            // Encode the number on the top of the stack and remove it
            static int EncodeNumber(LuaRunner self)
            {
                Debug.Assert(self.state.Type(self.state.StackTop) == LuaType.Number, "Expected number on top of stack");

                var number = self.state.CheckNumber(self.state.StackTop);

                Span<byte> space = stackalloc byte[64];

                if (!number.TryFormat(space, out var written, "G", CultureInfo.InvariantCulture))
                {
                    return self.LuaWrappedError(1, self.constStrs.UnableToFormatNumber);
                }

                var into = self.scratchBufferBuilder.ViewRemainingArgSlice(written).Span;
                space[..written].CopyTo(into);
                self.scratchBufferBuilder.MoveOffset(written);

                self.state.Pop(1);

                return 1;
            }

            // Encode the string on the top of the stack and remove it
            static int EncodeString(LuaRunner self)
            {
                Debug.Assert(self.state.Type(self.state.StackTop) == LuaType.String, "Expected string on top of stack");

                self.state.KnownStringToBuffer(self.state.StackTop, out var buff);

                self.scratchBufferBuilder.ViewRemainingArgSlice(1).Span[0] = (byte)'"';
                self.scratchBufferBuilder.MoveOffset(1);

                var escapeIx = buff.IndexOfAny((byte)'"', (byte)'\\');
                while (escapeIx != -1)
                {
                    var into = self.scratchBufferBuilder.ViewRemainingArgSlice(escapeIx + 2).Span;
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

                    self.scratchBufferBuilder.MoveOffset(escapeIx + 2);

                    buff = buff[(escapeIx + 1)..];
                    escapeIx = buff.IndexOfAny((byte)'"', (byte)'\\');
                }

                var tailInto = self.scratchBufferBuilder.ViewRemainingArgSlice(buff.Length + 1).Span;
                buff.CopyTo(tailInto);
                tailInto[buff.Length] = (byte)'"';
                self.scratchBufferBuilder.MoveOffset(buff.Length + 1);

                self.state.Pop(1);

                return 1;
            }

            // Encode the table on the top of the stack and remove it
            static int EncodeTable(LuaRunner self, int depth)
            {
                Debug.Assert(self.state.Type(self.state.StackTop) == LuaType.Table, "Expected table on top of stack");

                // 1 for key, 1 for value
                const int NeededStackSpace = 2;

                // Space for key & value
                if (!self.state.TryEnsureMinimumStackCapacity(NeededStackSpace))
                {
                    return self.LuaWrappedError(1, self.constStrs.InsufficientLuaStackSpace);
                }

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
                    return EncodeArray(self, arrayLength, depth);
                }
                else
                {
                    return EncodeObject(self, depth);
                }
            }

            // Encode the table on the top of the stack as an array and remove it
            static int EncodeArray(LuaRunner self, int length, int depth)
            {
                Debug.Assert(self.state.Type(self.state.StackTop) == LuaType.Table, "Expected table on top of stack");

                // 1 for value
                const int NeededStackSpace = 1;

                // Space for value
                if (!self.state.TryEnsureMinimumStackCapacity(NeededStackSpace))
                {
                    return self.LuaWrappedError(1, self.constStrs.InsufficientLuaStackSpace);
                }

                var tableIndex = self.state.StackTop;

                self.scratchBufferBuilder.ViewRemainingArgSlice(1).Span[0] = (byte)'[';
                self.scratchBufferBuilder.MoveOffset(1);

                for (var ix = 1; ix <= length; ix++)
                {
                    if (ix != 1)
                    {
                        self.scratchBufferBuilder.ViewRemainingArgSlice(1).Span[0] = (byte)',';
                        self.scratchBufferBuilder.MoveOffset(1);
                    }

                    _ = self.state.RawGetInteger(null, tableIndex, ix);
                    var r = Encode(self, depth + 1);
                    if (r != 1)
                    {
                        return r;
                    }
                }

                self.scratchBufferBuilder.ViewRemainingArgSlice(1).Span[0] = (byte)']';
                self.scratchBufferBuilder.MoveOffset(1);

                // Remove table
                self.state.Pop(1);

                return 1;
            }

            // Encode the table on the top of the stack as an object and remove it
            static int EncodeObject(LuaRunner self, int depth)
            {
                Debug.Assert(self.state.Type(self.state.StackTop) == LuaType.Table, "Expected table on top of stack");

                // Space for key and value and a copy of key
                const int NeededStackSpace = 3;

                if (!self.state.TryEnsureMinimumStackCapacity(NeededStackSpace))
                {
                    return self.LuaWrappedError(1, self.constStrs.InsufficientLuaStackSpace);
                }

                var tableIndex = self.state.StackTop;

                self.scratchBufferBuilder.ViewRemainingArgSlice(1).Span[0] = (byte)'{';
                self.scratchBufferBuilder.MoveOffset(1);

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
                        self.scratchBufferBuilder.ViewRemainingArgSlice(1).Span[0] = (byte)',';
                        self.scratchBufferBuilder.MoveOffset(1);
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
                        if (!self.state.TryNumberToString(tableIndex + 3, out _))
                        {
                            return self.LuaWrappedError(1, self.constStrs.OutOfMemory);
                        }
                    }

                    // Encode key
                    var r1 = Encode(self, depth + 1);
                    if (r1 != 1)
                    {
                        return r1;
                    }

                    self.scratchBufferBuilder.ViewRemainingArgSlice(1).Span[0] = (byte)':';
                    self.scratchBufferBuilder.MoveOffset(1);

                    // Encode value
                    var r2 = Encode(self, depth + 1);
                    if (r2 != 1)
                    {
                        return r2;
                    }

                    firstValue = false;
                }

                self.scratchBufferBuilder.ViewRemainingArgSlice(1).Span[0] = (byte)'}';
                self.scratchBufferBuilder.MoveOffset(1);

                // Remove table
                self.state.Pop(1);

                return 1;
            }
        }

        /// <summary>
        /// Entry point for cjson.decode from a Lua script.
        /// </summary>
        internal int CJsonDecode(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1)
            {
                return LuaWrappedError(1, constStrs.BadArgDecode);
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
                return LuaWrappedError(1, constStrs.BadArgDecode);
            }

            state.KnownStringToBuffer(1, out var buff);

            try
            {
                var parsed = JsonNode.Parse(buff, documentOptions: new JsonDocumentOptions { MaxDepth = 1000 });
                return Decode(this, parsed);
            }
            catch (Exception e)
            {
                if (e.Message.Contains("maximum configured depth of 1000"))
                {
                    // Maximum depth exceeded, munge to a compatible Redis error
                    return LuaWrappedError(1, constStrs.FoundTooManyNested);
                }

                // Invalid token is implied (and matches Redis error replies)
                return LuaWrappedError(1, constStrs.ExpectedValueButFound);
            }

            // Convert the JsonNode into a Lua value on the stack
            static int Decode(LuaRunner self, JsonNode node)
            {
                if (node is JsonValue v)
                {
                    return DecodeValue(self, v);
                }
                else if (node is JsonArray a)
                {
                    return DecodeArray(self, a);
                }
                else if (node is JsonObject o)
                {
                    return DecodeObject(self, o);
                }
                else if (node == null)
                {
                    self.state.PushNil();
                    return 1;
                }
                else
                {
                    self.logger?.LogError("Unexpected json node type: {typeName}", node.GetType().Name);
                    return self.LuaWrappedError(1, self.constStrs.UnexpectedJsonValueKind);
                }
            }

            // Convert the JsonValue int to a Lua string, nil, or number on the stack
            static int DecodeValue(LuaRunner self, JsonValue value)
            {
                // Space for the value
                const int NeededStackSpace = 1;

                if (!self.state.TryEnsureMinimumStackCapacity(NeededStackSpace))
                {
                    return self.LuaWrappedError(1, self.constStrs.InsufficientLuaStackSpace);
                }

                switch (value.GetValueKind())
                {
                    case JsonValueKind.Null: self.state.PushNil(); break;
                    case JsonValueKind.True: self.state.PushBoolean(true); break;
                    case JsonValueKind.False: self.state.PushBoolean(false); break;
                    case JsonValueKind.Number: self.state.PushNumber(value.GetValue<double>()); break;
                    case JsonValueKind.String:
                        var str = value.GetValue<string>();

                        self.scratchBufferBuilder.Reset();
                        var buf = self.scratchBufferBuilder.UTF8EncodeString(str);

                        if (!self.state.TryPushBuffer(buf))
                        {
                            return self.LuaWrappedError(1, self.constStrs.OutOfMemory);
                        }
                        break;
                    case JsonValueKind.Undefined:
                    case JsonValueKind.Object:
                    case JsonValueKind.Array:
                    default:
                        self.logger?.LogError("Unexpected json value kind: {kind}", value.GetValueKind());
                        return self.LuaWrappedError(1, self.constStrs.UnexpectedJsonValueKind);
                }

                return 1;
            }

            // Convert the JsonArray into a Lua table on the stack
            static int DecodeArray(LuaRunner self, JsonArray arr)
            {
                // Space for the table
                const int NeededStackSpace = 1;

                if (!self.state.TryEnsureMinimumStackCapacity(NeededStackSpace))
                {
                    return self.LuaWrappedError(1, self.constStrs.InsufficientLuaStackSpace);
                }

                if (!self.state.TryCreateTable(arr.Count, 0))
                {
                    return self.LuaWrappedError(1, self.constStrs.OutOfMemory);
                }

                var tableIndex = self.state.StackTop;

                var storeAtIx = 1;
                foreach (var item in arr)
                {
                    // Places item on the stack
                    var r = Decode(self, item);
                    if (r != 1)
                    {
                        // Propogate error return
                        return r;
                    }

                    // Save into the table
                    self.state.RawSetInteger(arr.Count, tableIndex, storeAtIx);
                    storeAtIx++;
                }

                return 1;
            }

            // Convert the JsonObject into a Lua table on the stack
            static int DecodeObject(LuaRunner self, JsonObject obj)
            {
                // Space for table and key
                const int NeededStackSpace = 2;

                if (!self.state.TryEnsureMinimumStackCapacity(NeededStackSpace))
                {
                    return self.LuaWrappedError(1, self.constStrs.InsufficientLuaStackSpace);
                }

                if (!self.state.TryCreateTable(0, obj.Count))
                {
                    return self.LuaWrappedError(1, self.constStrs.OutOfMemory);
                }
                var setInTable = 0;

                var tableIndex = self.state.StackTop;

                foreach (var (key, value) in obj)
                {
                    // Decode key to string
                    self.scratchBufferBuilder.Reset();
                    var buf = self.scratchBufferBuilder.UTF8EncodeString(key);
                    if (!self.state.TryPushBuffer(buf))
                    {
                        return self.LuaWrappedError(1, self.constStrs.OutOfMemory);
                    }

                    // Decode value
                    var r = Decode(self, value);
                    if (r != 1)
                    {
                        return r;
                    }

                    self.state.RawSet(obj.Count, tableIndex, ref setInTable);
                }

                Debug.Assert(setInTable == obj.Count, "Didn't fill table with records");

                return 1;
            }
        }

        /// <summary>
        /// Entry point for cmsgpack.pack from a Lua script.
        /// </summary>
        internal int CMsgPackPack(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var numLuaArgs = state.StackTop;

            if (numLuaArgs == 0)
            {
                return LuaWrappedError(1, constStrs.BadArgPack);
            }

            // Redis concatenates all the message packs together if there are multiple
            //
            // Somewhat odd, but we match that behavior

            scratchBufferBuilder.Reset();

            for (var argIx = 1; argIx <= numLuaArgs; argIx++)
            {
                // Because each encode removes the encoded value
                // we always encode the argument at position 1
                if (!TryEncode(this, 1, 0, out var errIndex))
                {
                    return LuaWrappedError(1, errIndex);
                }
            }

            // After all encoding, stack should be empty
            state.ExpectLuaStackEmpty();

            var ret = scratchBufferBuilder.ViewFullArgSlice().ReadOnlySpan;
            if (!state.TryPushBuffer(ret))
            {
                return LuaWrappedError(1, constStrs.OutOfMemory);
            }

            return 1;

            // Encode a single item at the top of the stack, and remove it
            static bool TryEncode(LuaRunner self, int stackIndex, int depth, out int constStrRegisteryIndex)
            {
                var type = self.state.Type(stackIndex);
                switch (type)
                {
                    case LuaType.Boolean: return TryEncodeBool(self, stackIndex, out constStrRegisteryIndex);
                    case LuaType.Number: return TryEncodeNumber(self, stackIndex, out constStrRegisteryIndex);
                    case LuaType.String: return TryEncodeBytes(self, stackIndex, out constStrRegisteryIndex);
                    case LuaType.Table:
                        if (depth == 16)
                        {
                            // Redis treats a too deeply nested table as a null
                            //
                            // This is weird, but we match it
                            self.scratchBufferBuilder.ViewRemainingArgSlice(1).Span[0] = 0xC0;
                            self.scratchBufferBuilder.MoveOffset(1);

                            self.state.Remove(stackIndex);

                            constStrRegisteryIndex = -1;
                            return true;
                        }

                        return TryEncodeTable(self, stackIndex, depth, out constStrRegisteryIndex);

                    // Everything else maps to null, NOT an error
                    case LuaType.Function:
                    case LuaType.LightUserData:
                    case LuaType.Nil:
                    case LuaType.None:
                    case LuaType.Thread:
                    case LuaType.UserData:
                    default: return TryEncodeNull(self, stackIndex, out constStrRegisteryIndex);
                }
            }

            // Encode a null-ish value at stackIndex, and remove it
            static bool TryEncodeNull(LuaRunner self, int stackIndex, out int constStrRegisteryIndex)
            {
                Debug.Assert(self.state.Type(stackIndex) is not (LuaType.Boolean or LuaType.Number or LuaType.String or LuaType.Table), "Expected null-ish type");

                self.scratchBufferBuilder.ViewRemainingArgSlice(1).Span[0] = 0xC0;
                self.scratchBufferBuilder.MoveOffset(1);

                self.state.Remove(stackIndex);

                constStrRegisteryIndex = -1;
                return true;
            }

            // Encode a boolean at stackIndex, and remove it
            static bool TryEncodeBool(LuaRunner self, int stackIndex, out int constStrRegisteryIndex)
            {
                Debug.Assert(self.state.Type(stackIndex) == LuaType.Boolean, "Expected boolean");

                var value = (byte)(self.state.ToBoolean(stackIndex) ? 0xC3 : 0xC2);

                self.scratchBufferBuilder.ViewRemainingArgSlice(1).Span[0] = value;
                self.scratchBufferBuilder.MoveOffset(1);

                self.state.Remove(stackIndex);

                constStrRegisteryIndex = -1;
                return true;
            }

            // Encode a number at stackIndex, and remove it
            static bool TryEncodeNumber(LuaRunner self, int stackIndex, out int constStrRegisteryIndex)
            {
                Debug.Assert(self.state.Type(stackIndex) == LuaType.Number, "Expected number");

                var numRaw = self.state.CheckNumber(stackIndex);
                var isInt = numRaw == (long)numRaw;

                bool ret;
                if (isInt)
                {
                    ret = TryEncodeInteger(self, (long)numRaw, out constStrRegisteryIndex);
                }
                else
                {
                    ret = TryEncodeFloatingPoint(self, numRaw, out constStrRegisteryIndex);
                }

                self.state.Remove(stackIndex);
                return ret;
            }

            // Encode an integer
            static bool TryEncodeInteger(LuaRunner self, long value, out int constStrRegisteryIndex)
            {
                // positive 7-bit fixint
                if ((byte)(value & 0b0111_1111) == value)
                {
                    self.scratchBufferBuilder.ViewRemainingArgSlice(1).Span[0] = (byte)value;
                    self.scratchBufferBuilder.MoveOffset(1);

                    constStrRegisteryIndex = -1;
                    return true;
                }

                // negative 5-bit fixint
                if ((sbyte)(value | 0b1110_0000) == value)
                {
                    self.scratchBufferBuilder.ViewRemainingArgSlice(1).Span[0] = (byte)value;
                    self.scratchBufferBuilder.MoveOffset(1);

                    constStrRegisteryIndex = -1;
                    return true;
                }

                // 8-bit int
                if (value is >= sbyte.MinValue and <= sbyte.MaxValue)
                {
                    var into = self.scratchBufferBuilder.ViewRemainingArgSlice(2).Span;

                    into[0] = 0xD0;
                    into[1] = (byte)value;
                    self.scratchBufferBuilder.MoveOffset(2);

                    constStrRegisteryIndex = -1;
                    return true;
                }

                // 8-bit uint
                if (value is >= byte.MinValue and <= byte.MaxValue)
                {
                    var into = self.scratchBufferBuilder.ViewRemainingArgSlice(2).Span;

                    into[0] = 0xCC;
                    into[1] = (byte)value;
                    self.scratchBufferBuilder.MoveOffset(2);

                    constStrRegisteryIndex = -1;
                    return true;
                }

                // 16-bit int
                if (value is >= short.MinValue and <= short.MaxValue)
                {
                    var into = self.scratchBufferBuilder.ViewRemainingArgSlice(3).Span;

                    into[0] = 0xD1;
                    BinaryPrimitives.WriteInt16BigEndian(into[1..], (short)value);
                    self.scratchBufferBuilder.MoveOffset(3);

                    constStrRegisteryIndex = -1;
                    return true;
                }

                // 16-bit uint
                if (value is >= ushort.MinValue and <= ushort.MaxValue)
                {
                    var into = self.scratchBufferBuilder.ViewRemainingArgSlice(3).Span;

                    into[0] = 0xCD;
                    BinaryPrimitives.WriteUInt16BigEndian(into[1..], (ushort)value);
                    self.scratchBufferBuilder.MoveOffset(3);

                    constStrRegisteryIndex = -1;
                    return true;
                }

                // 32-bit int
                if (value is >= int.MinValue and <= int.MaxValue)
                {
                    var into = self.scratchBufferBuilder.ViewRemainingArgSlice(5).Span;

                    into[0] = 0xD2;
                    BinaryPrimitives.WriteInt32BigEndian(into[1..], (int)value);
                    self.scratchBufferBuilder.MoveOffset(5);

                    constStrRegisteryIndex = -1;
                    return true;
                }

                // 32-bit uint
                if (value is >= uint.MinValue and <= uint.MaxValue)
                {
                    var into = self.scratchBufferBuilder.ViewRemainingArgSlice(5).Span;

                    into[0] = 0xCE;
                    BinaryPrimitives.WriteUInt32BigEndian(into[1..], (uint)value);
                    self.scratchBufferBuilder.MoveOffset(5);

                    constStrRegisteryIndex = -1;
                    return true;
                }

                // 64-bit uint
                if (value > uint.MaxValue)
                {
                    var into = self.scratchBufferBuilder.ViewRemainingArgSlice(9).Span;

                    into[0] = 0xCF;
                    BinaryPrimitives.WriteUInt64BigEndian(into[1..], (ulong)value);
                    self.scratchBufferBuilder.MoveOffset(9);

                    constStrRegisteryIndex = -1;
                    return true;
                }

                // 64-bit int
                {
                    var into = self.scratchBufferBuilder.ViewRemainingArgSlice(9).Span;

                    into[0] = 0xD3;
                    BinaryPrimitives.WriteInt64BigEndian(into[1..], value);
                    self.scratchBufferBuilder.MoveOffset(9);

                    constStrRegisteryIndex = -1;
                    return true;
                }
            }

            // Encode a floating point value
            static bool TryEncodeFloatingPoint(LuaRunner self, double value, out int constStrRegisteryIndex)
            {
                // While Redis has code that attempts to pack doubles into floats
                // it doesn't appear to do anything, so we just always write a double

                var into = self.scratchBufferBuilder.ViewRemainingArgSlice(9).Span;

                into[0] = 0xCB;
                BinaryPrimitives.WriteDoubleBigEndian(into[1..], value);
                self.scratchBufferBuilder.MoveOffset(9);

                constStrRegisteryIndex = -1;
                return true;
            }

            // Encodes a string as at stackIndex, and remove it
            static bool TryEncodeBytes(LuaRunner self, int stackIndex, out int constStrRegisteryIndex)
            {
                Debug.Assert(self.state.Type(stackIndex) == LuaType.String, "Expected string");

                self.state.KnownStringToBuffer(stackIndex, out var data);

                if (data.Length < 32)
                {
                    var into = self.scratchBufferBuilder.ViewRemainingArgSlice(1 + data.Length).Span;

                    into[0] = (byte)(0xA0 | data.Length);
                    data.CopyTo(into[1..]);
                    self.scratchBufferBuilder.MoveOffset(1 + data.Length);
                }
                else if (data.Length <= byte.MaxValue)
                {
                    var into = self.scratchBufferBuilder.ViewRemainingArgSlice(2 + data.Length).Span;

                    into[0] = 0xD9;
                    into[1] = (byte)data.Length;
                    data.CopyTo(into[2..]);
                    self.scratchBufferBuilder.MoveOffset(2 + data.Length);
                }
                else if (data.Length <= ushort.MaxValue)
                {
                    var into = self.scratchBufferBuilder.ViewRemainingArgSlice(3 + data.Length).Span;

                    into[0] = 0xDA;
                    BinaryPrimitives.WriteUInt16BigEndian(into[1..], (ushort)data.Length);
                    data.CopyTo(into[3..]);
                    self.scratchBufferBuilder.MoveOffset(3 + data.Length);
                }
                else
                {
                    var into = self.scratchBufferBuilder.ViewRemainingArgSlice(5 + data.Length).Span;

                    into[0] = 0xDB;
                    BinaryPrimitives.WriteUInt32BigEndian(into[1..], (uint)data.Length);
                    data.CopyTo(into[5..]);
                    self.scratchBufferBuilder.MoveOffset(5 + data.Length);
                }

                self.state.Remove(stackIndex);

                constStrRegisteryIndex = -1;
                return true;
            }

            // Encode a table at stackIndex, and remove it
            static bool TryEncodeTable(LuaRunner self, int stackIndex, int depth, out int constStrRegisteryIndex)
            {
                Debug.Assert(self.state.Type(stackIndex) == LuaType.Table, "Expected table");

                // Space for key and value
                const int NeededStackSpace = 2;

                if (!self.state.TryEnsureMinimumStackCapacity(NeededStackSpace))
                {
                    constStrRegisteryIndex = self.constStrs.InsufficientLuaStackSpace;
                    return false;
                }

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
                    return TryEncodeArray(self, stackIndex, depth, count, out constStrRegisteryIndex);
                }
                else
                {
                    return TryEncodeMap(self, stackIndex, depth, count, out constStrRegisteryIndex);
                }
            }

            // Encode a table at stackIndex into an array, and remove it
            static bool TryEncodeArray(LuaRunner self, int stackIndex, int depth, int count, out int constStrRegisteryIndex)
            {
                Debug.Assert(self.state.Type(stackIndex) == LuaType.Table, "Expected table");
                Debug.Assert(count >= 0, "Array should have positive length");

                // Space for value
                const int NeededStackSpace = 1;

                if (!self.state.TryEnsureMinimumStackCapacity(NeededStackSpace))
                {
                    constStrRegisteryIndex = self.constStrs.InsufficientLuaStackSpace;
                    return false;
                }

                var tableIndex = stackIndex;
                var valueIndex = tableIndex + 1;

                // Encode length
                if (count <= 15)
                {
                    var into = self.scratchBufferBuilder.ViewRemainingArgSlice(1).Span;
                    into[0] = (byte)(0b1001_0000 | count);
                    self.scratchBufferBuilder.MoveOffset(1);
                }
                else if (count <= ushort.MaxValue)
                {
                    var into = self.scratchBufferBuilder.ViewRemainingArgSlice(3).Span;

                    into[0] = 0xDC;
                    BinaryPrimitives.WriteUInt16BigEndian(into[1..], (ushort)count);
                    self.scratchBufferBuilder.MoveOffset(3);
                }
                else
                {
                    var into = self.scratchBufferBuilder.ViewRemainingArgSlice(5).Span;

                    into[0] = 0xDD;
                    BinaryPrimitives.WriteUInt32BigEndian(into[1..], (uint)count);
                    self.scratchBufferBuilder.MoveOffset(5);
                }

                // Write each element out
                for (var ix = 1; ix <= count; ix++)
                {
                    _ = self.state.RawGetInteger(null, tableIndex, ix);
                    if (!TryEncode(self, valueIndex, depth + 1, out constStrRegisteryIndex))
                    {
                        return false;
                    }
                }

                self.state.Remove(tableIndex);

                constStrRegisteryIndex = -1;
                return true;
            }

            // Encode a table at stackIndex into a map, and remove it
            static bool TryEncodeMap(LuaRunner self, int stackIndex, int depth, int count, out int constStrRegisteryIndex)
            {
                Debug.Assert(self.state.Type(stackIndex) == LuaType.Table, "Expected table");
                Debug.Assert(count >= 0, "Map should have positive length");

                // Space for key, value, and copy of key
                const int NeededStackSpace = 3;

                if (!self.state.TryEnsureMinimumStackCapacity(NeededStackSpace))
                {
                    constStrRegisteryIndex = self.constStrs.InsufficientLuaStackSpace;
                    return false;
                }

                var tableIndex = stackIndex;
                var keyIndex = self.state.StackTop + 1;
                var valueIndex = keyIndex + 1;
                var keyCopyIndex = valueIndex + 1;

                // Encode length
                if (count <= 15)
                {
                    var into = self.scratchBufferBuilder.ViewRemainingArgSlice(1).Span;

                    into[0] = (byte)(0b1000_0000 | count);
                    self.scratchBufferBuilder.MoveOffset(1);
                }
                else if (count <= ushort.MaxValue)
                {
                    var into = self.scratchBufferBuilder.ViewRemainingArgSlice(3).Span;

                    into[0] = 0xDE;
                    BinaryPrimitives.WriteUInt16BigEndian(into[1..], (ushort)count);
                    self.scratchBufferBuilder.MoveOffset(3);
                }
                else
                {
                    var into = self.scratchBufferBuilder.ViewRemainingArgSlice(5).Span;

                    into[0] = 0xDF;
                    BinaryPrimitives.WriteUInt32BigEndian(into[1..], (uint)count);
                    self.scratchBufferBuilder.MoveOffset(5);
                }

                self.state.PushNil();
                while (self.state.Next(tableIndex) != 0)
                {
                    // Make a copy of the key
                    self.state.PushValue(keyIndex);

                    // Write the key
                    if (!TryEncode(self, keyCopyIndex, depth + 1, out constStrRegisteryIndex))
                    {
                        return false;
                    }

                    // Write the value
                    if (!TryEncode(self, valueIndex, depth + 1, out constStrRegisteryIndex))
                    {
                        return false;
                    }
                }

                self.state.Remove(tableIndex);

                constStrRegisteryIndex = -1;
                return true;
            }
        }

        /// <summary>
        /// Entry point for cmsgpack.unpack from a Lua script.
        /// </summary>
        internal int CMsgPackUnpack(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var numLuaArgs = state.StackTop;

            if (numLuaArgs == 0 || state.Type(1) != LuaType.String)
            {
                // This method returns variable numbers of arguments, so the error goes in the first slot
                return LuaWrappedError(0, constStrs.BadArgUnpack);
            }

            // 1 for error slot and 1 for count
            const int NeededStackSpace = 2;

            // 1 for each unpacked result
            const int AdditionalStackSpace = 1;

            if (!state.TryEnsureMinimumStackCapacity(NeededStackSpace))
            {
                return LuaWrappedError(0, constStrs.InsufficientLuaStackSpace);
            }

            state.KnownStringToBuffer(1, out var data);

            var decodedCount = 0;
            while (!data.IsEmpty)
            {
                // Reserve space for the result
                if (!state.TryEnsureMinimumStackCapacity(AdditionalStackSpace))
                {
                    return LuaWrappedError(0, constStrs.InsufficientLuaStackSpace);
                }

                try
                {
                    if (!TryDecode(this, ref data, out var constStrErrId))
                    {
                        return LuaWrappedError(0, constStrErrId);
                    }
                    decodedCount++;
                }
                catch (Exception e)
                {
                    logger?.LogError(e, "During cmsgpack.unpack");

                    // Best effort at matching Redis behavior
                    return LuaWrappedError(0, constStrs.MissingBytesInInput);
                }
            }

            // Error and count for error_wrapper_rvar
            state.PushNil();
            state.PushInteger(decodedCount);
            state.Rotate(2, 2);

            // +2 for the (nil) error slot and the count
            return decodedCount + 2;

            // Decode a msg pack
            static bool TryDecode(LuaRunner self, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                var sigil = data[0];
                data = data[1..];

                switch (sigil)
                {
                    case 0xC0: return TryDecodeNull(self, ref data, out constStrErrId);
                    case 0xC2: return TryDecodeBoolean(self, false, ref data, out constStrErrId);
                    case 0xC3: return TryDecodeBoolean(self, true, ref data, out constStrErrId);
                    // 7-bit positive integers handled below
                    // 5-bit negative integers handled below
                    case 0xCC: return TryDecodeUInt8(self, ref data, out constStrErrId);
                    case 0xCD: return TryDecodeUInt16(self, ref data, out constStrErrId);
                    case 0xCE: return TryDecodeUInt32(self, ref data, out constStrErrId);
                    case 0xCF: return TryDecodeUInt64(self, ref data, out constStrErrId);
                    case 0xD0: return TryDecodeInt8(self, ref data, out constStrErrId);
                    case 0xD1: return TryDecodeInt16(self, ref data, out constStrErrId);
                    case 0xD2: return TryDecodeInt32(self, ref data, out constStrErrId);
                    case 0xD3: return TryDecodeInt64(self, ref data, out constStrErrId);
                    case 0xCA: return TryDecodeSingle(self, ref data, out constStrErrId);
                    case 0xCB: return TryDecodeDouble(self, ref data, out constStrErrId);
                    // <= 31 byte strings handled below
                    case 0xD9: return TryDecodeSmallString(self, ref data, out constStrErrId);
                    case 0xDA: return TryDecodeMidString(self, ref data, out constStrErrId);
                    case 0xDB: return TryDecodeLargeString(self, ref data, out constStrErrId);
                    // We treat bins as strings
                    case 0xC4: goto case 0xD9;
                    case 0xC5: goto case 0xDA;
                    case 0xC6: goto case 0xDB;
                    // <= 15 element arrays are handled below
                    case 0xDC: return TryDecodeMidArray(self, ref data, out constStrErrId);
                    case 0xDD: return TryDecodeLargeArray(self, ref data, out constStrErrId);
                    // <= 15 pair maps are handled below
                    case 0xDE: return TryDecodeMidMap(self, ref data, out constStrErrId);
                    case 0xDF: return TryDecodeLargeMap(self, ref data, out constStrErrId);

                    default:
                        if ((sigil & 0b1000_0000) == 0)
                        {
                            return TryDecodeTinyUInt(self, sigil, out constStrErrId);
                        }
                        else if ((sigil & 0b1110_0000) == 0b1110_0000)
                        {
                            return TryDecodeTinyInt(self, sigil, out constStrErrId);
                        }
                        else if ((sigil & 0b1110_0000) == 0b1010_0000)
                        {
                            return TryDecodeTinyString(self, sigil, ref data, out constStrErrId);
                        }
                        else if ((sigil & 0b1111_0000) == 0b1001_0000)
                        {
                            return TryDecodeSmallArray(self, sigil, ref data, out constStrErrId);
                        }
                        else if ((sigil & 0b1111_0000) == 0b1000_0000)
                        {
                            return TryDecodeSmallMap(self, sigil, ref data, out constStrErrId);
                        }

                        self.logger?.LogError("Unexpected MsgPack sigil {sigil}", sigil);

                        constStrErrId = self.constStrs.UnexpectedMsgPackSigil;
                        return false;
                }
            }

            // Decode a null push it to the stack
            static bool TryDecodeNull(LuaRunner self, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                self.state.PushNil();

                constStrErrId = -1;
                return true;
            }

            // Decode a boolean and push it to the stack
            static bool TryDecodeBoolean(LuaRunner self, bool b, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                self.state.PushBoolean(b);

                constStrErrId = -1;
                return true;
            }

            // Decode a byte, moving past it in data and pushing it to the stack
            static bool TryDecodeUInt8(LuaRunner self, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                self.state.PushNumber(data[0]);
                data = data[1..];

                constStrErrId = -1;
                return true;
            }

            // Decode a positive 7-bit value, pushing it to the stack
            static bool TryDecodeTinyUInt(LuaRunner self, byte sigil, out int constStrErrId)
            {
                self.state.PushNumber(sigil);

                constStrErrId = -1;
                return true;
            }

            // Decode a ushort, moving past it in data and pushing it to the stack
            static bool TryDecodeUInt16(LuaRunner self, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                self.state.PushNumber(BinaryPrimitives.ReadUInt16BigEndian(data));
                data = data[2..];

                constStrErrId = -1;
                return true;
            }

            // Decode a uint, moving past it in data and pushing it to the stack
            static bool TryDecodeUInt32(LuaRunner self, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                self.state.PushNumber(BinaryPrimitives.ReadUInt32BigEndian(data));
                data = data[4..];

                constStrErrId = -1;
                return true;
            }

            // Decode a ulong, moving past it in data and pushing it to the stack
            static bool TryDecodeUInt64(LuaRunner self, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                self.state.PushNumber(BinaryPrimitives.ReadUInt64BigEndian(data));
                data = data[8..];

                constStrErrId = -1;
                return true;
            }

            // Decode a negative 5-bit value, pushing it to the stack
            static bool TryDecodeTinyInt(LuaRunner self, byte sigil, out int constStrErrId)
            {
                var signExtended = (int)(0xFFFF_FF00 | sigil);
                self.state.PushNumber(signExtended);

                constStrErrId = -1;
                return true;
            }

            // Decode a sbyte, moving past it in data and pushing it to the stack
            static bool TryDecodeInt8(LuaRunner self, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                self.state.PushNumber((sbyte)data[0]);
                data = data[1..];

                constStrErrId = -1;
                return true;
            }

            // Decode a short, moving past it in data and pushing it to the stack
            static bool TryDecodeInt16(LuaRunner self, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                self.state.PushNumber(BinaryPrimitives.ReadInt16BigEndian(data));
                data = data[2..];

                constStrErrId = -1;
                return true;
            }

            // Decode a int, moving past it in data and pushing it to the stack
            static bool TryDecodeInt32(LuaRunner self, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                self.state.PushNumber(BinaryPrimitives.ReadInt32BigEndian(data));
                data = data[4..];

                constStrErrId = -1;
                return true;
            }

            // Decode a long, moving past it in data and pushing it to the stack
            static bool TryDecodeInt64(LuaRunner self, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                self.state.PushNumber(BinaryPrimitives.ReadInt64BigEndian(data));
                data = data[8..];

                constStrErrId = -1;
                return true;
            }

            // Decode a float, moving past it in data and pushing it to the stack
            static bool TryDecodeSingle(LuaRunner self, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                self.state.PushNumber(BinaryPrimitives.ReadSingleBigEndian(data));
                data = data[4..];

                constStrErrId = -1;
                return true;
            }

            // Decode a double, moving past it in data and pushing it to the stack
            static bool TryDecodeDouble(LuaRunner self, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                self.state.PushNumber(BinaryPrimitives.ReadDoubleBigEndian(data));
                data = data[8..];

                constStrErrId = -1;
                return true;
            }

            // Decode a string size <= 31, moving past it in data and pushing it to the stack
            static bool TryDecodeTinyString(LuaRunner self, byte sigil, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                var len = sigil & 0b0001_1111;
                var str = data[..len];

                if (!self.state.TryPushBuffer(str))
                {
                    constStrErrId = self.constStrs.OutOfMemory;
                    return false;
                }

                data = data[len..];

                constStrErrId = -1;
                return true;
            }

            // Decode a string size <= 255, moving past it in data and pushing it to the stack
            static bool TryDecodeSmallString(LuaRunner self, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                var len = data[0];
                data = data[1..];

                var str = data[..len];

                if (!self.state.TryPushBuffer(str))
                {
                    constStrErrId = self.constStrs.OutOfMemory;
                    return false;
                }

                data = data[str.Length..];

                constStrErrId = -1;
                return true;
            }

            // Decode a string size <= 65,535, moving past it in data and pushing it to the stack
            static bool TryDecodeMidString(LuaRunner self, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                var len = BinaryPrimitives.ReadUInt16BigEndian(data);
                data = data[2..];

                var str = data[..(int)len];

                if (!self.state.TryPushBuffer(str))
                {
                    constStrErrId = self.constStrs.OutOfMemory;
                    return false;
                }

                data = data[str.Length..];

                constStrErrId = -1;
                return true;
            }

            // Decode a string size <= 4,294,967,295, moving past it in data and pushing it to the stack
            static bool TryDecodeLargeString(LuaRunner self, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                var len = BinaryPrimitives.ReadUInt32BigEndian(data);
                data = data[4..];

                if ((int)len < 0)
                {
                    self.logger?.LogError("String length is too long: {len}", len);

                    constStrErrId = self.constStrs.MsgPackStringTooLong;
                    return false;
                }

                var str = data[..(int)len];

                if (!self.state.TryPushBuffer(str))
                {
                    constStrErrId = self.constStrs.OutOfMemory;
                    return false;
                }

                data = data[str.Length..];

                constStrErrId = -1;
                return true;
            }

            // Decode an array with <= 15 items, moving past it in data and pushing it to the stack
            static bool TryDecodeSmallArray(LuaRunner self, byte sigil, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                // Space for the temporary item
                const int NeededStackSpace = 1;

                if (!self.state.TryEnsureMinimumStackCapacity(NeededStackSpace))
                {
                    constStrErrId = self.constStrs.InsufficientLuaStackSpace;
                    return false;
                }

                var len = sigil & 0b0000_1111;

                if (!self.state.TryCreateTable(len, 0))
                {
                    constStrErrId = self.constStrs.OutOfMemory;
                    return false;
                }
                var arrayIndex = self.state.StackTop;

                for (var i = 1; i <= len; i++)
                {
                    // Push the element onto the stack
                    if (!TryDecode(self, ref data, out constStrErrId))
                    {
                        return false;
                    }

                    self.state.RawSetInteger(len, arrayIndex, i);
                }

                constStrErrId = -1;
                return true;
            }

            // Decode an array with <= 65,535 items, moving past it in data and pushing it to the stack
            static bool TryDecodeMidArray(LuaRunner self, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                // Space for the temporary item
                const int NeededStackSpace = 1;

                if (!self.state.TryEnsureMinimumStackCapacity(NeededStackSpace))
                {
                    constStrErrId = self.constStrs.InsufficientLuaStackSpace;
                    return false;
                }

                var len = BinaryPrimitives.ReadUInt16BigEndian(data);
                data = data[2..];

                if (!self.state.TryCreateTable(len, 0))
                {
                    constStrErrId = self.constStrs.OutOfMemory;
                    return false;
                }
                var arrayIndex = self.state.StackTop;

                for (var i = 1; i <= len; i++)
                {
                    // Push the element onto the stack
                    if (!TryDecode(self, ref data, out constStrErrId))
                    {
                        return false;
                    }

                    self.state.RawSetInteger(len, arrayIndex, i);
                }

                constStrErrId = -1;
                return true;
            }

            // Decode an array with <= 4,294,967,295 items, moving past it in data and pushing it to the stack
            static bool TryDecodeLargeArray(LuaRunner self, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                // Space for the temporary item
                const int NeededStackSpace = 1;

                if (!self.state.TryEnsureMinimumStackCapacity(NeededStackSpace))
                {
                    constStrErrId = self.constStrs.InsufficientLuaStackSpace;
                    return false;
                }

                var len = BinaryPrimitives.ReadUInt32BigEndian(data);
                data = data[4..];

                if ((int)len < 0)
                {
                    self.logger?.LogError("Array length is too long: {len}", len);

                    constStrErrId = self.constStrs.MsgPackArrayTooLong;
                    return false;
                }

                if (!self.state.TryCreateTable((int)len, 0))
                {
                    constStrErrId = self.constStrs.OutOfMemory;
                    return false;
                }
                var arrayIndex = self.state.StackTop;

                for (var i = 1; i <= len; i++)
                {
                    // Push the element onto the stack
                    if (!TryDecode(self, ref data, out constStrErrId))
                    {
                        return false;
                    }

                    self.state.RawSetInteger((int)len, arrayIndex, i);
                }

                constStrErrId = -1;
                return true;
            }

            // Decode an map with <= 15 key-value pairs, moving past it in data and pushing it to the stack
            static bool TryDecodeSmallMap(LuaRunner self, byte sigil, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                // Reserve extra space for the 
                // Space for the temporary key & value
                const int NeededStackSpace = 2;

                if (!self.state.TryEnsureMinimumStackCapacity(NeededStackSpace))
                {
                    constStrErrId = self.constStrs.InsufficientLuaStackSpace;
                    return false;
                }

                var len = sigil & 0b0000_1111;

                if (!self.state.TryCreateTable(0, len))
                {
                    constStrErrId = self.constStrs.OutOfMemory;
                    return false;
                }
                var setInTable = 0;

                var mapIndex = self.state.StackTop;

                for (var i = 1; i <= len; i++)
                {
                    // Push the key onto the stack
                    if (!TryDecode(self, ref data, out constStrErrId))
                    {
                        return false;
                    }

                    // Push the value onto the stack
                    if (!TryDecode(self, ref data, out constStrErrId))
                    {
                        return false;
                    }

                    self.state.RawSet(len, mapIndex, ref setInTable);
                }

                Debug.Assert(setInTable == len, "Didn't fill table with records");

                constStrErrId = -1;
                return true;
            }

            // Decode a map with <= 65,535 key-value pairs, moving past it in data and pushing it to the stack
            static bool TryDecodeMidMap(LuaRunner self, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                // Reserve extra space for the 
                // Space for the temporary key & value
                const int NeededStackSpace = 2;

                if (!self.state.TryEnsureMinimumStackCapacity(NeededStackSpace))
                {
                    constStrErrId = self.constStrs.InsufficientLuaStackSpace;
                    return false;
                }

                var len = BinaryPrimitives.ReadUInt16BigEndian(data);
                data = data[2..];

                if (!self.state.TryCreateTable(0, len))
                {
                    constStrErrId = self.constStrs.OutOfMemory;
                    return false;
                }
                var setInTable = 0;

                var mapIndex = self.state.StackTop;

                for (var i = 1; i <= len; i++)
                {
                    // Push the key onto the stack
                    if (!TryDecode(self, ref data, out constStrErrId))
                    {
                        return false;
                    }

                    // Push the value onto the stack
                    if (!TryDecode(self, ref data, out constStrErrId))
                    {
                        return false;
                    }

                    self.state.RawSet(len, mapIndex, ref setInTable);
                }
                Debug.Assert(setInTable == len, "Didn't fill table with records");

                constStrErrId = -1;
                return true;
            }

            // Decode a map with <= 4,294,967,295 key-value pairs, moving past it in data and pushing it to the stack
            static bool TryDecodeLargeMap(LuaRunner self, ref ReadOnlySpan<byte> data, out int constStrErrId)
            {
                // Reserve extra space for the 
                // Space for the temporary key & value
                const int NeededStackSpace = 2;

                if (!self.state.TryEnsureMinimumStackCapacity(NeededStackSpace))
                {
                    constStrErrId = self.constStrs.InsufficientLuaStackSpace;
                    return false;
                }

                var len = BinaryPrimitives.ReadUInt32BigEndian(data);
                data = data[4..];

                if ((int)len < 0)
                {
                    self.logger?.LogError("Map length is too long: {len}", len);

                    constStrErrId = self.constStrs.MsgPackMapTooLong;
                    return false;
                }

                if (!self.state.TryCreateTable(0, (int)len))
                {
                    constStrErrId = self.constStrs.OutOfMemory;
                    return false;
                }
                var setInTable = 0;

                var mapIndex = self.state.StackTop;

                for (var i = 1; i <= len; i++)
                {
                    // Push the key onto the stack
                    if (!TryDecode(self, ref data, out constStrErrId))
                    {
                        return false;
                    }

                    // Push the value onto the stack
                    if (!TryDecode(self, ref data, out constStrErrId))
                    {
                        return false;
                    }

                    self.state.RawSet((int)len, mapIndex, ref setInTable);
                }
                Debug.Assert(setInTable == len, "Didn't fill table with records");

                constStrErrId = -1;
                return true;
            }
        }

        /// <summary>
        /// Entry point for redis.setresp(...) from a Lua script.
        /// </summary>
        internal int SetResp(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount != 1)
            {
                return LuaWrappedError(0, constStrs.ErrRedisSetRespArg);
            }

            double num;
            if (state.Type(1) != LuaType.Number || (num = state.CheckNumber(1)) is not (2 or 3))
            {
                return LuaWrappedError(0, constStrs.ErrRespVersion);
            }

            respServerSession.UpdateRespProtocolVersion((byte)num);

            return 0;
        }

        /// <summary>
        /// Entry point for redis.acl_check_cmd(...) from a Lua script.
        /// </summary>
        internal int AclCheckCommand(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var luaArgCount = state.StackTop;
            if (luaArgCount == 0)
            {
                return LuaWrappedError(1, constStrs.PleaseSpecifyRedisCall);
            }

            if (state.Type(1) != LuaType.String)
            {
                return LuaWrappedError(1, constStrs.ErrBadArg);
            }

            state.KnownStringToBuffer(1, out var cmdSpan);

            // It's most accurate to use our existing parsing code
            // But it requires correct argument counts, and redis.acl_check_cmd doesn't.
            //
            // So we need to determine the expected minimum and maximum counts and truncate or add
            // any arguments

            var cmdStr = Encoding.UTF8.GetString(cmdSpan);
            if (!RespCommandsInfo.TryGetRespCommandInfo(cmdStr, out var info, externalOnly: false, includeSubCommands: true))
            {
                return LuaWrappedError(1, constStrs.ErrInvalidCommand);
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
                const int NeededStackSpace = 1;
                if (!state.TryEnsureMinimumStackCapacity(NeededStackSpace))
                {
                    return LuaWrappedError(1, constStrs.InsufficientLuaStackSpace);
                }

                success = true;
                foreach (var subCommand in RespCommand.BITOP.ExpandForACLs())
                {
                    switch (subCommand)
                    {
                        case RespCommand.BITOP_AND: state.PushConstantString(constStrs.AND); break;
                        case RespCommand.BITOP_OR: state.PushConstantString(constStrs.OR); break;
                        case RespCommand.BITOP_XOR: state.PushConstantString(constStrs.XOR); break;
                        case RespCommand.BITOP_NOT: state.PushConstantString(constStrs.NOT); break;
                        case RespCommand.BITOP_DIFF: state.PushConstantString(constStrs.DIFF); break;

                        default: throw new InvalidOperationException($"Unexpected BITOP sub command: {subCommand}");
                    }

                    var (parsedCmd, badArg) = PrepareAndCheckRespRequest(ref state, respServerSession, scratchBufferBuilder, info, cmdSpan, luaArgCount: 2);

                    // Remove the BITOP sub command
                    state.Pop(1);

                    if (badArg)
                    {
                        return LuaWrappedError(1, constStrs.ErrBadArg);
                    }

                    if (parsedCmd == RespCommand.INVALID)
                    {
                        return LuaWrappedError(1, constStrs.ErrInvalidCommand);
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
                const int NeededStackSpace = 1;
                if (!state.TryEnsureMinimumStackCapacity(NeededStackSpace))
                {
                    return LuaWrappedError(1, constStrs.InsufficientLuaStackSpace);
                }

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

                        if (!state.TryPushBuffer(subCommandBuf))
                        {
                            return LuaWrappedError(1, constStrs.OutOfMemory);
                        }

                        var (parsedCmd, badArg) = PrepareAndCheckRespRequest(ref state, respServerSession, scratchBufferBuilder, subCommand, cmdSpan, luaArgCount: 2);

                        // Remove the extra sub-command
                        state.Pop(1);

                        if (badArg)
                        {
                            return LuaWrappedError(1, constStrs.ErrBadArg);
                        }

                        if (parsedCmd == RespCommand.INVALID)
                        {
                            return LuaWrappedError(1, constStrs.ErrInvalidCommand);
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
                var (parsedCommand, badArg) = PrepareAndCheckRespRequest(ref state, respServerSession, scratchBufferBuilder, info, cmdSpan, luaArgCount);

                if (badArg)
                {
                    return LuaWrappedError(1, constStrs.ErrBadArg);
                }

                if (parsedCommand == RespCommand.INVALID)
                {
                    return LuaWrappedError(1, constStrs.ErrInvalidCommand);
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
                ScratchBufferBuilder scratchBufferManager,
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

        // Implementation details

        /// <summary>
        /// Compile script, writing errors out to given response.
        /// </summary>
        private unsafe int CompileCommon<TResponse>(nint luaState, ref TResponse resp)
            where TResponse : struct, IResponseAdapter
        {
            // 1 for function, 1 for code string
            const int NeededStackSpace = 2;

            Debug.Assert(functionRegistryIndex == -1, "Shouldn't compile multiple times");

            state.KnownCallFromLuaEntered(luaState, 0);

            Debug.Assert(state.TryEnsureMinimumStackCapacity(NeededStackSpace), "LUA_MIN_STACK should be high enough that this cannot happen");

            _ = state.RawGetInteger(LuaType.Function, (int)LuaRegistry.Index, loadSandboxedRegistryIndex);
            if (!state.TryPushBuffer(source.Span))
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.LUA_out_of_memory, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();

                return 0;
            }

            var callRes = state.PCall(1, 2);

            // On success the stack will have two things on it:
            //  1. The error (nil if not error)
            //  2. The function (nil if error)

            if (callRes == LuaStatus.OK && state.StackTop == 2 && state.Type(1) == LuaType.Nil)
            {
                // No error, success!

                Debug.Assert(state.Type(2) == LuaType.Function, "Unexpected type returned from load_sandboxed");

                if (!state.TryRef(out functionRegistryIndex))
                {
                    // Uh-oh, couldn't save the function under the registry
                    while (!RespWriteUtils.TryWriteError(CmdStrings.LUA_out_of_memory, ref resp.BufferCur, resp.BufferEnd))
                        resp.SendAndReset();

                    return 0;
                }
            }
            else
            {
                string errStr;
                if (state.StackTop >= 1 && state.Type(1) == LuaType.String)
                {
                    // We control the definition of load_sandboxed, so we know this will be the error
                    state.KnownStringToBuffer(1, out var errorBuf);
                    errStr = $"Compilation error: {Encoding.UTF8.GetString(errorBuf)}";
                }
                else
                {
                    errStr = "Compilation error, cause unknown";
                }

                while (!RespWriteUtils.TryWriteError(errStr, ref resp.BufferCur, resp.BufferEnd))
                    resp.SendAndReset();
            }

            return 0;
        }

        /// <summary>
        /// Entry point method for executing commands from a Lua Script
        /// </summary>
        private unsafe int ProcessCommandFromScripting<TGarnetApi>(nint luaStatePtr, ref TGarnetApi api)
            where TGarnetApi : IGarnetApi
        {
            // 1 slot for the result (if any)
            const int NeededStackSpace = 1;

            state.CallFromLuaEntered(luaStatePtr);

            try
            {
                var argCount = state.StackTop;

                if (argCount <= 0)
                {
                    return LuaWrappedError(1, constStrs.PleaseSpecifyRedisCall);
                }

                if (!state.TryEnsureMinimumStackCapacity(NeededStackSpace))
                {
                    return LuaWrappedError(1, constStrs.InsufficientLuaStackSpace);
                }

                if (state.Type(1) != LuaType.String)
                {
                    return LuaWrappedError(1, constStrs.ErrBadArg);
                }

                state.KnownStringToBuffer(1, out var cmdSpan);

                // We special-case a few performance-sensitive operations to directly invoke via the storage API
                if (AsciiUtils.EqualsUpperCaseSpanIgnoringCase(cmdSpan, "SET"u8) && argCount == 3)
                {
                    if (!respServerSession.CheckACLPermissions(RespCommand.SET))
                    {
                        return LuaWrappedError(1, constStrs.ErrNoAuth);
                    }

                    ReadOnlySpan<byte> keySpan;
                    var keyType = state.Type(2);
                    if (keyType == LuaType.String)
                    {
                        state.KnownStringToBuffer(2, out keySpan);
                    }
                    else if (keyType == LuaType.Number)
                    {
                        if (!state.TryNumberToString(2, out keySpan))
                        {
                            return LuaWrappedError(1, constStrs.OutOfMemory);
                        }
                    }
                    else
                    {
                        return LuaWrappedError(1, constStrs.ErrBadArg);
                    }

                    ReadOnlySpan<byte> valSpan;
                    var valType = state.Type(3);
                    if (valType == LuaType.String)
                    {
                        state.KnownStringToBuffer(3, out valSpan);
                    }
                    else if (valType == LuaType.Number)
                    {
                        if (!state.TryNumberToString(3, out valSpan))
                        {
                            return LuaWrappedError(1, constStrs.OutOfMemory);
                        }
                    }
                    else
                    {
                        return LuaWrappedError(1, constStrs.ErrBadArg);
                    }

                    // Note these spans are implicitly pinned, as they're actually on the Lua stack
                    var key = PinnedSpanByte.FromPinnedSpan(keySpan);
                    var value = PinnedSpanByte.FromPinnedSpan(valSpan);

                    _ = api.SET(key, value);

                    state.PushConstantString(constStrs.Ok);
                    return 1;
                }
                else if (AsciiUtils.EqualsUpperCaseSpanIgnoringCase(cmdSpan, "GET"u8) && argCount == 2)
                {
                    if (!respServerSession.CheckACLPermissions(RespCommand.GET))
                    {
                        return LuaWrappedError(1, constStrs.ErrNoAuth);
                    }

                    ReadOnlySpan<byte> keySpan;
                    var keyType = state.Type(2);
                    if (keyType == LuaType.String)
                    {
                        state.KnownStringToBuffer(2, out keySpan);
                    }
                    else if (keyType == LuaType.Number)
                    {
                        if (!state.TryNumberToString(2, out keySpan))
                        {
                            return LuaWrappedError(1, constStrs.OutOfMemory);
                        }
                    }
                    else
                    {
                        return LuaWrappedError(1, constStrs.ErrBadArg);
                    }

                    // Span is (implicitly) pinned since it's actually on the Lua stack
                    var key = PinnedSpanByte.FromPinnedSpan(keySpan);
                    var status = api.GET(key, out PinnedSpanByte value);

                    if (status == GarnetStatus.OK)
                    {
                        if (!state.TryPushBuffer(value.ReadOnlySpan))
                        {
                            return LuaWrappedError(1, constStrs.OutOfMemory);
                        }
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

                scratchBufferBuilder.Reset();
                scratchBufferBuilder.StartCommand(cmdSpan, argCount - 1);

                for (var i = 0; i < argCount - 1; i++)
                {
                    var argIx = 2 + i;

                    var argType = state.Type(argIx);
                    if (argType == LuaType.Nil)
                    {
                        scratchBufferBuilder.WriteNullArgument();
                    }
                    else if (argType is LuaType.String)
                    {
                        state.KnownStringToBuffer(argIx, out var span);
                        scratchBufferBuilder.WriteArgument(span);
                    }
                    else if (argType is LuaType.Number)
                    {
                        if (!state.TryNumberToString(argIx, out var span))
                        {
                            return LuaWrappedError(1, constStrs.OutOfMemory);
                        }
                        scratchBufferBuilder.WriteArgument(span);
                    }
                    else
                    {
                        return LuaWrappedError(1, constStrs.ErrBadArg);
                    }
                }

                var request = scratchBufferBuilder.ViewFullArgSlice();

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

                return LuaWrappedError(1, constStrs.UnexpectedError);
            }
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
        /// Called if the 'exceptions cannot be propogated back to Lua'-invariant is violated.
        /// 
        /// In this case, we have no option but to crash.
        /// 
        /// We do try and log a bit before then.
        /// </summary>
        private static int FailOnException(Exception e, [CallerMemberName] string method = null)
        {
            const string FormatString = "Attempted to propogate exception back to Lua from {0}, this will corrupt the runtime.  Failing fast.";

            CallbackContext?.logger?.LogCritical(e, FormatString, method);
            Environment.FailFast(string.Format(FormatString, method ?? "!!UNKNOWN!!"), e);

            // Invalid return which will also crash Lua, but we should never get here
            return -1;
        }

        /// <summary>
        /// Entry point for Lua PCall'ing into <see cref="LuaRunner.UnsafeCompileForRunner"/>.
        /// 
        /// We need this indirection to allow Lua to detect and report internal and memory errors
        /// without crashing the process.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int CompileForRunner(nint luaState)
        {
            try
            {
                return CallbackContext.UnsafeCompileForRunner(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for Lua PCall'ing into <see cref="LuaRunner.UnsafeCompileForSession"/>.
        /// 
        /// We need this indirection to allow Lua to detect and report internal and memory errors
        /// without crashing the process.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int CompileForSession(nint luaState)
        {
            try
            {
                return CallbackContext.UnsafeCompileForSession(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for Lua PCall'ing into <see cref="LuaRunner.UnsafeRunPreambleForRunner"/>.
        /// 
        /// We need this indirection to allow Lua to detect and report internal and memory errors
        /// without crashing the process.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int RunPreambleForRunner(nint luaState)
        {
            try
            {
                return CallbackContext.UnsafeRunPreambleForRunner(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for Lua PCall'ing into <see cref="LuaRunner.UnsafeRunPreambleForSession"/>.
        /// 
        /// We need this indirection to allow Lua to detect and report internal and memory errors
        /// without crashing the process.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int RunPreambleForSession(nint luaState)
        {
            try
            {
                return CallbackContext.UnsafeRunPreambleForSession(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for Lua calling back into Garnet via redis.call(...).
        /// 
        /// This entry point is for when there isn't an active <see cref="RespServerSession"/>.
        /// This should only happen during testing.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int GarnetCallNoSession(nint luaState)
        {
            try
            {
                return CallbackContext.NoSessionResponse(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for Lua calling back into Garnet via redis.call(...).
        /// 
        /// This entry point is for when a transaction is in effect.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int GarnetCallWithTransaction(nint luaState)
        {
            try
            {
                return CallbackContext.GarnetCallWithTransaction(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for Lua calling back into Garnet via redis.call(...).
        /// 
        /// This entry point is for when a transaction is not necessary.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int GarnetCallNoTransaction(nint luaState)
        {
            try
            {
                return CallbackContext.GarnetCall(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for checking timeouts, called periodically from Lua.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "Callback must take these parameters")]
        internal static void RequestTimeout(nint luaState, nint debugState)
        {
            try
            {
                CallbackContext?.RequestTimeout(luaState);
            }
            catch (Exception e)
            {
                _ = FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to redis.sha1hex.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int SHA1Hex(nint luaState)
        {
            try
            {
                return CallbackContext.SHA1Hex(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to redis.log.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Log(nint luaState)
        {
            try
            {
                return CallbackContext.Log(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to redis.acl_check_cmd.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int AclCheckCommand(nint luaState)
        {
            try
            {
                return CallbackContext.AclCheckCommand(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to redis.setresp.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int SetResp(nint luaState)
        {
            try
            {
                return CallbackContext.SetResp(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to math.atan2.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Atan2(nint luaState)
        {
            try
            {
                return CallbackContext.Atan2(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to math.cosh.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Cosh(nint luaState)
        {
            try
            {
                return CallbackContext.Cosh(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to math.frexp.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Frexp(nint luaState)
        {
            try
            {
                return CallbackContext.Frexp(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to math.ldexp.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Ldexp(nint luaState)
        {
            try
            {
                return CallbackContext.Ldexp(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to math.log10.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Log10(nint luaState)
        {
            try
            {
                return CallbackContext.Log10(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to math.pow.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Pow(nint luaState)
        {
            try
            {
                return CallbackContext.Pow(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to math.sinh.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Sinh(nint luaState)
        {
            try
            {
                return CallbackContext.Sinh(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to math.tanh.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Tanh(nint luaState)
        {
            try
            {
                return CallbackContext.Tanh(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to table.maxn.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Maxn(nint luaState)
        {
            try
            {
                return CallbackContext.Maxn(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to loadstring.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int LoadString(nint luaState)
        {
            try
            {
                return CallbackContext.LoadString(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to cjson.encode.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int CJsonEncode(nint luaState)
        {
            try
            {
                return CallbackContext.CJsonEncode(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to cjson.decode.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int CJsonDecode(nint luaState)
        {
            try
            {
                return CallbackContext.CJsonDecode(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to bit.tobit.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int BitToBit(nint luaState)
        {
            try
            {
                return CallbackContext.BitToBit(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to bit.tohex.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int BitToHex(nint luaState)
        {
            try
            {
                return CallbackContext.BitToHex(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to garnet_bitop, which backs
        /// bit.bnot, bit.bor, etc.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int Bitop(nint luaState)
        {
            try
            {
                return CallbackContext.Bitop(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to bit.bswap.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int BitBswap(nint luaState)
        {
            try
            {
                return CallbackContext.BitBswap(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to cmsgpack.pack.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int CMsgPackPack(nint luaState)
        {
            try
            {
                return CallbackContext.CMsgPackPack(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to cmsgpack.unpack.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int CMsgPackUnpack(nint luaState)
        {
            try
            {
                return CallbackContext.CMsgPackUnpack(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to struct.pack.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int StructPack(nint luaState)
        {
            try
            {
                return CallbackContext.StructPack(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to struct.unpack.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int StructUnpack(nint luaState)
        {
            try
            {
                return CallbackContext.StructUnpack(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to struct.size.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int StructSize(nint luaState)
        {
            try
            {
                return CallbackContext.StructSize(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }

        /// <summary>
        /// Entry point for calls to garnet_unpack_trampoline.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        internal static int UnpackTrampoline(nint luaState)
        {
            try
            {
                return CallbackContext.UnpackTrampoline(luaState);
            }
            catch (Exception e)
            {
                return FailOnException(e);
            }
        }
    }
}