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
            private readonly ScratchBufferBuilder bufferBuilder;
            private byte* origin;
            private byte* curHead;

            internal RunnerAdapter(ScratchBufferBuilder bufferBuilder)
            {
                this.bufferBuilder = bufferBuilder;
                this.bufferBuilder.Reset();

                var scratchSpace = bufferBuilder.FullBuffer();

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

                    var full = bufferBuilder.FullBuffer();

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
                bufferBuilder.GrowBuffer(copyLengthOverride: len);

                var scratchSpace = bufferBuilder.FullBuffer();

                origin = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(scratchSpace));
                BufferEnd = origin + scratchSpace.Length;
                curHead = origin + len;
            }
        }

        private static (int Start, ulong[] ByteMask) NoScriptDetails = InitializeNoScriptDetails();

        // References into Registry on the Lua side
        //
        // These are mix of objects we regularly update,
        // constants we want to avoid copying from .NET to Lua,
        // and the compiled function definition.
        readonly int sandboxEnvRegistryIndex;
        readonly int loadSandboxedRegistryIndex;
        readonly int resetKeysAndArgvRegistryIndex;
        readonly int requestTimeoutRegsitryIndex;
        readonly ConstantStringRegistryIndexes constStrs;

        readonly LuaLoggingMode logMode;
        readonly HashSet<string> allowedFunctions;
        readonly ReadOnlyMemory<byte> source;
        readonly ScratchBufferNetworkSender scratchBufferNetworkSender;
        readonly RespServerSession respServerSession;

        readonly ScratchBufferBuilder scratchBufferBuilder;
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

        // Size of the array compontents of KEYS and ARGV
        // We keep these up to date so we can reason about allocations
        int keysArrCapacity, argvArrCapacity;

        /// <summary>
        /// If an invocation ran into an error that has left the <see cref="LuaRunner"/> in a bad (but legal) state, 
        /// returns true indicating the <see cref="LuaRunner"/> should be recreated at the next convenience.
        /// </summary>
        public bool NeedsDispose
        => state.NeedsDispose;

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
            // KEYS and ARGV are always access by index, and to avoid allocation concerns
            // we also want to track their 'array'-bits sizes
            //
            // So we explicitly create them with these capacities, and recreate them as necessary
            const int InitialKeysCapacity = 5;
            const int InitialArgvCapacity = 5;

            this.source = source;
            this.txnMode = txnMode;
            this.respServerSession = respServerSession;
            this.scratchBufferNetworkSender = scratchBufferNetworkSender;
            this.logMode = logMode;
            this.allowedFunctions = allowedFunctions;
            this.logger = logger;

            scratchBufferBuilder = respServerSession?.scratchBufferBuilder ?? new();

            // Explicitly force to RESP2 for now
            if (respServerSession != null)
            {
                respServerSession.UpdateRespProtocolVersion(2);

                // The act of setting these fields causes NoScript checks to be performed
                (respServerSession.noScriptStart, respServerSession.noScriptBitmap) = NoScriptDetails;
            }

            sandboxEnvRegistryIndex = -1;
            loadSandboxedRegistryIndex = -1;
            functionRegistryIndex = -1;

            state = new LuaStateWrapper(memMode, memLimitBytes, this.logger);

            if (!state.TryCreateTable(InitialKeysCapacity, 0) || !state.TrySetGlobal("KEYS\0"u8))
            {
                throw new GarnetException("Insufficient space in Lua VM for KEYS");
            }
            keysArrCapacity = InitialKeysCapacity;

            if (!state.TryCreateTable(InitialArgvCapacity, 0) || !state.TrySetGlobal("ARGV\0"u8))
            {
                throw new GarnetException("Insufficient space in Lua VM for ARGV");
            }
            argvArrCapacity = InitialArgvCapacity;

            delegate* unmanaged[Cdecl]<nint, int> garnetCall;
            if (txnMode)
            {
                txnKeyEntries = new TxnKeyEntries(16,
                    respServerSession.storageSession.unifiedTransactionalContext);

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

            // Lua 5.4 does not provide these functions, but 5.1 does - so implement them
            Register(ref state, "garnet_atan2\0"u8, &LuaRunnerTrampolines.Atan2);
            Register(ref state, "garnet_cosh\0"u8, &LuaRunnerTrampolines.Cosh);
            Register(ref state, "garnet_frexp\0"u8, &LuaRunnerTrampolines.Frexp);
            Register(ref state, "garnet_ldexp\0"u8, &LuaRunnerTrampolines.Ldexp);
            Register(ref state, "garnet_log10\0"u8, &LuaRunnerTrampolines.Log10);
            Register(ref state, "garnet_pow\0"u8, &LuaRunnerTrampolines.Pow);
            Register(ref state, "garnet_sinh\0"u8, &LuaRunnerTrampolines.Sinh);
            Register(ref state, "garnet_tanh\0"u8, &LuaRunnerTrampolines.Tanh);
            Register(ref state, "garnet_maxn\0"u8, &LuaRunnerTrampolines.Maxn);
            Register(ref state, "garnet_loadstring\0"u8, &LuaRunnerTrampolines.LoadString);

            // Things provided as Lua libraries, which we actually implement in .NET
            Register(ref state, "garnet_cjson_encode\0"u8, &LuaRunnerTrampolines.CJsonEncode);
            Register(ref state, "garnet_cjson_decode\0"u8, &LuaRunnerTrampolines.CJsonDecode);
            Register(ref state, "garnet_bit_tobit\0"u8, &LuaRunnerTrampolines.BitToBit);
            Register(ref state, "garnet_bit_tohex\0"u8, &LuaRunnerTrampolines.BitToHex);
            // garnet_bitop implements bnot, bor, band, xor, etc. but isn't directly exposed
            Register(ref state, "garnet_bitop\0"u8, &LuaRunnerTrampolines.Bitop);
            Register(ref state, "garnet_bit_bswap\0"u8, &LuaRunnerTrampolines.BitBswap);
            Register(ref state, "garnet_cmsgpack_pack\0"u8, &LuaRunnerTrampolines.CMsgPackPack);
            Register(ref state, "garnet_cmsgpack_unpack\0"u8, &LuaRunnerTrampolines.CMsgPackUnpack);
            Register(ref state, "garnet_struct_pack\0"u8, &LuaRunnerTrampolines.StructPack);
            Register(ref state, "garnet_struct_unpack\0"u8, &LuaRunnerTrampolines.StructUnpack);
            Register(ref state, "garnet_struct_size\0"u8, &LuaRunnerTrampolines.StructSize);
            Register(ref state, "garnet_call\0"u8, garnetCall);
            Register(ref state, "garnet_sha1hex\0"u8, &LuaRunnerTrampolines.SHA1Hex);
            Register(ref state, "garnet_log\0"u8, &LuaRunnerTrampolines.Log);
            Register(ref state, "garnet_acl_check_cmd\0"u8, &LuaRunnerTrampolines.AclCheckCommand);
            Register(ref state, "garnet_setresp\0"u8, &LuaRunnerTrampolines.SetResp);
            Register(ref state, "garnet_unpack_trampoline\0"u8, &LuaRunnerTrampolines.UnpackTrampoline);

            var redisVersionBytes = Encoding.UTF8.GetBytes(redisVersion);
            if (!state.TryPushBuffer(redisVersionBytes) || !state.TrySetGlobal("garnet_REDIS_VERSION\0"u8))
            {
                throw new GarnetException("Insufficient space in Lua VM for redis version global");
            }

            var redisVersionParsed = Version.Parse(redisVersion);
            var redisVersionNum =
                ((byte)redisVersionParsed.Major << 16) |
                ((byte)redisVersionParsed.Minor << 8) |
                ((byte)redisVersionParsed.Build << 0);
            state.PushInteger(redisVersionNum);
            if (!state.TrySetGlobal("garnet_REDIS_VERSION_NUM\0"u8))
            {
                throw new GarnetException("Insufficient space in Lua VM for redis version number global");
            }

            var loadRes = state.LoadBuffer(PrepareLoaderBlockBytes(allowedFunctions, logger).Span);
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

            state.GetGlobal(LuaType.Table, "sandbox_env\0"u8);
            if (!state.TryRef(out sandboxEnvRegistryIndex))
            {
                throw new GarnetException("Insufficient space in VM for sandbox_env ref");
            }

            state.GetGlobal(LuaType.Function, "load_sandboxed\0"u8);
            if (!state.TryRef(out loadSandboxedRegistryIndex))
            {
                throw new GarnetException("Insufficient space in VM for load_sandboxed ref");
            }

            state.GetGlobal(LuaType.Function, "reset_keys_and_argv\0"u8);
            if (!state.TryRef(out resetKeysAndArgvRegistryIndex))
            {
                throw new GarnetException("Insufficient space in VM for reset_keys_and_argv ref");
            }

            state.GetGlobal(LuaType.Function, "request_timeout\0"u8);
            if (!state.TryRef(out requestTimeoutRegsitryIndex))
            {
                throw new GarnetException("Insufficient space in VM for request_timeout ref");
            }

            // Load all the constant strings into the VM
            constStrs = new(ref state);

            state.ExpectLuaStackEmpty();

            // Attempt to register a function under the given name, throwing an exception if that fails
            static void Register(ref LuaStateWrapper state, ReadOnlySpan<byte> name, delegate* unmanaged[Cdecl]<nint, int> function)
            {
                if (!state.TryRegister(name, function))
                {
                    throw new GarnetException($"Insufficient space in VM for {Encoding.UTF8.GetString(name[..^1])} global");
                }
            }
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

            runnerAdapter = new RunnerAdapter(scratchBufferBuilder);
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

            Debug.Assert(state.TryEnsureMinimumStackCapacity(nonErrorReturns + 1), "Bounds check above should mean this never fails");

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

            Debug.Assert(state.TryEnsureMinimumStackCapacity(nonErrorReturns + 1), "Should never fail, as nonErrorReturns+1 <= LUA_MINSTACK");

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
                        if (!state.TryCreateTable(0, 1))
                        {
                            respPtr = respEnd;
                            return LuaWrappedError(1, constStrs.OutOfMemory);
                        }
                        state.PushConstantString(constStrs.OkLower);
                        var setInOkTable = 0;

                        if (!state.TryPushBuffer(resultSpan))
                        {
                            return LuaWrappedError(1, constStrs.OutOfMemory);
                        }

                        state.RawSet(1, curTop + 1, ref setInOkTable);
                        Debug.Assert(setInOkTable == 1, "Didn't fill table with expected records");

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

                            if (!state.TryCreateTable(arrayItemCount, 0))
                            {
                                respPtr = respEnd;
                                return LuaWrappedError(1, constStrs.OutOfMemory);
                            }

                            for (var itemIx = 0; itemIx < arrayItemCount; itemIx++)
                            {
                                // Pushes the item to the top of the stack
                                _ = ProcessSingleRespTerm(respProtocolVersion, ref respPtr, respEnd);

                                // Store the item into the table
                                state.RawSetInteger(arrayItemCount, curTop + 1, itemIx + 1);
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
                        if (!state.TryCreateTable(0, 1))
                        {
                            respPtr = respEnd;
                            return LuaWrappedError(1, constStrs.OutOfMemory);
                        }
                        var setRecordsInTable = 0;

                        state.PushConstantString(constStrs.Map);
                        if (!state.TryCreateTable(0, mapPairCount))
                        {
                            respPtr = respEnd;
                            return LuaWrappedError(1, constStrs.OutOfMemory);
                        }
                        var setRecordsInMap = 0;

                        for (var pair = 0; pair < mapPairCount; pair++)
                        {
                            // Read key
                            _ = ProcessSingleRespTerm(respProtocolVersion, ref respPtr, respEnd);

                            // Read value
                            _ = ProcessSingleRespTerm(respProtocolVersion, ref respPtr, respEnd);

                            // Set t[k] = v
                            state.RawSet(mapPairCount, curTop + 3, ref setRecordsInMap);
                        }
                        Debug.Assert(mapPairCount == setRecordsInMap, "Didn't fill table with records");

                        // Store the sub-table into the parent table
                        state.RawSet(1, curTop + 1, ref setRecordsInTable);
                        Debug.Assert(setRecordsInMap == 1, "Didn't fill table with records");

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
                        if (!state.TryCreateTable(0, 1))
                        {
                            respPtr = respEnd;
                            return LuaWrappedError(1, constStrs.OutOfMemory);
                        }
                        var setRecordsInTable = 0;

                        state.PushConstantString(constStrs.Set);
                        if (!state.TryCreateTable(0, setItemCount))
                        {
                            respPtr = respEnd;
                            return LuaWrappedError(1, constStrs.OutOfMemory);
                        }
                        var setRecordsInSet = 0;

                        for (var pair = 0; pair < setItemCount; pair++)
                        {
                            // Read value, which we use as a key
                            _ = ProcessSingleRespTerm(respProtocolVersion, ref respPtr, respEnd);

                            // Unconditionally the value under the key is true
                            state.PushBoolean(true);

                            // Set t[value] = true
                            state.RawSet(setItemCount, curTop + 3, ref setRecordsInSet);
                        }
                        Debug.Assert(setItemCount == setRecordsInSet, "Didn't fill table with records");

                        // Store the sub-table into the parent table
                        state.RawSet(1, curTop + 1, ref setRecordsInTable);
                        Debug.Assert(setRecordsInTable == 1, "Didn't fill table with records");

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
                            if (!state.TryCreateTable(0, 1))
                            {
                                respPtr = respEnd;
                                return LuaWrappedError(1, constStrs.OutOfMemory);
                            }
                            var setRecordsInTable = 0;

                            state.PushConstantString(constStrs.Double);
                            state.PushNumber(parsed);

                            state.RawSet(1, curTop + 1, ref setRecordsInTable);
                            Debug.Assert(setRecordsInTable == 1, "Didn't fill table with records");

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
                                if (!state.TryCreateTable(0, 1))
                                {
                                    respPtr = respEnd;
                                    return LuaWrappedError(1, constStrs.OutOfMemory);
                                }
                                var setRecordsInTable = 0;

                                state.PushConstantString(constStrs.BigNumber);

                                if (!state.TryPushBuffer(bigNumSpan))
                                {
                                    respPtr = respEnd;
                                    return LuaWrappedError(1, constStrs.OutOfMemory);
                                }

                                state.RawSet(1, curTop + 1, ref setRecordsInTable);
                                Debug.Assert(setRecordsInTable == 1, "Didn't fill table with records");

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

                            if (!state.TryCreateTable(0, 2))
                            {
                                respPtr = respEnd;
                                return LuaWrappedError(1, constStrs.OutOfMemory);
                            }
                            var setRecordsInTable = 0;

                            state.PushConstantString(constStrs.Format);

                            if (!state.TryPushBuffer(format))
                            {
                                return LuaWrappedError(1, constStrs.OutOfMemory);
                            }

                            state.RawSet(2, curTop + 1, ref setRecordsInTable);

                            state.PushConstantString(constStrs.String);

                            if (!state.TryPushBuffer(data))
                            {
                                return LuaWrappedError(1, constStrs.OutOfMemory);
                            }

                            state.RawSet(2, curTop + 1, ref setRecordsInTable);
                            Debug.Assert(setRecordsInTable == 2, "Didn't fill table with records");

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

            Debug.Assert(state.TryEnsureMinimumStackCapacity(NeededStackSize), "LUA_MINSTACK should be large enough that this never fails");

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

            Debug.Assert(state.TryEnsureMinimumStackCapacity(NeededStackSize), "LUA_MINSTACK should be large enough that this never fails");

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
                        var _key = scratchBufferBuilder.CreateArgSlice(key);
                        txnKeyEntries.AddKey(_key, Tsavorite.core.LockType.Exclusive);
                    }

                    adapter = new(scratchBufferBuilder);
                    RunInTransaction(ref adapter);
                }
                else
                {
                    adapter = new(scratchBufferBuilder);
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
            var txnVersion = respServerSession.storageSession.stateMachineDriver.AcquireTransactionVersion();
            try
            {
                respServerSession.storageSession.stringTransactionalContext.BeginTransaction();
                if (!respServerSession.storageSession.objectTransactionalContext.IsNull)
                    respServerSession.storageSession.objectTransactionalContext.BeginTransaction();
                if (!respServerSession.storageSession.unifiedTransactionalContext.IsNull)
                    respServerSession.storageSession.unifiedTransactionalContext.BeginTransaction();
                respServerSession.SetTransactionMode(true);
                txnKeyEntries.LockAllKeys();

                txnVersion = respServerSession.storageSession.stateMachineDriver.VerifyTransactionVersion(txnVersion);
                respServerSession.storageSession.stringTransactionalContext.LocksAcquired(txnVersion);
                if (!respServerSession.storageSession.objectTransactionalContext.IsNull)
                    respServerSession.storageSession.objectTransactionalContext.LocksAcquired(txnVersion);
                if (!respServerSession.storageSession.unifiedTransactionalContext.IsNull)
                    respServerSession.storageSession.unifiedTransactionalContext.LocksAcquired(txnVersion);
                RunCommon(ref response);
            }
            finally
            {
                txnKeyEntries.UnlockAllKeys();
                respServerSession.SetTransactionMode(false);
                respServerSession.storageSession.stringTransactionalContext.EndTransaction();
                if (!respServerSession.storageSession.objectTransactionalContext.IsNull)
                    respServerSession.storageSession.objectTransactionalContext.EndTransaction();
                if (!respServerSession.storageSession.unifiedTransactionalContext.IsNull)
                    respServerSession.storageSession.unifiedTransactionalContext.EndTransaction();
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
        => state.TrySetHook(&LuaRunnerTrampolines.RequestTimeout, LuaHookMask.Count, 1);

        /// <summary>
        /// Remove extra keys and args from KEYS and ARGV globals.
        /// </summary>
        internal bool TryResetParameters(int nKeys, int nArgs, out LuaStatus failingStatus)
        {
            // Space for key count, value count, and function
            const int NeededStackSize = 3;

            Debug.Assert(state.TryEnsureMinimumStackCapacity(NeededStackSize), "LUA_MINSTACK should be large enough that this never fails");

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
        /// Replace KEYS in sandbox_env with a new table with the given array capacity.
        /// </summary>
        private bool TryRecreateKEYS(int length)
        {
            // 1 for sandbox_env, 1 for "KEYS", and 1 for new table
            const int NeededStackSpace = 3;

            Debug.Assert(state.TryEnsureMinimumStackCapacity(NeededStackSpace), "LUA_MINSTACK should ensure this always succeeds");

            // Get sandbox_env and "KEYS" on the stack
            _ = state.RawGetInteger(LuaType.Table, (int)LuaRegistry.Index, sandboxEnvRegistryIndex);
            var sandboxEnvIndex = state.StackTop;
            state.PushConstantString(constStrs.KEYS);

            // Make new KEYS
            if (!state.TryCreateTable(length, 0))
            {
                return false;
            }

            // Save it, which should have NO allocation impact because we're updating an existing slot
            var ignored = 0;
            state.RawSet(1, sandboxEnvIndex, ref ignored);

            // Get sandbox_env off the stack
            state.Pop(1);

            keysArrCapacity = length;

            return true;
        }

        /// <summary>
        /// Replace ARGV in sandbox_env with a new table with the given array capacity.
        /// </summary>
        private bool TryRecreateARGV(int length)
        {
            // 1 for sandbox_env, 1 for "ARGV", and 1 for new table
            const int NeededStackSpace = 3;

            Debug.Assert(state.TryEnsureMinimumStackCapacity(NeededStackSpace), "LUA_MINSTACK should ensure this always succeeds");

            // Get sandbox_env and "KEYS" on the stack
            _ = state.RawGetInteger(LuaType.Table, (int)LuaRegistry.Index, sandboxEnvRegistryIndex);
            var sandboxEnvIndex = state.StackTop;
            state.PushConstantString(constStrs.ARGV);

            // Make new ARGV
            if (!state.TryCreateTable(length, 0))
            {
                return false;
            }

            // Save it, which should have NO allocation impact because we're updating an existing slot
            var ignored = 0;
            state.RawSet(1, sandboxEnvIndex, ref ignored);

            // Get sandbox_env off the stack
            state.Pop(1);

            argvArrCapacity = length;

            return true;
        }

        /// <summary>
        /// Runs the precompiled Lua function.
        /// </summary>
        private unsafe void RunCommon<TResponse>(ref TResponse resp)
            where TResponse : struct, IResponseAdapter
        {
            // Space for function
            const int NeededStackSize = 1;

            Debug.Assert(state.TryEnsureMinimumStackCapacity(NeededStackSize), "LUA_MINSTACK should be large enough that this never fails");

            try
            {
                // Every invocation starts in RESP2
                if (respServerSession != null)
                {
                    respServerSession.UpdateRespProtocolVersion(2);
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

            Debug.Assert(state.TryEnsureMinimumStackCapacity(TopLevelNeededStackSpace), "Caller should have ensured we're < LUA_MINSTACK");

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

                    if (!runner.state.TryEnsureMinimumStackCapacity(KeyNeededStackSpace))
                    {
                        errConstStrIndex = runner.constStrs.OutOfMemory;
                        return false;
                    }

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
                            if (!runner.state.TryNumberToString(curTop + 1, out _))
                            {
                                // Fail the whole serialization
                                errConstStrIndex = runner.constStrs.OutOfMemory;
                                return false;
                            }

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
        /// Attempts a call into Lua libraries.
        /// 
        /// If this fails, it's basically impossible for any other Lua functionality to work.
        /// </summary>
        public static bool TryProbeSupport(out string errorMessage)
        {
            try
            {
                _ = NativeMethods.Version();
                errorMessage = null;
                return true;
            }
            catch (Exception e)
            {
                errorMessage = e.Message;
                return false;
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
}