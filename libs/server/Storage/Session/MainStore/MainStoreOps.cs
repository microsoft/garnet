// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus GET<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var status = dualContext.Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, userContext: default);

            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, ref output);

            if (status.Found)
            {
                incr_session_found();
                return GarnetStatus.OK;
            }
            else
            {
                incr_session_notfound();
                return GarnetStatus.NOTFOUND;
            }
        }

        public GarnetStatus GET<TKeyLocker, TEpochGuard>(ref HashEntryInfo hei, ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            ReadOptions readOptions = default;
            var status = MainContext.Read<TKeyLocker>(ref hei, ref key, ref input, ref output, ref readOptions, recordMetadata: out _, userContext: default);
            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, ref output);

            if (status.Found)
            {
                incr_session_found();
                return GarnetStatus.OK;
            }
            else
            {
                incr_session_notfound();
                return GarnetStatus.NOTFOUND;
            }
        }


        public unsafe GarnetStatus ReadWithUnsafeContext<TContext>(ArgSlice key, ref SpanByte input, ref SpanByteAndMemory output, long localHeadAddress, out bool epochChanged, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {

            var _key = key.SpanByte;
            long ctx = default;

            epochChanged = false;
            var status = context.Read(ref _key, ref Unsafe.AsRef(in input), ref output, ctx);

            if (status.IsPending)
            {
                KernelSession.EndUnsafe(this);
                StartPendingMetrics();
                CompletePendingForSession(ref status, ref output, ref context);
                StopPendingMetrics();
                KernelSession.BeginUnsafe(this, context.Session);
                // Start read of pointers from beginning if epoch changed
                if (HeadAddress == localHeadAddress)
                {
                    KernelSession.EndUnsafe(this);
                    epochChanged = true;
                }
            }
            else if (status.NotFound)
            {
                incr_session_notfound();
                return GarnetStatus.NOTFOUND;
            }
            else
            {
                incr_session_found();
            }

            return GarnetStatus.OK;
        }

        public unsafe GarnetStatus GET<TContext>(ArgSlice key, out ArgSlice value, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            int inputSize = sizeof(int) + RespInputHeader.Size;

            byte* pbCmdInput = stackalloc byte[inputSize];
            byte* pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            (*(RespInputHeader*)pcurr).cmd = RespCommand.GET; // Proxy for "get value without RESP header"
            (*(RespInputHeader*)pcurr).flags = 0;

            var _key = key.SpanByte;
            var _output = new SpanByteAndMemory { SpanByte = scratchBufferManager.ViewRemainingArgSlice().SpanByte };
            var ret = GET(ref _key, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref _output, ref context);
            value = default;
            if (ret == GarnetStatus.OK)
            {
                if (!_output.IsSpanByte)
                {
                    value = scratchBufferManager.FormatScratch(0, _output.AsReadOnlySpan());
                    _output.Memory.Dispose();
                }
                else
                {
                    value = scratchBufferManager.CreateArgSlice(_output.Length);
                }
            }
            return ret;
        }

        public unsafe GarnetStatus GET<TContext>(ArgSlice key, out MemoryResult<byte> value, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            int inputSize = sizeof(int) + RespInputHeader.Size;

            byte* pbCmdInput = stackalloc byte[inputSize];
            byte* pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            (*(RespInputHeader*)pcurr).cmd = RespCommand.GET; // Proxy for "get value without RESP header"
            (*(RespInputHeader*)pcurr).flags = 0;

            var _key = key.SpanByte;
            var _output = new SpanByteAndMemory();
            var ret = GET(ref _key, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref _output, ref context);
            value = new MemoryResult<byte>(_output.Memory, _output.Length);
            return ret;
        }

        /// <summary>
        /// GETDEL command - Gets the value corresponding to the given key and deletes the key.
        /// </summary>
        /// <param name="key">The key to get the value for.</param>
        /// <param name="output">Span to allocate the output of the operation</param>
        /// <param name="context">Basic Context of the store</param>
        /// <returns> Operation status </returns>
        public unsafe GarnetStatus GETDEL<TContext>(ArgSlice key, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            var _key = key.SpanByte;
            return GETDEL(ref _key, ref output, ref context);
        }

        /// <summary>
        /// GETDEL command - Gets the value corresponding to the given key and deletes the key.
        /// </summary>
        /// <param name="key">The key to get the value for.</param>
        /// <param name="output">Span to allocate the output of the operation</param>
        /// <param name="context">Basic Context of the store</param>
        /// <returns> Operation status </returns>
        public unsafe GarnetStatus GETDEL<TContext>(ref SpanByte key, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            // size data + header
            int inputSize = sizeof(int) + RespInputHeader.Size;
            byte* pbCmdInput = stackalloc byte[inputSize];

            byte* pcurr = pbCmdInput;
            *(int*)pbCmdInput = inputSize - sizeof(int);
            pcurr += sizeof(int);
            ((RespInputHeader*)pcurr)->cmd = RespCommand.GETDEL;
            ((RespInputHeader*)pcurr)->flags = 0;

            ref var input = ref SpanByte.Reinterpret(pbCmdInput);

            var status = context.RMW(ref key, ref input, ref output);
            Debug.Assert(output.IsSpanByte);

            if (status.IsPending)
                CompletePendingForSession(ref status, ref output, ref context);

            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public unsafe GarnetStatus GETRANGE<TContext>(ref SpanByte key, int sliceStart, int sliceLength, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            int inputSize = sizeof(int) + RespInputHeader.Size + sizeof(int) * 2;
            byte* pbCmdInput = stackalloc byte[inputSize];

            byte* pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            (*(RespInputHeader*)(pcurr)).cmd = RespCommand.GETRANGE;
            (*(RespInputHeader*)(pcurr)).flags = 0;
            pcurr += RespInputHeader.Size;
            *(int*)(pcurr) = sliceStart;
            *(int*)(pcurr + 4) = sliceLength;

            var status = context.Read(ref key, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref output);

            if (status.IsPending)
            {
                StartPendingMetrics();
                CompletePendingForSession(ref status, ref output, ref context);
                StopPendingMetrics();
            }

            if (status.Found)
            {
                incr_session_found();
                return GarnetStatus.OK;
            }
            else
            {
                incr_session_notfound();
                return GarnetStatus.NOTFOUND;
            }
        }

        public GarnetStatus SET<TContext>(ref SpanByte key, ref SpanByte value, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            context.Upsert(ref key, ref value);
            return GarnetStatus.OK;
        }

        public unsafe GarnetStatus SET_Conditional<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            byte* pbOutput = stackalloc byte[8];
            var output = new SpanByteAndMemory(pbOutput, 8);
            return SET_Conditional<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);
        }

        public unsafe GarnetStatus SET_Conditional<TKeyLocker, TEpochGuard>(ref HashEntryInfo hei, ref SpanByte key, ref SpanByte input)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            byte* pbOutput = stackalloc byte[8];
            var output = new SpanByteAndMemory(pbOutput, 8);
            return SET_Conditional<TKeyLocker>(ref hei, ref key, ref input, ref output);
        }

        public unsafe GarnetStatus SET_Conditional<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var status = dualContext.RMW<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);
            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, ref output);

            if (status.NotFound)
            {
                incr_session_notfound();
                return GarnetStatus.NOTFOUND;
            }
            incr_session_found();
            return GarnetStatus.OK;
        }

        public unsafe GarnetStatus SET_Conditional<TKeyLocker>(ref HashEntryInfo hei, ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
        {
            var status = MainContext.RMW<TKeyLocker>(ref hei, ref key, ref input, ref output, recordMetadata: out _, userContext: default);
            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, ref output);

            if (status.NotFound)
            {
                incr_session_notfound();
                return GarnetStatus.NOTFOUND;
            }
            incr_session_found();
            return GarnetStatus.OK;
        }

        public GarnetStatus SET<TContext>(ArgSlice key, ArgSlice value, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            var _key = key.SpanByte;
            var _value = value.SpanByte;
            return SET(ref _key, ref _value, ref context);
        }

        public GarnetStatus SET<TContext>(ArgSlice key, Memory<byte> value, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            var _key = key.SpanByte;
            unsafe
            {
                fixed (byte* ptr = value.Span)
                {
                    var _value = SpanByte.FromPinnedPointer(ptr, value.Length);
                    return SET(ref _key, ref _value, ref context);
                }
            }
        }

        public unsafe GarnetStatus SETEX<TContext>(ArgSlice key, ArgSlice value, ArgSlice expiryMs, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            => SETEX(key, value, TimeSpan.FromMilliseconds(NumUtils.BytesToLong(expiryMs.Length, expiryMs.ptr)), ref context);

        public GarnetStatus SETEX<TContext>(ArgSlice key, ArgSlice value, TimeSpan expiry, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            var _key = key.SpanByte;
            var valueSB = scratchBufferManager.FormatScratch(sizeof(long), value).SpanByte;
            valueSB.ExtraMetadata = DateTimeOffset.UtcNow.Ticks + expiry.Ticks;
            return SET(ref _key, ref valueSB, ref context);
        }

        /// <summary>
        /// APPEND command - appends value at the end of existing string
        /// </summary>
        /// <typeparam name="TContext">Context type</typeparam>
        /// <param name="key">Key whose value is to be appended</param>
        /// <param name="value">Value to be appended</param>
        /// <param name="output">Length of updated value</param>
        /// <param name="context">Store context</param>
        /// <returns>Operation status</returns>
        public unsafe GarnetStatus APPEND<TContext>(ArgSlice key, ArgSlice value, ref ArgSlice output, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            var _key = key.SpanByte;
            var _value = value.SpanByte;
            var _output = new SpanByteAndMemory(output.SpanByte);

            return APPEND(ref _key, ref _value, ref _output, ref context);
        }

        /// <summary>
        /// APPEND command - appends value at the end of existing string
        /// </summary>
        /// <typeparam name="TContext">Context type</typeparam>
        /// <param name="key">Key whose value is to be appended</param>
        /// <param name="value">Value to be appended</param>
        /// <param name="output">Length of updated value</param>
        /// <param name="context">Store context</param>
        /// <returns>Operation status</returns>
        public unsafe GarnetStatus APPEND<TContext>(ref SpanByte key, ref SpanByte value, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            int inputSize = sizeof(int) + RespInputHeader.Size + sizeof(int) + sizeof(long);
            byte* pbCmdInput = stackalloc byte[inputSize];

            byte* pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            (*(RespInputHeader*)(pcurr)).cmd = RespCommand.APPEND;
            (*(RespInputHeader*)(pcurr)).flags = 0;

            pcurr += RespInputHeader.Size;
            *(int*)pcurr = value.Length;
            pcurr += sizeof(int);
            *(long*)pcurr = (long)value.ToPointer();

            var status = context.RMW(ref key, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref output);
            if (status.IsPending)
            {
                StartPendingMetrics();
                CompletePendingForSession(ref status, ref output, ref context);
                StopPendingMetrics();
            }

            Debug.Assert(output.IsSpanByte);

            return GarnetStatus.OK;
        }

        /// <summary>
        /// For existing keys - overwrites part of the value at a specified offset (in-place if possible)
        /// For non-existing keys - creates a new string with the value at a specified offset (padded with '\0's)
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="key">The key for which to set the range</param>
        /// <param name="value">The value to place at an offset</param>
        /// <param name="offset">The offset at which to place the value</param>
        /// <param name="output">The length of the updated string</param>
        /// <param name="context">Basic context for the main store</param>
        /// <returns></returns>
        public unsafe GarnetStatus SETRANGE<TContext>(ArgSlice key, ArgSlice value, int offset, ref ArgSlice output, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            var sbKey = key.SpanByte;
            SpanByteAndMemory sbmOut = new(output.SpanByte);

            // Total size + Offset size + New value size + Address of new value
            int inputSize = sizeof(int) + sizeof(int) + sizeof(int) + sizeof(long);
            byte* pbCmdInput = stackalloc byte[inputSize];

            byte* pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            (*(RespInputHeader*)(pcurr)).cmd = RespCommand.SETRANGE;
            (*(RespInputHeader*)(pcurr)).flags = 0;
            pcurr += RespInputHeader.Size;
            *(int*)(pcurr) = offset;
            pcurr += sizeof(int);
            *(int*)pcurr = value.Length;
            pcurr += sizeof(int);
            *(long*)pcurr = (long)(value.ptr);

            var status = context.RMW(ref sbKey, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref sbmOut);
            if (status.IsPending)
                CompletePendingForSession(ref status, ref sbmOut, ref context);

            Debug.Assert(sbmOut.IsSpanByte);
            output.length = sbmOut.Length;

            return GarnetStatus.OK;
        }

        public GarnetStatus Increment<TContext>(ArgSlice key, ArgSlice input, ref ArgSlice output, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            var _key = key.SpanByte;
            var _input = input.SpanByte;
            SpanByteAndMemory _output = new(output.SpanByte);

            var status = context.RMW(ref _key, ref _input, ref _output);
            if (status.IsPending)
                CompletePendingForSession(ref status, ref _output, ref context);
            Debug.Assert(_output.IsSpanByte);
            output.length = _output.Length;
            return GarnetStatus.OK;
        }

        public unsafe GarnetStatus Increment<TContext>(ArgSlice key, out long output, long increment, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            var cmd = RespCommand.INCRBY;
            if (increment < 0)
            {
                cmd = RespCommand.DECRBY;
                increment = -increment;
            }

            var incrementNumDigits = NumUtils.NumDigitsInLong(increment);
            var inputByteSize = RespInputHeader.Size + incrementNumDigits;
            var input = stackalloc byte[inputByteSize];
            ((RespInputHeader*)input)->cmd = cmd;
            ((RespInputHeader*)input)->flags = 0;
            var longOutput = input + RespInputHeader.Size;
            NumUtils.LongToBytes(increment, incrementNumDigits, ref longOutput);

            const int outputBufferLength = NumUtils.MaximumFormatInt64Length + 1;
            byte* outputBuffer = stackalloc byte[outputBufferLength];

            var _key = key.SpanByte;
            var _input = SpanByte.FromPinnedPointer(input, inputByteSize);
            var _output = new SpanByteAndMemory(outputBuffer, outputBufferLength);

            var status = context.RMW(ref _key, ref _input, ref _output);
            if (status.IsPending)
                CompletePendingForSession(ref status, ref _output, ref context);
            Debug.Assert(_output.IsSpanByte);
            Debug.Assert(_output.Length == outputBufferLength);

            output = NumUtils.BytesToLong(_output.Length, outputBuffer);
            return GarnetStatus.OK;
        }

        public void WATCH(ArgSlice key, StoreType type)
        {
            txnManager.Watch(key, type);
            txnManager.VerifyKeyOwnership(key, LockType.Shared);
        }

        public unsafe void WATCH(byte[] key, StoreType type)
        {
            fixed (byte* ptr = key)
            {
                WATCH(new ArgSlice(ptr, key.Length), type);
            }
        }

        public unsafe GarnetStatus SCAN<TContext>(long cursor, ArgSlice match, long count, ref TContext context)
        {
            return GarnetStatus.OK;
        }
    }
}