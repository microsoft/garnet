// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus GET<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var status = dualContext.Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, userContext: default);

            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, out output);

            if (status.Found)
            {
                incr_session_found();
                return GarnetStatus.OK;
            }
            incr_session_notfound();
            return GarnetStatus.NOTFOUND;
        }

        public GarnetStatus GET<TKeyLocker>(ref HashEntryInfo hei, ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
        {
            ReadOptions readOptions = default;
            var status = MainContext.Read<TKeyLocker>(ref hei, ref key, ref input, ref output, ref readOptions, recordMetadata: out _, userContext: default);
            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, out output);

            if (status.Found)
            {
                incr_session_found();
                return GarnetStatus.OK;
            }
            incr_session_notfound();
            return GarnetStatus.NOTFOUND;
        }


        public unsafe GarnetStatus ReadWithUnsafeContext<TKeyLocker, TEpochGuard>(ArgSlice key, ref SpanByte input, ref SpanByteAndMemory output, long localHeadAddress, out bool epochChanged)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var _key = key.SpanByte;
            epochChanged = false;
            var status = dualContext.Read<TKeyLocker, TEpochGuard>(ref _key, ref Unsafe.AsRef(in input), ref output);

            if (status.IsPending)
            {
                CompletePending<TKeyLocker>(out status, out output);

                // Start read of pointers from beginning if epoch changed
                if (HeadAddress == localHeadAddress)
                {
                    dualContext.KernelSession.EndUnsafe();
                    epochChanged = true;
                }
            }

            if (status.NotFound)
            {
                incr_session_notfound();
                return GarnetStatus.NOTFOUND;
            }
            incr_session_found();
            return GarnetStatus.OK;
        }

        public unsafe GarnetStatus GET<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice value)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var inputSize = sizeof(int) + RespInputHeader.Size;

            var pbCmdInput = stackalloc byte[inputSize];
            var pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            (*(RespInputHeader*)pcurr).cmd = RespCommand.GET; // Proxy for "get value without RESP header"
            (*(RespInputHeader*)pcurr).flags = 0;

            var _key = key.SpanByte;
            var _output = new SpanByteAndMemory { SpanByte = scratchBufferManager.ViewRemainingArgSlice().SpanByte };
            var ret = GET<TKeyLocker, TEpochGuard>(ref _key, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref _output);
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

        public unsafe GarnetStatus GET<TKeyLocker, TEpochGuard>(ArgSlice key, out MemoryResult<byte> value)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var inputSize = sizeof(int) + RespInputHeader.Size;

            var pbCmdInput = stackalloc byte[inputSize];
            var pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            (*(RespInputHeader*)pcurr).cmd = RespCommand.GET; // Proxy for "get value without RESP header"
            (*(RespInputHeader*)pcurr).flags = 0;

            var _key = key.SpanByte;
            var _output = new SpanByteAndMemory();
            var ret = GET<TKeyLocker, TEpochGuard>(ref _key, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref _output);
            value = new MemoryResult<byte>(_output.Memory, _output.Length);
            return ret;
        }

        /// <summary>
        /// GETDEL command - Gets the value corresponding to the given key and deletes the key.
        /// </summary>
        /// <param name="key">The key to get the value for.</param>
        /// <param name="output">Span to allocate the output of the operation</param>
        /// <returns> Operation status </returns>
        public GarnetStatus GETDEL<TKeyLocker, TEpochGuard>(ArgSlice key, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var _key = key.SpanByte;
            return GETDEL<TKeyLocker, TEpochGuard>(ref _key, ref output);
        }

        /// <summary>
        /// GETDEL command - Gets the value corresponding to the given key and deletes the key.
        /// </summary>
        /// <param name="key">The key to get the value for.</param>
        /// <param name="output">Span to allocate the output of the operation</param>
        /// <returns> Operation status </returns>
        public unsafe GarnetStatus GETDEL<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            // size data + header
            var inputSize = sizeof(int) + RespInputHeader.Size;
            var pbCmdInput = stackalloc byte[inputSize];

            var pcurr = pbCmdInput;
            *(int*)pbCmdInput = inputSize - sizeof(int);
            pcurr += sizeof(int);
            ((RespInputHeader*)pcurr)->cmd = RespCommand.GETDEL;
            ((RespInputHeader*)pcurr)->flags = 0;

            ref var input = ref SpanByte.Reinterpret(pbCmdInput);

            var status = dualContext.RMW<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);
            Debug.Assert(output.IsSpanByte);

            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, out output);

            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public unsafe GarnetStatus GETRANGE<TKeyLocker, TEpochGuard>(ref SpanByte key, int sliceStart, int sliceLength, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var inputSize = sizeof(int) + RespInputHeader.Size + (sizeof(int) * 2);
            var pbCmdInput = stackalloc byte[inputSize];

            var pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            (*(RespInputHeader*)pcurr).cmd = RespCommand.GETRANGE;
            (*(RespInputHeader*)pcurr).flags = 0;
            pcurr += RespInputHeader.Size;
            *(int*)pcurr = sliceStart;
            *(int*)(pcurr + 4) = sliceLength;

            var status = dualContext.Read<TKeyLocker, TEpochGuard>(ref key, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref output);

            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, out output);

            if (status.Found)
            {
                incr_session_found();
                return GarnetStatus.OK;
            }
            incr_session_notfound();
            return GarnetStatus.NOTFOUND;
        }

        public GarnetStatus SET<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte value)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            _ = dualContext.Upsert<TKeyLocker, TEpochGuard>(ref key, ref value);
            return GarnetStatus.OK;
        }

        public GarnetStatus SET<TKeyLocker>(ref HashEntryInfo hei, ref SpanByte key, ref SpanByte value)
            where TKeyLocker : struct, ISessionLocker
        {
            SpanByte input = default;
            SpanByteAndMemory output = default;
            _ = MainContext.Upsert<TKeyLocker>(ref hei, ref key, ref input, ref value, ref output, recordMetadata: out _);
            return GarnetStatus.OK;
        }

        public unsafe GarnetStatus SET_Conditional<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var pbOutput = stackalloc byte[8];
            var output = new SpanByteAndMemory(pbOutput, 8);
            return SET_Conditional<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);
        }

        public unsafe GarnetStatus SET_Conditional<TKeyLocker>(ref HashEntryInfo hei, ref SpanByte key, ref SpanByte input)
            where TKeyLocker : struct, ISessionLocker
        {
            var pbOutput = stackalloc byte[8];
            var output = new SpanByteAndMemory(pbOutput, 8);
            return SET_Conditional<TKeyLocker>(ref hei, ref key, ref input, ref output);
        }

        public unsafe GarnetStatus SET_Conditional<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var status = dualContext.RMW<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);
            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, out output);

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
                CompletePending<TKeyLocker>(out status, out output);

            if (status.NotFound)
            {
                incr_session_notfound();
                return GarnetStatus.NOTFOUND;
            }
            incr_session_found();
            return GarnetStatus.OK;
        }

        public GarnetStatus SET<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice value)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var _key = key.SpanByte;
            var _value = value.SpanByte;
            return SET<TKeyLocker, TEpochGuard>(ref _key, ref _value);
        }

        public GarnetStatus SET<TKeyLocker, TEpochGuard>(ArgSlice key, Memory<byte> value)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var _key = key.SpanByte;
            unsafe
            {
                fixed (byte* ptr = value.Span)
                {
                    var _value = SpanByte.FromPinnedPointer(ptr, value.Length);
                    return SET<TKeyLocker, TEpochGuard>(ref _key, ref _value);
                }
            }
        }

        public unsafe GarnetStatus SETEX<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice value, ArgSlice expiryMs)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => SETEX<TKeyLocker, TEpochGuard>(key, value, TimeSpan.FromMilliseconds(NumUtils.BytesToLong(expiryMs.Length, expiryMs.ptr)));

        public GarnetStatus SETEX<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice value, TimeSpan expiry)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var _key = key.SpanByte;
            var valueSB = scratchBufferManager.FormatScratch(sizeof(long), value).SpanByte;
            valueSB.ExtraMetadata = DateTimeOffset.UtcNow.Ticks + expiry.Ticks;
            return SET<TKeyLocker, TEpochGuard>(ref _key, ref valueSB);
        }

        public GarnetStatus SETEX<TKeyLocker>(ref HashEntryInfo hei, ArgSlice key, ArgSlice value, TimeSpan expiry)
            where TKeyLocker : struct, ISessionLocker
        {
            var _key = key.SpanByte;
            var valueSB = scratchBufferManager.FormatScratch(sizeof(long), value).SpanByte;
            valueSB.ExtraMetadata = DateTimeOffset.UtcNow.Ticks + expiry.Ticks;
            return SET<TKeyLocker>(ref hei, ref _key, ref valueSB);
        }

        /// <summary>
        /// APPEND command - appends value at the end of existing string
        /// </summary>
        /// <param name="key">Key whose value is to be appended</param>
        /// <param name="value">Value to be appended</param>
        /// <param name="output">Length of updated value</param>
        /// <returns>Operation status</returns>
        public unsafe GarnetStatus APPEND<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice value, ref ArgSlice output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var _key = key.SpanByte;
            var _value = value.SpanByte;
            var _output = new SpanByteAndMemory(output.SpanByte);

            return APPEND<TKeyLocker, TEpochGuard>(ref _key, ref _value, ref _output);
        }

        /// <summary>
        /// APPEND command - appends value at the end of existing string
        /// </summary>
        /// <param name="key">Key whose value is to be appended</param>
        /// <param name="value">Value to be appended</param>
        /// <param name="output">Length of updated value</param>
        /// <returns>Operation status</returns>
        public unsafe GarnetStatus APPEND<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte value, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var inputSize = sizeof(int) + RespInputHeader.Size + sizeof(int) + sizeof(long);
            var pbCmdInput = stackalloc byte[inputSize];

            var pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            (*(RespInputHeader*)pcurr).cmd = RespCommand.APPEND;
            (*(RespInputHeader*)pcurr).flags = 0;

            pcurr += RespInputHeader.Size;
            *(int*)pcurr = value.Length;
            pcurr += sizeof(int);
            *(long*)pcurr = (long)value.ToPointer();

            var status = dualContext.RMW<TKeyLocker, TEpochGuard>(ref key, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref output);
            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, out output);

            Debug.Assert(output.IsSpanByte);
            return GarnetStatus.OK;
        }

        /// <summary>
        /// For existing keys - overwrites part of the value at a specified offset (in-place if possible)
        /// For non-existing keys - creates a new string with the value at a specified offset (padded with '\0's)
        /// </summary>
        /// <param name="key">The key for which to set the range</param>
        /// <param name="value">The value to place at an offset</param>
        /// <param name="offset">The offset at which to place the value</param>
        /// <param name="output">The length of the updated string</param>
        /// <returns></returns>
        public unsafe GarnetStatus SETRANGE<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice value, int offset, ref ArgSlice output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var sbKey = key.SpanByte;
            SpanByteAndMemory sbmOut = new(output.SpanByte);

            // Total size + Offset size + New value size + Address of new value
            var inputSize = sizeof(int) + sizeof(int) + sizeof(int) + sizeof(long);
            var pbCmdInput = stackalloc byte[inputSize];

            var pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            (*(RespInputHeader*)pcurr).cmd = RespCommand.SETRANGE;
            (*(RespInputHeader*)pcurr).flags = 0;
            pcurr += RespInputHeader.Size;
            *(int*)pcurr = offset;
            pcurr += sizeof(int);
            *(int*)pcurr = value.Length;
            pcurr += sizeof(int);
            *(long*)pcurr = (long)value.ptr;

            var status = dualContext.RMW<TKeyLocker, TEpochGuard>(ref sbKey, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref sbmOut);
            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, out sbmOut);

            Debug.Assert(sbmOut.IsSpanByte);
            output.length = sbmOut.Length;
            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public GarnetStatus Increment<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice input, ref ArgSlice output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var _key = key.SpanByte;
            var _input = input.SpanByte;
            SpanByteAndMemory _output = new(output.SpanByte);

            var status = dualContext.RMW<TKeyLocker, TEpochGuard>(ref _key, ref _input, ref _output);
            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, out _output);
            Debug.Assert(_output.IsSpanByte);
            output.length = _output.Length;
            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public unsafe GarnetStatus Increment<TKeyLocker, TEpochGuard>(ArgSlice key, out long output, long increment)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
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
            var outputBuffer = stackalloc byte[outputBufferLength];

            var _key = key.SpanByte;
            var _input = SpanByte.FromPinnedPointer(input, inputByteSize);
            var _output = new SpanByteAndMemory(outputBuffer, outputBufferLength);

            var status = dualContext.RMW<TKeyLocker, TEpochGuard>(ref _key, ref _input, ref _output);
            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, out _output);
            Debug.Assert(_output.IsSpanByte);
            Debug.Assert(_output.Length == outputBufferLength);

            output = NumUtils.BytesToLong(_output.Length, outputBuffer);
            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public void WATCH<TKeyLocker, TEpochGuard>(ArgSlice key, StoreType type)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            txnManager.Watch<TKeyLocker, TEpochGuard>(key, type);
            txnManager.VerifyKeyOwnership(key, LockType.Shared);
        }

        public unsafe void WATCH<TKeyLocker, TEpochGuard>(byte[] key, StoreType type)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            fixed (byte* ptr = key)
            {
                WATCH<TKeyLocker, TEpochGuard>(new ArgSlice(ptr, key.Length), type);
            }
        }
    }
}