// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using Garnet.common;
using Garnet.networking;

namespace Garnet.client
{
    public sealed partial class GarnetClient : IServerHook, IMessageConsumer, IDisposable
    {
        /// <inheritdoc />
        public unsafe int TryConsumeMessages(byte* reqBuffer, int bytesRead)
            => ProcessReplies(reqBuffer, bytesRead);

        unsafe bool ProcessReplyAsString(ref byte* ptr, byte* end, out string result, out string error)
        {
            result = null;
            error = null;

            switch (*ptr)
            {
                case (byte)'+':
                    // Handle common case of "+OK\r\n"
                    if ((ptr + 5 <= end) && (*(int*)(ptr + 1) == 168643407))
                    {
                        ptr += 5;
                        result = "OK";
                        break;
                    }
                    if (!RespReadUtils.ReadSimpleString(out result, ref ptr, end))
                        return false;
                    break;

                case (byte)':':
                    if (!RespReadUtils.ReadIntegerAsString(out result, ref ptr, end))
                        return false;
                    break;

                case (byte)'-':
                    if (!RespReadUtils.ReadErrorAsString(out error, ref ptr, end))
                        return false;
                    break;

                case (byte)'$':
                    if (!RespReadUtils.ReadStringWithLengthHeader(out result, ref ptr, end))
                        return false;
                    break;

                case (byte)'*':
                    if (!RespReadUtils.ReadStringArrayWithLengthHeader(out var resultArray, ref ptr, end))
                        return false;
                    // Return first element of array
                    result = resultArray[0];
                    break;

                default:
                    ThrowException(new Exception("Unexpected response: " + Encoding.UTF8.GetString(new Span<byte>(ptr, (int)(end - ptr))).Replace("\n", "|").Replace("\r", "") + "]"));
                    break;
            }

            return true;
        }

        unsafe bool ProcessReplyAsNumber(ref byte* ptr, byte* end, out long number, out string error)
        {
            error = null;
            number = default;

            switch (*ptr)
            {
                case (byte)':':
                    if (!RespReadUtils.Read64Int(out number, ref ptr, end))
                        return false;
                    break;

                case (byte)'-':
                    if (!RespReadUtils.ReadErrorAsString(out error, ref ptr, end))
                        return false;
                    break;

                case (byte)'$':
                    if (!RespReadUtils.ReadIntWithLengthHeader(out var intWithLength, ref ptr, end))
                        return false;
                    number = intWithLength;
                    break;

                default:
                    ThrowException(new Exception("Unexpected response: " + Encoding.UTF8.GetString(new Span<byte>(ptr, (int)(end - ptr))).Replace("\n", "|").Replace("\r", "") + "]"));
                    break;
            }
            return true;
        }


        unsafe bool ProcessReplyAsStringArray(ref byte* ptr, byte* end, out string[] result, out string error)
        {
            result = null;
            error = null;

            switch (*ptr)
            {
                case (byte)'+':
                    // Handle common case of "+OK\r\n"
                    if ((ptr + 5 <= end) && (*(int*)(ptr + 1) == 168643407))
                    {
                        ptr += 5;
                        result = new[] { "OK" };
                        break;
                    }
                    if (!RespReadUtils.ReadSimpleString(out var _result, ref ptr, end))
                        return false;
                    result = new[] { _result };
                    break;
                case (byte)':':
                    if (!RespReadUtils.ReadIntegerAsString(out _result, ref ptr, end))
                        return false;
                    result = new[] { _result };
                    break;

                case (byte)'-':
                    if (!RespReadUtils.ReadErrorAsString(out error, ref ptr, end))
                        return false;
                    break;

                case (byte)'$':
                    if (!RespReadUtils.ReadStringWithLengthHeader(out _result, ref ptr, end))
                        return false;
                    result = new[] { _result };
                    break;

                case (byte)'*':
                    if (!RespReadUtils.ReadStringArrayWithLengthHeader(out result, ref ptr, end))
                        return false;
                    break;

                default:
                    ThrowException(new Exception("Unexpected response: " + Encoding.UTF8.GetString(new Span<byte>(ptr, (int)(end - ptr))).Replace("\n", "|").Replace("\r", "") + "]"));
                    break;
            }

            return true;
        }

        unsafe bool ProcessReplyAsMemoryByte(ref byte* ptr, byte* end, out MemoryResult<byte> result, out string error)
        {
            result = default;
            error = null;

            switch (*ptr)
            {
                case (byte)'+':
                    // Handle common case of "+OK\r\n"
                    if ((ptr + 5 <= end) && (*(int*)(ptr + 1) == 168643407))
                    {
                        ptr += 5;
                        result = RESP_OK;
                        break;
                    }
                    if (!RespReadUtils.ReadSimpleString(memoryPool, out result, ref ptr, end))
                        return false;
                    break;
                case (byte)':':
                    if (!RespReadUtils.ReadIntegerAsString(memoryPool, out result, ref ptr, end))
                        return false;
                    break;

                case (byte)'-':
                    if (!RespReadUtils.ReadErrorAsString(out error, ref ptr, end))
                        return false;
                    break;

                case (byte)'$':
                    if (!RespReadUtils.ReadStringWithLengthHeader(memoryPool, out result, ref ptr, end))
                        return false;
                    break;

                case (byte)'*':
                    if (!RespReadUtils.ReadStringArrayWithLengthHeader(memoryPool, out var resultArray, ref ptr, end))
                        return false;
                    // Return first element of array
                    for (int i = 1; i < resultArray.Length; i++)
                        resultArray[i].Dispose();
                    result = resultArray[0];
                    break;

                default:
                    ThrowException(new Exception("Unexpected response: " + Encoding.UTF8.GetString(new Span<byte>(ptr, (int)(end - ptr))).Replace("\n", "|").Replace("\r", "") + "]"));
                    break;
            }

            return true;
        }

        unsafe bool ProcessReplyAsMemoryByteArray(ref byte* ptr, byte* end, out MemoryResult<byte>[] result, out string error)
        {
            result = default;
            error = null;
            switch (*ptr)
            {
                case (byte)'*':
                    if (!RespReadUtils.ReadStringArrayWithLengthHeader(memoryPool, out var resultArray, ref ptr, end))
                        return false;
                    result = resultArray;
                    break;
                default:
                    ThrowException(new Exception("Unexpected response: " + Encoding.UTF8.GetString(new Span<byte>(ptr, (int)(end - ptr))).Replace("\n", "|").Replace("\r", "") + "]"));
                    break;
            }
            return true;
        }

        unsafe int ProcessReplies(byte* recvBufferPtr, int bytesRead)
        {
            // Debug.WriteLine("RECV: [" + Encoding.UTF8.GetString(new Span<byte>(recvBufferPtr, bytesRead)).Replace("\n", "|").Replace("\r", "") + "]");

            int readHead = 0;
            byte* ptr = recvBufferPtr;
            byte* end = recvBufferPtr + bytesRead;

            while (readHead < bytesRead)
            {
                var shortTaskId = tcsOffset & (maxOutstandingTasks - 1);
                var tcs = tcsArray[shortTaskId];

                // Console.WriteLine($"Processing {tcs.taskType} at offset {tcsOffset}");
                if (tcs.taskType == TaskType.None)
                {
                    Thread.Yield();
                    continue;
                }
                switch (tcs.taskType)
                {
                    case TaskType.None:
                        return readHead;

                    case TaskType.StringCallback:
                        if (!ProcessReplyAsString(ref ptr, end, out var resultString, out var error))
                            return readHead;
                        ConsumeTcsOffset(shortTaskId);
                        if (error != null) tcs.stringCallback?.Invoke(tcs.context, error);
                        else tcs.stringCallback?.Invoke(tcs.context, resultString);
                        break;

                    case TaskType.MemoryByteCallback:
                        if (!ProcessReplyAsMemoryByte(ref ptr, end, out var resultMemory, out error))
                            return readHead;
                        ConsumeTcsOffset(shortTaskId);
                        if (error != null)
                            tcs.memoryByteCallback?.Invoke(tcs.context, CopyErrorToSpan(error));
                        else tcs.memoryByteCallback?.Invoke(tcs.context, resultMemory);
                        break;

                    case TaskType.MemoryByteArrayCallback:
                        if (!ProcessReplyAsMemoryByteArray(ref ptr, end, out var resultMemoryByteArray, out error))
                            return readHead;
                        ConsumeTcsOffset(shortTaskId);
                        Debug.Assert(tcs.memoryByteArrayCallback != null);
                        if (error != null) tcs.memoryByteArrayCallback?.Invoke(tcs.context, resultMemoryByteArray, CopyErrorToSpan(error));
                        else tcs.memoryByteArrayCallback?.Invoke(tcs.context, resultMemoryByteArray, default);
                        break;

                    case TaskType.StringArrayCallback:
                        if (!ProcessReplyAsStringArray(ref ptr, end, out var resultStringArray, out error))
                            return readHead;
                        ConsumeTcsOffset(shortTaskId);
                        Debug.Assert(tcs.stringArrayCallback != null);
                        if (error != null) tcs.stringArrayCallback?.Invoke(tcs.context, resultStringArray, error);
                        else tcs.stringArrayCallback?.Invoke(tcs.context, resultStringArray, default);
                        break;

                    case TaskType.StringAsync:
                        if (!ProcessReplyAsString(ref ptr, end, out resultString, out error))
                            return readHead;
                        ConsumeTcsOffset(shortTaskId);
                        Debug.Assert(tcs.stringTcs != null);
                        if (error != null) tcs.stringTcs?.TrySetException(new Exception(error));
                        else tcs.stringTcs?.TrySetResult(resultString);
                        break;

                    case TaskType.MemoryByteAsync:
                        if (!ProcessReplyAsMemoryByte(ref ptr, end, out resultMemory, out error))
                            return readHead;
                        ConsumeTcsOffset(shortTaskId);
                        Debug.Assert(tcs.memoryByteTcs != null);
                        if (error != null) tcs.memoryByteTcs?.TrySetException(new Exception(error));
                        else tcs.memoryByteTcs?.TrySetResult(resultMemory);
                        break;

                    case TaskType.StringArrayAsync:
                        if (!ProcessReplyAsStringArray(ref ptr, end, out var resultArray, out error))
                            return readHead;
                        ConsumeTcsOffset(shortTaskId);
                        Debug.Assert(tcs.stringArrayTcs != null);
                        if (error != null) tcs.stringArrayTcs?.TrySetException(new Exception(error));
                        else tcs.stringArrayTcs?.TrySetResult(resultArray);
                        break;

                    case TaskType.MemoryByteArrayAsync:
                        if (!ProcessReplyAsMemoryByteArray(ref ptr, end, out var resultByteArray, out error))
                            return readHead;
                        ConsumeTcsOffset(shortTaskId);
                        Debug.Assert(tcs.memoryByteArrayTcs != null);
                        if (error != null) tcs.memoryByteArrayTcs?.TrySetException(new Exception(error));
                        else tcs.memoryByteArrayTcs?.TrySetResult(resultByteArray);
                        break;

                    case TaskType.LongAsync:
                        if (!ProcessReplyAsNumber(ref ptr, end, out var resultLong, out error))
                            return readHead;
                        ConsumeTcsOffset(shortTaskId);
                        Debug.Assert(tcs.longTcs != null);
                        if (error != null) tcs.longTcs?.TrySetException(new Exception(error));
                        else tcs.longTcs?.TrySetResult(resultLong);
                        break;

                    case TaskType.LongCallback:
                        if (!ProcessReplyAsNumber(ref ptr, end, out var resultLongCallback, out error))
                            return readHead;
                        ConsumeTcsOffset(shortTaskId);
                        Debug.Assert(tcs.longCallback != null);

                        if (error != null) tcs.longCallback?.Invoke(tcs.context, resultLongCallback, error);
                        else tcs.longCallback?.Invoke(tcs.context, resultLongCallback, default);
                        break;

                }
                latency?.RecordValue(Stopwatch.GetTimestamp() - tcs.timestamp);
                readHead = (int)(ptr - recvBufferPtr);
            }

            return readHead;
        }

        MemoryResult<byte> CopyErrorToSpan(string error)
        {
            var errorByteArray = Encoding.ASCII.GetBytes(error);
            var memResultError = MemoryResult<byte>.Create(memoryPool, errorByteArray.Length);
            new ReadOnlySpan<byte>(errorByteArray).CopyTo(memResultError.Span);

            return memResultError;
        }
    }
}