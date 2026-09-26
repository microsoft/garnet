// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using Garnet.common;

namespace Garnet.client
{
    public sealed partial class GarnetLightClient
    {
        static readonly MemoryResult<byte> RESP_OK = new(default(OK_MEM));

        /// <summary>
        /// Parse a RESP reply as a string result (or error).
        /// </summary>
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
                    if (!RespReadResponseUtils.TryReadSimpleString(out result, ref ptr, end))
                        return false;
                    break;

                case (byte)':':
                    if (!RespReadResponseUtils.TryReadIntegerAsString(out result, ref ptr, end))
                        return false;
                    break;

                case (byte)'-':
                    if (!RespReadResponseUtils.TryReadErrorAsString(out error, ref ptr, end))
                        return false;
                    break;

                case (byte)'$':
                    if (!RespReadResponseUtils.TryReadStringWithLengthHeader(out result, ref ptr, end))
                        return false;
                    break;

                case (byte)'*':
                    if (!RespReadResponseUtils.TryReadStringArrayWithLengthHeader(out var resultArray, ref ptr, end))
                        return false;
                    // Return first element of array
                    if (resultArray != null && resultArray.Length > 0) result = resultArray[0];
                    break;

                default:
                    ThrowException(new Exception("Unexpected response: " + Encoding.UTF8.GetString(new Span<byte>(ptr, (int)(end - ptr))).Replace("\n", "|").Replace("\r", "") + "]"));
                    break;
            }

            return true;
        }

        /// <summary>
        /// Parse a RESP reply as a numeric result (or error).
        /// </summary>
        unsafe bool ProcessReplyAsNumber(ref byte* ptr, byte* end, out long number, out string error)
        {
            error = null;
            number = default;

            switch (*ptr)
            {
                case (byte)':':
                    if (!RespReadUtils.TryReadInt64(out number, ref ptr, end))
                        return false;
                    break;

                case (byte)'-':
                    if (!RespReadResponseUtils.TryReadErrorAsString(out error, ref ptr, end))
                        return false;
                    break;

                case (byte)'$':
                    if (!RespReadResponseUtils.TryReadIntWithLengthHeader(out var intWithLength, ref ptr, end))
                        return false;
                    number = intWithLength;
                    break;

                default:
                    ThrowException(new Exception("Unexpected response: " + Encoding.UTF8.GetString(new Span<byte>(ptr, (int)(end - ptr))).Replace("\n", "|").Replace("\r", "") + "]"));
                    break;
            }
            return true;
        }

        /// <summary>
        /// Parse a RESP reply as a string array result (or error).
        /// </summary>
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
                        result = ["OK"];
                        break;
                    }
                    if (!RespReadResponseUtils.TryReadSimpleString(out var _result, ref ptr, end))
                        return false;
                    result = [_result];
                    break;
                case (byte)':':
                    if (!RespReadResponseUtils.TryReadIntegerAsString(out _result, ref ptr, end))
                        return false;
                    result = [_result];
                    break;

                case (byte)'-':
                    if (!RespReadResponseUtils.TryReadErrorAsString(out error, ref ptr, end))
                        return false;
                    break;

                case (byte)'$':
                    if (!RespReadResponseUtils.TryReadStringWithLengthHeader(out _result, ref ptr, end))
                        return false;
                    result = [_result];
                    break;

                case (byte)'*':
                    if (!RespReadResponseUtils.TryReadStringArrayWithLengthHeader(out result, ref ptr, end))
                        return false;
                    break;

                default:
                    ThrowException(new Exception("Unexpected response: " + Encoding.UTF8.GetString(new Span<byte>(ptr, (int)(end - ptr))).Replace("\n", "|").Replace("\r", "") + "]"));
                    break;
            }

            return true;
        }

        /// <summary>
        /// Parse a RESP reply as a <see cref="MemoryResult{Byte}"/> result (or error).
        /// </summary>
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
                    if (!RespReadResponseUtils.TryReadSimpleString(memoryPool, out result, ref ptr, end))
                        return false;
                    break;
                case (byte)':':
                    if (!RespReadResponseUtils.TryReadIntegerAsString(memoryPool, out result, ref ptr, end))
                        return false;
                    break;

                case (byte)'-':
                    if (!RespReadResponseUtils.TryReadErrorAsString(out error, ref ptr, end))
                        return false;
                    break;

                case (byte)'$':
                    if (!RespReadResponseUtils.TryReadStringWithLengthHeader(memoryPool, out result, ref ptr, end))
                        return false;
                    break;

                case (byte)'*':
                    if (!RespReadResponseUtils.TryReadStringArrayWithLengthHeader(memoryPool, out var resultArray, ref ptr, end))
                        return false;
                    // Return first element of array
                    for (var i = 1; i < resultArray.Length; i++)
                        resultArray[i].Dispose();
                    result = resultArray[0];
                    break;

                default:
                    ThrowException(new Exception("Unexpected response: " + Encoding.UTF8.GetString(new Span<byte>(ptr, (int)(end - ptr))).Replace("\n", "|").Replace("\r", "") + "]"));
                    break;
            }

            return true;
        }

        /// <summary>
        /// Parse a RESP reply as a <see cref="MemoryResult{Byte}"/> array result (or error).
        /// </summary>
        unsafe bool ProcessReplyAsMemoryByteArray(ref byte* ptr, byte* end, out MemoryResult<byte>[] result, out string error)
        {
            result = default;
            error = null;
            switch (*ptr)
            {
                case (byte)'*':
                    if (!RespReadResponseUtils.TryReadStringArrayWithLengthHeader(memoryPool, out var resultArray, ref ptr, end))
                        return false;
                    result = resultArray;
                    break;
                default:
                    ThrowException(new Exception("Unexpected response: " + Encoding.UTF8.GetString(new Span<byte>(ptr, (int)(end - ptr))).Replace("\n", "|").Replace("\r", "") + "]"));
                    break;
            }
            return true;
        }

        /// <summary>
        /// Process a batch of received bytes, matching each RESP reply to its outstanding completion in
        /// monotonic ticket order. Completions live in the ring's reply-gated completion lane; the flusher
        /// registers each one in address order immediately before sending its request, so replies arrive in
        /// exactly the ticket order this reader consumes them. Returns the number of fully consumed bytes;
        /// a partial trailing reply leaves <c>readHead</c> short so the caller retains the remainder.
        /// </summary>
        unsafe int ProcessReplies(byte* recvBufferPtr, int bytesRead)
        {
            var readHead = 0;
            var ptr = recvBufferPtr;
            var end = recvBufferPtr + bytesRead;

            while (readHead < bytesRead)
            {
                // A reply arrived while the completion lane has already caught up to the tail, so no
                // response-expecting request is outstanding. This means the server produced a reply for a
                // fire-and-forget command, which violates the no-response invariant of out-of-line execution.
                if (tcsOffset == networkWriter.CompletionTail)
                    ThrowException(new InvalidOperationException("Received a reply while no response was outstanding; fire-and-forget out-of-line commands must not produce a response."));

                // The completion for this reply may not be published yet: the flusher registers it in address
                // order just before sending the request, so a reply can momentarily precede its visibility.
                if (!networkWriter.TryReadCompletion(tcsOffset, out var tcs))
                {
                    Thread.Yield();
                    continue;
                }

                switch (tcs.taskType)
                {
                    case TaskType.None:
                        Thread.Yield();
                        continue;

                    case TaskType.StringCallback:
                        if (!ProcessReplyAsString(ref ptr, end, out var resultString, out var error))
                            return readHead;
                        ConsumeTcsOffset();
                        if (error != null) tcs.stringCallback?.Invoke(tcs.context, error);
                        else tcs.stringCallback?.Invoke(tcs.context, resultString);
                        break;

                    case TaskType.MemoryByteCallback:
                        if (!ProcessReplyAsMemoryByte(ref ptr, end, out var resultMemory, out error))
                            return readHead;
                        ConsumeTcsOffset();
                        if (error != null) tcs.memoryByteCallback?.Invoke(tcs.context, CopyErrorToSpan(error));
                        else tcs.memoryByteCallback?.Invoke(tcs.context, resultMemory);
                        break;

                    case TaskType.MemoryByteArrayCallback:
                        if (!ProcessReplyAsMemoryByteArray(ref ptr, end, out var resultMemoryByteArray, out error))
                            return readHead;
                        ConsumeTcsOffset();
                        if (error != null) tcs.memoryByteArrayCallback?.Invoke(tcs.context, resultMemoryByteArray, CopyErrorToSpan(error));
                        else tcs.memoryByteArrayCallback?.Invoke(tcs.context, resultMemoryByteArray, default);
                        break;

                    case TaskType.StringArrayCallback:
                        if (!ProcessReplyAsStringArray(ref ptr, end, out var resultStringArray, out error))
                            return readHead;
                        ConsumeTcsOffset();
                        if (error != null) tcs.stringArrayCallback?.Invoke(tcs.context, resultStringArray, error);
                        else tcs.stringArrayCallback?.Invoke(tcs.context, resultStringArray, default);
                        break;

                    case TaskType.StringAsync:
                        if (!ProcessReplyAsString(ref ptr, end, out resultString, out error))
                            return readHead;
                        ConsumeTcsOffset();
                        if (error != null) tcs.stringTcs?.TrySetException(new Exception(error));
                        else tcs.stringTcs?.TrySetResult(resultString);
                        break;

                    case TaskType.MemoryByteAsync:
                        if (!ProcessReplyAsMemoryByte(ref ptr, end, out resultMemory, out error))
                            return readHead;
                        ConsumeTcsOffset();
                        if (error != null) tcs.memoryByteTcs?.TrySetException(new Exception(error));
                        else tcs.memoryByteTcs?.TrySetResult(resultMemory);
                        break;

                    case TaskType.StringArrayAsync:
                        if (!ProcessReplyAsStringArray(ref ptr, end, out var resultArray, out error))
                            return readHead;
                        ConsumeTcsOffset();
                        if (error != null) tcs.stringArrayTcs?.TrySetException(new Exception(error));
                        else tcs.stringArrayTcs?.TrySetResult(resultArray);
                        break;

                    case TaskType.MemoryByteArrayAsync:
                        if (!ProcessReplyAsMemoryByteArray(ref ptr, end, out var resultByteArray, out error))
                            return readHead;
                        ConsumeTcsOffset();
                        if (error != null) tcs.memoryByteArrayTcs?.TrySetException(new Exception(error));
                        else tcs.memoryByteArrayTcs?.TrySetResult(resultByteArray);
                        break;

                    case TaskType.LongAsync:
                        if (!ProcessReplyAsNumber(ref ptr, end, out var resultLong, out error))
                            return readHead;
                        ConsumeTcsOffset();
                        if (error != null) tcs.longTcs?.TrySetException(new Exception(error));
                        else tcs.longTcs?.TrySetResult(resultLong);
                        break;

                    case TaskType.LongCallback:
                        if (!ProcessReplyAsNumber(ref ptr, end, out var resultLongCallback, out error))
                            return readHead;
                        ConsumeTcsOffset();
                        if (error != null) tcs.longCallback?.Invoke(tcs.context, resultLongCallback, error);
                        else tcs.longCallback?.Invoke(tcs.context, resultLongCallback, default);
                        break;
                }

                readHead = (int)(ptr - recvBufferPtr);
            }

            return readHead;
        }

        /// <summary>
        /// Copy a RESP error string into a pooled <see cref="MemoryResult{Byte}"/> span for callback delivery.
        /// </summary>
        MemoryResult<byte> CopyErrorToSpan(string error)
        {
            var errorByteArray = Encoding.ASCII.GetBytes(error);
            var memResultError = MemoryResult<byte>.Create(memoryPool, errorByteArray.Length);
            new ReadOnlySpan<byte>(errorByteArray).CopyTo(memResultError.Span);

            return memResultError;
        }
    }
}
