// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Text;
using Garnet.common;

namespace Garnet.client
{
    struct OK_MEM : IMemoryOwner<byte>
    {
        static readonly Memory<byte> RESP_OK = "OK"u8.ToArray();
        public Memory<byte> Memory => RESP_OK;
        public void Dispose() { }
    }

    public sealed partial class GarnetClient
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
                    // Return first element of array; a null or empty array yields no result
                    if (resultArray is { Length: > 0 })
                    {
                        for (var i = 1; i < resultArray.Length; i++)
                            resultArray[i].Dispose();
                        result = resultArray[0];
                    }
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
        /// monotonic ticket order. Completions live in the ring's reply-gated completion lane; each producer
        /// publishes its completion before its request becomes flushable, so replies arrive in exactly the ticket
        /// order this reader consumes them. Returns the number of fully consumed bytes;
        /// a partial trailing reply leaves <c>readHead</c> short so the caller retains the remainder.
        /// </summary>
        unsafe int ProcessReplies(byte* recvBufferPtr, int bytesRead)
        {
            // Completion slots consumed by this batch are released once at the end, rather than per reply, so
            // the shared reservation counter and its waiter signalling are touched once per receive.
            var writer = networkWriter;
            var consumed = 0;
            try
            {
                return ProcessRepliesCore(recvBufferPtr, bytesRead, ref consumed);
            }
            finally
            {
                if (consumed > 0)
                    writer.AdvanceReplied(consumed);
            }
        }

        unsafe int ProcessRepliesCore(byte* recvBufferPtr, int bytesRead, ref int consumed)
        {
            var readHead = 0;
            var ptr = recvBufferPtr;
            var end = recvBufferPtr + bytesRead;

            while (readHead < bytesRead)
            {
                // A producer publishes its completion before the request becomes flushable, so the completion for
                // every reply is already published. An unpublished completion means no response-expecting request
                // is outstanding: the server replied to a fire-and-forget command, violating the no-response
                // invariant of out-of-line execution.
                if (!networkWriter.TryReadCompletion(tcsOffset, out var tcs))
                    ThrowException(new InvalidOperationException("Received a reply while no response was outstanding; fire-and-forget out-of-line commands must not produce a response."));

                switch (tcs.taskType)
                {
                    case TaskType.StringCallback:
                        if (!ProcessReplyAsString(ref ptr, end, out var resultString, out var error))
                            return readHead;
                        ConsumeTcsOffset(ref consumed, tcs.timestamp);
                        if (error != null) tcs.stringCallback?.Invoke(tcs.context, error);
                        else tcs.stringCallback?.Invoke(tcs.context, resultString);
                        break;

                    case TaskType.MemoryByteCallback:
                        if (!ProcessReplyAsMemoryByte(ref ptr, end, out var resultMemory, out error))
                            return readHead;
                        ConsumeTcsOffset(ref consumed, tcs.timestamp);
                        if (tcs.memoryByteCallback == null) resultMemory.Dispose();
                        else if (error != null) tcs.memoryByteCallback.Invoke(tcs.context, CopyErrorToSpan(error));
                        else tcs.memoryByteCallback?.Invoke(tcs.context, resultMemory);
                        break;

                    case TaskType.MemoryByteArrayCallback:
                        if (!ProcessReplyAsMemoryByteArray(ref ptr, end, out var resultMemoryByteArray, out error))
                            return readHead;
                        ConsumeTcsOffset(ref consumed, tcs.timestamp);
                        if (tcs.memoryByteArrayCallback == null) DisposeMemoryResults(resultMemoryByteArray);
                        else if (error != null) tcs.memoryByteArrayCallback.Invoke(tcs.context, resultMemoryByteArray, CopyErrorToSpan(error));
                        else tcs.memoryByteArrayCallback?.Invoke(tcs.context, resultMemoryByteArray, default);
                        break;

                    case TaskType.StringArrayCallback:
                        if (!ProcessReplyAsStringArray(ref ptr, end, out var resultStringArray, out error))
                            return readHead;
                        ConsumeTcsOffset(ref consumed, tcs.timestamp);
                        if (error != null) tcs.stringArrayCallback?.Invoke(tcs.context, resultStringArray, error);
                        else tcs.stringArrayCallback?.Invoke(tcs.context, resultStringArray, default);
                        break;

                    case TaskType.StringAsync:
                        if (!ProcessReplyAsString(ref ptr, end, out resultString, out error))
                            return readHead;
                        ConsumeTcsOffset(ref consumed, tcs.timestamp);
                        if (error != null) tcs.stringTcs?.TrySetException(new Exception(error));
                        else tcs.stringTcs?.TrySetResult(resultString);
                        break;

                    case TaskType.MemoryByteAsync:
                        if (!ProcessReplyAsMemoryByte(ref ptr, end, out resultMemory, out error))
                            return readHead;
                        ConsumeTcsOffset(ref consumed, tcs.timestamp);
                        if (error != null) tcs.memoryByteTcs?.TrySetException(new Exception(error));
                        else if (tcs.memoryByteTcs?.TrySetResult(resultMemory) != true) resultMemory.Dispose();
                        break;

                    case TaskType.StringArrayAsync:
                        if (!ProcessReplyAsStringArray(ref ptr, end, out var resultArray, out error))
                            return readHead;
                        ConsumeTcsOffset(ref consumed, tcs.timestamp);
                        if (error != null) tcs.stringArrayTcs?.TrySetException(new Exception(error));
                        else tcs.stringArrayTcs?.TrySetResult(resultArray);
                        break;

                    case TaskType.MemoryByteArrayAsync:
                        if (!ProcessReplyAsMemoryByteArray(ref ptr, end, out var resultByteArray, out error))
                            return readHead;
                        ConsumeTcsOffset(ref consumed, tcs.timestamp);
                        if (error != null) tcs.memoryByteArrayTcs?.TrySetException(new Exception(error));
                        else if (tcs.memoryByteArrayTcs?.TrySetResult(resultByteArray) != true) DisposeMemoryResults(resultByteArray);
                        break;

                    case TaskType.LongAsync:
                        if (!ProcessReplyAsNumber(ref ptr, end, out var resultLong, out error))
                            return readHead;
                        ConsumeTcsOffset(ref consumed, tcs.timestamp);
                        if (error != null) tcs.longTcs?.TrySetException(new Exception(error));
                        else tcs.longTcs?.TrySetResult(resultLong);
                        break;

                    case TaskType.LongCallback:
                        if (!ProcessReplyAsNumber(ref ptr, end, out var resultLongCallback, out error))
                            return readHead;
                        ConsumeTcsOffset(ref consumed, tcs.timestamp);
                        if (error != null) tcs.longCallback?.Invoke(tcs.context, resultLongCallback, error);
                        else tcs.longCallback?.Invoke(tcs.context, resultLongCallback, default);
                        break;

                    default:
                        ThrowException(new InvalidOperationException($"Published completion has unexpected task type {tcs.taskType}."));
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

        static void DisposeMemoryResults(MemoryResult<byte>[] results)
        {
            if (results != null)
                foreach (var result in results)
                    result.Dispose();
        }
    }
}