// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;

namespace Garnet.client
{
    public sealed partial class GarnetLightClient
    {
        /// <summary>
        /// Execute a command whose response is returned as a string.
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Command arguments</param>
        /// <returns>Task that completes with the string reply</returns>
        public Task<string> ExecuteForStringResultAsync(Memory<byte> respOp, ICollection<Memory<byte>> args = null)
        {
            var tcs = new TcsWrapper { taskType = TaskType.StringAsync, stringTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously) };
            _ = InternalExecuteAsync(tcs, respOp, args);
            return tcs.stringTcs.Task;
        }

        /// <summary>
        /// Execute a command whose response is returned as a string.
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Command arguments</param>
        /// <returns>Task that completes with the string reply</returns>
        public Task<string> ExecuteForStringResultAsync(Memory<byte> respOp, ICollection<string> args)
            => ExecuteForStringResultAsync(respOp, ToMemoryArgs(args));

        /// <summary>
        /// Execute a command whose response is returned as a string, honoring a cancellation token.
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Command arguments</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task that completes with the string reply</returns>
        public async Task<string> ExecuteForStringResultWithCancellationAsync(Memory<byte> respOp, ICollection<Memory<byte>> args = null, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.StringAsync, stringTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously) };
            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationStringCallback, tcs.stringTcs))
                {
                    _ = InternalExecuteAsync(tcs, respOp, args, token);
                    return await tcs.stringTcs.Task.ConfigureAwait(false);
                }
            }

            _ = InternalExecuteAsync(tcs, respOp, args, token);
            return await tcs.stringTcs.Task.ConfigureAwait(false);
        }

        /// <summary>
        /// Execute a command whose response is returned as a string, honoring a cancellation token.
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Command arguments</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task that completes with the string reply</returns>
        public Task<string> ExecuteForStringResultWithCancellationAsync(Memory<byte> respOp, ICollection<string> args, CancellationToken token = default)
            => ExecuteForStringResultWithCancellationAsync(respOp, ToMemoryArgs(args), token);

        /// <summary>
        /// Execute a command whose response is returned as a binary-safe <see cref="MemoryResult{Byte}"/>.
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Command arguments</param>
        /// <returns>Task that completes with the binary reply</returns>
        public Task<MemoryResult<byte>> ExecuteForMemoryResultAsync(Memory<byte> respOp, ICollection<Memory<byte>> args = null)
        {
            var tcs = new TcsWrapper { taskType = TaskType.MemoryByteAsync, memoryByteTcs = new TaskCompletionSource<MemoryResult<byte>>(TaskCreationOptions.RunContinuationsAsynchronously) };
            _ = InternalExecuteAsync(tcs, respOp, args);
            return tcs.memoryByteTcs.Task;
        }

        /// <summary>
        /// Execute a command whose response is returned as a binary-safe <see cref="MemoryResult{Byte}"/>.
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Command arguments</param>
        /// <returns>Task that completes with the binary reply</returns>
        public Task<MemoryResult<byte>> ExecuteForMemoryResultAsync(Memory<byte> respOp, ICollection<string> args)
            => ExecuteForMemoryResultAsync(respOp, ToMemoryArgs(args));

        /// <summary>
        /// Execute a command whose response is returned as a binary-safe <see cref="MemoryResult{Byte}"/>,
        /// honoring a cancellation token.
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Command arguments</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task that completes with the binary reply</returns>
        public async Task<MemoryResult<byte>> ExecuteForMemoryResultWithCancellationAsync(Memory<byte> respOp, ICollection<Memory<byte>> args = null, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.MemoryByteAsync, memoryByteTcs = new TaskCompletionSource<MemoryResult<byte>>(TaskCreationOptions.RunContinuationsAsynchronously) };
            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationMemoryByteCallback, tcs.memoryByteTcs))
                {
                    _ = InternalExecuteAsync(tcs, respOp, args, token);
                    return await tcs.memoryByteTcs.Task.ConfigureAwait(false);
                }
            }

            _ = InternalExecuteAsync(tcs, respOp, args, token);
            return await tcs.memoryByteTcs.Task.ConfigureAwait(false);
        }

        /// <summary>
        /// Execute a command whose response is returned as a binary-safe <see cref="MemoryResult{Byte}"/>,
        /// honoring a cancellation token.
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Command arguments</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task that completes with the binary reply</returns>
        public Task<MemoryResult<byte>> ExecuteForMemoryResultWithCancellationAsync(Memory<byte> respOp, ICollection<string> args, CancellationToken token = default)
            => ExecuteForMemoryResultWithCancellationAsync(respOp, ToMemoryArgs(args), token);

        /// <summary>
        /// Execute a command whose response is returned as a 64-bit integer.
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Command arguments</param>
        /// <returns>Task that completes with the integer reply</returns>
        public Task<long> ExecuteForLongResultAsync(Memory<byte> respOp, ICollection<Memory<byte>> args = null)
        {
            var tcs = new TcsWrapper { taskType = TaskType.LongAsync, longTcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously) };
            _ = InternalExecuteAsync(tcs, respOp, args);
            return tcs.longTcs.Task;
        }

        /// <summary>
        /// Execute a command whose response is returned as a 64-bit integer.
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Command arguments</param>
        /// <returns>Task that completes with the integer reply</returns>
        public Task<long> ExecuteForLongResultAsync(Memory<byte> respOp, ICollection<string> args)
            => ExecuteForLongResultAsync(respOp, ToMemoryArgs(args));

        /// <summary>
        /// Execute a command whose response is returned as a 64-bit integer, honoring a cancellation token.
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Command arguments</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task that completes with the integer reply</returns>
        public async Task<long> ExecuteForLongResultWithCancellationAsync(Memory<byte> respOp, ICollection<Memory<byte>> args = null, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.LongAsync, longTcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously) };
            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationLongCallback, tcs.longTcs))
                {
                    _ = InternalExecuteAsync(tcs, respOp, args, token);
                    return await tcs.longTcs.Task.ConfigureAwait(false);
                }
            }

            _ = InternalExecuteAsync(tcs, respOp, args, token);
            return await tcs.longTcs.Task.ConfigureAwait(false);
        }

        /// <summary>
        /// Execute a command whose response is returned as a 64-bit integer, honoring a cancellation token.
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Command arguments</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task that completes with the integer reply</returns>
        public Task<long> ExecuteForLongResultWithCancellationAsync(Memory<byte> respOp, ICollection<string> args, CancellationToken token = default)
            => ExecuteForLongResultWithCancellationAsync(respOp, ToMemoryArgs(args), token);

        /// <summary>
        /// Execute a fixed four-token command without expecting a response (fire-and-forget).
        /// Intended for producer-only traffic such as pub/sub message forwarding; the command must not
        /// produce a reply or the reply reader will fault the connection.
        /// </summary>
        /// <param name="op">Operation in resp format</param>
        /// <param name="param1">First argument</param>
        /// <param name="param2">Second argument</param>
        /// <param name="param3">Third argument</param>
        /// <param name="token">Cancellation token</param>
        public void ExecuteNoResponse(Memory<byte> op, ReadOnlySpan<byte> param1, Span<byte> param2, Span<byte> param3, CancellationToken token = default)
            => InternalExecuteNoResponse(op, param1, param2, param3, token);

        static Memory<byte>[] ToMemoryArgs(ICollection<string> args)
        {
            if (args == null)
                return null;

            var result = new Memory<byte>[args.Count];
            var i = 0;
            foreach (var arg in args)
                result[i++] = arg != null ? Encoding.UTF8.GetBytes(arg) : Memory<byte>.Empty;
            return result;
        }

        void TokenRegistrationStringCallback(object s) => ((TaskCompletionSource<string>)s).TrySetCanceled();

        void TokenRegistrationMemoryByteCallback(object s) => ((TaskCompletionSource<MemoryResult<byte>>)s).TrySetCanceled();

        void TokenRegistrationLongCallback(object s) => ((TaskCompletionSource<long>)s).TrySetCanceled();
    }
}