// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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
            _ = InternalExecuteChunkedAsync(tcs, respOp, args);
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
                    _ = InternalExecuteChunkedAsync(tcs, respOp, args, token);
                    return await tcs.stringTcs.Task.ConfigureAwait(false);
                }
            }

            _ = InternalExecuteChunkedAsync(tcs, respOp, args, token);
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
    }
}
