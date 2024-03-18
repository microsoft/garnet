// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;

namespace Garnet.client
{
    /// <summary>
    /// RESP client session for remote Garnet (a session makes a single remote connection, and expects mono-threaded client access)
    /// </summary>
    public sealed partial class GarnetClientSession : IDisposable
    {
        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="command"></param>
        /// <returns></returns>
        public Task<string> ExecuteAsync(params string[] command)
        {
            var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsQueue.Enqueue(tcs);
            InternalExecute(command);
            Flush();
            return tcs.Task;
        }

        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="command"></param>
        /// <returns></returns>
        public Task<string> ExecuteAsyncBatch(params string[] command)
        {
            var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsQueue.Enqueue(tcs);
            InternalExecute(command);
            return tcs.Task;
        }

        /// <summary>
        /// Execute command (async) for array return type
        /// </summary>
        /// <param name="command"></param>
        /// <returns>Array of results</returns>
        public Task<string[]> ExecuteForArrayAsync(params string[] command)
        {
            var tcs = new TaskCompletionSource<string[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsArrayQueue.Enqueue(tcs);
            InternalExecute(command);
            Flush();
            return tcs.Task;
        }

        /// <summary>
        /// Execute command (async) for array return type
        /// </summary>
        /// <param name="command"></param>
        /// <returns>Array of results</returns>
        public Task<string[]> ExecuteForArrayAsyncBatch(params string[] command)
        {
            var tcs = new TaskCompletionSource<string[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsArrayQueue.Enqueue(tcs);
            InternalExecute(command);
            return tcs.Task;
        }
    }
}