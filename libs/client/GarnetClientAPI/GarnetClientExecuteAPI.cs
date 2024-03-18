// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;

namespace Garnet.client
{
    public sealed partial class GarnetClient
    {

        #region executeForStringResult

        #region Async Methods
        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1">Parameter</param>
        /// <param name="param2">Parameter</param>
        /// <returns></returns>
        public Task<string> ExecuteForStringResultAsync(Memory<byte> respOp, string param1 = null, string param2 = null)
        {
            var tcs = new TcsWrapper { taskType = TaskType.StringAsync, stringTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously) };
            var _ = InternalExecuteAsync(tcs, respOp, param1, param2);
            return tcs.stringTcs.Task;
        }

        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1">Parameter</param>
        /// <param name="param2">Parameter</param>
        /// 
        /// 
        /// <returns></returns>
        public Task<string> ExecuteForStringResultAsync(Memory<byte> respOp, Memory<byte> param1 = default, Memory<byte> param2 = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.StringAsync, stringTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously) };
            var _ = InternalExecuteAsync(tcs, respOp, param1, param2);
            return tcs.stringTcs.Task;
        }

        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="op">Operation</param>
        /// <param name="args">Operation arguments</param>
        /// <returns></returns>
        public Task<string> ExecuteForStringResultAsync(string op, ICollection<string> args = null)
        {
            var tcs = new TcsWrapper { taskType = TaskType.StringAsync, stringTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously) };
            var _ = InternalExecuteAsync(tcs, op, args);
            return tcs.stringTcs.Task;
        }

        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Command arguments</param>
        /// <returns></returns>
        public Task<string> ExecuteForStringResultAsync(Memory<byte> respOp, ICollection<Memory<byte>> args = null)
        {
            var tcs = new TcsWrapper { taskType = TaskType.StringAsync, stringTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously) };
            var _ = InternalExecuteAsync(tcs, respOp, args);
            return tcs.stringTcs.Task;
        }

        #endregion

        #region Callback Methods

        /// <summary>
        /// Execute command (sync)
        /// </summary>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1">Parameter</param>
        /// <param name="param2">Parameter</param>
        /// <returns></returns>
        public void ExecuteForStringResult(Action<long, string> callback, long context, Memory<byte> respOp, string param1 = null, string param2 = null)
        {
            var _ = InternalExecuteAsync(new TcsWrapper { taskType = TaskType.StringCallback, stringCallback = callback, context = context }, respOp, param1, param2);
        }

        /// <summary>
        /// Execute command (sync)
        /// </summary>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1">Parameter</param>
        /// <param name="param2">Parameter</param>
        /// <returns></returns>
        public void ExecuteForStringResult(Action<long, string> callback, long context, Memory<byte> respOp, Memory<byte> param1 = default, Memory<byte> param2 = default)
        {
            var _ = InternalExecuteAsync(new TcsWrapper { taskType = TaskType.StringCallback, stringCallback = callback, context = context }, respOp, param1, param2);
        }

        /// <summary>
        /// Execute command (sync)
        /// </summary>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <param name="op">Commands</param>
        /// <param name="args">Command arguments</param>
        /// <returns></returns>
        public void ExecuteForStringResult(Action<long, string> callback, long context, string op, ICollection<string> args = null)
        {
            var _ = InternalExecuteAsync(new TcsWrapper { taskType = TaskType.StringCallback, stringCallback = callback, context = context }, op, args);
        }

        /// <summary>
        /// Execute command (sync)
        /// </summary>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Operation arguments</param>
        /// <returns></returns>
        public void ExecuteForStringResult(Action<long, string> callback, long context, Memory<byte> respOp, ICollection<Memory<byte>> args = null)
        {
            var _ = InternalExecuteAsync(new TcsWrapper { taskType = TaskType.StringCallback, stringCallback = callback, context = context }, respOp, args);
        }

        #endregion

        #region Cancellation Token Methods

        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1"></param>
        /// <param name="param2"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<string> ExecuteForStringResultWithCancellationAsync(Memory<byte> respOp, string param1, string param2 = default, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.StringAsync, stringTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously) };
            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationStringCallback, tcs.stringTcs))
                {
                    var _ = InternalExecuteAsync(tcs, respOp, param1, param2, token);
                    return await tcs.stringTcs.Task;
                }
            }
            else
            {
                var _ = InternalExecuteAsync(tcs, respOp, param1, param2, token);
                return await tcs.stringTcs.Task;
            }
        }

        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="clusterOp"></param>
        /// <param name="nodeId"></param>
        /// <param name="currentAddress"></param>
        /// <param name="nextAddress"></param>
        /// <param name="payloadPtr"></param>
        /// <param name="payloadLength"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task ExecuteForVoidResultWithCancellationAsync(Memory<byte> respOp, Memory<byte> clusterOp, string nodeId, long currentAddress, long nextAddress, long payloadPtr, int payloadLength, CancellationToken token = default)
        {
            await InternalExecuteAsync(respOp, clusterOp, nodeId, currentAddress, nextAddress, payloadPtr, payloadLength, token);
        }

        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1"></param>
        /// <param name="param2"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<string> ExecuteForStringResultWithCancellationAsync(Memory<byte> respOp, Memory<byte> param1 = default, Memory<byte> param2 = default, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.StringAsync, stringTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously) };
            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationStringCallback, tcs.stringTcs))
                {
                    var _ = InternalExecuteAsync(tcs, respOp, param1, param2, token);
                    return await tcs.stringTcs.Task;
                }
            }
            else
            {
                var _ = InternalExecuteAsync(tcs, respOp, param1, param2, token);
                return await tcs.stringTcs.Task;
            }
        }

        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="op">Operation</param>
        /// <param name="args">Operation arguments</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task<string> ExecuteForStringResultWithCancellationAsync(string op, ICollection<string> args = null, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.StringAsync, stringTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously) };
            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationStringCallback, tcs.stringTcs))
                {
                    var _ = InternalExecuteAsync(tcs, op, args, token);
                    return await tcs.stringTcs.Task;
                }
            }
            else
            {
                var _ = InternalExecuteAsync(tcs, op, args, token);
                return await tcs.stringTcs.Task;
            }

        }

        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Operation arguments</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task<string> ExecuteForStringResultWithCancellationAsync(Memory<byte> respOp, ICollection<Memory<byte>> args = null, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.StringAsync, stringTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously) };
            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationStringCallback, tcs.stringTcs))
                {
                    var _ = InternalExecuteAsync(tcs, respOp, args, token);
                    return await tcs.stringTcs.Task.ConfigureAwait(false);
                }
            }
            else
            {
                var _ = InternalExecuteAsync(tcs, respOp, args, token);
                return await tcs.stringTcs.Task.ConfigureAwait(false);
            }
        }

        #endregion

        void TokenRegistrationStringCallback(object s) => ((TaskCompletionSource<string>)s).TrySetCanceled();

        #endregion

        #region ExecuteForMemoryResult

        #region MemoryResult Async methods

        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1">Parameter</param>
        /// <param name="param2">Parameter</param>
        /// <returns></returns>
        public Task<MemoryResult<byte>> ExecuteForMemoryResultAsync(Memory<byte> respOp, string param1 = null, string param2 = null)
        {
            var tcs = new TcsWrapper { taskType = TaskType.MemoryByteAsync, memoryByteTcs = new TaskCompletionSource<MemoryResult<byte>>(TaskCreationOptions.RunContinuationsAsynchronously) };
            var _ = InternalExecuteAsync(tcs, respOp, param1, param2);
            return tcs.memoryByteTcs.Task;
        }

        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1">Parameter</param>
        /// <param name="param2">Parameter</param>
        /// <returns></returns>
        public Task<MemoryResult<byte>> ExecuteForMemoryResultAsync(Memory<byte> respOp, Memory<byte> param1 = default, Memory<byte> param2 = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.MemoryByteAsync, memoryByteTcs = new TaskCompletionSource<MemoryResult<byte>>(TaskCreationOptions.RunContinuationsAsynchronously) };
            var _ = InternalExecuteAsync(tcs, respOp, param1, param2);
            return tcs.memoryByteTcs.Task;
        }

        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="op">Operation</param>
        /// <param name="args">Operation arguments</param>
        /// <returns></returns>
        public Task<MemoryResult<byte>> ExecuteForMemoryResultAsync(string op, ICollection<string> args = null)
        {
            var tcs = new TcsWrapper { taskType = TaskType.MemoryByteAsync, memoryByteTcs = new TaskCompletionSource<MemoryResult<byte>>(TaskCreationOptions.RunContinuationsAsynchronously) };
            var _ = InternalExecuteAsync(tcs, op, args);
            return tcs.memoryByteTcs.Task;
        }

        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Operation arguments</param>
        /// <returns></returns>
        public Task<MemoryResult<byte>> ExecuteForMemoryResultAsync(Memory<byte> respOp, ICollection<Memory<byte>> args = null)
        {
            var tcs = new TcsWrapper { taskType = TaskType.MemoryByteAsync, memoryByteTcs = new TaskCompletionSource<MemoryResult<byte>>(TaskCreationOptions.RunContinuationsAsynchronously) };
            var _ = InternalExecuteAsync(tcs, respOp, args);
            return tcs.memoryByteTcs.Task;
        }

        #endregion

        #region MemoryResult callback methods

        /// <summary>
        /// Execute command (sync)
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1">Parameter</param>
        /// <param name="param2">Parameter</param>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <returns></returns>ok 1
        public void ExecuteForMemoryResult(Action<long, MemoryResult<byte>> callback, long context, Memory<byte> respOp, string param1 = null, string param2 = null)
        {
            var _ = InternalExecuteAsync(new TcsWrapper { taskType = TaskType.MemoryByteCallback, memoryByteCallback = callback, context = context }, respOp, param1, param2);
        }

        /// <summary>
        /// Execute command (sync)
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1">Parameter</param>
        /// <param name="param2">Parameter</param>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <returns></returns>ok 2
        public void ExecuteForMemoryResult(Action<long, MemoryResult<byte>> callback, long context, Memory<byte> respOp, Memory<byte> param1 = default, Memory<byte> param2 = default)
        {
            var _ = InternalExecuteAsync(new TcsWrapper { taskType = TaskType.MemoryByteCallback, memoryByteCallback = callback, context = context }, respOp, param1, param2);
        }

        /// <summary>
        /// Execute command (sync)
        /// </summary>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <param name="op">Commands</param>
        /// <param name="args">Operation arguments</param>
        /// <returns></returns> ok 3
        public void ExecuteForMemoryResult(Action<long, MemoryResult<byte>> callback, long context, string op, ICollection<string> args = null)
        {
            var _ = InternalExecuteAsync(new TcsWrapper { taskType = TaskType.MemoryByteCallback, memoryByteCallback = callback, context = context }, op, args);
        }

        /// <summary>
        /// Execute command with callback function
        /// </summary>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Operation arguments</param>
        public void ExecuteForMemoryResult(Action<long, MemoryResult<byte>> callback, long context, Memory<byte> respOp, ICollection<Memory<byte>> args = null)
        {
            var _ = InternalExecuteAsync(new TcsWrapper { taskType = TaskType.MemoryByteCallback, memoryByteCallback = callback, context = context }, respOp, args);
        }

        #endregion

        #region MemoryResult cancellationtoken methods

        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1"></param>
        /// <param name="param2"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<MemoryResult<byte>> ExecuteForMemoryResultWithCancellationAsync(Memory<byte> respOp, string param1, string param2 = default, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.MemoryByteAsync, memoryByteTcs = new TaskCompletionSource<MemoryResult<byte>>(TaskCreationOptions.RunContinuationsAsynchronously) };
            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationMemoryResultCallback, tcs.memoryByteTcs))
                {
                    var _ = InternalExecuteAsync(tcs, respOp, param1, param2, token);
                    return await tcs.memoryByteTcs.Task;
                }
            }
            else
            {
                var _ = InternalExecuteAsync(tcs, respOp, param1, param2, token);
                return await tcs.memoryByteTcs.Task;
            }
        }

        /// <summary>
        /// Execute command (async) with cancellation token
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1"></param>
        /// <param name="param2"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<MemoryResult<byte>> ExecuteForMemoryResultWithCancellationAsync(Memory<byte> respOp, Memory<byte> param1 = default, Memory<byte> param2 = default, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.MemoryByteAsync, memoryByteTcs = new TaskCompletionSource<MemoryResult<byte>>(TaskCreationOptions.RunContinuationsAsynchronously) };
            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationMemoryResultCallback, tcs.memoryByteTcs))
                {
                    var _ = InternalExecuteAsync(tcs, respOp, param1, param2, token);
                    return await tcs.memoryByteTcs.Task;
                }
            }
            else
            {
                var _ = InternalExecuteAsync(tcs, respOp, param1, param2, token);
                return await tcs.memoryByteTcs.Task;
            }
        }

        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="op">Commands</param>
        /// <param name="args">Operation arguments</param>
        /// <param name="token">Commands</param>
        /// <returns></returns>
        public async Task<MemoryResult<byte>> ExecuteForMemoryResultWithCancellationAsync(string op, ICollection<string> args = null, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.MemoryByteAsync, memoryByteTcs = new TaskCompletionSource<MemoryResult<byte>>(TaskCreationOptions.RunContinuationsAsynchronously) };
            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationMemoryResultCallback, tcs.memoryByteTcs))
                {
                    var _ = InternalExecuteAsync(tcs, op, args, token);
                    return await tcs.memoryByteTcs.Task;
                }
            }
            else
            {
                var _ = InternalExecuteAsync(tcs, op, args, token);
                return await tcs.memoryByteTcs.Task;
            }
        }

        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Operation arguments</param>
        /// <param name="token">Commands</param>
        public async Task<MemoryResult<byte>> ExecuteForMemoryResultWithCancellationAsync(Memory<byte> respOp, ICollection<Memory<byte>> args = null, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.MemoryByteAsync, memoryByteTcs = new TaskCompletionSource<MemoryResult<byte>>(TaskCreationOptions.RunContinuationsAsynchronously) };
            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationMemoryResultCallback, tcs.memoryByteTcs))
                {
                    var _ = InternalExecuteAsync(tcs, respOp, args, token);
                    return await tcs.memoryByteTcs.Task;
                }
            }
            else
            {
                var _ = InternalExecuteAsync(tcs, respOp, args, token);
                return await tcs.memoryByteTcs.Task;
            }
        }

        void TokenRegistrationMemoryResultCallback(object s) => ((TaskCompletionSource<MemoryResult<byte>>)s).TrySetCanceled();

        #endregion

        #endregion

        #region executeForStringArrayResult

        #region StringArrayResult async methods

        /// <summary>
        /// Execute command (async) for array return type
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1"></param>
        /// <param name="param2"></param>
        /// <returns>Array of results</returns>
        public Task<string[]> ExecuteForStringArrayResultAsync(Memory<byte> respOp, string param1 = null, string param2 = null)
        {
            var tcs = new TcsWrapper { taskType = TaskType.StringArrayAsync, stringArrayTcs = new TaskCompletionSource<string[]>(TaskCreationOptions.RunContinuationsAsynchronously) };
            var _ = InternalExecuteAsync(tcs, respOp, param1, param2);
            return tcs.stringArrayTcs.Task;
        }

        /// <summary>
        /// Execute command (async) for array return type
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1"></param>
        /// <param name="param2"></param>
        /// <returns></returns>
        public Task<string[]> ExecuteForStringArrayResultAsync(Memory<byte> respOp, Memory<byte> param1 = default, Memory<byte> param2 = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.StringArrayAsync, stringArrayTcs = new TaskCompletionSource<string[]>(TaskCreationOptions.RunContinuationsAsynchronously) };
            var _ = InternalExecuteAsync(tcs, respOp, param1, param2);
            return tcs.stringArrayTcs.Task;
        }

        /// <summary>
        /// Execute command (async) for array return type
        /// </summary>
        /// <param name="op"></param>
        /// <param name="args">Operation arguments</param>
        /// <returns></returns>
        public Task<string[]> ExecuteForStringArrayResultAsync(string op, ICollection<string> args = null)
        {
            var tcs = new TcsWrapper { taskType = TaskType.StringArrayAsync, stringArrayTcs = new TaskCompletionSource<string[]>(TaskCreationOptions.RunContinuationsAsynchronously) };
            var _ = InternalExecuteAsync(tcs, op, args);
            return tcs.stringArrayTcs.Task;
        }

        /// <summary>
        /// Execute command (async) for array return type
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Operation arguments</param>
        /// <returns></returns>
        public Task<string[]> ExecuteForStringArrayResultAsync(Memory<byte> respOp, ICollection<Memory<byte>> args = null)
        {
            var tcs = new TcsWrapper { taskType = TaskType.StringArrayAsync, stringArrayTcs = new TaskCompletionSource<string[]>(TaskCreationOptions.RunContinuationsAsynchronously) };
            var _ = InternalExecuteAsync(tcs, respOp, args);
            return tcs.stringArrayTcs.Task;
        }

        #endregion

        #region StringArrayResult callback methods

        /// <summary>
        /// Execute command (sync)
        /// </summary>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context"></param>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1"></param>
        /// <param name="param2"></param>
        public void ExecuteForStringArrayResult(Action<long, string[], string> callback, long context, Memory<byte> respOp, string param1 = null, string param2 = null)
        {
            var _ = InternalExecuteAsync(new TcsWrapper { taskType = TaskType.StringArrayCallback, stringArrayCallback = callback, context = context }, respOp, param1, param2);
        }

        /// <summary>
        /// Execute command (sync)
        /// </summary>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context"></param>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1"></param>
        /// <param name="param2"></param>
        public void ExecuteForStringArrayResult(Action<long, string[], string> callback, long context, Memory<byte> respOp, Memory<byte> param1, Memory<byte> param2 = default)
        {
            var _ = InternalExecuteAsync(new TcsWrapper { taskType = TaskType.StringArrayCallback, stringArrayCallback = callback, context = context }, respOp, param1, param2);
        }


        /// <summary>
        /// Execute command (sync)
        /// </summary>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context"></param>
        /// <param name="op"></param>
        /// <param name="args">Operation arguments</param>
        public void ExecuteForStringArrayResult(Action<long, string[], string> callback, long context, string op, ICollection<string> args = null)
        {
            var _ = InternalExecuteAsync(new TcsWrapper { taskType = TaskType.StringArrayCallback, stringArrayCallback = callback, context = context }, op, args);
        }

        /// <summary>
        /// Execute command (sync)
        /// </summary>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context"></param>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Operation arguments</param>
        public void ExecuteForStringArrayResult(Action<long, string[], string> callback, long context, Memory<byte> respOp, ICollection<Memory<byte>> args = null)
        {
            var _ = InternalExecuteAsync(new TcsWrapper { taskType = TaskType.StringArrayCallback, stringArrayCallback = callback, context = context }, respOp, args);
        }

        #endregion

        #region StringArrayCancellationToken methods

        /// <summary>
        /// Executes a command (async) with a cancellation token
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1"></param>
        /// <param name="param2"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<string[]> ExecuteForStringArrayResultWithCancellationAsync(Memory<byte> respOp, string param1 = default, string param2 = default, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.StringArrayAsync, stringArrayTcs = new TaskCompletionSource<string[]>(TaskCreationOptions.RunContinuationsAsynchronously) };
            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationStringArrayCallback, tcs.stringArrayTcs))
                {
                    var _ = InternalExecuteAsync(tcs, respOp, param1, param2, token);
                    return await tcs.stringArrayTcs.Task;
                }
            }
            else
            {
                var _ = InternalExecuteAsync(tcs, respOp, param1, param2, token);
                return await tcs.stringArrayTcs.Task;
            }
        }


        /// <summary>
        ///  Executes a command (async) with a cancellation token
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1"></param>
        /// <param name="param2"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<string[]> ExecuteForStringArrayResultWithCancellationAsync(Memory<byte> respOp, Memory<byte> param1 = default, Memory<byte> param2 = default, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.StringArrayAsync, stringArrayTcs = new TaskCompletionSource<string[]>(TaskCreationOptions.RunContinuationsAsynchronously) };
            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationStringArrayCallback, tcs.stringArrayTcs))
                {
                    var _ = InternalExecuteAsync(tcs, respOp, param1, param2, token);
                    return await tcs.stringArrayTcs.Task;
                }
            }
            else
            {
                var _ = InternalExecuteAsync(tcs, respOp, param1, param2, token);
                return await tcs.stringArrayTcs.Task;
            }
        }


        /// <summary>
        /// Execute command (async) for array return type with a cancellation token
        /// </summary>
        /// <param name="op"></param>
        /// <param name="args">Operation arguments</param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<string[]> ExecuteForStringArrayResultWithCancellationAsync(string op, ICollection<string> args = null, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.StringArrayAsync, stringArrayTcs = new TaskCompletionSource<string[]>(TaskCreationOptions.RunContinuationsAsynchronously) };
            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationStringArrayCallback, tcs.stringArrayTcs))
                {
                    var _ = InternalExecuteAsync(tcs, op, args, token);
                    return await tcs.stringArrayTcs.Task;
                }
            }
            else
            {
                var _ = InternalExecuteAsync(tcs, op, args, token);
                return await tcs.stringArrayTcs.Task;
            }
        }

        /// <summary>
        /// Execute command (async) for array return type with a cancellation token
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Operation arguments</param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<string[]> ExecuteForStringArrayResultWithCancellationAsync(Memory<byte> respOp, ICollection<Memory<byte>> args = null, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.StringArrayAsync, stringArrayTcs = new TaskCompletionSource<string[]>(TaskCreationOptions.RunContinuationsAsynchronously) };
            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationStringArrayCallback, tcs.stringArrayTcs))
                {
                    var _ = InternalExecuteAsync(tcs, respOp, args, token);
                    return await tcs.stringArrayTcs.Task;
                }
            }
            else
            {
                var _ = InternalExecuteAsync(tcs, respOp, args, token);
                return await tcs.stringArrayTcs.Task;
            }
        }

        #endregion

        void TokenRegistrationStringArrayCallback(object s) => ((TaskCompletionSource<string[]>)s).TrySetCanceled();

        #endregion

        #region executeForMemoryResultArray

        #region executeForMemoryResultArray async methods

        /// <summary>
        /// Executes a command
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1"></param>
        /// <param name="param2"></param>
        /// <returns>MemoryResult byte array</returns>
        public Task<MemoryResult<byte>[]> ExecuteForMemoryResultArrayAsync(Memory<byte> respOp, string param1 = null, string param2 = null)
        {
            var tcs = new TcsWrapper { taskType = TaskType.MemoryByteArrayAsync, memoryByteArrayTcs = new TaskCompletionSource<MemoryResult<byte>[]>(TaskCreationOptions.RunContinuationsAsynchronously) };
            var _ = InternalExecuteAsync(tcs, respOp, param1, param2);
            return tcs.memoryByteArrayTcs.Task;
        }

        /// <summary>
        /// Executes a command
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1"></param>
        /// <param name="param2"></param>
        /// <returns>MemoryResult byte array</returns>
        public Task<MemoryResult<byte>[]> ExecuteForMemoryResultArrayAsync(Memory<byte> respOp, Memory<byte> param1 = default, Memory<byte> param2 = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.MemoryByteArrayAsync, memoryByteArrayTcs = new TaskCompletionSource<MemoryResult<byte>[]>(TaskCreationOptions.RunContinuationsAsynchronously) };
            var _ = InternalExecuteAsync(tcs, respOp, param1, param2);
            return tcs.memoryByteArrayTcs.Task;
        }

        /// <summary>
        /// Executes command with string param array
        /// </summary>
        /// <param name="op"></param>
        /// <param name="args">Operation arguments</param>
        /// <returns>MemoryResult byte array</returns>
        public Task<MemoryResult<byte>[]> ExecuteForMemoryResultArrayAsync(string op, ICollection<string> args = null)
        {
            var tcs = new TcsWrapper { taskType = TaskType.MemoryByteArrayAsync, memoryByteArrayTcs = new TaskCompletionSource<MemoryResult<byte>[]>(TaskCreationOptions.RunContinuationsAsynchronously) };
            var _ = InternalExecuteAsync(tcs, op, args);
            return tcs.memoryByteArrayTcs.Task;
        }

        /// <summary>
        /// Executes command with memorybyte param array
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Operation arguments</param>
        /// <returns>MemoryResult byte array</returns>
        public Task<MemoryResult<byte>[]> ExecuteForMemoryResultArrayAsync(Memory<byte> respOp, ICollection<Memory<byte>> args = null)
        {
            var tcs = new TcsWrapper { taskType = TaskType.MemoryByteArrayAsync, memoryByteArrayTcs = new TaskCompletionSource<MemoryResult<byte>[]>(TaskCreationOptions.RunContinuationsAsynchronously) };
            var _ = InternalExecuteAsync(tcs, respOp, args);
            return tcs.memoryByteArrayTcs.Task;
        }

        #endregion

        #region executeForMemoryResultArray callback methods

        /// <summary>
        /// 
        /// </summary>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1"></param>
        /// <param name="param2"></param>
        public void ExecuteForMemoryResultArray(Action<long, MemoryResult<byte>[], MemoryResult<byte>> callback, long context, Memory<byte> respOp, string param1 = null, string param2 = null)
        {
            var _ = InternalExecuteAsync(new TcsWrapper { taskType = TaskType.MemoryByteArrayCallback, memoryByteArrayCallback = callback, context = context }, respOp, param1, param2);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1"></param>
        /// <param name="param2"></param>
        public void ExecuteForMemoryResultArray(Action<long, MemoryResult<byte>[], MemoryResult<byte>> callback, long context, Memory<byte> respOp, Memory<byte> param1, Memory<byte> param2 = default)
        {
            var _ = InternalExecuteAsync(new TcsWrapper { taskType = TaskType.MemoryByteArrayCallback, memoryByteArrayCallback = callback, context = context }, respOp, param1, param2);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// <param name="op"></param>
        /// <param name="args">Operation arguments</param>
        public void ExecuteForMemoryResultArray(Action<long, MemoryResult<byte>[], MemoryResult<byte>> callback, long context, string op, ICollection<string> args = null)
        {
            var _ = InternalExecuteAsync(new TcsWrapper { taskType = TaskType.MemoryByteArrayCallback, memoryByteArrayCallback = callback, context = context }, op, args);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Operation arguments</param>
        public void ExecuteForMemoryResultArray(Action<long, MemoryResult<byte>[], MemoryResult<byte>> callback, long context, Memory<byte> respOp, ICollection<Memory<byte>> args = null)
        {
            var _ = InternalExecuteAsync(new TcsWrapper { taskType = TaskType.MemoryByteArrayCallback, memoryByteArrayCallback = callback, context = context }, respOp, args);
        }

        #endregion


        #region executeForMemoryResultArray cancellation token methods

        /// <summary>
        /// Executes command with memorybyte param array
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1"></param>
        /// <param name="param2"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<MemoryResult<byte>[]> ExecuteForMemoryResultArrayWithCancellationAsync(Memory<byte> respOp, string param1 = null, string param2 = null, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.MemoryByteArrayAsync, memoryByteArrayTcs = new TaskCompletionSource<MemoryResult<byte>[]>(TaskCreationOptions.RunContinuationsAsynchronously) };

            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationMemoryResultArrayCallback, tcs.memoryByteArrayTcs))
                {
                    var _ = InternalExecuteAsync(tcs, respOp, param1, param2, token);
                    return await tcs.memoryByteArrayTcs.Task;
                }
            }
            else
            {
                var _ = InternalExecuteAsync(tcs, respOp, param1, param2, token);
                return await tcs.memoryByteArrayTcs.Task;
            }
        }

        /// <summary>
        /// Executes command with memorybyte param array 
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="param1"></param>
        /// <param name="param2"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<MemoryResult<byte>[]> ExecuteForMemoryResultArrayWithCancellationAsync(Memory<byte> respOp, Memory<byte> param1 = default, Memory<byte> param2 = default, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.MemoryByteArrayAsync, memoryByteArrayTcs = new TaskCompletionSource<MemoryResult<byte>[]>(TaskCreationOptions.RunContinuationsAsynchronously) };

            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationMemoryResultArrayCallback, tcs.memoryByteArrayTcs))
                {
                    var _ = InternalExecuteAsync(tcs, respOp, param1, param2, token);
                    return await tcs.memoryByteArrayTcs.Task;
                }
            }
            else
            {
                var _ = InternalExecuteAsync(tcs, respOp, param1, param2, token);
                return await tcs.memoryByteArrayTcs.Task;
            }
        }


        /// <summary>
        /// Executes command with memorybyte param array
        /// </summary>
        /// <param name="op"></param>
        /// <param name="args">Operation arguments</param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<MemoryResult<byte>[]> ExecuteForMemoryResultArrayWithCancellationAsync(string op, ICollection<string> args = null, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.MemoryByteArrayAsync, memoryByteArrayTcs = new TaskCompletionSource<MemoryResult<byte>[]>(TaskCreationOptions.RunContinuationsAsynchronously) };

            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationMemoryResultArrayCallback, tcs.memoryByteArrayTcs))
                {
                    var _ = InternalExecuteAsync(tcs, op, args, token);
                    return await tcs.memoryByteArrayTcs.Task;
                }
            }
            else
            {
                var _ = InternalExecuteAsync(tcs, op, args, token);
                return await tcs.memoryByteArrayTcs.Task;
            }
        }


        /// <summary>
        /// Executes command with memorybyte param array
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Operation arguments</param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<MemoryResult<byte>[]> ExecuteForMemoryResultArrayWithCancellationAsync(Memory<byte> respOp, ICollection<Memory<byte>> args = null, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.MemoryByteArrayAsync, memoryByteArrayTcs = new TaskCompletionSource<MemoryResult<byte>[]>(TaskCreationOptions.RunContinuationsAsynchronously) };

            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationMemoryResultArrayCallback, tcs.memoryByteArrayTcs))
                {
                    var _ = InternalExecuteAsync(tcs, respOp, args, token);
                    return await tcs.memoryByteArrayTcs.Task.ConfigureAwait(false);
                }
            }
            else
            {
                var _ = InternalExecuteAsync(tcs, respOp, args, token);
                return await tcs.memoryByteArrayTcs.Task.ConfigureAwait(false);
            }
        }


        void TokenRegistrationMemoryResultArrayCallback(object s) => ((TaskCompletionSource<MemoryResult<byte>[]>)s).TrySetCanceled();

        #endregion

        #endregion

        #region executeForIntegerResult

        #region async methods

        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="op">Commands</param>
        /// <param name="args">Operation arguments</param>
        /// <returns></returns>
        public Task<long> ExecuteForLongResultAsync(string op, ICollection<string> args = null)
        {
            var tcs = new TcsWrapper { taskType = TaskType.LongAsync, longTcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously) };
            var _ = InternalExecuteAsync(tcs, op, args);
            return tcs.longTcs.Task;
        }

        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Command arguments</param>
        /// <returns></returns>
        public Task<long> ExecuteForLongResultAsync(Memory<byte> respOp, ICollection<Memory<byte>> args = null)
        {
            var tcs = new TcsWrapper { taskType = TaskType.LongAsync, longTcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously) };
            var _ = InternalExecuteAsync(tcs, respOp, args);
            return tcs.longTcs.Task;
        }

        /// <summary>
        /// Execute command (async)
        /// </summary>
        /// <param name="respOp"></param>
        /// <param name="param1"></param>
        /// <param name="param2"></param>
        /// <returns></returns>
        public Task<long> ExecuteForLongResultAsync(Memory<byte> respOp, string param1 = null, string param2 = null)
        {
            var tcs = new TcsWrapper { taskType = TaskType.LongAsync, longTcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously) };
            var _ = InternalExecuteAsync(tcs, respOp, param1, param2);
            return tcs.longTcs.Task;
        }

        #endregion

        #region callback methods

        /// <summary>
        /// Execute command (sync)
        /// </summary>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <param name="op">Commands</param>
        /// <param name="args">Command arguments</param>
        /// <returns></returns>
        public void ExecuteForLongResult(Action<long, long, string> callback, long context, string op, ICollection<string> args = null)
        {
            var _ = InternalExecuteAsync(new TcsWrapper { taskType = TaskType.LongCallback, longCallback = callback, context = context }, op, args);
        }

        /// <summary>
        /// Execute command (sync)
        /// </summary>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <param name="respOp">Command in resp format</param>
        /// <param name="args">Commands arguments</param>
        /// <returns></returns>
        public void ExecuteForLongResult(Action<long, long, string> callback, long context, Memory<byte> respOp, ICollection<Memory<byte>> args = null)
        {
            var _ = InternalExecuteAsync(new TcsWrapper { taskType = TaskType.LongCallback, longCallback = callback, context = context }, respOp, args);
        }


        /// <summary>
        /// Execute command (sync)
        /// </summary>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// <param name="respOp"></param>
        /// <param name="param1"></param>
        /// <param name="param2"></param>
        public void ExecuteForLongResult(Action<long, long, string> callback, long context, Memory<byte> respOp, string param1 = null, string param2 = null)
        {
            var _ = InternalExecuteAsync(new TcsWrapper { taskType = TaskType.LongCallback, longCallback = callback, context = context }, respOp, param1, param2);
        }

        #endregion

        #region cancellation token methods

        /// <summary>
        /// Execute command (async) with cancellation token
        /// </summary>
        /// <param name="op">Commands</param>
        /// <param name="args">Operation arguments</param>
        /// <param name="token">Commands</param>
        /// <returns></returns>
        public async Task<long> ExecuteForLongResultWithCancellationAsync(string op, ICollection<string> args = null, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.LongAsync, longTcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously) };
            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationLongCallback, tcs.longTcs))
                {
                    var _ = InternalExecuteAsync(tcs, op, args, token);
                    return await tcs.longTcs.Task;
                }
            }
            else
            {
                var _ = InternalExecuteAsync(tcs, op, args, token);
                return await tcs.longTcs.Task;
            }
        }


        /// <summary>
        /// Execute command (async) with cancellation token
        /// </summary>
        /// <param name="respOp">Operation in resp format</param>
        /// <param name="args">Operation arguments</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task<long> ExecuteForLongResultWithCancellationAsync(Memory<byte> respOp, Memory<byte>[] args = null, CancellationToken token = default)
        {
            var tcs = new TcsWrapper { taskType = TaskType.LongAsync, longTcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously) };
            if (token.CanBeCanceled)
            {
                using (token.Register(TokenRegistrationLongCallback, tcs.longTcs))
                {
                    var _ = InternalExecuteAsync(tcs, respOp, args, token);
                    return await tcs.longTcs.Task.ConfigureAwait(false);
                }
            }
            else
            {
                var _ = InternalExecuteAsync(tcs, respOp, args, token);
                return await tcs.longTcs.Task.ConfigureAwait(false);
            }
        }


        #endregion

        void TokenRegistrationLongCallback(object s) => ((TaskCompletionSource<long>)s).TrySetCanceled();

        #endregion
    }
}