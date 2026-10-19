// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Writes the contents of a configuration file to the start of its device, one write at a time. A write requested
    /// while another is in flight joins the next one, which the completion of the write in flight issues with the
    /// contents current then. So a write follows the one before it on the thread that completes that one, and a caller
    /// that waits for its write, on whatever thread, never holds up another's.
    /// </summary>
    internal sealed class ConfigFileWriter
    {
        readonly IDevice device;
        readonly SectorAlignedBufferPool pool;
        readonly Func<byte[]> contents;
        readonly ILogger logger;
        readonly object sync = new();

        /// <summary>Completes with the write that follows the one in flight; null when none has been requested.</summary>
        TaskCompletionSource next;

        /// <summary>Whether a write is in flight.</summary>
        bool writing;

        /// <param name="device">The device whose start the file is written to.</param>
        /// <param name="pool">The pool to rent sector-aligned write buffers from.</param>
        /// <param name="contents">Returns the file's current contents, when a write is issued.</param>
        /// <param name="logger">Logs a failed write.</param>
        public ConfigFileWriter(IDevice device, SectorAlignedBufferPool pool, Func<byte[]> contents, ILogger logger)
        {
            this.device = device;
            this.pool = pool;
            this.contents = contents;
            this.logger = logger;
        }

        /// <summary>
        /// Writes the file's current contents, holding the calling thread until a write issued after this call completes.
        /// </summary>
        /// <remarks>
        /// The thread that completes the write wakes this one itself, rather than through the thread pool, so a pool
        /// thread waiting here needs no other to be free.
        /// </remarks>
#pragma warning disable VSTHRD002 // The wait is released by the thread completing the write, not by a continuation
        public void Write() => WriteAsync().GetAwaiter().GetResult();
#pragma warning restore VSTHRD002

        /// <summary>
        /// Writes the file's current contents, completing once a write issued after this call does.
        /// </summary>
        public Task WriteAsync()
        {
            TaskCompletionSource completion;
            lock (sync)
            {
                if (writing)
                    return (next ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).Task;

                writing = true;
                completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            Issue(completion);
            return completion.Task;
        }

        unsafe void Issue(TaskCompletionSource completion)
        {
            while (completion != null)
            {
                SectorAlignedMemory buffer = null;
                try
                {
                    buffer = ClusterUtils.PrepareWrite(device, pool, contents(), out var numBytes);
                    device.WriteAsync((IntPtr)buffer.aligned_pointer, 0, numBytes, OnWritten, new PendingWrite(buffer, completion));
                    return;
                }
                catch (Exception ex)
                {
                    buffer?.Return();
                    completion = Complete(completion, ex);
                }
            }
        }

        void OnWritten(uint errorCode, uint numBytes, object context, Exception ioException)
        {
            var write = (PendingWrite)context;
            logger?.LogIOError(errorCode, numBytes, context);
            write.Buffer.Return();
            Issue(Complete(write.Completion, null));
        }

        /// <summary>
        /// Completes the write in flight, returning the completion of the one to issue next, if one has been requested.
        /// </summary>
        TaskCompletionSource Complete(TaskCompletionSource completion, Exception exception)
        {
            TaskCompletionSource following;
            lock (sync)
            {
                following = next;
                next = null;
                writing = following != null;
            }

            if (exception == null)
                completion.SetResult();
            else
                completion.SetException(exception);
            return following;
        }

        sealed record PendingWrite(SectorAlignedMemory Buffer, TaskCompletionSource Completion);
    }
}