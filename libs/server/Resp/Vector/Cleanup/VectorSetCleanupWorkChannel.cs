// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// An unbounded single-consumer queue of Vector Set cleanup work, where each item is a unit of work.
    /// </summary>
    internal sealed class VectorSetCleanupWorkChannel<T>
    {
        private readonly Channel<T> channel;

        public VectorSetCleanupWorkChannel()
        {
            channel = Channel.CreateUnbounded<T>(new() { SingleWriter = false, SingleReader = true, AllowSynchronousContinuations = false });
        }

        /// <summary>
        /// Publish an item. False once completed, i.e. during shutdown.
        /// </summary>
        public bool TryPublish(T item) => channel.Writer.TryWrite(item);

        /// <summary>
        /// Completes when an item may be available. False once completed and drained.
        /// </summary>
        public ValueTask<bool> WaitToReadAsync() => channel.Reader.WaitToReadAsync();

        /// <summary>
        /// Whether any item is queued.
        /// </summary>
        public bool HasPending => channel.Reader.TryPeek(out _);

        /// <summary>
        /// Take the next item, if any.
        /// </summary>
        public bool TryRead(out T item) => channel.Reader.TryRead(out item);

        /// <summary>
        /// Stop accepting items and block until they drain and <paramref name="consumerTask"/> exits.
        /// </summary>
        public void CompleteAndWaitForConsumerTask(Task consumerTask)
        {
            channel.Writer.Complete();
            AsyncUtils.BlockingWait(channel.Reader.Completion);
            AsyncUtils.BlockingWait(consumerTask);
        }
    }

    internal static class VectorSetCleanupWorkChannelExtensions
    {
        /// <summary>
        /// Publish an item with no payload, similar to a signal, for a queue whose work is described elsewhere.
        /// </summary>
        public static bool TryPublish(this VectorSetCleanupWorkChannel<object> channel) => channel.TryPublish(null);

        /// <summary>
        /// Discard every queued item, for a consumer whose pass services the whole backlog anyway. Only for
        /// payload-free queues, since discarding an item that carries one drops the work it describes.
        /// </summary>
        public static void DrainPending(this VectorSetCleanupWorkChannel<object> channel)
        {
            while (channel.TryRead(out _))
            {
            }
        }
    }
}
