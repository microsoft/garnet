// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Unit tests for <see cref="VectorSetCleanupWorkChannel{T}"/>.
    /// </summary>
    [TestFixture]
    public class VectorSetCleanupWorkChannelTests
    {
        [Test]
        public async Task PublishedItemIsReadable()
        {
            var channel = new VectorSetCleanupWorkChannel<int>();

            ClassicAssert.IsFalse(channel.HasPending);
            ClassicAssert.IsTrue(channel.TryPublish(7));

            ClassicAssert.IsTrue(channel.HasPending);
            ClassicAssert.IsTrue(await channel.WaitToReadAsync());

            ClassicAssert.IsTrue(channel.TryRead(out var item));
            ClassicAssert.AreEqual(7, item);

            ClassicAssert.IsFalse(channel.HasPending);
            ClassicAssert.IsFalse(channel.TryRead(out _));
        }

        [Test]
        public async Task CompletedChannelRejectsPublishesAndReleasesWaiters()
        {
            var channel = new VectorSetCleanupWorkChannel<int>();
            channel.CompleteAndWaitForConsumerTask(Task.CompletedTask);

            ClassicAssert.IsFalse(channel.TryPublish(7));
            ClassicAssert.IsFalse(await channel.WaitToReadAsync());
        }

        [Test]
        public void CompleteAndWaitForConsumerTaskDrainsBeforeReturning()
        {
            var channel = new VectorSetCleanupWorkChannel<int>();
            var consumed = 0;

            var consumer = Task.Run(async () =>
            {
                while (await channel.WaitToReadAsync())
                {
                    while (channel.TryRead(out _))
                    {
                        consumed++;
                    }
                }
            });

            _ = channel.TryPublish(1);
            _ = channel.TryPublish(2);

            channel.CompleteAndWaitForConsumerTask(consumer);

            ClassicAssert.IsTrue(consumer.IsCompleted);
            ClassicAssert.AreEqual(2, consumed);
        }
    }
}
