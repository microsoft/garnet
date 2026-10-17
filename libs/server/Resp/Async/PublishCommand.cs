// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Tsavorite.core;

namespace Garnet.server
{
    // Not declared unsafe: an async method body cannot sit in an unsafe context, and the unsafe modifier
    // on a partial type declaration applies only to the part that carries it.
    internal sealed partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// The deliveries of the message being published that were held back because the subscriber could not
        /// take them: its connection had no room, or it was suspended holding its response object. A session runs
        /// one command at a time, so its PUBLISH commands share this one.
        /// </summary>
        DeferredDeliveries deferredDeliveries;

        /// <summary>
        /// Replies to <c>PUBLISH</c>/<c>SPUBLISH</c> once the deliveries that were held back have been made, and
        /// the message has also reached the other cluster nodes. <paramref name="forward"/> is the in-flight
        /// forward, or a default task when the node is not clustered.
        /// </summary>
        /// <remarks>
        /// The publisher parks for a subscriber that is slow to read, as it would wait for one inside the
        /// broadcast on a thread that could be held, so that subscriber's backlog still holds the publisher back
        /// and cannot grow without bound. The wait is on the subscriber's connection and ends as soon as its
        /// client reads, or as soon as the subscriber resumes from a suspension that holds its response object
        /// (see <see cref="pushesHeldBack"/>). If the publisher's session is torn down first, the body is not
        /// resumed, and the wait's registration with the subscriber's connection is taken back by the teardown
        /// (<see cref="SendCapacityWait"/>). Inside a transaction or a script it does not wait at all: see
        /// <see cref="DeliverDeferredWithoutWaiting"/>.
        /// </remarks>
        bool CompleteDeferredPublish(ValueTask forward, int numClients)
        {
            ValueTask body;
            using (BeginAsyncCommand())
                body = DeferredPublishBodyAsync(forward, numClients);

            return CompleteAsyncCommand(body);
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask DeferredPublishBodyAsync(ValueTask forward, int numClients)
        {
            var deferred = deferredDeliveries;
            var wait = SendCapacityWait;

            // In the order the deliveries were made, so a subscriber receives what it is owed in that order.
            for (var i = 0; i < deferred.Count; i++)
            {
                // A completion that frees room can precede the registration, so the connection is
                // tried again after each wait rather than trusting the signal alone.
                while (!TryDeliver(i))
                {
                    if (wait.TryRegisterWithPeer(deferred[i].Session))
                        await wait.WaitAsync().ConfigureAwait(false);
                }
            }

            // Lets go of the subscribers' sessions, and leaves the list empty for the next publish
            deferred.Clear();

            await forward.ConfigureAwait(false);
            WriteInt32(numClients);
        }

        /// <summary>
        /// Makes one of the deliveries held back, if the subscriber can take it now.
        /// </summary>
        /// <param name="index">Which delivery, in the order they were held back.</param>
        /// <returns>False if nothing was written.</returns>
        /// <remarks>
        /// The message is read from the command's arguments, which stay addressable while the command is
        /// suspended, so it need not be carried through the wait.
        /// </remarks>
        unsafe bool TryDeliver(int index)
        {
            var delivery = deferredDeliveries[index];
            var key = parseState.GetArgSliceByRef(0);
            var value = parseState.GetArgSliceByRef(1);

            if (!delivery.IsPattern)
                return delivery.Session.TryPublish(key, value);

            // Patterns are stored unpinned
            var pattern = delivery.Pattern.ReadOnlySpan;
            fixed (byte* patternPtr = pattern)
                return delivery.Session.TryPatternPublish(PinnedSpanByte.FromPinnedPointer(patternPtr, pattern.Length), key, value);
        }

        /// <summary>
        /// Makes the deliveries held back now, for a publisher that may not wait (<see cref="DenyBlocking"/>): past
        /// each full connection's limit on outstanding sends, and to a subscriber suspended holding its response
        /// object by leaving the message for it to write out when it resumes.
        /// </summary>
        /// <remarks>
        /// Waiting there would hold a transaction's key locks, or a script's thread, for as long as the subscriber
        /// does not read or stays suspended. A full connection instead buffers one more message per such publish,
        /// as Redis buffers every publish. They are made in the order they were held back, so each subscriber
        /// still receives its messages in order.
        /// </remarks>
        unsafe void DeliverDeferredWithoutWaiting()
        {
            var deferred = deferredDeliveries;
            var key = parseState.GetArgSliceByRef(0);
            var value = parseState.GetArgSliceByRef(1);

            for (var i = 0; i < deferred.Count; i++)
            {
                var delivery = deferred[i];
                if (!delivery.IsPattern)
                {
                    delivery.Session.Publish(key, value);
                    continue;
                }

                // Patterns are stored unpinned
                var pattern = delivery.Pattern.ReadOnlySpan;
                fixed (byte* patternPtr = pattern)
                    delivery.Session.PatternPublish(PinnedSpanByte.FromPinnedPointer(patternPtr, pattern.Length), key, value);
            }

            // Lets go of the subscribers' sessions, and leaves the list empty for the next publish
            deferred.Clear();
        }
    }
}