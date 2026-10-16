// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Garnet.server
{
    /// <summary>
    /// A delivery of a published message that the publisher holds back, because the subscriber's connection
    /// has no room for it yet.
    /// </summary>
    internal readonly struct DeferredDelivery
    {
        /// <summary>The subscriber the message is for.</summary>
        internal readonly ServerSessionBase Session;

        /// <summary>The pattern the channel matched. Meaningful only when <see cref="IsPattern"/>.</summary>
        internal readonly ByteArrayWrapper Pattern;

        /// <summary>Whether the subscriber gets the message as a match of a pattern, rather than of the channel.</summary>
        internal readonly bool IsPattern;

        internal DeferredDelivery(ServerSessionBase session)
        {
            Session = session;
            Pattern = default;
            IsPattern = false;
        }

        internal DeferredDelivery(ServerSessionBase session, ByteArrayWrapper pattern)
        {
            Session = session;
            Pattern = pattern;
            IsPattern = true;
        }
    }

    /// <summary>
    /// The deliveries of one published message that the publisher holds back, in the order they would have been
    /// written had the connections had room.
    /// </summary>
    /// <remarks>
    /// A publisher session keeps one instance and reuses it for each message it publishes, so a publisher that
    /// meets a full connection on every message does not allocate for it.
    /// </remarks>
    internal sealed class DeferredDeliveries
    {
        readonly List<DeferredDelivery> deliveries = [];
        readonly HashSet<ServerSessionBase> sessions = [];

        /// <summary>The number of deliveries held back.</summary>
        internal int Count => deliveries.Count;

        /// <summary>A delivery held back, by the order in which it was held.</summary>
        internal DeferredDelivery this[int index] => deliveries[index];

        /// <summary>
        /// Whether a delivery to the subscriber is already held back. A later delivery of the same message to it
        /// has to be held back too, or it could overtake the earlier one.
        /// </summary>
        internal bool Holds(ServerSessionBase session) => sessions.Contains(session);

        internal void Add(in DeferredDelivery delivery)
        {
            deliveries.Add(delivery);
            _ = sessions.Add(delivery.Session);
        }

        internal void Clear()
        {
            deliveries.Clear();
            sessions.Clear();
        }
    }
}