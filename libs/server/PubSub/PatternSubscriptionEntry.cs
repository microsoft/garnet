// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Entry in the table of pattern subscriptions.
    /// </summary>
    struct PatternSubscriptionEntry : IEquatable<PatternSubscriptionEntry>
    {
        /// <summary>
        /// The pattern to which the subscriptions are subscribed.
        /// </summary>
        public ByteArrayWrapper pattern;

        /// <summary>
        /// The set of subscriptions.
        /// </summary>
        public ReadOptimizedConcurrentSet<ServerSessionBase> subscriptions;

        bool IEquatable<PatternSubscriptionEntry>.Equals(PatternSubscriptionEntry other)
            => pattern.ReadOnlySpan.SequenceEqual(other.pattern.ReadOnlySpan);
    }
}