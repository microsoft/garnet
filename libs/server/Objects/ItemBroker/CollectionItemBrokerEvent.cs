// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Base class for events handled by CollectionItemBroker's main loop
    /// </summary>
    internal abstract class BrokerEventBase
    {
    }

    /// <summary>
    /// Event to notify CollectionItemBroker that a collection has been updated
    /// </summary>
    internal class CollectionUpdatedEvent : BrokerEventBase
    {
        /// <summary>
        /// Key of updated collection
        /// </summary>
        internal byte[] Key { get; }

        public CollectionUpdatedEvent(byte[] key)
        {
            Key = key;
        }
    }

    /// <summary>
    /// Event to notify CollectionItemBroker that a new observer was created
    /// </summary>
    internal class NewObserverEvent : BrokerEventBase
    {
        /// <summary>
        /// The new observer instance
        /// </summary>
        internal CollectionItemObserver Observer { get; }

        /// <summary>
        /// The keys that the observer requests to subscribe on
        /// </summary>
        internal byte[][] Keys { get; }

        internal NewObserverEvent(CollectionItemObserver observer, byte[][] keys)
        {
            Observer = observer;
            Keys = keys;
        }
    }
}