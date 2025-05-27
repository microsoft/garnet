// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Garnet.server
{
    /// <summary>
    /// Event types handled by CollectionItemBroker
    /// </summary>
    internal enum CollectionItemBrokerEventType : byte
    {
        NotSet = 0,
        NewObserver = 1,
        CollectionUpdated = 2,
    }

    /// <summary>
    /// Struct that holds data for different event types handled by CollectionItemBroker
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 17)]
    internal struct CollectionItemBrokerEvent
    {
        /// <summary>
        /// Key of updated collection (for a CollectionUpdated event)
        /// </summary>
        [FieldOffset(0)]
        internal byte[] Key = null;

        /// <summary>
        /// The keys that the observer requests to subscribe on (for a NewObserver event)
        /// </summary>
        [FieldOffset(0)]
        internal byte[][] Keys = null;

        /// <summary>
        /// The new observer instance (for a NewObserver event)
        /// </summary>
        [FieldOffset(8)]
        internal CollectionItemObserver Observer = null;

        /// <summary>
        /// The type of event represented
        /// </summary>
        [FieldOffset(16)]
        internal CollectionItemBrokerEventType EventType = CollectionItemBrokerEventType.NotSet;

        public CollectionItemBrokerEvent()
        {

        }

        /// <summary>
        /// Creates a CollectionUpdated event
        /// </summary>
        /// <param name="key">Key of updated collection</param>
        public static CollectionItemBrokerEvent CreateCollectionUpdatedEvent(byte[] key)
        {
            return new CollectionItemBrokerEvent
            {
                EventType = CollectionItemBrokerEventType.CollectionUpdated,
                Key = key
            };
        }

        /// <summary>
        /// Creates a NewObserver event
        /// </summary>
        /// <param name="observer">The new observer instance</param>
        /// <param name="keys">The keys that the observer requests to subscribe on</param>
        public static CollectionItemBrokerEvent CreateNewObserverEvent(CollectionItemObserver observer, byte[][] keys)
        {
            return new CollectionItemBrokerEvent
            {
                EventType = CollectionItemBrokerEventType.NewObserver,
                Observer = observer,
                Keys = keys,
            };
        }

        public bool IsDefault() => EventType == CollectionItemBrokerEventType.NotSet;
    }
}