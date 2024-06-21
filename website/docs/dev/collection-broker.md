---
id: collection-broker
sidebar_label: Collection Broker
title: Collection Broker
slug: collection-broker
---

The role of the collection broker is to facilitate blocking commands on collections in Garnet.\
A blocking command is a command that returns immediately if an item in a collection is available or blocks the client until an item is avilable (see: [BLPOP](../commands/data-structures.md#blpop), [BLMOVE](../commands/data-structures.md#blmove) etc).\
The broker runs its main loop on a dedicated Task whenever there are active clients waiting on collection items.

### Logical Flow

#### Incoming blocking command:
1. A client sends a blocking command, the command handler in turn calls `CollectionItemBroker.GetCollectionItemAsync`
    1. If the broker's main loop is not running, it will start running and wait on the next event in its event queue (`brokerEventsQueue`).
    2. A new `CollectionItemObserver` object is created and an event of type `NewObserverEvent` is pushed into the event queue.
    3. The command handler awaits on one of two occurrences:
        1. A semaphore signal coming from the main loop to notify an item has been found.
        2. A timeout specified by the client has been reached.

#### Incoming "releasing" command:
2. A client inserts an item into a collection the command handler in turn calls `CollectionItemBroker.HandleCollectionUpdate`\
    1. If the collection is not observed by and clients, nothing to do.
    2. Otherwise, a new event of type `CollectionUpdatedEvent` is pushed into the event queue.

#### Main broker loop:
3. The main loop (`CollectionItemBroker.Start`) continuously listens on an `AsyncQueue` for new incoming events and synchronously calls `HandleBrokerEvent` for each new event.\
    1. For events of type `NewObserverEvent`, `InitializeObserver` is called. `InitializeObserver` takes an array of keys and checks the collection values for available item in the order in which they where specified by the client.
    If an item is found, the observer is updated which sets the result and signals the semaphore to release the awaiting thread. If no item is found, the observer is being added to each key's observer queue.
    2. For events of type `CollectionUpdatedEvent`, `TryAssignItemFromKey` is called. This method gets the key's observer queue and tries to assign the next available item from the collection stored at key to the next observer.
    If an available item was indeed found, the observer is updated which sets the result and signals the semaphore to release the awaiting thread. 

#### Disposed sessions:
4. If a `RespServerSession` that has an active observer is disposed, its `Dispose` method will call `CollectionItemBroker.HandleSessionDisposed`, which in turn will update the observer and signal the semaphore awaiter to stop. Once the observer's status is changed to `SessionDisposed`, the main loop will not assign it any item.