---
slug: etags-when-and-how
title: ETags, When and How
authors: hkhalid 
tags: [garnet, concurrency, caching, lock-free, etags]
---

Garnet recently announced native support for ETag-based commands. ETags are a powerful feature that enable techniques such as **Optimistic Concurrency Control** and **more efficient network bandwidth utilization for caching**.  

Currently, Garnet provides ETags as raw strings. The feature is available without requiring any migration, meaning your existing key-value pairs can start leveraging ETags on the fly without impacting performance metrics. You can find the [ETag API documentation here](../commands/garnet-specific-commands#native-etag-support).

This article explores when and how you can use this new Garnet feature for both your current and future applications.  

---

## Why Read This Article?  
If you need or want:  

1. **Reduced network bandwidth utilization for caching**.  
2. **A way to avoid the cost of transactions when working with non-atomic values in your cache store**.  

These are common scenarios that we'll cover case by case.  

---

## Reducing Network Bandwidth Utilization for Caching  

Every network call incurs a cost: the amount of data transmitted and the distance over which it travels. In performance-sensitive scenarios, it's beneficial to fetch data only if it has changed in the cache, thereby reducing bandwidth usage and network latency.  

### Scenario: Server-Cache-Database Interaction  
Consider the following setup:  

#### High-Level Diagram  
```
(server 1)----[server 1 reads from cache]----(cache)
                                                |
                                                |
                                    [server 2 writes to the cache]
                                                |
                                                |
                                            (server 2)
```

### Sequence Diagram
```
    server 1                                        cache 1                             server 2
1. initial read from cache for k1-------------------->
2.        <----------------------------------------Send Data and ETag
                                                        <-------------------------update value for k1 , that invalidates k1
3. second read to cache for k1 ------------------------>
                <--------------------------------------(What is sent back?)
````

Reading through the above scenario will bring you to the last item in our sequence, the second read and what is returned for it.
In the situation where no ETag is used, the entire payload is returned for k1 everytime. Regardless of whether or not value associated with k1 was changed you have incurred the cost of sending the entire value.

While this may not be a concern when working with a single machine transfering 100 Bytes of data where the network bandwidth is 10Gbps over fiber optic. This matters when you have 50 machines egressing 1MB of data from your network on your favorite cloud provider from Garnet (or any other software), you are paying with your cost of egressing, bandwidth usage, and you are incurring the delay of recieving more bits over the internet.

This is where in our above sequence diagram we can use the `GETIFNOTMATCH` [here](../commands/garnet-specific-commands#getifnotmatch) Garnet API to only retrieve the entire payload from the server when the value on the server has changed from what we had retrieved last. Server-1 can use application memory to store the value of the ETag it recieved in the initial payload, and employ the `GETIFNOTMATCH` api and refresh the latest in-memory and etag of the object in it's local state.

In a read heavy systems where the items are read very frequently but not updated as frequently using the above technique can help reduce your network bandwidth utilization. I recommend that in the case where you know the value of your key will be changed very heavily, you should continue to use the regular `GET` call since the updated the data will always need to be sent by the cache.

Take a look at our ETag caching sample to see the usage of the `GETIFNOTMATCH` API in action.

## Avoiding Costly Transactions When Working With Non-Atomic Operations

Any ACI( ignore the D) compliant database that gives you the ability to data transactions has some level of non-trivial synchronization that it creates. Garnet too while having state of the art transaction concurrency control does depend on Locks via 2 phase locking to be able to isolate transactions from each other. On top of the use of locks Transactions are not allowed to run for the initial phase of a server when concurrently using the Checkpointing mechanism for durability.

ETags provide a way to handle logic that could previously only been provided either by you making a Garnet server side change to create custom commands or to employ the use of Transactions and Transaction Procedures in Garnet. ETag semantics let multiple clients co-ordinate their update logic without the use of locking and ensuring no-missed updates. Let's see how.

Imagine a scenario where we have multiple clients actively mutating an XML(could be any non-atomic value like JSON or even protobufs) field that they store in Garnet.

Client 1 read the XML value for a key, updates field A and writes back. Client 2 also concurrently reads the XML from Garnet for the same key, updates a different field and writes back.

Now in our concurrent scenario we have multiple different interleavings that exist between the time we read the value, modify it, and write back. Depending on how these interleavings happen we could overwrite, double write, or by chance even end up with the correct value for the XML. This is by definition a race condition. Below is an example sequence showing an unwanted interleaving.

1. Client 1 read value for k1 at v0
2. Client 1 mutate value at v0 for field A and now we v0 is still on server but locally we have v1`
3. Client 2 reads the value for k1 at v0 since client 1 has still not written it's value at v1 to Garnet
4. Client 2 mutates value for v0 for field B and now we have v0 at v1`` but there are client 1 and client 2 hold 2 different copies of data. The mutated copy of v0 on client 1 does not have the mutation on field A that client 1 makes, and vice versa for client 2.
5. Now based on who writes back the server first, the last write on the server will be the v1 that exists but either way we will miss the update from eiter client 1 or client 2!

To eliminate this race condition we can make use of the ETag semantics, let's create an ETag for our existing key value pair in Garnet.

Using the `SETIFMATCH`[here](../docs/commands/garnet-specific-commands#setifmatch) API for our updates to the value we can create a Compare And Swap like loop that guarantees no lost updates.

The code snippets shown at the bottom, taken from the ETag samples illustrate this well.

Every read-(extra logic/modify)-write call starts by first reading the latest etag and value for a key, it then wraps it's update logic in a callback action and then calls the `PerformLockFreeSafeUpdate` method in ETagAbstractions to safely apply the update.

Internally the `PerformLockFreeSafeUpdate` runs a loop that retrieves the data that performs your update on the object and sends a `SETIFMATCH` request, the server only then updates the value if your ETag indicates that at the time of your decision you had performed your update on the latest copy of the data. If the server sees that between your read and write there were any updates the value, the server sends the latest copy of the data along with the updated etag, your client code then reapplies the changes on the latest copy and resends the request back to the server for the update, this form of update will guarantees that eventually all changes synchronize themselves on the server one after other.

In a read-heavy system where contention is not high on the same key this update will be performed in the very first loop itself, and be easier to manage than having a custom transaction. However, in a heavy key contention scenario this could result in multiple attempts to write to the latest copy especially if the logic between your read and write is slow.

```
static async Task Client(string userKey)
{
    Random random = new Random();
    using var redis = await ConnectionMultiplexer.ConnectAsync(GarnetConnectionStr);
    var db = redis.GetDatabase(0);

    // initially read latest etag
    var res = await EtagAbstractions.GetWithEtag<ContosoUserInfo>(userKey)
    long etag = (long)res[0];
    ContosoUserInfo userInfo = res[1];
    while (true)
    {
        token.ThrowIfCancellationRequested();
        // 
        (etag, userInfo) = await ETagAbstractions.PerformLockFreeSafeUpdate<ContosoUserInfo>(db, userKey, etag, userInfo, (ContosoUserInfo info) =>
        {
            info.TooManyCats = info.NumberOfCats % 5 == 0;
        });
        await Task.Delay(TimeSpan.FromSeconds(random.Next(0, 15)), token);
    }
}

....
....
....
....

/// <summary>
/// Retrieves an item from the database along with its ETag.
/// </summary>
/// <typeparam name="T">The type of the item to retrieve.</typeparam>
/// <param name="db">The database instance to query.</param>
/// <param name="key">The key of the item to retrieve.</param>
/// <returns>
/// A tuple containing the ETag as a long and the item casted to type T.
/// If the database call returns null, the ETag will be -1 and the item will be null.
/// </returns>
public static async Task<(long, T?)> GetWithEtag<T>(IDatabase db, string key)
{
    var executeResult = await db.ExecuteAsync("GETWITHETAG", key);
    // If key is not found we get null
    if (executeResult.IsNull)
    {
        return (-1, default(T));
    }

    RedisResult[] result = (RedisResult[])executeResult!;
    long etag = (long)result[0];
    T item = JsonSerializer.Deserialize<T>((string)result[1]!)!;
    return (etag, item);
}

/// <summary>
/// Performs a lock-free update on an item in the database using a compare-and-swap mechanism.
/// </summary>
/// <typeparam name="T">The type of the item to be updated.</typeparam>
/// <param name="db">The database instance where the item is stored.</param>
/// <param name="key">The key identifying the item in the database.</param>
/// <param name="initialEtag">The initial ETag value of the item.</param>
/// <param name="initialItem">The initial state of the item.</param>
/// <param name="updateAction">The action to perform on the item before updating it in the database.</param>
/// <returns>A task that represents the asynchronous operation. The task result contains a tuple with the final ETag value and the updated item.</returns>
public static async Task<(long, T)> PerformLockFreeSafeUpdate<T>(IDatabase db, string key, long initialEtag, T initialItem, Action<T> updateAction)
{
    // Compare and Swap Updating
    long etag = initialEtag;
    T item = initialItem;
    while (true)
    {
        // perform custom action, since item is updated to it's correct latest state by the server this action is performed exactly once on
        // an item before it is finally updated on the server
        updateAction(item);

        var (updatedSuccesful, newEtag, newItem) = await _updateItemIfMatch(db, etag, key, item);
        etag = newEtag;
        item = newItem;

        if (updatedSuccesful)
            break;
    }

    return (etag, item);
}

private static async Task<(bool updated, long etag, T)> _updateItemIfMatch<T>(IDatabase db, long etag, string key, T value)
{
    // You may notice the "!" that is because we know that SETIFMATCH doesn't return null
    string serializedItem = JsonSerializer.Serialize<T>(value);
    RedisResult[] res = (RedisResult[])(await db.ExecuteAsync("SETIFMATCH", key, serializedItem, etag))!;

    if (res[1].IsNull)
        return (true, (long)res[0], value);

    T deserializedItem = JsonSerializer.Deserialize<T>((string)res[1]!)!;
    return (false, (long)res[0], deserializedItem);
}

```

Please refer to the Garnet Samples to see complete examples.

ETags are not a silver bullet to your transaction overhead. However, in a system where the lock contention on the same key is low, ETags can be used to avoid transaction locking overhead as well as reduce network bandwidth usage.
