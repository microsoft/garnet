---
slug: etags-when-and-how
title: ETags, When and How
authors: hkhalid 
tags: [garnet, concurrency, caching, lock-free, etags]
---

**Garnet recently announced native support for ETag-based commands.**  
ETags are a powerful feature that enable techniques such as **Optimistic Concurrency Control** and **more efficient network bandwidth utilization for caching.**

Currently, Garnet provides ETags as raw strings. This feature is available without requiring any migration, allowing your existing key-value pairs to start leveraging ETags immediately without impacting performance metrics.  
You can find the [ETag API documentation here](/docs/commands/garnet-specific-commands#native-etag-support).

This article explores when and how you can use this new Garnet feature for both your current and future applications.

<!--truncate-->

---

## Why Read This Article?  
If you're looking to:  

1. **Reduce network bandwidth utilization for caching.**  
2. **Avoid the cost of transactions when working with non-atomic values in your cache store.**

We'll cover these scenarios case by case.


---

## Reducing Network Bandwidth Utilization for Caching  

Every network call incurs a cost: the amount of data transmitted and the distance over which it travels. In performance-sensitive scenarios, it's beneficial to fetch data only if it has changed in the cache, thereby reducing bandwidth usage and network latency.  

### Scenario: Cache Invalidation 
Consider the following setup:  

#### High-Level Diagram  
```
(server 1)----[server 1 reads from cache]----(cache)
                                                |
                                                |
                                    [server 2 writes to the cache, invalidating whatever server 1 had read]
                                                |
                                                |
                                            (server 2)
```

#### Sequence Diagram  
```
    server 1                                            cache                              server 2
1. initial read from cache for k1----------------------->
2.        <----------------------------------------Send Data and ETag
                                                        <-------------------------update value for k1 (invalidates k1)
3. second read to cache for k1 ------------------------->
                <--------------------------------------(What is sent back?)
```

In the absence of ETags, the entire payload for `k1` is returned on every read, regardless of whether the value associated with `k1` has changed.  

While this might not matter when transferring small payloads (e.g., 100 bytes of data within a high-bandwidth local network), it becomes significant when you have **multiple machines egressing larger payloads (e.g., 1MB each)** on a cloud provider. You pay the cost of egress, bandwidth usage, and experience delays due to the transmission of larger amounts of data.  

To address this, Garnet provides the `GETIFNOTMATCH` API [here](/docs/commands/garnet-specific-commands#getifnotmatch).
, allowing you to fetch data only if it has changed since your last retrieval. Server 1 can store the ETag received in the initial payload in application memory and use `GETIFNOTMATCH` to refresh the local copy only if the value has changed.

This approach is particularly beneficial in read-heavy systems where data changes infrequently. However, for frequently updated keys, using the regular `GET` API may still be preferable, as updated data will always need to be transmitted.  

Take a look at the ETag caching sample to see the usage of the `GETIFNOTMATCH` API in action.

---

## Avoiding Costly Transactions When Working with Non-Atomic Operations

Databases with ACID compliance (ignoring the durability for this discussion) rely on synchronization mechanisms like locks to ensure isolation. Garnet employs **state-of-the-art transaction concurrency control** using two-phase locking. However, transactions in Garnet are not permitted during certain initial server states, such as when the checkpointing mechanism is active for durability.

ETags offer an alternative to transactions when working with a single key for handling the update logic, enabling coordination between multiple clients without locking while ensuring no missed updates.  

### Scenario: Concurrent Updates to a Non-Atomic Value  

Imagine multiple clients concurrently modifying an XML document stored in Garnet.  
For example:  

- Client 1 reads the XML, updates Field A, and writes it back.
- Client 2 reads the same XML, updates Field B, and writes it back concurrently.  

Without ETags, the following sequence of events might occur:  

1. Client 1 reads value `v0` for key `k1`.  
2. Client 1 modifies Field A, creating a local copy `v1`.  
3. Client 2 reads the same value `v0` before Client 1 writes `v1`.  
4. Client 2 modifies Field B, creating another local copy `v2`.  
5. Either Client 1 or Client 2 writes its version back to the server, potentially overwriting the other’s changes since `v1` and `v2` both don't have either's changes.

This race condition results in lost updates.  

With ETags, you can use the `SETIFMATCH` API [here](/docs/commands/garnet-specific-commands#setifmatch) to implement a **compare-and-swap** mechanism that guarantees no updates are lost. The following code snippets demonstrate how this can be achieved.

---

### Example Code  

```csharp
static async Task Client(string userKey)
{
    Random random = new Random();
    using var redis = await ConnectionMultiplexer.ConnectAsync(GarnetConnectionStr);
    var db = redis.GetDatabase(0);

    // Initially read the latest ETag
    var res = await EtagAbstractions.GetWithEtag<ContosoUserInfo>(userKey);
    long etag = res.Item1;
    ContosoUserInfo userInfo = res.Item2;
    
    while (true)
    {
        token.ThrowIfCancellationRequested();
        (etag, userInfo) = await ETagAbstractions.PerformLockFreeSafeUpdate<ContosoUserInfo>(
            db, userKey, etag, userInfo, (ContosoUserInfo info) =>
        {
            info.TooManyCats = info.NumberOfCats % 5 == 0;
        });

        await Task.Delay(TimeSpan.FromSeconds(random.Next(0, 15)), token);
    }
}
```

#### Supporting Methods  

```csharp
public static async Task<(long, T?)> GetWithEtag<T>(IDatabase db, string key)
{
    var executeResult = await db.ExecuteAsync("GETWITHETAG", key);
    if (executeResult.IsNull) return (-1, default(T));

    RedisResult[] result = (RedisResult[])executeResult!;
    long etag = (long)result[0];
    T item = JsonSerializer.Deserialize<T>((string)result[1]!)!;
    return (etag, item);
}

public static async Task<(long, T)> PerformLockFreeSafeUpdate<T>(IDatabase db, string key, long initialEtag, T initialItem, Action<T> updateAction)
{
    long etag = initialEtag;
    T item = initialItem;

    while (true)
    {
        updateAction(item);

        var (updated, newEtag, newItem) = await _updateItemIfMatch(db, etag, key, item);
        etag = newEtag;
        item = newItem;

        if (updated) break;
    }

    return (etag, item);
}

private static async Task<(bool, long, T)> _updateItemIfMatch<T>(IDatabase db, long etag, string key, T value)
{
    string serializedItem = JsonSerializer.Serialize(value);
    RedisResult[] res = (RedisResult[])(await db.ExecuteAsync("SETIFMATCH", key, serializedItem, etag))!;

    if (res[1].IsNull) return (true, (long)res[0], value);

    T deserializedItem = JsonSerializer.Deserialize<T>((string)res[1]!)!;
    return (false, (long)res[0], deserializedItem);
}
```

Every read-(extra logic/modify)-write call starts by first reading the latest etag and value for a key using `GETWITHETAG` [here](/docs/commands/garnet-specific-commands#getwithetag), it then wraps it's update logic in a callback action and then calls the `PerformLockFreeSafeUpdate` method in `ETagAbstractions` to safely apply the update.

Internally the `PerformLockFreeSafeUpdate` method runs a loop that retrieves the data that performs your update on the object and sends a `SETIFMATCH` request, the server only then updates the value if your ETag indicates that at the time of your decision you had performed your update on the latest copy of the data. If the server sees that between your read and write there were any updates the value, the server sends the latest copy of the data along with the updated etag, your client code then reapplies the changes on the latest copy and resends the request back to the server for the update, this form of update will guarantees that eventually all changes synchronize themselves on the server one after other.

In a read-heavy system where contention is not high on the same key this update will be performed in the very first loop itself, and be easier to manage than having a custom transaction. However, in a heavy key contention scenario this could result in multiple attempts to write to the latest copy especially if the logic between your read and write is slow.

---

ETags are not a silver bullet. However, in low contention scenarios, they reduce transaction overhead and network bandwidth usage, offering a lightweight alternative to traditional locking mechanisms.