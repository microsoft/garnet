// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ETag;

public static class ETagAbstractions
{
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
            // an item before it is finally updated on the server.
            // NOTE: Based on your application's needs you can modify this method to update a pure function that returns a copy of the data and does not use mutations as side effects.
            updateAction(item);
            var (updatedSuccesful, newEtag, newItem) = await _updateItemIfMatch(db, etag, key, item);
            etag = newEtag;
            if (!updatedSuccesful)
                item = newItem!;
            else
                break;
        }

        return (etag, item);
    }

    /// <summary>
    /// Retrieves an item from the database if the provided ETag does not match the existing ETag.
    /// Saves the network badwidth usage for cases where we have the right state in-memory already
    /// </summary>
    /// <typeparam name="T">The type of the item to be retrieved.</typeparam>
    /// <param name="db">The database instance to execute the command on.</param>
    /// <param name="key">The key of the item to be retrieved.</param>
    /// <param name="existingEtag">The existing ETag to compare against.</param>
    /// <returns>
    /// A tuple containing the new ETag and the item if the ETag does not match; otherwise, a tuple with -1 and the default value of T.
    /// </returns>
    public static async Task<(long, T?)> GetIfNotMatch<T>(IDatabase db, string key, long existingEtag, T existingItem, ILogger? logger = null)
    {
        RedisResult res = await db.ExecuteAsync("GETIFNOTMATCH", key, existingEtag);
        if (res.IsNull)
            return (-1, default);

        long etag = (long)res[0];

        if (res[1].IsNull)
        {
            logger?.LogInformation("Network overhead saved, what we have is already good.");
            return (etag, existingItem);
        }

        logger?.LogInformation("Network overhead incurred, entire item retrieved over network.");
        T item = JsonSerializer.Deserialize<T>((string)res[1]!)!;
        return (etag, item);
    }

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
            return (-1, default(T));

        RedisResult[] result = (RedisResult[])executeResult!;
        long etag = (long)result[0];
        T item = JsonSerializer.Deserialize<T>((string)result[1]!)!;
        return (etag, item);
    }

    private static async Task<(bool updated, long etag, T)> _updateItemIfMatch<T>(IDatabase db, long etag, string key, T value)
    {
        string serializedItem = JsonSerializer.Serialize<T>(value);
        RedisResult[] res = (RedisResult[])(await db.ExecuteAsync("SETIFMATCH", key, serializedItem, etag))!;
        // successful update does not return updated value so we can just return what was passed for value. 
        if (res[1].IsNull)
            return (true, (long)res[0], value);

        T deserializedItem = JsonSerializer.Deserialize<T>((string)res[1]!)!;

        return (false, (long)res[0], deserializedItem);
    }
}