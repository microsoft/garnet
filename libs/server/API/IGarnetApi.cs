// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Interface for Garnet API
    /// </summary>
    public interface IGarnetApi : IGarnetReadApi, IGarnetAdvancedApi
    {
        #region GETEX

        /// <summary>
        /// GETEX
        /// </summary>
        GarnetStatus GETEX(PinnedSpanByte key, ref StringInput input, ref StringOutput output);

        #endregion

        #region SET
        /// <summary>
        /// SET
        /// </summary>
        GarnetStatus SET(PinnedSpanByte key, PinnedSpanByte value);

        /// <summary>
        /// SET
        /// </summary>
        GarnetStatus SET(PinnedSpanByte key, ref StringInput input, PinnedSpanByte value);

        /// <summary>
        /// SET Conditional
        /// </summary>
        GarnetStatus SET_Conditional(PinnedSpanByte key, ref StringInput input);

        /// <summary>
        /// DEL Conditional
        /// </summary>
        GarnetStatus DEL_Conditional(PinnedSpanByte key, ref StringInput input);

        /// <summary>
        /// SET Conditional
        /// </summary>
        GarnetStatus SET_Conditional(PinnedSpanByte key, ref StringInput input, ref StringOutput output);

        /// <summary>
        /// SET
        /// </summary>
        GarnetStatus SET(PinnedSpanByte key, Memory<byte> value);

        /// <summary>
        /// SET
        /// </summary>
        GarnetStatus SET(PinnedSpanByte key, IGarnetObject value);

        /// <summary>
        /// SET
        /// </summary>
        GarnetStatus SET<TSourceLogRecord>(in TSourceLogRecord srcLogRecord) where TSourceLogRecord : ISourceLogRecord;

        /// <summary>
        /// SET
        /// </summary>
        GarnetStatus SET<TSourceLogRecord>(PinnedSpanByte key, ref UnifiedInput input, in TSourceLogRecord srcLogRecord) where TSourceLogRecord : ISourceLogRecord;

        #endregion

        #region SETEX
        /// <summary>
        /// SETEX
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <param name="expiryMs">Expiry in milliseconds, formatted as ASCII digits</param>
        /// <returns></returns>
        GarnetStatus SETEX(PinnedSpanByte key, PinnedSpanByte value, PinnedSpanByte expiryMs);

        /// <summary>
        /// SETEX
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <param name="expiry">Expiry</param>
        GarnetStatus SETEX(PinnedSpanByte key, PinnedSpanByte value, TimeSpan expiry);

        #endregion

        #region SETRANGE

        /// <summary>
        /// SETRANGE
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input"></param>
        /// <param name="output">The output of the operation</param>
        /// <returns></returns>
        GarnetStatus SETRANGE(PinnedSpanByte key, ref StringInput input, ref PinnedSpanByte output);


        #endregion

        #region MSETNX
        /// <summary>
        /// MSETNX
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        GarnetStatus MSET_Conditional(ref StringInput input);
        #endregion

        #region APPEND

        /// <summary>
        /// APPEND command
        /// </summary>
        /// <param name="key">Key whose value is to be appended</param>
        /// <param name="input"></param>
        /// <param name="output">Length of updated value</param>
        /// <returns>Operation status</returns>
        GarnetStatus APPEND(PinnedSpanByte key, ref StringInput input, ref StringOutput output);

        /// <summary>
        /// APPEND command
        /// </summary>
        /// <param name="key">Key whose value is to be appended</param>
        /// <param name="value">Value to be appended</param>
        /// <param name="output">Length of updated value</param>
        /// <returns>Operation status</returns>
        GarnetStatus APPEND(PinnedSpanByte key, PinnedSpanByte value, ref PinnedSpanByte output);
        #endregion

        #region RENAME
        /// <summary>
        /// RENAME
        /// </summary>
        /// <param name="oldKey">The old key to be renamed.</param>
        /// <param name="newKey">The new key name.</param>
        /// <param name="withEtag">Whether to include the ETag in the operation</param>
        /// <returns></returns>
        GarnetStatus RENAME(PinnedSpanByte oldKey, PinnedSpanByte newKey, bool withEtag = false);

        /// <summary>
        /// Renames key to newkey if newkey does not yet exist. It returns an error when key does not exist.
        /// </summary>
        /// <param name="oldKey">The old key to be renamed.</param>
        /// <param name="newKey">The new key name.</param>
        /// <param name="result">The result of the operation.</param>
        /// <param name="withEtag">Whether to include the ETag in the operation</param>
        /// <returns></returns>
        GarnetStatus RENAMENX(PinnedSpanByte oldKey, PinnedSpanByte newKey, out int result, bool withEtag = false);
        #endregion

        #region EXISTS

        /// <summary>
        /// EXISTS
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus EXISTS(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output);

        /// <summary>
        /// EXISTS
        /// </summary>
        /// <param name="key">Key</param>
        /// <returns></returns>
        GarnetStatus EXISTS(PinnedSpanByte key);

        #endregion

        #region EXPIRE
        /// <summary>
        /// Set a timeout on key
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="expiryMs">Expiry in milliseconds, formatted as ASCII digits</param>
        /// <param name="timeoutSet">Whether timeout was set by the call</param>
        /// <param name="expireOption">Expire option</param>
        /// <returns></returns>
        GarnetStatus EXPIRE(PinnedSpanByte key, PinnedSpanByte expiryMs, out bool timeoutSet, ExpireOption expireOption = ExpireOption.None);

        /// <summary>
        /// Set a timeout on key using a timeSpan in seconds
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus EXPIRE(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output);

        /// <summary>
        /// Set a timeout on key using a timeSpan in seconds
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="expiry">Expiry in TimeSpan</param>
        /// <param name="timeoutSet">Whether timeout was set by the call</param>
        /// <param name="expireOption">Expire option</param>
        /// <returns></returns>
        GarnetStatus EXPIRE(PinnedSpanByte key, TimeSpan expiry, out bool timeoutSet, ExpireOption expireOption = ExpireOption.None);
        #endregion

        #region EXPIREAT

        /// <summary>
        /// Set a timeout on key using absolute Unix timestamp (seconds since January 1, 1970) in seconds
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="expiryTimestamp">Absolute Unix timestamp in seconds</param>
        /// <param name="timeoutSet">Whether timeout was set by the call</param>
        /// <param name="expireOption">Expire option</param>
        /// <returns></returns>
        GarnetStatus EXPIREAT(PinnedSpanByte key, long expiryTimestamp, out bool timeoutSet, ExpireOption expireOption = ExpireOption.None);

        /// <summary>
        /// Set a timeout on key using absolute Unix timestamp (seconds since January 1, 1970) in milliseconds
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="expiryTimestamp">Absolute Unix timestamp in milliseconds</param>
        /// <param name="timeoutSet">Whether timeout was set by the call</param>
        /// <param name="expireOption">Expire option</param>
        /// <returns></returns>
        GarnetStatus PEXPIREAT(PinnedSpanByte key, long expiryTimestamp, out bool timeoutSet, ExpireOption expireOption = ExpireOption.None);

        #endregion

        #region PERSIST

        /// <summary>
        /// PERSIST
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus PERSIST(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output);
        #endregion

        #region Increment (INCR, INCRBY, DECR, DECRBY)
        /// <summary>
        /// Increment (INCR, INCRBY, INCRBYFLOAT, DECR, DECRBY)
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus Increment(PinnedSpanByte key, ref StringInput input, ref PinnedSpanByte output);

        /// <summary>
        /// Increment (INCR, INCRBY)
        /// </summary>
        /// <param name="key"></param>
        /// <param name="output"></param>
        /// <param name="incrementCount"></param>
        /// <returns></returns>
        GarnetStatus Increment(PinnedSpanByte key, out long output, long incrementCount = 1);

        /// <summary>
        /// Decrement (DECR, DECRBY)
        /// </summary>
        /// <param name="key"></param>
        /// <param name="output"></param>
        /// <param name="decrementCount"></param>
        /// <returns></returns>
        GarnetStatus Decrement(PinnedSpanByte key, out long output, long decrementCount = 1);

        /// <summary>
        /// Increment by float (INCRBYFLOAT)
        /// </summary>
        /// <param name="key"></param>
        /// <param name="val"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus IncrementByFloat(PinnedSpanByte key, ref PinnedSpanByte output, double val);

        /// <summary>
        /// Increment by float (INCRBYFLOAT)
        /// </summary>
        /// <param name="key"></param>
        /// <param name="output"></param>
        /// <param name="val"></param>
        /// <returns></returns>
        GarnetStatus IncrementByFloat(PinnedSpanByte key, out double output, double val);
        #endregion

        #region DELETE

        /// <summary>
        /// Deletes a key from the unified store
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        GarnetStatus DELETE(PinnedSpanByte key);

        /// <summary>
        /// Deletes a key if it is in memory and expired
        /// </summary>
        GarnetStatus DELIFEXPIM(PinnedSpanByte key);

        #endregion

        #region GETDEL
        /// <summary>
        /// GETDEL
        /// </summary>
        /// <param name="key"> Key to get and delete </param>
        /// <param name="output"> Current value of key </param>
        /// <returns> Operation status </returns>
        GarnetStatus GETDEL(PinnedSpanByte key, ref StringOutput output);
        #endregion

        #region TYPE

        /// <summary>
        /// Returns the string representation of the type of the value stored at key.
        /// string, list, set, zset, and hash.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus TYPE(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output);

        #endregion

        #region MEMORY

        /// <summary>
        ///  Gets the number of bytes that a key and its value require to be stored in RAM.
        /// </summary>
        /// <param name="key">Name of the key or object to get the memory usage</param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns>GarnetStatus</returns>
        GarnetStatus MEMORYUSAGE(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output);

        #endregion

        #region SortedSet Methods

        /// <summary>
        /// Adds the specified member with the specified score to the sorted set stored at key.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="score">Score</param>
        /// <param name="member">Member</param>
        /// <param name="zaddCount">Number of adds performed</param>
        /// <returns></returns>
        GarnetStatus SortedSetAdd(PinnedSpanByte key, PinnedSpanByte score, PinnedSpanByte member, out int zaddCount);

        /// <summary>
        /// Adds all the specified members with the specified scores to the sorted set stored at key.
        /// Current members get the score updated and reordered.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="inputs">Input key-value pairs to add</param>
        /// <param name="zaddCount">Number of adds performed</param>
        /// <returns></returns>
        GarnetStatus SortedSetAdd(PinnedSpanByte key, (PinnedSpanByte score, PinnedSpanByte member)[] inputs, out int zaddCount);

        /// <summary>
        /// Adds all the specified members with the specified scores to the sorted set stored at key.
        /// Current members get the score updated and reordered.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetAdd(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Stores a range of sorted set elements in the specified key space.
        /// </summary>
        /// <param name="dstKey">The distribution key for the sorted set.</param>
        /// <param name="srcKey">The sub-key for the sorted set.</param>
        /// <param name="input">The input object containing the elements to store.</param>
        /// <param name="result">The result of the store operation.</param>
        /// <returns>A <see cref="GarnetStatus"/> indicating the status of the operation.</returns>
        GarnetStatus SortedSetRangeStore(PinnedSpanByte dstKey, PinnedSpanByte srcKey, ref ObjectInput input, out int result);

        /// <summary>
        /// Removes the specified member from the sorted set stored at key.
        /// </summary>
        GarnetStatus SortedSetRemove(PinnedSpanByte key, PinnedSpanByte member, out int zremCount);

        /// <summary>
        /// Removes the specified members from the sorted set stored at key.
        /// Non existing members are ignored.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="members">Input members to remove</param>
        /// <param name="zremCount">Number of removes performed</param>
        /// <returns></returns>
        GarnetStatus SortedSetRemove(PinnedSpanByte key, PinnedSpanByte[] members, out int zremCount);

        /// <summary>
        /// Removes the specified members from the sorted set stored at key.
        /// Non existing members are ignored.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRemove(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output);

        /// <summary>
        /// Removes all elements in the sorted set between the
        /// lexicographical range specified by min and max.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRemoveRangeByLex(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output);

        /// <summary>
        /// Removes and returns the first element from the sorted set stored at key,
        /// with the scores ordered from low to high (min) or high to low (max).
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetPop(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Removes and returns multiple elements from a sorted set.
        /// </summary>
        /// <param name="keys">The keys of the sorted set.</param>
        /// <param name="count">The number of elements to pop.</param>
        /// <param name="lowScoresFirst">If true, elements with the lowest scores are popped first; otherwise, elements with the highest scores are popped first.</param>
        /// <param name="poppedKey">The key of the popped element.</param>
        /// <param name="pairs">An array of tuples containing the member and score of each popped element.</param>
        /// <returns>A <see cref="GarnetStatus"/> indicating the result of the operation.</returns>
        GarnetStatus SortedSetMPop(ReadOnlySpan<PinnedSpanByte> keys, int count, bool lowScoresFirst, out PinnedSpanByte poppedKey, out (PinnedSpanByte member, PinnedSpanByte score)[] pairs);

        /// <summary>
        /// Removes and returns up to count members with the highest or lowest scores in the sorted set stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="pairs"></param>
        /// <param name="count"></param>
        /// <param name="lowScoresFirst">When true, return the members with the lowest scores, otherwise return the highest scores.</param>
        /// <returns></returns>
        GarnetStatus SortedSetPop(PinnedSpanByte key, out (PinnedSpanByte member, PinnedSpanByte score)[] pairs, int count = 1, bool lowScoresFirst = true);

        /// <summary>
        /// Increments the score of member in the sorted set stored at key by increment.
        /// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetIncrement(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Increments the score of member in the sorted set stored at key by increment.
        /// Returns the new score of member.
        /// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
        /// </summary>
        /// <param name="key"></param>
        /// <param name="increment"></param>
        /// <param name="member"></param>
        /// <param name="newScore"></param>
        /// <returns></returns>
        GarnetStatus SortedSetIncrement(PinnedSpanByte key, double increment, PinnedSpanByte member, out double newScore);

        /// <summary>
        /// ZREMRANGEBYRANK: Removes all elements in the sorted set stored at key with rank between start and stop.
        /// Both start and stop are 0 -based indexes with 0 being the element with the lowest score.
        /// ZREMRANGEBYSCORE: Removes all elements in the sorted set stored at key with a score between min and max (inclusive by default).
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRemoveRange(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Removes all elements in the range specified by min and max, having the same score.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="min"></param>
        /// <param name="max"></param>
        /// <param name="countRemoved"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRemoveRangeByLex(PinnedSpanByte key, string min, string max, out int countRemoved);

        /// <summary>
        /// Removes all elements that have a score in the range specified by min and max.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="min"></param>
        /// <param name="max"></param>
        /// <param name="countRemoved"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRemoveRangeByScore(PinnedSpanByte key, string min, string max, out int countRemoved);

        /// <summary>
        /// Removes all elements with the index in the range specified by start and stop.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="start"></param>
        /// <param name="stop"></param>
        /// <param name="countRemoved"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRemoveRangeByRank(PinnedSpanByte key, int start, int stop, out int countRemoved);

        /// <summary>
        /// Computes the difference between the first and all successive sorted sets and store resulting pairs in the output key.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="destinationKey"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        GarnetStatus SortedSetDifferenceStore(PinnedSpanByte destinationKey, ReadOnlySpan<PinnedSpanByte> keys, out int count);

        /// <summary>
        /// Adds geospatial items (longitude, latitude, name) to the specified key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus GeoAdd(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Geospatial search and store in destination key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="destinationKey"></param>
        /// <param name="opts"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus GeoSearchStore(PinnedSpanByte key, PinnedSpanByte destinationKey, ref GeoSearchOptions opts,
                                    ref ObjectInput input, ref SpanByteAndMemory output);

        /// <summary>
        /// Intersects multiple sorted sets and stores the result in the destination key.
        /// </summary>
        /// <param name="destinationKey">The key where the result will be stored.</param>
        /// <param name="keys">The keys of the sorted sets to intersect.</param>
        /// <param name="weights">The weights to apply to each sorted set during the intersection.</param>
        /// <param name="aggregateType">The type of aggregation to use for the intersection.</param>
        /// <param name="count">The number of elements in the resulting sorted set.</param>
        /// <returns>A <see cref="GarnetStatus"/> indicating the status of the operation.</returns>
        GarnetStatus SortedSetIntersectStore(PinnedSpanByte destinationKey, ReadOnlySpan<PinnedSpanByte> keys, double[] weights, SortedSetAggregateType aggregateType, out int count);

        /// <summary>
        /// Performs a union of multiple sorted sets and stores the result in the destination key.
        /// </summary>
        /// <param name="destinationKey">The key where the result will be stored.</param>
        /// <param name="keys">The keys of the sorted sets to union.</param>
        /// <param name="count">The number of elements in the resulting sorted set.</param>
        /// <param name="weights">Optional weights to apply to each sorted set.</param>
        /// <param name="aggregateType">The type of aggregation to perform (e.g., Sum, Min, Max).</param>
        /// <returns>A <see cref="GarnetStatus"/> indicating the status of the operation.</returns>
        GarnetStatus SortedSetUnionStore(PinnedSpanByte destinationKey, ReadOnlySpan<PinnedSpanByte> keys, double[] weights, SortedSetAggregateType aggregateType, out int count);

        /// <summary>
        /// Sets an expiration time on a sorted set member.
        /// </summary>
        /// <param name="key">The key of the sorted set.</param>
        /// <param name="input">The input object containing additional parameters.</param>
        /// <param name="output">The output object to store the result.</param>
        /// <returns>The status of the operation.</returns>
        GarnetStatus SortedSetExpire(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Sets an expiration time on a sorted set member.
        /// </summary>
        /// <param name="key">The key of the sorted set.</param>
        /// <param name="members">The members to set expiration for.</param>
        /// <param name="expireAt">The expiration time.</param>
        /// <param name="expireOption">The expiration option to apply.</param>
        /// <param name="results">The results of the operation.</param>
        /// <returns>The status of the operation.</returns>
        GarnetStatus SortedSetExpire(PinnedSpanByte key, ReadOnlySpan<PinnedSpanByte> members, DateTimeOffset expireAt, ExpireOption expireOption, out int[] results);

        /// <summary>
        /// Persists the specified sorted set member, removing any expiration time set on it.
        /// </summary>
        /// <param name="key">The key of the sorted set to persist.</param>
        /// <param name="input">The input object containing additional parameters.</param>
        /// <param name="output">The output object to store the result.</param>
        /// <returns>The status of the operation.</returns>
        GarnetStatus SortedSetPersist(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Persists the specified sorted set members, removing any expiration time set on them.
        /// </summary>
        /// <param name="key">The key of the sorted set.</param>
        /// <param name="members">The members to persist.</param>
        /// <param name="results">The results of the operation.</param>
        /// <returns>The status of the operation.</returns>
        GarnetStatus SortedSetPersist(PinnedSpanByte key, ReadOnlySpan<PinnedSpanByte> members, out int[] results);

        /// <summary>
        /// Deletes already expired members from the sorted set.
        /// </summary>
        /// <param name="keys">The keys of the sorted set members to check for expiration.</param>
        /// <param name="input">The input object containing additional parameters.</param>
        /// <returns>The status of the operation.</returns>
        GarnetStatus SortedSetCollect(ReadOnlySpan<PinnedSpanByte> keys, ref ObjectInput input);

        /// <summary>
        /// Collects expired elements from the sorted set.
        /// </summary>
        /// <returns>The status of the operation.</returns>
        GarnetStatus SortedSetCollect();

        /// <summary>
        /// Collects expired elements from the sorted set for the specified keys.
        /// </summary>
        /// <param name="keys">The keys of the sorted sets to collect expired elements from.</param>
        /// <returns>The status of the operation.</returns>
        GarnetStatus SortedSetCollect(ReadOnlySpan<PinnedSpanByte> keys);

        #endregion

        #region Set Methods

        /// <summary>
        ///  Adds the specified member to the set at key.
        ///  If key does not exist, a new set is created.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="member"></param>
        /// <param name="saddCount"></param>
        /// <returns></returns>
        GarnetStatus SetAdd(PinnedSpanByte key, PinnedSpanByte member, out int saddCount);

        /// <summary>
        ///  Adds the specified members to the set at key.
        ///  Specified members that are already a member of this set are ignored.
        ///  If key does not exist, a new set is created.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="members"></param>
        /// <param name="saddCount"></param>
        /// <returns></returns>
        GarnetStatus SetAdd(PinnedSpanByte key, PinnedSpanByte[] members, out int saddCount);

        /// <summary>
        ///  Adds the specified members to the set at key.
        ///  Specified members that are already a member of this set are ignored.
        ///  If key does not exist, a new set is created.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SetAdd(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output);

        /// <summary>
        /// Removes the specified member from the set.
        /// Specified members that are not a member of this set are ignored.
        /// If key does not exist, this command returns 0.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="member"></param>
        /// <param name="sremCount"></param>
        /// <returns></returns>
        GarnetStatus SetRemove(PinnedSpanByte key, PinnedSpanByte member, out int sremCount);

        /// <summary>
        /// Removes the specified members from the set.
        /// Specified members that are not a member of this set are ignored.
        /// If key does not exist, this command returns 0.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="members"></param>
        /// <param name="sremCount"></param>
        /// <returns></returns>
        GarnetStatus SetRemove(PinnedSpanByte key, PinnedSpanByte[] members, out int sremCount);

        /// <summary>
        /// Removes the specified members from the set.
        /// Specified members that are not a member of this set are ignored.
        /// If key does not exist, this command returns 0.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SetRemove(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output);

        /// <summary>
        /// Removes and returns one random member from the set at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="member"></param>
        /// <returns></returns>
        GarnetStatus SetPop(PinnedSpanByte key, out PinnedSpanByte member);

        /// <summary>
        /// Removes and returns random members from the set at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="count"></param>
        /// <param name="members"></param>
        /// <returns></returns>
        GarnetStatus SetPop(PinnedSpanByte key, int count, out PinnedSpanByte[] members);

        /// <summary>
        /// Removes and returns random members from the set at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SetPop(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Moves a member from a source set to a destination set.
        /// If the move was performed, this command returns 1.
        /// If the member was not found in the source set, or if no operation was performed, this command returns 0.
        /// </summary>
        /// <param name="sourceKey"></param>
        /// <param name="destinationKey"></param>
        /// <param name="member"></param>
        /// <param name="smoveResult"></param>
        /// <returns></returns>
        GarnetStatus SetMove(PinnedSpanByte sourceKey, PinnedSpanByte destinationKey, PinnedSpanByte member, out int smoveResult);

        /// <summary>
        /// When called with just the key argument, return a random element from the set value stored at key.
        /// If the provided count argument is positive, return an array of distinct elements.
        /// The array's length is either count or the set's cardinality (SCARD), whichever is lower.
        /// If called with a negative count, the behavior changes and the command is allowed to return the same element multiple times.
        /// In this case, the number of returned elements is the absolute value of the specified count.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SetRandomMember(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// This command is equal to SUNION, but instead of returning the resulting set, it is stored in destination.
        /// If destination already exists, it is overwritten.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="keys"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        GarnetStatus SetUnionStore(PinnedSpanByte key, PinnedSpanByte[] keys, out int count);

        /// <summary>
        /// This command is equal to SINTER, but instead of returning the resulting set, it is stored in destination.
        /// If destination already exists, it is overwritten.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="keys"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        GarnetStatus SetIntersectStore(PinnedSpanByte key, PinnedSpanByte[] keys, out int count);

        /// <summary>
        /// This command is equal to SDIFF, but instead of returning the resulting set, it is stored in destination.
        /// If destination already exists, it is overwritten.
        /// </summary>
        /// <param name="key">destination</param>
        /// <param name="keys"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public GarnetStatus SetDiffStore(PinnedSpanByte key, PinnedSpanByte[] keys, out int count);
        #endregion

        #region List Methods

        #region ListPush Methods

        /// <summary>
        /// The command returns the index of matching elements inside a Redis list.
        /// By default, when no options are given, it will scan the list from head to tail, looking for the first match of "element".
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus ListPosition(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// ListLeftPush ArgSlice version with OutputHeader output
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus ListLeftPush(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output);

        /// <summary>
        /// ListLeftPush ArgSlice version, one element
        /// </summary>
        /// <param name="key"></param>
        /// <param name="element"></param>
        /// <param name="count"></param>
        /// <param name="whenExists">When true the operation is executed only if the key already exists</param>
        /// <returns></returns>
        GarnetStatus ListLeftPush(PinnedSpanByte key, PinnedSpanByte element, out int count, bool whenExists = false);

        /// <summary>
        /// ListLeftPush ArgSlice version for multiple values
        /// </summary>
        /// <param name="key"></param>
        /// <param name="elements"></param>
        /// <param name="count"></param>
        /// <param name="whenExists">When true the operation is executed only if the key already exists</param>
        /// <returns></returns>
        GarnetStatus ListLeftPush(PinnedSpanByte key, PinnedSpanByte[] elements, out int count, bool whenExists = false);

        /// <summary>
        /// ListRightPush ArgSlice version with OutputHeader output
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        public GarnetStatus ListRightPush(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output);

        /// <summary>
        /// ListRightPush ArgSlice version, one element
        /// </summary>
        /// <param name="key"></param>
        /// <param name="element"></param>
        /// <param name="count"></param>
        /// <param name="whenExists">When true the operation is executed only if the key already exists</param>
        /// <returns></returns>
        GarnetStatus ListRightPush(PinnedSpanByte key, PinnedSpanByte element, out int count, bool whenExists = false);

        /// <summary>
        /// ListRightPush ArgSlice version for multiple values
        /// </summary>
        /// <param name="key"></param>
        /// <param name="elements"></param>
        /// <param name="count"></param>
        /// <param name="whenExists">When true the operation is executed only if the key already exists</param>
        /// <returns></returns>
        GarnetStatus ListRightPush(PinnedSpanByte key, PinnedSpanByte[] elements, out int count, bool whenExists = false);

        #endregion

        #region ListPop Methods

        /// <summary>
        /// ListLeftPop ArgSlice version, with ObjectOutput
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus ListLeftPop(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// ListLeftPop ArgSlice version, one element
        /// </summary>
        /// <param name="key"></param>
        /// <param name="element"></param>
        /// <returns></returns>
        GarnetStatus ListLeftPop(PinnedSpanByte key, out PinnedSpanByte element);

        /// <summary>
        /// ListLeftPop ArgSlice version for multiple values
        /// </summary>
        /// <param name="key"></param>
        /// <param name="elements"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        GarnetStatus ListLeftPop(PinnedSpanByte key, int count, out PinnedSpanByte[] elements);

        /// <summary>
        /// ListLeftPop ArgSlice version for multiple keys and values
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="count"></param>
        /// <param name="key"></param>
        /// <param name="elements"></param>
        /// <returns>GarnetStatus</returns>
        GarnetStatus ListLeftPop(PinnedSpanByte[] keys, int count, out PinnedSpanByte key, out PinnedSpanByte[] elements);

        /// <summary>
        /// ListRightPop ArgSlice version, with ObjectOutput
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus ListRightPop(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// ListRightPop ArgSlice version, one element
        /// </summary>
        /// <param name="key"></param>
        /// <param name="element"></param>
        /// <returns></returns>
        GarnetStatus ListRightPop(PinnedSpanByte key, out PinnedSpanByte element);

        /// <summary>
        /// ListRightPop ArgSlice version for multiple values
        /// </summary>
        /// <param name="key"></param>
        /// <param name="elements"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        GarnetStatus ListRightPop(PinnedSpanByte key, int count, out PinnedSpanByte[] elements);


        /// <summary>
        /// ListRightPop ArgSlice version for multiple keys and values
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="count"></param>
        /// <param name="key"></param>
        /// <param name="elements"></param>
        /// <returns>GarnetStatus</returns>
        GarnetStatus ListRightPop(PinnedSpanByte[] keys, int count, out PinnedSpanByte key, out PinnedSpanByte[] elements);

        #endregion

        /// <summary>
        /// Atomically removes the first/last element of the list stored at source
        /// and pushes it to the first/last element of the list stored at destination.
        /// </summary>
        /// <param name="sourceKey"></param>
        /// <param name="destinationKey"></param>
        /// <param name="sourceDirection"></param>
        /// <param name="destinationDirection"></param>
        /// <param name="element">The element being popped and pushed</param>
        /// <returns>GarnetStatus</returns>
        public GarnetStatus ListMove(PinnedSpanByte sourceKey, PinnedSpanByte destinationKey, OperationDirection sourceDirection, OperationDirection destinationDirection, out byte[] element);

        /// <summary>
        /// Trim an existing list so it only contains the specified range of elements.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="start"></param>
        /// <param name="stop"></param>
        /// <returns></returns>
        public bool ListTrim(PinnedSpanByte key, int start, int stop);

        /// <summary>
        /// Trim an existing list so it only contains the specified range of elements.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <returns></returns>
        GarnetStatus ListTrim(PinnedSpanByte key, ref ObjectInput input);

        /// <summary>
        /// Inserts a new element in the list stored at key either before or after a value pivot
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus ListInsert(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output);

        /// <summary>
        /// Removes the first count occurrences of elements equal to element from the list.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus ListRemove(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output);

        /// <summary>
        /// Sets the list element at index to element.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus ListSet(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        #endregion

        #region Hash Methods

        /// <summary>
        /// Sets the specified field to their respective value in the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="value"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        GarnetStatus HashSet(PinnedSpanByte key, PinnedSpanByte field, PinnedSpanByte value, out int count);

        /// <summary>
        /// Sets the specified fields to their respective values in the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="elements"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        GarnetStatus HashSet(PinnedSpanByte key, (PinnedSpanByte field, PinnedSpanByte value)[] elements, out int count);

        /// <summary>
        /// Sets or updates the values of the specified fields that exist in the hash.
        /// if the Hash doesn't exist, a new a new hash is created.
        /// HashSet with nx parameter
        /// HashSet key field value
        /// HashSet key field value [field value...]
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus HashSet(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output);

        /// <summary>
        /// Set only if field does not yet exist. If key does not exist, a new key holding a hash is created.
        /// If field already exists, no action is performed.
        /// HashSet only when field does not exist
        /// </summary>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="value"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        GarnetStatus HashSetWhenNotExists(PinnedSpanByte key, PinnedSpanByte field, PinnedSpanByte value, out int count);

        /// <summary>
        /// Removes the specified field from the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="count">Number of fields removed</param>
        /// <returns></returns>
        GarnetStatus HashDelete(PinnedSpanByte key, PinnedSpanByte field, out int count);

        /// <summary>
        /// Removes the specified fields from the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="fields"></param>
        /// <param name="count">Number of fields removed</param>
        /// <returns></returns>
        GarnetStatus HashDelete(PinnedSpanByte key, PinnedSpanByte[] fields, out int count);

        /// <summary>
        /// Removes the specified fields from the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus HashDelete(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output);

        /// <summary>
        /// Increments the number stored at field in the hash key by increment parameter.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus HashIncrement(PinnedSpanByte key, PinnedSpanByte input, out OutputHeader output);

        /// <summary>
        /// Increments the number stored at field representing a floating point value
        /// in the hash key by increment parameter.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus HashIncrement(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Sets an expiration time on a hash field.
        /// </summary>
        /// <param name="key">The key of the hash.</param>
        /// <param name="input">The input object containing additional parameters.</param>
        /// <param name="output">The output object to store the result.</param>
        /// <returns>The status of the operation.</returns>
        GarnetStatus HashExpire(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Persists the specified hash key, removing any expiration time set on it.
        /// </summary>
        /// <param name="key">The key of the hash to persist.</param>
        /// <param name="input">The input object containing additional parameters.</param>
        /// <param name="output">The output object to store the result.</param>
        /// <returns>The status of the operation.</returns>
        GarnetStatus HashPersist(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Deletes already expired fields from the hash.
        /// </summary>
        /// <param name="keys">The keys of the hash fields to check for expiration.</param>
        /// <param name="input">The input object containing additional parameters.</param>
        /// <returns>The status of the operation.</returns>
        GarnetStatus HashCollect(ReadOnlySpan<PinnedSpanByte> keys, ref ObjectInput input);

        #endregion

        #region BitMaps Methods

        /// <summary>
        ///
        /// </summary>
        /// <param name="key"></param>
        /// <param name="offset"></param>
        /// <param name="bit"></param>
        /// <param name="previous"></param>
        /// <returns></returns>
        GarnetStatus StringSetBit(PinnedSpanByte key, PinnedSpanByte offset, bool bit, out bool previous);

        /// <summary>
        /// Sets or clears the bit at offset in the given key.
        /// The bit is either set or cleared depending on value, which can be either 0 or 1.
        /// When key does not exist, a new key is created.The key is grown to make sure it can hold a bit at offset.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus StringSetBit(PinnedSpanByte key, ref StringInput input, ref StringOutput output);

        /// <summary>
        /// Performs a bitwise operations on multiple keys
        /// </summary>
        /// <param name="input"></param>
        /// <param name="bitOp"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        GarnetStatus StringBitOperation(ref StringInput input, BitmapOperation bitOp, out long result);

        /// <summary>
        /// Perform a bitwise operation between multiple keys
        /// and store the result in the destination key.
        /// </summary>
        /// <param name="bitop"></param>
        /// <param name="destinationKey"></param>
        /// <param name="keys"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        GarnetStatus StringBitOperation(BitmapOperation bitop, PinnedSpanByte destinationKey, PinnedSpanByte[] keys, out long result);

        /// <summary>
        /// Performs arbitrary bitfield integer operations on strings.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="secondaryCommand"></param>
        /// <returns></returns>
        GarnetStatus StringBitField(PinnedSpanByte key, ref StringInput input, RespCommand secondaryCommand, ref StringOutput output);

        /// <summary>
        /// Performs arbitrary bitfield integer operations on strings.
        /// </summary>
        GarnetStatus StringBitField(PinnedSpanByte key, List<BitFieldCmdArgs> commandArguments, out List<long?> result);
        #endregion

        #region HyperLogLog Methods

        /// <summary>
        /// Adds all the element arguments to the HyperLogLog data structure stored at the variable name specified as key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus HyperLogLogAdd(PinnedSpanByte key, ref StringInput input, ref StringOutput output);

        /// <summary>
        /// Adds all the element arguments to the HyperLogLog data structure stored at the variable name specified as key.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="elements"></param>
        /// <param name="updated">true if at least 1 HyperLogLog internal register was altered</param>
        /// <returns></returns>
        GarnetStatus HyperLogLogAdd(PinnedSpanByte keys, string[] elements, out bool updated);

        /// <summary>
        /// Merge multiple HyperLogLog values into a unique value that will approximate the cardinality
        /// of the union of the observed Sets of the source HyperLogLog structures.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="error"></param>
        /// <returns></returns>
        GarnetStatus HyperLogLogMerge(ref StringInput input, out bool error);

        #endregion
    }

    /// <summary>
    /// Interface for Garnet API
    /// </summary>
    public interface IGarnetReadApi
    {
        #region GET
        /// <summary>
        /// GET
        /// </summary>
        GarnetStatus GET(PinnedSpanByte key, ref StringInput input, ref StringOutput output);

        /// <summary>
        /// GET
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        GarnetStatus GETForMemoryResult(PinnedSpanByte key, out MemoryResult<byte> value);

        /// <summary>
        /// GET
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        GarnetStatus GET(PinnedSpanByte key, out PinnedSpanByte value);

        /// <summary>
        /// GET
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        GarnetStatus GET(PinnedSpanByte key, out ObjectOutput value);

        /// <summary>
        /// Finds the longest common subsequence (LCS) between two keys.
        /// </summary>
        /// <param name="key1">The first key to compare.</param>
        /// <param name="key2">The second key to compare.</param>
        /// <param name="output">The output containing the LCS result.</param>
        /// <param name="lenOnly">If true, only the length of the LCS is returned.</param>
        /// <param name="withIndices">If true, the indices of the LCS in both keys are returned.</param>
        /// <param name="withMatchLen">If true, the length of each match is returned.</param>
        /// <param name="minMatchLen">The minimum length of a match to be considered.</param>
        /// <returns>The status of the operation.</returns>
        GarnetStatus LCS(PinnedSpanByte key1, PinnedSpanByte key2, ref StringOutput output, bool lenOnly = false, bool withIndices = false, bool withMatchLen = false, int minMatchLen = 0);
        #endregion

        #region GETRANGE

        /// <summary>
        /// GETRANGE
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus GETRANGE(PinnedSpanByte key, ref StringInput input, ref StringOutput output);
        #endregion

        #region TTL

        /// <summary>
        /// Returns the remaining time to live in seconds of a key that has a timeout.
        /// </summary>
        /// <param name="key">The key to return the remaining time to live in the store</param>
        /// <param name="input"></param>
        /// <param name="output">The span to allocate the output of the operation.</param>
        /// <returns></returns>
        GarnetStatus TTL(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output);

        #endregion

        #region EXPIRETIME

        /// <summary>
        /// Returns the absolute Unix timestamp (since January 1, 1970) in seconds at which the given key will expire.
        /// </summary>
        /// <param name="key">The key to get the expiration time for.</param>
        /// <param name="input"></param>
        /// <param name="output">The output containing the expiration time.</param>
        /// <returns>The status of the operation.</returns>
        GarnetStatus EXPIRETIME(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output);

        #endregion

        #region SortedSet Methods

        /// <summary>
        /// Returns the sorted set cardinality (number of elements) of the sorted set
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="zcardCount"></param>
        /// <returns></returns>
        GarnetStatus SortedSetLength(PinnedSpanByte key, out int zcardCount);

        /// <summary>
        /// Returns the sorted set cardinality (number of elements) of the sorted set
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetLength(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output);

        /// <summary>
        /// Returns the specified range of elements in the sorted set stored at key.
        /// Both start and stop are zero-based indexes, where 0 is the first element, 1 is the next element and so on.
        /// There can also be negative numbers indicating offsets from the end of the sorted set, with -1 being the last element of the sorted set, -2 the penultimate element and so on.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRange(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Returns the score of member in the sorted set at key.
        /// If member does not exist in the sorted set, or key does not exist, nil is returned.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetScore(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Returns the scores associated with the specified members in the sorted set stored at key.
        /// For every member that does not exist in the sorted set, a nil value is returned.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetScores(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Returns the number of elements in the sorted set at key with a score between min and max.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="minScore">Min Score</param>
        /// <param name="maxScore">Max score</param>
        /// <param name="numElements">Number of elements</param>
        /// <returns></returns>
        GarnetStatus SortedSetCount(PinnedSpanByte key, PinnedSpanByte minScore, PinnedSpanByte maxScore, out int numElements);

        /// <summary>
        /// Returns the number of elements in the sorted set at key with a score between min and max.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetCount(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Returns the number of elements in the sorted set with a value between min and max.
        /// When all the elements in a sorted set have the same score,
        /// this command forces lexicographical ordering.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetLengthByValue(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output);

        /// <summary>
        /// ZRANK: Returns the rank of member in the sorted set, the scores in the sorted set are ordered from low to high
        /// ZREVRANK: Returns the rank of member in the sorted set, with the scores ordered from high to low
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRank(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// ZRANK: Returns the rank of member in the sorted set, the scores in the sorted set are ordered from low to high
        /// ZREVRANK: Returns the rank of member in the sorted set, with the scores ordered from high to low
        /// </summary>
        /// <param name="key"></param>
        /// <param name="member"></param>
        /// <param name="reverse"></param>
        /// <param name="rank"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRank(PinnedSpanByte key, PinnedSpanByte member, bool reverse, out long? rank);

        /// <summary>
        /// Returns a random element from the sorted set key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRandomMember(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Returns the specified range of elements in the sorted set stored at key, using byscore, bylex and rev modifiers.
        /// Min and max are range boundaries, where 0 is the first element, 1 is the next element and so on.
        /// There can also be negative numbers indicating offsets from the end of the sorted set, with -1 being the last element of the sorted set, -2 the penultimate element and so on.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="min"></param>
        /// <param name="max"></param>
        /// <param name="sortedSetOrderOperation"></param>
        /// <param name="elements"></param>
        /// <param name="error"></param>
        /// <param name="withScores"></param>
        /// <param name="reverse"></param>
        /// <param name="limit"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRange(PinnedSpanByte key, PinnedSpanByte min, PinnedSpanByte max, SortedSetOrderOperation sortedSetOrderOperation, out PinnedSpanByte[] elements, out string error, bool withScores = false, bool reverse = false, (string, int) limit = default);

        /// <summary>
        /// Computes the difference between the first and all successive sorted sets and returns resulting pairs.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="pairs"></param>
        /// <returns></returns>
        GarnetStatus SortedSetDifference(PinnedSpanByte[] keys, out SortedSet<(double, byte[])> pairs);

        /// <summary>
        /// Performs a union of multiple sorted sets and stores the result in a dictionary.
        /// </summary>
        /// <param name="keys">A read-only span of ArgSlice representing the keys of the sorted sets to union.</param>
        /// <param name="pairs">An output sorted set where the result of the union will be stored.</param>
        /// <param name="weights">An optional array of doubles representing the weights to apply to each sorted set during the union.</param>
        /// <param name="aggregateType">The type of aggregation to use when combining scores from the sorted sets. Defaults to <see cref="SortedSetAggregateType.Sum"/>.</param>
        /// <returns>A <see cref="GarnetStatus"/> indicating the status of the operation.</returns>
        GarnetStatus SortedSetUnion(ReadOnlySpan<PinnedSpanByte> keys, double[] weights, SortedSetAggregateType aggregateType, out SortedSet<(double Element, byte[] Score)> pairs);

        /// <summary>
        /// Iterates members of SortedSet key and their associated scores using a cursor,
        /// a match pattern and count parameters
        /// </summary>
        /// <param name="key">The key of the sorted set</param>
        /// <param name="cursor">The value of the cursor</param>
        /// <param name="match">The pattern to match the members</param>
        /// <param name="count">Limit number for the response</param>
        /// <param name="items">The list of items for the response</param>
        /// <returns></returns>
        GarnetStatus SortedSetScan(PinnedSpanByte key, long cursor, string match, int count, out PinnedSpanByte[] items);

        /// <summary>
        /// Intersects multiple sorted sets and returns the result.
        /// </summary>
        /// <param name="keys">The keys of the sorted sets to intersect.</param>
        /// <param name="weights">The weights to apply to each sorted set.</param>
        /// <param name="aggregateType">The type of aggregation to perform.</param>
        /// <param name="pairs">The resulting dictionary of intersected elements and their scores.</param>
        /// <returns>A <see cref="GarnetStatus"/> indicating the status of the operation.</returns>
        GarnetStatus SortedSetIntersect(ReadOnlySpan<PinnedSpanByte> keys, double[] weights, SortedSetAggregateType aggregateType, out SortedSet<(double, byte[])> pairs);

        /// <summary>
        /// Computes the intersection of multiple sorted sets and counts the elements.
        /// </summary>
        /// <param name="keys">Input sorted set keys</param>
        /// <param name="limit">Optional max count limit</param>
        /// <param name="count">The count of elements in the intersection</param>
        /// <returns>Operation status</returns>
        GarnetStatus SortedSetIntersectLength(ReadOnlySpan<PinnedSpanByte> keys, int? limit, out int count);

        /// <summary>
        /// Returns the time to live for a sorted set members.
        /// </summary>
        /// <param name="key">The key of the sorted set.</param>
        /// <param name="input">The input object containing additional parameters.</param>
        /// <param name="output">The output object to store the result.</param>
        /// <returns>The status of the operation.</returns>
        GarnetStatus SortedSetTimeToLive(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Returns the time to live for a sorted set members.
        /// </summary>
        /// <param name="key">The key of the sorted set.</param>
        /// <param name="members">The members to get the time to live for.</param>
        /// <param name="expireIn">The output array containing the time to live for each member.</param>
        /// <returns>The status of the operation.</returns>
        GarnetStatus SortedSetTimeToLive(PinnedSpanByte key, ReadOnlySpan<PinnedSpanByte> members, out TimeSpan[] expireIn);

        #endregion

        #region Geospatial Methods

        /// <summary>
        /// GEOHASH: Returns valid Geohash strings representing the position of one or more elements in a geospatial data of the sorted set.
        /// GEODIST: Returns the distance between two members in the geospatial index represented by the sorted set.
        /// GEOPOS: Returns the positions (longitude,latitude) of all the specified members in the sorted set.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus GeoCommands(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// GEORADIUS (read variant): Return the members of a sorted set populated with geospatial data, which are inside the circular area delimited by center and radius.
        /// GEORADIUS_RO: Return the members of a sorted set populated with geospatial data, which are inside the circular area delimited by center and radius.
        /// GEORADIUSBYMEMBER (read variant): Return the members of a sorted set populated with geospatial data, which are inside the circular area delimited by center (derived from member) and radius.
        /// GEORADIUSBYMEMBER_RO: Return the members of a sorted set populated with geospatial data, which are inside the circular area delimited by center (derived from member) and radius.
        /// GEOSEARCH: Returns the members of a sorted set populated with geospatial data, which are within the borders of the area specified by a given shape.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="opts"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus GeoSearchReadOnly(PinnedSpanByte key, ref GeoSearchOptions opts, ref ObjectInput input, ref SpanByteAndMemory output);

        #endregion

        #region List Methods

        /// <summary>
        /// Gets length of the list
        /// </summary>
        /// <param name="key"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        GarnetStatus ListLength(PinnedSpanByte key, out int count);

        /// <summary>
        /// Gets length of the list, RESP version
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus ListLength(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output);

        /// <summary>
        /// Gets the specified elements of the list stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus ListRange(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Returns the element at index.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus ListIndex(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        #endregion

        #region Set Methods

        /// <summary>
        /// SCARD key
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="count"></param>
        /// <returns></returns>
        GarnetStatus SetLength(PinnedSpanByte key, out int count);

        /// <summary>
        /// Returns the number of elements of the set.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SetLength(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output);

        /// <summary>
        /// SMEMBERS key
        /// </summary>
        /// <param name="key"></param>
        /// <param name="members"></param>
        /// <returns></returns>
        GarnetStatus SetMembers(PinnedSpanByte key, out PinnedSpanByte[] members);

        /// <summary>
        /// Returns all members of the set at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SetMembers(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Returns if member is a member of the set stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SetIsMember(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Returns whether each member is a member of the set stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="members"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        GarnetStatus SetIsMember(PinnedSpanByte key, PinnedSpanByte[] members, out int[] result);

        /// <summary>
        /// Iterates over the members of the Set with the given key using a cursor,
        /// a match pattern and count parameters.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="cursor"></param>
        /// <param name="match"></param>
        /// <param name="count"></param>
        /// <param name="items"></param>
        /// <returns></returns>
        GarnetStatus SetScan(PinnedSpanByte key, long cursor, string match, int count, out PinnedSpanByte[] items);

        /// <summary>
        /// Returns the members of the set resulting from the union of all the given sets.
        /// Keys that do not exist are considered to be empty sets.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SetUnion(PinnedSpanByte[] keys, out HashSet<byte[]> output);

        /// <summary>
        /// Returns the members of the set resulting from the intersection of all the given sets.
        /// Keys that do not exist are considered to be empty sets.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SetIntersect(PinnedSpanByte[] keys, out HashSet<byte[]> output);

        /// <summary>
        /// Returns the members of the set resulting from the difference between the first set and all the successive sets.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="members"></param>
        /// <returns></returns>
        GarnetStatus SetDiff(PinnedSpanByte[] keys, out HashSet<byte[]> members);

        /// <summary>
        /// Returns the cardinality of the intersection between multiple sets.
        /// When limit is greater than 0, stops counting when reaching limit.
        /// </summary>
        /// <param name="keys">Keys of the sets to intersect</param>
        /// <param name="limit">Optional limit to stop counting at</param>
        /// <param name="count">The cardinality of the intersection</param>
        /// <returns>Operation status</returns>
        GarnetStatus SetIntersectLength(ReadOnlySpan<PinnedSpanByte> keys, int? limit, out int count);
        #endregion

        #region Hash Methods

        /// <summary>
        /// Returns the value associated to the field in the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        GarnetStatus HashGet(PinnedSpanByte key, PinnedSpanByte field, out PinnedSpanByte value);

        /// <summary>
        /// Returns the values associated with the fields in the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="fields"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        GarnetStatus HashGetMultiple(PinnedSpanByte key, PinnedSpanByte[] fields, out PinnedSpanByte[] values);

        /// <summary>
        /// Returns the value associated with field in the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input">The metadata input for the operation</param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus HashGet(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Returns all fields and values of the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input">The metadata input for the operation</param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus HashGetAll(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Returns the values associated with the specified fields in the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input">The metadata input for the operation</param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus HashGetMultiple(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Returns ALL the values in the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        GarnetStatus HashGetAll(PinnedSpanByte key, out PinnedSpanByte[] values);

        /// <summary>
        /// Returns the number of fields contained in the hash Key
        /// </summary>
        /// <param name="key"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        GarnetStatus HashLength(PinnedSpanByte key, out int count);

        /// <summary>
        ///Returns the string length of the value associated with field in the hash stored at key. If the key or the field do not exist, 0 is returned.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus HashStrLength(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output);

        /// <summary>
        /// Returns the number of fields contained in the hash Key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus HashLength(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output);

        /// <summary>
        /// Returns if field is an existing field in the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="exists"></param>
        /// <returns></returns>
        GarnetStatus HashExists(PinnedSpanByte key, PinnedSpanByte field, out bool exists);

        /// <summary>
        /// Returns if field is an existing field in the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus HashExists(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output);

        /// <summary>
        /// Returns count random fields from the hash value.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="count"></param>
        /// <param name="withValues"></param>
        /// <param name="fields"></param>
        /// <returns></returns>
        GarnetStatus HashRandomField(PinnedSpanByte key, int count, bool withValues, out PinnedSpanByte[] fields);

        /// <summary>
        /// Returns a random field from the hash value stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        GarnetStatus HashRandomField(PinnedSpanByte key, out PinnedSpanByte field);

        /// <summary>
        /// Returns a random field(s) from the hash value stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus HashRandomField(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Returns all field names in the hash key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus HashKeys(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Returns all values in the hash key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus HashVals(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Iterates fields of Hash key and their associated values using a cursor,
        /// a match pattern and count parameters
        /// </summary>
        /// <param name="key"></param>
        /// <param name="cursor"></param>
        /// <param name="match"></param>
        /// <param name="count"></param>
        /// <param name="items"></param>
        /// <returns></returns>
        GarnetStatus HashScan(PinnedSpanByte key, long cursor, string match, int count, out PinnedSpanByte[] items);

        /// <summary>
        /// Returns the time to live for a hash key.
        /// </summary>
        /// <param name="key">The key of the hash.</param>
        /// <param name="isMilliseconds">Indicates if the time to live is in milliseconds.</param>
        /// <param name="isTimestamp">Indicates if the time to live is a timestamp.</param>
        /// <param name="input">The input object containing additional parameters.</param>
        /// <param name="output">The output object to store the result.</param>
        /// <returns>The status of the operation.</returns>
        GarnetStatus HashTimeToLive(PinnedSpanByte key, bool isMilliseconds, bool isTimestamp, ref ObjectInput input, ref ObjectOutput output);

        #endregion

        #region Bitmaps Methods

        /// <summary>
        /// Returns the bit value at offset in the key stored.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus StringGetBit(PinnedSpanByte key, ref StringInput input, ref StringOutput output);

        /// <summary>
        /// Returns the bit value at offset in the key stored.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="offset"></param>
        /// <param name="bValue"></param>
        /// <returns></returns>
        GarnetStatus StringGetBit(PinnedSpanByte key, PinnedSpanByte offset, out bool bValue);

        /// <summary>
        /// Count the number of set bits in a string.
        /// It can be specified an interval for counting, passing the start and end arguments.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus StringBitCount(PinnedSpanByte key, ref StringInput input, ref StringOutput output);

        /// <summary>
        /// Count the number of set bits in a string.
        /// It can be specified an interval for counting, passing the start and end arguments.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="result"></param>
        /// <param name="useBitInterval"></param>
        /// <returns></returns>
        GarnetStatus StringBitCount(PinnedSpanByte key, long start, long end, out long result, bool useBitInterval = false);

        /// <summary>
        /// Returns the position of the first bit set to 1 or 0 in a key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus StringBitPosition(PinnedSpanByte key, ref StringInput input, ref StringOutput output);

        /// <summary>
        /// Read-only variant of the StringBitField method.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="secondaryCommand"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus StringBitFieldReadOnly(PinnedSpanByte key, ref StringInput input, RespCommand secondaryCommand, ref StringOutput output);

        #endregion

        #region HLL Methods
        /// <summary>
        /// Returns the approximated cardinality computed by the HyperLogLog data structure stored at the specified key,
        /// or 0 if the key does not exist.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="count"></param>
        /// <param name="error"></param>
        /// <returns></returns>
        GarnetStatus HyperLogLogLength(ref StringInput input, out long count, out bool error);

        /// <summary>
        ///
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        GarnetStatus HyperLogLogLength(Span<PinnedSpanByte> keys, out long count);
        #endregion

        #region Server Methods

        /// <summary>
        /// Gets the keys store in the DB matching the given pattern
        /// </summary>
        /// <param name="pattern">Expression to match the keys name</param>
        /// <returns></returns>
        List<byte[]> GetDbKeys(PinnedSpanByte pattern);

        /// <summary>
        /// Gets the number of existing keys in both stores
        /// </summary>
        /// <returns></returns>
        int GetDbSize();

        /// <summary>
        /// Iterates the set of keys in the main store.
        /// </summary>
        /// <param name="patternB">The pattern to apply for filtering</param>
        /// <param name="allKeys">When true the filter is ommited</param>
        /// <param name="cursor">The value of the cursor in the command request</param>
        /// <param name="storeCursor">Value of the cursor returned</param>
        /// <param name="Keys">The list of keys from the stores</param>
        /// <param name="count">The size of the batch of keys</param>
        /// <param name="type">Type of key to filter out</param>
        /// <returns></returns>
        public bool DbScan(PinnedSpanByte patternB, bool allKeys, long cursor, out long storeCursor, out List<byte[]> Keys, long count = 10, ReadOnlySpan<byte> type = default);

        /// <summary>
        /// Iterate the contents of the store
        /// </summary>
        /// <typeparam name="TScanFunctions"></typeparam>
        /// <param name="scanFunctions"></param>
        /// <param name="untilAddress"></param>
        /// <param name="maxAddress"></param>
        /// <param name="cursor"></param>
        /// <param name="includeTombstones"></param>
        /// <returns></returns>
        public bool IterateStore<TScanFunctions>(ref TScanFunctions scanFunctions, ref long cursor, long untilAddress = -1, long maxAddress = long.MaxValue, bool includeTombstones = false)
            where TScanFunctions : IScanIteratorFunctions;

        /// <summary>
        /// Iterate the contents of the store (pull based)
        /// </summary>
        /// <returns></returns>
        public ITsavoriteScanIterator IterateStore();

        #endregion

        #region Common Methods

        /// <summary>
        /// Iterates over the items of a collection object using a cursor,
        /// a match pattern and count parameters
        /// </summary>
        /// <param name="key">The key of the sorted set</param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        GarnetStatus ObjectScan(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output);

        /// <summary>
        /// Retrieve the current scratch buffer offset.
        /// </summary>
        /// <returns>Current offset</returns>
        int GetScratchBufferOffset();

        /// <summary>
        /// Resets the scratch buffer to the given offset.
        /// </summary>
        /// <param name="offset">Offset to reset to</param>
        /// <returns>True if successful, else false</returns>
        bool ResetScratchBuffer(int offset);

        #endregion

    }

    /// <summary>
    /// Garnet Watch API
    /// </summary>
    public interface IGarnetWatchApi
    {
        /// <summary>
        /// WATCH
        /// </summary>
        /// <param name="key"></param>
        /// <param name="type"></param>
        void WATCH(PinnedSpanByte key, StoreType type);
    }
}