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
        #region SET
        /// <summary>
        /// SET
        /// </summary>
        GarnetStatus SET(ref SpanByte key, ref SpanByte value);

        /// <summary>
        /// SET Conditional
        /// </summary>
        GarnetStatus SET_Conditional(ref SpanByte key, ref SpanByte input);

        /// <summary>
        /// SET Conditional
        /// </summary>
        GarnetStatus SET_Conditional(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output);

        /// <summary>
        /// SET
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        GarnetStatus SET(ArgSlice key, Memory<byte> value);

        /// <summary>
        /// SET
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        GarnetStatus SET(ArgSlice key, ArgSlice value);

        /// <summary>
        /// SET
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        GarnetStatus SET(byte[] key, IGarnetObject value);
        #endregion

        #region SETEX
        /// <summary>
        /// SETEX
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <param name="expiryMs">Expiry in milliseconds, formatted as ASCII digits</param>
        /// <returns></returns>
        GarnetStatus SETEX(ArgSlice key, ArgSlice value, ArgSlice expiryMs);

        /// <summary>
        /// SETEX
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <param name="expiry">Expiry</param>
        GarnetStatus SETEX(ArgSlice key, ArgSlice value, TimeSpan expiry);

        #endregion

        #region SETRANGE

        /// <summary>
        /// SETRANGE
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <param name="offset">Offset in Bytes</param>
        /// <param name="output">The output of the operation</param>
        /// <returns></returns>
        GarnetStatus SETRANGE(ArgSlice key, ArgSlice value, int offset, ref ArgSlice output);


        #endregion

        #region APPEND
        /// <summary>
        /// APPEND command
        /// </summary>
        /// <param name="key">Key whose value is to be appended</param>
        /// <param name="value">Value to be appended</param>
        /// <param name="output">Length of updated value</param>
        /// <returns>Operation status</returns>
        GarnetStatus APPEND(ref SpanByte key, ref SpanByte value, ref SpanByteAndMemory output);

        /// <summary>
        /// APPEND command
        /// </summary>
        /// <param name="key">Key whose value is to be appended</param>
        /// <param name="value">Value to be appended</param>
        /// <param name="output">Length of updated value</param>
        /// <returns>Operation status</returns>
        GarnetStatus APPEND(ArgSlice key, ArgSlice value, ref ArgSlice output);
        #endregion

        #region RENAME
        /// <summary>
        /// RENAME
        /// </summary>
        /// <param name="oldKey"></param>
        /// <param name="newKey"></param>
        /// <param name="storeType"></param>
        /// <returns></returns>
        GarnetStatus RENAME(ArgSlice oldKey, ArgSlice newKey, StoreType storeType = StoreType.All);
        #endregion

        #region EXISTS
        /// <summary>
        /// EXISTS
        /// </summary>
        /// <param name="key"></param>
        /// <param name="storeType"></param>
        /// <returns></returns>
        GarnetStatus EXISTS(ArgSlice key, StoreType storeType = StoreType.All);
        #endregion

        #region EXPIRE
        /// <summary>
        /// Set a timeout on key
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="expiryMs">Expiry in milliseconds, formatted as ASCII digits</param>
        /// <param name="timeoutSet">Whether timeout was set by the call</param>
        /// <param name="storeType">Store type: main, object, or both</param>
        /// <param name="expireOption">Expire option</param>
        /// <returns></returns>
        GarnetStatus EXPIRE(ArgSlice key, ArgSlice expiryMs, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None);

        /// <summary>
        /// Set a timeout on key using a timeSpan in seconds
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="expiry">Expiry in TimeSpan</param>
        /// <param name="timeoutSet">Whether timeout was set by the call</param>
        /// <param name="storeType">Store type: main, object, or both</param>
        /// <param name="expireOption">Expire option</param>
        /// <returns></returns>
        GarnetStatus EXPIRE(ArgSlice key, TimeSpan expiry, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None);

        /// <summary>
        /// Set a timeout on key using a timeSpan in milliseconds
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="expiry">Expiry in TimeSpan</param>
        /// <param name="timeoutSet">Whether timeout was set by the call</param>
        /// <param name="storeType">Store type: main, object, or both</param>
        /// <param name="expireOption">Expire option</param>
        /// <returns></returns>
        GarnetStatus PEXPIRE(ArgSlice key, TimeSpan expiry, out bool timeoutSet, StoreType storeType = StoreType.All, ExpireOption expireOption = ExpireOption.None);

        #endregion

        #region PERSIST
        /// <summary>
        /// PERSIST
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="storeType">Store type: main, object, or both</param>
        /// <returns></returns>
        GarnetStatus PERSIST(ArgSlice key, StoreType storeType = StoreType.All);
        #endregion

        #region Increment (INCR, INCRBY, DECR, DECRBY)
        /// <summary>
        /// Increment (INCR, INCRBY, DECR, DECRBY)
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus Increment(ArgSlice key, ArgSlice input, ref ArgSlice output);
        #endregion

        #region DELETE
        /// <summary>
        /// DELETE
        /// </summary>
        /// <param name="key"></param>
        /// <param name="storeType"></param>
        /// <returns></returns>
        GarnetStatus DELETE(ArgSlice key, StoreType storeType = StoreType.All);

        /// <summary>
        /// DELETE
        /// </summary>
        /// <param name="key"></param>
        /// <param name="storeType"></param>
        /// <returns></returns>
        GarnetStatus DELETE(ref SpanByte key, StoreType storeType = StoreType.All);

        /// <summary>
        /// DELETE
        /// </summary>
        /// <param name="key"></param>
        /// <param name="storeType"></param>
        /// <returns></returns>
        GarnetStatus DELETE(byte[] key, StoreType storeType = StoreType.All);
        #endregion

        #region GETDEL
        /// <summary>
        /// GETDEL
        /// </summary>
        /// <param name="key"> Key to get and delete </param>
        /// <param name="output"> Current value of key </param>
        /// <returns> Operation status </returns>
        GarnetStatus GETDEL(ref SpanByte key, ref SpanByteAndMemory output);

        /// <summary>
        /// GETDEL
        /// </summary>
        /// <param name="key"> Key to get and delete </param>
        /// <param name="output"> Current value of key </param>
        /// <returns> Operation status </returns>
        GarnetStatus GETDEL(ArgSlice key, ref SpanByteAndMemory output);
        #endregion

        #region TYPE

        /// <summary>
        /// Returns the string representation of the type of the value stored at key.
        /// string, list, set, zset, and hash.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="typeName"></param>
        /// <returns></returns>
        GarnetStatus GetKeyType(ArgSlice key, out string typeName);

        #endregion

        #region MEMORY

        /// <summary>
        ///  Gets the number of bytes that a key and its value require to be stored in RAM.
        /// </summary>
        /// <param name="key">Name of the key or object to get the memory usage</param>
        /// <param name="memoryUsage">The value in bytes the key or object is using</param>
        /// <param name="samples">Number of sampled nested values</param> 
        /// <returns>GarnetStatus</returns>
        GarnetStatus MemoryUsageForKey(ArgSlice key, out long memoryUsage, int samples = 0);

        #endregion

        #region SortedSet Methods

        /// <summary>
        /// Adds all the specified members with the specified scores to the sorted set stored at key.
        /// Current members get the score updated and reordered.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Formatted input arguments with header [ObjectInputHeader][RESP score][RESP member]...</param>
        /// <param name="zaddCount">Number of adds performed</param>
        /// <returns></returns>
        GarnetStatus SortedSetAdd(byte[] key, ArgSlice input, out int zaddCount);

        /// <summary>
        /// Adds the specified member with the specified score to the sorted set stored at key.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="score">Score</param>
        /// <param name="member">Member</param>
        /// <param name="zaddCount">Number of adds performed</param>
        /// <returns></returns>
        GarnetStatus SortedSetAdd(ArgSlice key, ArgSlice score, ArgSlice member, out int zaddCount);

        /// <summary>
        /// Adds all the specified members with the specified scores to the sorted set stored at key.
        /// Current members get the score updated and reordered.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="inputs">Input key-value pairs to add</param>
        /// <param name="zaddCount">Number of adds performed</param>
        /// <returns></returns>
        GarnetStatus SortedSetAdd(ArgSlice key, (ArgSlice score, ArgSlice member)[] inputs, out int zaddCount);

        /// <summary>
        /// Adds all the specified members with the specified scores to the sorted set stored at key.
        /// Current members get the score updated and reordered.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetAdd(byte[] key, ArgSlice input, out ObjectOutputHeader output);

        /// <summary>
        /// Removes the specified member from the sorted set stored at key.
        /// </summary>
        GarnetStatus SortedSetRemove(ArgSlice key, ArgSlice member, out int zremCount);

        /// <summary>
        /// Removes the specified members from the sorted set stored at key.
        /// Non existing members are ignored.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="members">Input members to remove</param>
        /// <param name="zremCount">Number of removes performed</param>
        /// <returns></returns>
        GarnetStatus SortedSetRemove(ArgSlice key, ArgSlice[] members, out int zremCount);

        /// <summary>
        /// Removes the specified members from the sorted set stored at key.
        /// Non existing members are ignored.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRemove(byte[] key, ArgSlice input, out ObjectOutputHeader output);

        /// <summary>
        /// Removes all elements in the sorted set between the
        /// lexicographical range specified by min and max.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRemoveRangeByLex(byte[] key, ArgSlice input, out ObjectOutputHeader output);

        /// <summary>
        /// Removes and returns the first element from the sorted set stored at key, 
        /// with the scores ordered from low to high (min) or high to low (max).
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        GarnetStatus SortedSetPop(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter);

        /// <summary>
        /// Removes and returns up to count members with the highest or lowest scores in the sorted set stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="pairs"></param>
        /// <param name="count"></param>
        /// <param name="lowScoresFirst">When true, return the members with the lowest scores, otherwise return the highest scores.</param>
        /// <returns></returns>
        GarnetStatus SortedSetPop(ArgSlice key, out (ArgSlice score, ArgSlice member)[] pairs, int count = 1, bool lowScoresFirst = true);

        /// <summary>
        /// Increments the score of member in the sorted set stored at key by increment. 
        /// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        GarnetStatus SortedSetIncrement(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter);

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
        GarnetStatus SortedSetIncrement(ArgSlice key, Double increment, ArgSlice member, out double newScore);

        /// <summary>
        /// ZREMRANGEBYRANK: Removes all elements in the sorted set stored at key with rank between start and stop.
        /// Both start and stop are 0 -based indexes with 0 being the element with the lowest score.
        /// ZREMRANGEBYSCORE: Removes all elements in the sorted set stored at key with a score between min and max (inclusive by default).
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRemoveRange(byte[] key, ArgSlice input, out ObjectOutputHeader output);

        /// <summary>
        /// Removes all elements in the range specified by min and max, having the same score.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="min"></param>
        /// <param name="max"></param>
        /// <param name="countRemoved"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRemoveRangeByLex(ArgSlice key, string min, string max, out int countRemoved);

        /// <summary>
        /// Removes all elements that have a score in the range specified by min and max.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="min"></param>
        /// <param name="max"></param>
        /// <param name="countRemoved"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRemoveRangeByScore(ArgSlice key, string min, string max, out int countRemoved);

        /// <summary>
        /// Removes all elements with the index in the range specified by start and stop.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="start"></param>
        /// <param name="stop"></param>
        /// <param name="countRemoved"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRemoveRangeByRank(ArgSlice key, int start, int stop, out int countRemoved);

        /// <summary>
        /// Adds geospatial items (longitude, latitude, name) to the specified key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus GeoAdd(byte[] key, ArgSlice input, out ObjectOutputHeader output);

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
        GarnetStatus SetAdd(ArgSlice key, ArgSlice member, out int saddCount);

        /// <summary>
        ///  Adds the specified members to the set at key.
        ///  Specified members that are already a member of this set are ignored. 
        ///  If key does not exist, a new set is created.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="members"></param>
        /// <param name="saddCount"></param>
        /// <returns></returns>
        GarnetStatus SetAdd(ArgSlice key, ArgSlice[] members, out int saddCount);

        /// <summary>
        ///  Adds the specified members to the set at key.
        ///  Specified members that are already a member of this set are ignored. 
        ///  If key does not exist, a new set is created.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SetAdd(byte[] key, ArgSlice input, out ObjectOutputHeader output);

        /// <summary>
        /// Removes the specified member from the set.
        /// Specified members that are not a member of this set are ignored. 
        /// If key does not exist, this command returns 0.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="member"></param>
        /// <param name="sremCount"></param>
        /// <returns></returns>
        GarnetStatus SetRemove(ArgSlice key, ArgSlice member, out int sremCount);

        /// <summary>
        /// Removes the specified members from the set.
        /// Specified members that are not a member of this set are ignored. 
        /// If key does not exist, this command returns 0.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="members"></param>
        /// <param name="sremCount"></param>
        /// <returns></returns>
        GarnetStatus SetRemove(ArgSlice key, ArgSlice[] members, out int sremCount);

        /// <summary>
        /// Removes the specified members from the set.
        /// Specified members that are not a member of this set are ignored. 
        /// If key does not exist, this command returns 0.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SetRemove(byte[] key, ArgSlice input, out ObjectOutputHeader output);

        /// <summary>
        /// Removes and returns one random member from the set at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="member"></param>
        /// <returns></returns>
        GarnetStatus SetPop(ArgSlice key, out ArgSlice member);

        /// <summary>
        /// Removes and returns random members from the set at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="count"></param>
        /// <param name="members"></param>
        /// <returns></returns>
        GarnetStatus SetPop(ArgSlice key, int count, out ArgSlice[] members);

        /// <summary>
        /// Removes and returns random members from the set at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        GarnetStatus SetPop(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter);

        #endregion

        #region List Methods

        #region ListPush Methods

        /// <summary>
        /// ListLeftPush ArgSlice version with ObjectOutputHeader output
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus ListLeftPush(byte[] key, ArgSlice input, out ObjectOutputHeader output);

        /// <summary>
        /// ListLeftPush ArgSlice version, one element
        /// </summary>
        /// <param name="key"></param>
        /// <param name="element"></param>
        /// <param name="count"></param>
        /// <param name="whenExists">When true the operation is executed only if the key already exists</param>
        /// <returns></returns>
        GarnetStatus ListLeftPush(ArgSlice key, ArgSlice element, out int count, bool whenExists = false);

        /// <summary>
        /// ListLeftPush ArgSlice version for multiple values
        /// </summary>
        /// <param name="key"></param>
        /// <param name="elements"></param>
        /// <param name="count"></param>
        /// <param name="whenExists">When true the operation is executed only if the key already exists</param>
        /// <returns></returns>
        GarnetStatus ListLeftPush(ArgSlice key, ArgSlice[] elements, out int count, bool whenExists = false);

        /// <summary>
        /// ListRightPush ArgSlice version with ObjectOutputHeader output
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        public GarnetStatus ListRightPush(byte[] key, ArgSlice input, out ObjectOutputHeader output);

        /// <summary>
        /// ListRightPush ArgSlice version, one element
        /// </summary>
        /// <param name="key"></param>
        /// <param name="element"></param>
        /// <param name="count"></param>
        /// <param name="whenExists">When true the operation is executed only if the key already exists</param>
        /// <returns></returns>
        GarnetStatus ListRightPush(ArgSlice key, ArgSlice element, out int count, bool whenExists = false);

        /// <summary>
        /// ListRightPush ArgSlice version for multiple values
        /// </summary>
        /// <param name="key"></param>
        /// <param name="elements"></param>
        /// <param name="count"></param>
        /// <param name="whenExists">When true the operation is executed only if the key already exists</param>
        /// <returns></returns>
        GarnetStatus ListRightPush(ArgSlice key, ArgSlice[] elements, out int count, bool whenExists = false);

        #endregion

        #region ListPop Methods

        /// <summary>
        /// ListLeftPop ArgSlice version, with GarnetObjectStoreOuput
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        GarnetStatus ListLeftPop(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter);

        /// <summary>
        /// ListLeftPop ArgSlice version, one element
        /// </summary>
        /// <param name="key"></param>
        /// <param name="element"></param>
        /// <returns></returns>
        GarnetStatus ListLeftPop(ArgSlice key, out ArgSlice element);

        /// <summary>
        /// ListLeftPop ArgSlice version for multiple values
        /// </summary>
        /// <param name="key"></param>
        /// <param name="elements"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        GarnetStatus ListLeftPop(ArgSlice key, int count, out ArgSlice[] elements);

        /// <summary>
        /// ListRightPop ArgSlice version, with GarnetObjectStoreOutput
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        GarnetStatus ListRightPop(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter);

        /// <summary>
        /// ListRightPop ArgSlice version, one element
        /// </summary>
        /// <param name="key"></param>
        /// <param name="element"></param>
        /// <returns></returns>
        GarnetStatus ListRightPop(ArgSlice key, out ArgSlice element);

        /// <summary>
        /// ListRightPop ArgSlice version for multiple values
        /// </summary>
        /// <param name="key"></param>
        /// <param name="elements"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        GarnetStatus ListRightPop(ArgSlice key, int count, out ArgSlice[] elements);
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
        /// <returns>true when success</returns>
        public bool ListMove(ArgSlice sourceKey, ArgSlice destinationKey, OperationDirection sourceDirection, OperationDirection destinationDirection, out byte[] element);

        /// <summary>
        /// Trim an existing list so it only contains the specified range of elements.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="start"></param>
        /// <param name="stop"></param>
        /// <returns></returns>
        public bool ListTrim(ArgSlice key, int start, int stop);

        /// <summary>
        /// Trim an existing list so it only contains the specified range of elements.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <returns></returns>
        GarnetStatus ListTrim(byte[] key, ArgSlice input);

        /// <summary>
        /// Inserts a new element in the list stored at key either before or after a value pivot
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus ListInsert(byte[] key, ArgSlice input, out ObjectOutputHeader output);

        /// <summary>
        /// Removes the first count occurrences of elements equal to element from the list.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus ListRemove(byte[] key, ArgSlice input, out ObjectOutputHeader output);

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
        GarnetStatus HashSet(ArgSlice key, ArgSlice field, ArgSlice value, out int count);

        /// <summary>
        /// Sets the specified fields to their respective values in the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="elements"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        GarnetStatus HashSet(ArgSlice key, (ArgSlice field, ArgSlice value)[] elements, out int count);

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
        GarnetStatus HashSet(byte[] key, ArgSlice input, out ObjectOutputHeader output);

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
        GarnetStatus HashSetWhenNotExists(ArgSlice key, ArgSlice field, ArgSlice value, out int count);

        /// <summary>
        /// Removes the specified field from the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="count">Number of fields removed</param>
        /// <returns></returns>
        GarnetStatus HashDelete(ArgSlice key, ArgSlice field, out int count);

        /// <summary>
        /// Removes the specified fields from the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="fields"></param>
        /// <param name="count">Number of fields removed</param>
        /// <returns></returns>
        GarnetStatus HashDelete(ArgSlice key, ArgSlice[] fields, out int count);

        /// <summary>
        /// Removes the specified fields from the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus HashDelete(byte[] key, ArgSlice input, out ObjectOutputHeader output);

        /// <summary>
        /// Increments the number stored at field in the hash key by increment parameter.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus HashIncrement(byte[] key, ArgSlice input, out ObjectOutputHeader output);

        /// <summary>
        /// Increments the number stored at field representing a floating point value
        /// in the hash key by increment parameter.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        GarnetStatus HashIncrement(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter);

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
        GarnetStatus StringSetBit(ArgSlice key, ArgSlice offset, bool bit, out bool previous);

        /// <summary>
        /// Sets or clears the bit at offset in the given key.
        /// The bit is either set or cleared depending on value, which can be either 0 or 1.
        /// When key does not exist, a new key is created.The key is grown to make sure it can hold a bit at offset.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus StringSetBit(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output);

        /// <summary>
        /// Performs a bitwise operations on multiple keys
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="bitop"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        GarnetStatus StringBitOperation(ArgSlice[] keys, BitmapOperation bitop, out long result);

        /// <summary>
        /// Perform a bitwise operation between multiple keys 
        /// and store the result in the destination key.
        /// </summary>
        /// <param name="bitop"></param>
        /// <param name="destinationKey"></param>
        /// <param name="keys"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        GarnetStatus StringBitOperation(BitmapOperation bitop, ArgSlice destinationKey, ArgSlice[] keys, out long result);

        /// <summary>
        /// Performs arbitrary bitfield integer operations on strings.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="secondaryCommand"></param>
        /// <returns></returns>
        GarnetStatus StringBitField(ref SpanByte key, ref SpanByte input, byte secondaryCommand, ref SpanByteAndMemory output);

        /// <summary>
        /// Performs arbitrary bitfield integer operations on strings.
        /// </summary>
        GarnetStatus StringBitField(ArgSlice key, List<BitFieldCmdArgs> commandArguments, out List<long?> result);
        #endregion

        #region HyperLogLog Methods

        /// <summary>
        /// Adds all the element arguments to the HyperLogLog data structure stored at the variable name specified as key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus HyperLogLogAdd(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output);

        /// <summary>
        /// Adds all the element arguments to the HyperLogLog data structure stored at the variable name specified as key.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="elements"></param>
        /// <param name="updated">true if at least 1 HyperLogLog internal register was altered</param>
        /// <returns></returns>
        GarnetStatus HyperLogLogAdd(ArgSlice keys, string[] elements, out bool updated);

        /// <summary>
        /// Merge multiple HyperLogLog values into a unique value that will approximate the cardinality 
        /// of the union of the observed Sets of the source HyperLogLog structures.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="error"></param>
        /// <returns></returns>
        GarnetStatus HyperLogLogMerge(ArgSlice[] keys, out bool error);

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
        GarnetStatus GET(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output);

        /// <summary>
        /// GET
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        GarnetStatus GETForMemoryResult(ArgSlice key, out MemoryResult<byte> value);

        /// <summary>
        /// GET
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        GarnetStatus GET(ArgSlice key, out ArgSlice value);

        /// <summary>
        /// GET
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        GarnetStatus GET(byte[] key, out GarnetObjectStoreOutput value);
        #endregion

        #region GETRANGE
        /// <summary>
        /// GETRANGE
        /// </summary>
        /// <param name="key"></param>
        /// <param name="sliceStart"></param>
        /// <param name="sliceLength"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus GETRANGE(ref SpanByte key, int sliceStart, int sliceLength, ref SpanByteAndMemory output);
        #endregion

        #region TTL

        /// <summary>
        /// Returns the remaining time to live in seconds of a key that has a timeout.
        /// </summary>
        /// <param name="key">The key to return the remaining time to live in the store</param>
        /// <param name="storeType">The store type to operate on.</param>
        /// <param name="output">The span to allocate the output of the operation.</param>
        /// <returns></returns>
        GarnetStatus TTL(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output);

        /// <summary>
        /// Returns the remaining time to live in milliseconds of a key that has a timeout.
        /// </summary>
        /// <param name="key">The key to return the remaining time to live in the store.</param>
        /// <param name="storeType">The store type to operate on.</param>
        /// <param name="output">The span to allocate the output of the operation.</param>
        /// <returns></returns>
        GarnetStatus PTTL(ref SpanByte key, StoreType storeType, ref SpanByteAndMemory output);

        #endregion

        #region SortedSet Methods

        /// <summary>
        /// Returns the sorted set cardinality (number of elements) of the sorted set
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="zcardCount"></param>
        /// <returns></returns>
        GarnetStatus SortedSetLength(ArgSlice key, out int zcardCount);

        /// <summary>
        /// Returns the sorted set cardinality (number of elements) of the sorted set
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetLength(byte[] key, ArgSlice input, out ObjectOutputHeader output);

        /// <summary>
        /// Returns the specified range of elements in the sorted set stored at key.
        /// Both start and stop are zero-based indexes, where 0 is the first element, 1 is the next element and so on.
        /// There can also be negative numbers indicating offsets from the end of the sorted set, with -1 being the last element of the sorted set, -2 the penultimate element and so on.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRange(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter);

        /// <summary>
        /// Returns the score of member in the sorted set at key.
        /// If member does not exist in the sorted set, or key does not exist, nil is returned.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        GarnetStatus SortedSetScore(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter);

        /// <summary>
        /// Returns the number of elements in the sorted set at key with a score between min and max.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetCount(byte[] key, ArgSlice input, out ObjectOutputHeader output);

        /// <summary>
        /// Returns the number of elements in the sorted set with a value between min and max.
        /// When all the elements in a sorted set have the same score, 
        /// this command forces lexicographical ordering.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetLengthByValue(byte[] key, ArgSlice input, out ObjectOutputHeader output);

        /// <summary>
        /// ZRANK: Returns the rank of member in the sorted set, the scores in the sorted set are ordered from low to high
        /// ZREVRANK: Returns the rank of member in the sorted set, with the scores ordered from high to low
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRank(byte[] key, ArgSlice input, out ObjectOutputHeader output);

        /// <summary>
        /// Returns a random element from the sorted set key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        GarnetStatus SortedSetRandomMember(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter);

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
        GarnetStatus SortedSetRange(ArgSlice key, ArgSlice min, ArgSlice max, SortedSetOrderOperation sortedSetOrderOperation, out ArgSlice[] elements, out string error, bool withScores = false, bool reverse = false, (string, int) limit = default);

        /// <summary>
        /// Computes the difference between the first and all successive sorted sets and returns resulting pairs.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="pairs"></param>
        /// <returns></returns>
        GarnetStatus SortedSetDifference(ArgSlice[] keys, out Dictionary<byte[], double> pairs);

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
        GarnetStatus SortedSetScan(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items);

        #endregion

        #region Geospatial Methods

        /// <summary>
        /// GEOHASH: Returns valid Geohash strings representing the position of one or more elements in a geospatial data of the sorted set.
        /// GEODIST: Returns the distance between two members in the geospatial index represented by the sorted set.
        /// GEOPOS: Returns the positions (longitude,latitude) of all the specified members in the sorted set.
        /// GEOSEARCH: Returns the members of a sorted set populated with geospatial data, which are within the borders of the area specified by a given shape.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        GarnetStatus GeoCommands(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter);

        #endregion

        #region List Methods

        /// <summary>
        /// Gets length of the list
        /// </summary>
        /// <param name="key"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        GarnetStatus ListLength(ArgSlice key, out int count);

        /// <summary>
        /// Gets length of the list, RESP version
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus ListLength(byte[] key, ArgSlice input, out ObjectOutputHeader output);

        /// <summary>
        /// Gets the specified elements of the list stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        GarnetStatus ListRange(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter);

        /// <summary>
        /// Returns the element at index.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        GarnetStatus ListIndex(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter);

        #endregion

        #region Set Methods

        /// <summary>
        /// SCARD key
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="count"></param>
        /// <returns></returns>
        GarnetStatus SetLength(ArgSlice key, out int count);

        /// <summary>
        /// Returns the number of elements of the set.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus SetLength(byte[] key, ArgSlice input, out ObjectOutputHeader output);

        /// <summary>
        /// SMEMBERS key
        /// </summary>
        /// <param name="key"></param>
        /// <param name="members"></param>
        /// <returns></returns>
        GarnetStatus SetMembers(ArgSlice key, out ArgSlice[] members);

        /// <summary>
        /// Returns all members of the set at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        GarnetStatus SetMembers(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter);

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
        GarnetStatus SetScan(ArgSlice key, long cursor, string match, int count, out ArgSlice[] items);

        #endregion

        #region Hash Methods

        /// <summary>
        /// Returns the value associated to the field in the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        GarnetStatus HashGet(ArgSlice key, ArgSlice field, out ArgSlice value);

        /// <summary>
        /// Returns the values associated with the fields in the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="fields"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        GarnetStatus HashGet(ArgSlice key, ArgSlice[] fields, out ArgSlice[] values);

        /// <summary>
        /// HashGet: Returns the value associated with field in the hash stored at key.
        /// HashGetAll: Returns all fields and values of the hash stored at key.
        /// HashGetMultiple: Returns the values associated with the specified fields in the hash stored at key.
        /// HashRandomField: Returns a random field from the hash value stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input">The metadata input for the operation</param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        GarnetStatus HashGet(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter);

        /// <summary>
        /// Returns ALL the values in the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        GarnetStatus HashGetAll(ArgSlice key, out ArgSlice[] values);

        /// <summary>
        /// Returns the number of fields contained in the hash Key
        /// </summary>
        /// <param name="key"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        GarnetStatus HashLength(ArgSlice key, out int count);

        /// <summary>
        /// Returns the number of fields contained in the hash Key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus HashLength(byte[] key, ArgSlice input, out ObjectOutputHeader output);

        /// <summary>
        /// Returns if field is an existing field in the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="exists"></param>
        /// <returns></returns>
        GarnetStatus HashExists(ArgSlice key, ArgSlice field, out bool exists);

        /// <summary>
        /// Returns if field is an existing field in the hash stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus HashExists(byte[] key, ArgSlice input, out ObjectOutputHeader output);

        /// <summary>
        /// Returns count random fields from the hash value.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="count"></param>
        /// <param name="withValues"></param>
        /// <param name="fields"></param>
        /// <returns></returns>
        GarnetStatus HashRandomField(ArgSlice key, int count, bool withValues, out ArgSlice[] fields);

        /// <summary>
        /// Returns a random field from the hash value stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        GarnetStatus HashRandomField(ArgSlice key, out ArgSlice field);

        /// <summary>
        /// Returns a random field(s) from the hash value stored at key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        GarnetStatus HashRandomField(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter);

        /// <summary>
        /// Returns all field names in the hash key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        GarnetStatus HashKeys(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter);

        /// <summary>
        /// Returns all values in the hash key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <returns></returns>
        GarnetStatus HashVals(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter);

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
        GarnetStatus HashScan(ArgSlice key, long cursor, string match, long count, out ArgSlice[] items);

        #endregion

        #region Bitmaps Methods

        /// <summary>
        /// Returns the bit value at offset in the key stored.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus StringGetBit(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output);

        /// <summary>
        /// Returns the bit value at offset in the key stored.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="offset"></param>
        /// <param name="bValue"></param>
        /// <returns></returns>
        GarnetStatus StringGetBit(ArgSlice key, ArgSlice offset, out bool bValue);

        /// <summary>
        /// Count the number of set bits in a string. 
        /// It can be specified an interval for counting, passing the start and end arguments.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus StringBitCount(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output);

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
        GarnetStatus StringBitCount(ArgSlice key, long start, long end, out long result, bool useBitInterval = false);

        /// <summary>
        /// Returns the position of the first bit set to 1 or 0 in a key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus StringBitPosition(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output);

        /// <summary>
        /// Read-only variant of the StringBitField method.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="secondaryCommand"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        GarnetStatus StringBitFieldReadOnly(ref SpanByte key, ref SpanByte input, byte secondaryCommand, ref SpanByteAndMemory output);

        #endregion

        #region HLL Methods
        /// <summary>
        /// Returns the approximated cardinality computed by the HyperLogLog data structure stored at the specified key,
        /// or 0 if the key does not exist.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="input"></param>
        /// <param name="count"></param>
        /// <param name="error"></param>
        /// <returns></returns>
        GarnetStatus HyperLogLogLength(ArgSlice[] keys, ref SpanByte input, out long count, out bool error);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        GarnetStatus HyperLogLogLength(ArgSlice[] keys, out long count);
        #endregion

        #region Server Methods

        /// <summary>
        /// Gets the keys store in the DB matching the given pattern
        /// </summary>
        /// <param name="pattern">Expression to match the keys name</param>
        /// <returns></returns>
        List<byte[]> GetDbKeys(ArgSlice pattern);

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
        public bool DbScan(ArgSlice patternB, bool allKeys, long cursor, out long storeCursor, out List<byte[]> Keys, long count = 10, Span<byte> type = default);

        /// <summary>
        /// Iterate the contents of the main store
        /// </summary>
        /// <typeparam name="TScanFunctions"></typeparam>
        /// <param name="scanFunctions"></param>
        /// <param name="untilAddress"></param>
        /// <returns></returns>
        public bool IterateMainStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions<SpanByte, SpanByte>;

        /// <summary>
        /// Iterate the contents of the main store (pull based)
        /// </summary>
        /// <returns></returns>
        public ITsavoriteScanIterator<SpanByte, SpanByte> IterateMainStore();

        /// <summary>
        /// Iterate the contents of the object store
        /// </summary>
        /// <typeparam name="TScanFunctions"></typeparam>
        /// <param name="scanFunctions"></param>
        /// <param name="untilAddress"></param>
        /// <returns></returns>
        public bool IterateObjectStore<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions<byte[], IGarnetObject>;

        /// <summary>
        /// Iterate the contents of the object store (pull based)
        /// </summary>
        /// <returns></returns>
        public ITsavoriteScanIterator<byte[], IGarnetObject> IterateObjectStore();

        #endregion

        #region Common Methods

        /// <summary>
        /// Iterates over the items of a collection object using a cursor,
        /// a match pattern and count parameters
        /// </summary>
        /// <param name="key">The key of the sorted set</param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        GarnetStatus ObjectScan(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter);

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
        void WATCH(ArgSlice key, StoreType type);

        /// <summary>
        /// WATCH
        /// </summary>
        /// <param name="key"></param>
        /// <param name="type"></param>
        void WATCH(byte[] key, StoreType type);
    }
}