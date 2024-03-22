// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Garnet.client.GarnetClientAPI;

namespace Garnet.client
{
    public sealed partial class GarnetClient
    {

        static readonly Memory<byte> ZCARD = "$5\r\nZCARD\r\n"u8.ToArray();
        static readonly Memory<byte> ZADD = "$4\r\nZADD\r\n"u8.ToArray();
        static readonly Memory<byte> ZREM = "$4\r\nZREM\r\n"u8.ToArray();

        /// <summary>
        /// Adds/Updates a member, score in a SortedSet
        /// </summary>
        /// <param name="key"></param>
        /// <param name="member"></param>
        /// <param name="score"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public void SortedSetAdd(string key, string member, double score, Action<long, long, string> callback, long context = 0)
        {
            var parameters = new List<Memory<byte>>
            {
                Encoding.ASCII.GetBytes(key),
                Encoding.ASCII.GetBytes(score.ToString()),
                Encoding.ASCII.GetBytes(member)
            };

            ExecuteForLongResult(callback, context, ZADD, parameters);
        }

        /// <summary>
        /// Creates/Update a SortedSet 
        /// new members are added, existing members are updated.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="sortedSetEntries"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public void SortedSetAdd(string key, SortedSetPairCollection sortedSetEntries, Action<long, long, string> callback, long context = 0)
        {
            sortedSetEntries.Elements.Insert(0, Encoding.ASCII.GetBytes(key));
            ExecuteForLongResult(callback, context, ZADD, sortedSetEntries.Elements);
        }

        /// <summary>
        /// Adds/Updates a member, score in a SortedSet
        /// </summary>
        /// <param name="key"></param>
        /// <param name="member"></param>
        /// <param name="score"></param>
        /// <returns></returns>
        public Task<long> SortedSetAddAsync(string key, string member, double score)
        {
            var parameters = new List<Memory<byte>>
            {
                Encoding.ASCII.GetBytes(key),
                Encoding.ASCII.GetBytes(score.ToString()),
                Encoding.ASCII.GetBytes(member)
            };

            return ExecuteForLongResultAsync(ZADD, parameters);
        }

        /// <summary>
        /// Creates/Update a SortedSet 
        /// new members are added, existing members are updated.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="sortedSetEntries"></param>
        /// <returns></returns>
        public Task<long> SortedSetAddAsync(string key, SortedSetPairCollection sortedSetEntries)
        {
            sortedSetEntries.Elements.Insert(0, Encoding.ASCII.GetBytes(key));
            var result = ExecuteForLongResultAsync(ZADD, sortedSetEntries.Elements);
            sortedSetEntries.Elements.RemoveAt(0);
            return result;
        }

        /// <summary>
        /// Removes a [member, score] pair from a SortedSet
        /// </summary>
        /// <param name="key"></param>
        /// <param name="member"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public void SortedSetRemove(string key, string member, Action<long, long, string> callback, long context = 0)
        {
            var parameters = new List<Memory<byte>>
            {
                Encoding.ASCII.GetBytes(key),
                Encoding.ASCII.GetBytes(member)
            };

            ExecuteForLongResult(callback, context, ZREM, parameters);
        }

        /// <summary>
        /// Removes elements from a SortedSet 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="members"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public void SortedSetRemove(string key, List<string> members, Action<long, long, string> callback, long context = 0)
        {
            members.Insert(0, key);
            ExecuteForLongResult(callback, context, "ZREM", members);
            members.RemoveAt(0);
        }

        /// <summary>
        /// Removes a [member, score] pair from a SortedSet
        /// </summary>
        /// <param name="key"></param>
        /// <param name="member"></param>
        /// <param name="score"></param>
        /// <returns></returns>
        public Task<long> SortedSetRemoveAsync(string key, string member, double score)
        {
            var parameters = new List<Memory<byte>>
            {
                Encoding.ASCII.GetBytes(key),
                Encoding.ASCII.GetBytes(score.ToString()),
                Encoding.ASCII.GetBytes(member)
            };

            return ExecuteForLongResultAsync(ZREM, parameters);
        }

        /// <summary>
        /// Removes a [member, score] pair from a SortedSet
        /// </summary>
        /// <param name="key"></param>
        /// <param name="sortedSetEntries"></param>
        /// <returns></returns>
        public Task<long> SortedSetRemoveAsync(string key, SortedSetPairCollection sortedSetEntries)
        {
            sortedSetEntries.Elements.Insert(0, Encoding.ASCII.GetBytes(key));
            var result = ExecuteForLongResultAsync(ZREM, sortedSetEntries.Elements);
            sortedSetEntries.Elements.RemoveAt(0);
            return result;
        }

        /// <summary>
        /// Gets the current Length of a SortedSet (async)
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public Task<long> SortedSetLengthAsync(string key) => ExecuteForLongResultAsync(ZCARD, key);

        /// <summary>
        /// Gets the current Length of a SortedSet(sync)
        /// </summary>
        /// <param name="key"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public void SortedSetLength(string key, Action<long, long, string> callback, long context = 0)
           => ExecuteForLongResult(callback, context, ZCARD, key);
    }
}