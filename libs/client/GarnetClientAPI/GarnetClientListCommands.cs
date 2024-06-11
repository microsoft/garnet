// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Garnet.client
{
    public sealed partial class GarnetClient
    {
        private static readonly Memory<byte> LPUSH = "$5\r\nLPUSH\r\n"u8.ToArray();
        private static readonly Memory<byte> RPUSH = "$5\r\nRPUSH\r\n"u8.ToArray();
        private static readonly Memory<byte> LRANGE = "$6\r\nLRANGE\r\n"u8.ToArray();
        private static readonly Memory<byte> LLEN = "$4\r\nLLEN\r\n"u8.ToArray();

        /// <summary>
        /// Add the specified element to the head of the list stored at key.
        /// </summary>
        /// <param name="key">The key of the list.</param>
        /// <param name="element">The element to be added.</param>
        /// <param name="callback">The callback function when operation completes.</param>
        /// <param name="context">An optional context to correlate request to callback.</param>
        public void ListLeftPush(string key, string element, Action<long, long, string> callback, long context = 0)
        {
            ListLeftPush(key, new[] { element }, callback, context);
        }

        /// <summary>
        /// Add the specified elements to the head of the list stored at key.
        /// </summary>
        /// <param name="key">The key of the list.</param>
        /// <param name="elements">The elements to be added.</param>
        /// <param name="callback">The callback function when operation completes.</param>
        /// <param name="context">An optional context to correlate request to callback.</param>
        public void ListLeftPush(string key, IEnumerable<string> elements, Action<long, long, string> callback, long context = 0)
        {
            ArgumentNullException.ThrowIfNull(key);
            ArgumentNullException.ThrowIfNull(elements);
            ArgumentNullException.ThrowIfNull(callback);

            var arrElem = new[] { key }.Union(elements).ToArray();

            if (arrElem.Length == 1)
            {
                throw new ArgumentException("Elements collection cannot be empty.", nameof(elements));
            }

            ExecuteForLongResult(callback, context, nameof(LPUSH), arrElem);
        }

        /// <summary>
        /// Asynchronously add the specified elements to the head of the list stored at key.
        /// </summary>
        /// <param name="key">The key of the list.</param>
        /// <param name="elements">The elements to be added.</param>
        /// <returns>The number of list elements after the addition.</returns>
        public async Task<long> ListLeftPushAsync(string key, params string[] elements)
        {
            ArgumentNullException.ThrowIfNull(key);
            ArgumentNullException.ThrowIfNull(elements);

            if (elements.Length == 0)
            {
                throw new ArgumentException("Elements collection cannot be empty.", nameof(elements));
            }

            return await ExecuteForLongResultAsync(nameof(LPUSH), [key, .. elements]);
        }

        /// <summary>
        /// Add the specified element to the tail of the list stored at key.
        /// </summary>
        /// <param name="key">The key of the list.</param>
        /// <param name="element">The element to be added.</param>
        /// <param name="callback">The callback function when operation completes.</param>
        /// <param name="context">An optional context to correlate request to callback.</param>
        public void ListRightPush(string key, string element, Action<long, long, string> callback, long context = 0)
        {
            ListRightPush(key, new[] { element }, callback, context);
        }

        /// <summary>
        /// Add the specified elements to the tail of the list stored at key.
        /// </summary>
        /// <param name="key">The key of the list.</param>
        /// <param name="elements">The elements to be added.</param>
        /// <param name="callback">The callback function when operation completes.</param>
        /// <param name="context">An optional context to correlate request to callback.</param>
        public void ListRightPush(string key, IEnumerable<string> elements, Action<long, long, string> callback, long context = 0)
        {
            ArgumentNullException.ThrowIfNull(key);
            ArgumentNullException.ThrowIfNull(elements);
            ArgumentNullException.ThrowIfNull(callback);

            var arrElem = new[] { key }.Union(elements).ToArray();

            if (arrElem.Length == 1)
            {
                throw new ArgumentException("Elements collection cannot be empty.", nameof(elements));
            }

            ExecuteForLongResult(callback, context, nameof(RPUSH), arrElem);
        }

        /// <summary>
        /// Asynchronously add the specified elements to the tail of the list stored at key.
        /// </summary>
        /// <param name="key">The key of the list.</param>
        /// <param name="elements">The elements to be added.</param>
        /// <returns>The number of list elements after the addition.</returns>
        public async Task<long> ListRightPushAsync(string key, params string[] elements)
        {
            ArgumentNullException.ThrowIfNull(key);
            ArgumentNullException.ThrowIfNull(elements);

            if (elements.Length == 0)
            {
                throw new ArgumentException("Elements collection cannot be empty.", nameof(elements));
            }

            return await ExecuteForLongResultAsync(nameof(RPUSH), [key, .. elements]);
        }

        /// <summary>
        /// Gets the values of the specified list key.
        /// </summary>
        /// <param name="key">The key of the list.</param>
        /// <param name="start">The offset start.</param>
        /// <param name="stop">The offset stop.</param>
        public async Task<string[]> ListRangeAsync(string key, int start, int stop)
        {
            ArgumentNullException.ThrowIfNull(key);

            return await ExecuteForStringArrayResultAsync(nameof(LRANGE), [key, start.ToString(), stop.ToString()]);
        }

        /// <summary>
        /// Gets the length of the list.
        /// </summary>
        /// <param name="key">The key of the list.</param>
        public async Task<long> ListLengthAsync(string key)
        {
            ArgumentNullException.ThrowIfNull(key);

            return await ExecuteForLongResultAsync(nameof(LLEN), [key]);
        }
    }
}