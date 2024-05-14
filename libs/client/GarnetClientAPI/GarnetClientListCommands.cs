// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Garnet.client
{
    public sealed partial class GarnetClient
    {
        private static readonly Memory<byte> LPUSH = "$5\r\nLPUSH\r\n"u8.ToArray();
        private static readonly Memory<byte> RPUSH = "$5\r\nRPUSH\r\n"u8.ToArray();

        /// <summary>
        /// Add the specified element to the head of the list stored at key.
        /// </summary>
        /// <param name="key">The key of the list.</param>
        /// <param name="element">The element to be added.</param>
        /// <param name="callback">The callback function when operation completes.</param>
        /// <param name="context">An optional context to correlate request to callback.</param>
        public void ListLeftPush(string key, string element, Action<long, long, string> callback, long context = 0)
        {
            var args = new List<Memory<byte>>
            {
                Encoding.ASCII.GetBytes(key),
                Encoding.ASCII.GetBytes(element)
            };

            ExecuteForLongResult(callback, context, LPUSH, args);
        }

        /// <summary>
        /// Add the specified elements to the head of the list stored at key.
        /// </summary>
        /// <param name="key">The key of the list.</param>
        /// <param name="elements">The elements to be added.</param>
        /// <param name="callback">The callback function when operation completes.</param>
        /// <param name="context">An optional context to correlate request to callback.</param>
        public void ListLeftPush(string key, List<string> elements, Action<long, long, string> callback, long context = 0)
        {
            elements.Insert(0, key);

            ExecuteForLongResult(callback, context, "LPUSH", elements);

            elements.RemoveAt(0);
        }

        /// <summary>
        /// Add the specified elements to the head of the list stored at key in asynchronous fashion.
        /// </summary>
        /// <param name="key">The key of the list.</param>
        /// <param name="elements">The elements to be added.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        /// <exception cref="ArgumentNullException"></exception>
        public async Task<long> ListLeftPushAsync(string key, params string[] elements)
        {
            if (string.IsNullOrEmpty(key))
            {
                throw new ArgumentException($"'{nameof(key)}' cannot be null or empty.", nameof(key));
            }

            if (elements is null)
            {
                throw new ArgumentNullException(nameof(elements));
            }

            return await ExecuteForLongResultAsync("LPUSH", [key, .. elements]);
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
            var args = new List<Memory<byte>>
            {
                Encoding.ASCII.GetBytes(key),
                Encoding.ASCII.GetBytes(element)
            };

            ExecuteForLongResult(callback, context, RPUSH, args);
        }

        /// <summary>
        /// Add the specified elements to the tail of the list stored at key.
        /// </summary>
        /// <param name="key">The key of the list.</param>
        /// <param name="elements">The elements to be added.</param>
        /// <param name="callback">The callback function when operation completes.</param>
        /// <param name="context">An optional context to correlate request to callback.</param>
        public void ListRightPush(string key, List<string> elements, Action<long, long, string> callback, long context = 0)
        {
            elements.Insert(0, key);

            ExecuteForLongResult(callback, context, "RPUSH", elements);

            elements.RemoveAt(0);
        }

        /// <summary>
        /// Add the specified elements to the tail of the list stored at key in asynchronous fashion.
        /// </summary>
        /// <param name="key">The key of the list.</param>
        /// <param name="elements">The elements to be added.</param>
        /// <returns>The number of the list elements.</returns>
        /// <exception cref="ArgumentException"></exception>
        /// <exception cref="ArgumentNullException"></exception>
        public async Task<long> ListRightPushAsync(string key, params string[] elements)
        {
            if (string.IsNullOrEmpty(key))
            {
                throw new ArgumentException($"'{nameof(key)}' cannot be null or empty.", nameof(key));
            }

            if (elements is null)
            {
                throw new ArgumentNullException(nameof(elements));
            }

            return await ExecuteForLongResultAsync("RPUSH", [key, .. elements]);
        }
    }
}