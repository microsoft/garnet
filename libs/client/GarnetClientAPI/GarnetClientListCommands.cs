// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Garnet.client;

public sealed partial class GarnetClient
{
    static readonly Memory<byte> LPUSH = "$5\r\nLPUSH\r\n"u8.ToArray();
    static readonly Memory<byte> RPUSH = "$5\r\nRPUSH\r\n"u8.ToArray();

    public void ListLeftPush(string key, string element, Action<long, long, string> callback, long context = 0)
    {
        var args = new List<Memory<byte>>
        {
            Encoding.ASCII.GetBytes(key),
            Encoding.ASCII.GetBytes(element)
        };

        ExecuteForLongResult(callback, context, LPUSH, args);
    }

    public void ListLeftPush(string key, List<string> elements, Action<long, long, string> callback, long context = 0)
    {
        elements.Insert(0, key);
        
        ExecuteForLongResult(callback, context, "LPUSH", elements);
        
        elements.RemoveAt(0);
    }

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

    public void ListRightPush(string key, string element, Action<long, long, string> callback, long context = 0)
    {
        var args = new List<Memory<byte>>
        {
            Encoding.ASCII.GetBytes(key),
            Encoding.ASCII.GetBytes(element)
        };

        ExecuteForLongResult(callback, context, RPUSH, args);
    }

    public void ListRightPush(string key, List<string> elements, Action<long, long, string> callback, long context = 0)
    {
        elements.Insert(0, key);

        ExecuteForLongResult(callback, context, "RPUSH", elements);

        elements.RemoveAt(0);
    }

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
