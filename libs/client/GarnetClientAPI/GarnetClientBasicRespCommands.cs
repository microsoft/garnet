// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;

namespace Garnet.client
{
    public sealed partial class GarnetClient
    {
        #region quitCMD
        /// <summary>
        /// Send ping
        /// </summary>
        /// <returns></returns>
        public Task<string> QuitAsync() => ExecuteForStringResultAsync(QUIT, default(string));

        /// <summary>
        /// Send ping
        /// </summary>
        /// <returns></returns>
        public Task<string> QuitAsync(CancellationToken token) => ExecuteForStringResultWithCancellationAsync(QUIT, default(string), null, token);

        /// <summary>
        /// Send ping
        /// </summary>
        /// <returns></returns>
        public void Quit(Action<long, string> callback, long context = 0)
            => ExecuteForStringResult(callback, context, QUIT, default(string));
        #endregion

        #region pingCMD
        /// <summary>
        /// Send ping
        /// </summary>
        /// <returns></returns>
        public Task<string> PingAsync() => ExecuteForStringResultAsync(PING, default(string));

        /// <summary>
        /// Send ping
        /// </summary>
        /// <returns></returns>
        public Task<string> PingAsync(CancellationToken token) => ExecuteForStringResultWithCancellationAsync(PING, default(string), null, token);

        /// <summary>
        /// Send ping
        /// </summary>
        /// <returns></returns>
        public void Ping(Action<long, string> callback, long context = 0)
            => ExecuteForStringResult(callback, context, PING, default(string));
        #endregion

        #region getCMD
        /// <summary>
        /// Get value for given key
        /// </summary>
        /// <param name="key">Key</param>
        /// <returns></returns>
        public Task<string> StringGetAsync(string key) => ExecuteForStringResultAsync(GET, key);

        /// <summary>
        /// Get value for given key
        /// </summary>
        /// <param name="key"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public Task<string> StringGetAsync(string key, CancellationToken token) => ExecuteForStringResultWithCancellationAsync(GET, key, null, token);

        /// <summary>
        /// Get value for given key
        /// </summary>
        /// <param name="key">Key</param>
        /// <returns></returns>
        public Task<MemoryResult<byte>> StringGetAsMemoryAsync(string key) => ExecuteForMemoryResultAsync(GET, key);

        /// <summary>
        /// Get value for given key
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="token"></param>
        /// <returns></returns>
        public Task<MemoryResult<byte>> StringGetAsMemoryAsync(string key, CancellationToken token) => ExecuteForMemoryResultWithCancellationAsync(GET, key, null, token);

        /// <summary>
        /// Get value for given key
        /// </summary>
        /// <param name="key">Key</param>
        /// <returns></returns>
        public Task<MemoryResult<byte>> StringGetAsMemoryAsync(Memory<byte> key) => ExecuteForMemoryResultAsync(GET, key);

        /// <summary>
        /// Get value for given key
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="token"></param>
        /// <returns></returns>
        public Task<MemoryResult<byte>> StringGetAsMemoryAsync(Memory<byte> key, CancellationToken token) => ExecuteForMemoryResultWithCancellationAsync(GET, key, null, token);

        /// <summary>
        /// Get value for given key
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <returns></returns>
        public void StringGet(string key, Action<long, string> callback, long context = 0)
            => ExecuteForStringResult(callback, context, GET, key);

        /// <summary>
        /// Get value for given key
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <returns></returns>
        public void StringGetAsMemory(Memory<byte> key, Action<long, MemoryResult<byte>> callback = default, long context = 0)
            => ExecuteForMemoryResult(callback, context, GET, key);
        #endregion

        #region mgetCMD

        /// <summary>
        /// Get for multiple keys  with string array
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public void StringGet(string[] keys, Action<long, string[], string> callback, long context = 0)
            => ExecuteForStringArrayResult(callback, context, "MGET", keys);

        /// <summary>
        /// Get for multiple keys with Memory array type
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public void StringGetAsMemory(Memory<byte>[] keys, Action<long, MemoryResult<byte>[], MemoryResult<byte>> callback, long context = 0)
        => ExecuteForMemoryResultArray(callback, context, MGET, keys);


        /// <summary>
        /// Async get for multiple keys with Memory array type
        /// </summary>
        /// <param name="keys"></param>
        /// <returns></returns>
        public Task<MemoryResult<byte>[]> StringGetAsMemoryAsync(Memory<byte>[] keys) => ExecuteForMemoryResultArrayAsync(MGET, keys);

        /// <summary>
        /// Async get for multiple keys with string array type
        /// </summary>
        /// <param name="keys"></param>
        /// <returns></returns>
        public Task<string[]> StringGetAsync(string[] keys) => ExecuteForStringArrayResultAsync("MGET", keys);

        /// <summary>
        /// Async get for multiple keys with string array type 
        /// using cancellation token
        /// </summary>  
        /// <param name="keys"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public Task<string[]> StringGetAsync(string[] keys, CancellationToken token) => ExecuteForStringArrayResultWithCancellationAsync("MGET", keys, token);

        /// <summary>
        /// Async Get for multiple keys as memory result
        /// </summary>
        /// <param name="keys"></param>
        /// <returns></returns>
        public Task<MemoryResult<byte>> StringGetAsMemoryAsync(string[] keys) => ExecuteForMemoryResultAsync("MGET", keys);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public Task<MemoryResult<byte>> StringGetAsMemoryAsync(string[] keys, CancellationToken token) => ExecuteForMemoryResultWithCancellationAsync("MGET", keys, token);

        #endregion

        #region setCMD
        /// <summary>
        /// Set given value, for given key
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <returns></returns>
        public async Task<bool> StringSetAsync(string key, string value)
            => await ExecuteForStringResultAsync(SET, key, value) == "OK";

        /// <summary>
        /// Set given value, for given key
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<bool> StringSetAsync(string key, string value, CancellationToken token)
            => await ExecuteForStringResultWithCancellationAsync(SET, key, value, token) == "OK";

        /// <summary>
        /// Set given value, for given key
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <returns></returns>
        public async Task<bool> StringSetAsync(Memory<byte> key, Memory<byte> value)
            => await ExecuteForStringResultAsync(SET, key, value) == "OK";

        /// <summary>
        /// Set given value, for given key
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <param name="token">Value</param>
        /// <returns></returns>
        public async Task<bool> StringSetAsync(Memory<byte> key, Memory<byte> value, CancellationToken token)
            => await ExecuteForStringResultWithCancellationAsync(SET, key, value, token) == "OK";

        /// <summary>
        /// Set given value, for given key
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <returns></returns>
        public void StringSet(string key, string value, Action<long, string> callback, long context = 0)
            => ExecuteForStringResult(callback, context, SET, key, value);

        /// <summary>
        /// Set given value, for given key
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <returns></returns>
        public void StringSet(Memory<byte> key, Memory<byte> value, Action<long, string> callback, long context = 0)
            => ExecuteForStringResult(callback, context, SET, key, value);
        #endregion

        #region delCMD
        /// <summary>
        /// Delete given key
        /// </summary>
        /// <param name="key">Key</param>
        /// <returns></returns>
        public async Task<bool> KeyDeleteAsync(string key)
            => await ExecuteForStringResultAsync(DEL, key) == "1";

        /// <summary>
        /// Delete given key
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="token">Key</param>
        /// <returns></returns>
        public async Task<bool> KeyDeleteAsync(string key, CancellationToken token)
            => await ExecuteForStringResultWithCancellationAsync(DEL, key, null, token) == "1";

        /// <summary>
        /// Delete given key
        /// </summary>
        /// <param name="key">Key</param>
        /// <returns></returns>
        public async Task<bool> KeyDeleteAsync(Memory<byte> key)
            => await ExecuteForStringResultAsync(DEL, key) == "1";

        /// <summary>
        /// Delete given key
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task<bool> KeyDeleteAsync(Memory<byte> key, CancellationToken token)
            => await ExecuteForStringResultWithCancellationAsync(DEL, key, null, token) == "1";

        /// <summary>
        /// Delete given key
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <returns></returns>
        public void KeyDelete(string key, Action<long, string> callback, long context = 0)
            => ExecuteForStringResult(callback, context, DEL, key);

        /// <summary>
        /// Delete given key
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <returns></returns>
        public void KeyDelete(Memory<byte> key, Action<long, string> callback, long context = 0)
            => ExecuteForStringResult(callback, context, DEL, key);

        #region bulkdeleteCmd

        /// <summary>
        /// Delete given keys
        /// </summary>
        /// <param name="keys">Keys for delete</param>
        /// <returns>The number of keys deleted, or -1 if there was an error</returns>
        public async Task<long> KeyDeleteAsync(string[] keys)
            => await ExecuteForLongResultAsync("DEL", keys);


        /// <summary>
        /// Delete given key with cancellation token
        /// </summary>
        /// <param name="keys">Keys for delete</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task<long> KeyDeleteAsync(Memory<byte>[] keys, CancellationToken token)
            => await ExecuteForLongResultWithCancellationAsync(DEL, keys, token);



        /// <summary>
        /// Delete given keys with a cancellation token
        /// </summary>
        /// <param name="keys">Keys to delete</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task<long> KeyDeleteAsync(string[] keys, CancellationToken token)
            => await ExecuteForLongResultWithCancellationAsync("DEL", keys, token);

        /// <summary>
        /// Delete the given keys
        /// </summary>
        /// <param name="keys">Keys</param>
        /// <returns></returns>
        public async Task<long> KeyDeleteAsync(Memory<byte>[] keys)
            => await ExecuteForLongResultAsync(DEL, keys);

        /// <summary>
        /// Delete the given keys with a callback fuction
        /// </summary>
        /// <param name="keys">Keys to delete</param>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <returns></returns>
        public void KeyDelete(string[] keys, Action<long, long, string> callback, long context = 0)
            => ExecuteForLongResult(callback, context, "DEL", keys);

        /// <summary>
        /// Delete given key
        /// </summary>
        /// <param name="keys">Key</param>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <returns></returns>
        public void KeyDelete(Memory<byte>[] keys, Action<long, long, string> callback, long context = 0)
            => ExecuteForLongResult(callback, context, DEL, keys);

        #endregion

        #endregion

        #region incrCmd
        /// <summary>
        /// Increment number stored at key by 1.
        /// </summary>
        /// <param name="key">Key</param>
        /// <returns></returns>
        public async Task<long> StringIncrement(string key)
            => long.Parse(await ExecuteForStringResultAsync(INCR, key));

        /// <summary>
        /// Increment number stored at key by 1.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="token">Value</param>
        /// <returns></returns>
        public async Task<long> StringIncrement(string key, CancellationToken token)
            => long.Parse(await ExecuteForStringResultWithCancellationAsync(INCR, key, null, token));

        /// <summary>
        /// Increment number stored at key by 1.
        /// </summary>
        /// <param name="key">Key</param>
        /// <returns></returns>
        public async Task<long> StringIncrement(Memory<byte> key)
            => long.Parse(await ExecuteForStringResultAsync(INCR, key));

        /// <summary>
        /// Increment number stored at key by 1.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="token">Key</param>
        /// <returns></returns>
        public async Task<long> StringIncrement(Memory<byte> key, CancellationToken token)
            => long.Parse(await ExecuteForStringResultWithCancellationAsync(INCR, key, null, token));

        /// <summary>
        /// Increment number stored at key by 1.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <returns></returns>
        public void StringIncrement(string key, Action<long, string> callback, long context = 0)
            => ExecuteForStringResult(callback, context, INCR, key);

        /// <summary>
        /// Increment number stored at key by 1.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <returns></returns>
        public void StringIncrement(Memory<byte> key, Action<long, string> callback, long context = 0)
            => ExecuteForStringResult(callback, context, INCR, key);
        #endregion

        #region incrByCmd
        /// <summary>
        /// Increment number stored at key by value.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <returns></returns>
        public async Task<long> StringIncrement(string key, long value)
            => long.Parse(await ExecuteForStringResultAsync(INCRBY, key, value.ToString()));

        /// <summary>
        /// Increment number stored at key by value.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <param name="token">Value</param>
        /// <returns></returns>
        public async Task<long> StringIncrement(string key, long value, CancellationToken token)
            => long.Parse(await ExecuteForStringResultWithCancellationAsync(INCRBY, key, value.ToString(), token));

        /// <summary>
        /// Increment number stored at key by value.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <returns></returns>
        public async Task<long> StringIncrement(Memory<byte> key, long value)
            => long.Parse(await ExecuteForStringResultAsync(INCRBY, key, Encoding.ASCII.GetBytes(value.ToString())));

        /// <summary>
        /// Increment number stored at key by value.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <param name="token">Value</param>
        /// <returns></returns>
        public async Task<long> StringIncrement(Memory<byte> key, long value, CancellationToken token)
            => long.Parse(await ExecuteForStringResultWithCancellationAsync(INCRBY, key, Encoding.ASCII.GetBytes(value.ToString()), token));

        /// <summary>
        /// Increment number stored at key by value.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <returns></returns>
        public void StringIncrement(string key, long value, Action<long, string> callback, long context = 0)
            => ExecuteForStringResult(callback, context, INCRBY, key, value.ToString());

        /// <summary>
        /// Increment number stored at key by value.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <returns></returns>
        public void StringIncrement(Memory<byte> key, long value, Action<long, string> callback, long context = 0)
            => ExecuteForStringResult(callback, context, INCRBY, key, Encoding.ASCII.GetBytes(value.ToString()));
        #endregion

        #region decrCmd
        /// <summary>
        /// Decrement number stored at key by 1.
        /// </summary>
        /// <param name="key">Key</param>
        /// <returns></returns>
        public async Task<long> StringDecrement(string key)
            => long.Parse(await ExecuteForStringResultAsync(DECR, key));

        /// <summary>
        /// Decrement number stored at key by 1.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="token">Value</param>
        /// <returns></returns>
        public async Task<long> StringDecrement(string key, CancellationToken token)
            => long.Parse(await ExecuteForStringResultWithCancellationAsync(DECR, key, null, token));

        /// <summary>
        /// Decrement number stored at key by 1.
        /// </summary>
        /// <param name="key">Key</param>
        /// <returns></returns>
        public async Task<long> StringDecrement(Memory<byte> key)
            => long.Parse(await ExecuteForStringResultAsync(DECR, key));

        /// <summary>
        /// Decrement number stored at key by 1.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="token">Key</param>
        /// <returns></returns>
        public async Task<long> StringDecrement(Memory<byte> key, CancellationToken token)
            => long.Parse(await ExecuteForStringResultWithCancellationAsync(DECR, key, null, token));

        /// <summary>
        /// Decrement number stored at key by 1.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <returns></returns>
        public void StringDecrement(string key, Action<long, string> callback, long context = 0)
            => ExecuteForStringResult(callback, context, DECR, key);

        /// <summary>
        /// Decrement number stored at key by 1.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <returns></returns>
        public void StringDecrement(Memory<byte> key, Action<long, string> callback, long context = 0)
            => ExecuteForStringResult(callback, context, DECR, key);
        #endregion

        #region decrByCmd
        /// <summary>
        /// Decrement number stored at key by value.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <returns></returns>
        public async Task<long> StringDecrement(string key, long value)
            => long.Parse(await ExecuteForStringResultAsync(DECRBY, key, value.ToString()));

        /// <summary>
        /// Decrement number stored at key by value.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <param name="token">Value</param>
        /// <returns></returns>
        public async Task<long> StringDecrement(string key, long value, CancellationToken token)
            => long.Parse(await ExecuteForStringResultWithCancellationAsync(DECRBY, key, value.ToString(), token));

        /// <summary>
        /// Increment number stored at key by value.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <returns></returns>
        public async Task<long> StringDecrement(Memory<byte> key, long value)
            => long.Parse(await ExecuteForStringResultAsync(DECRBY, key, Encoding.ASCII.GetBytes(value.ToString())));

        /// <summary>
        /// Decrement number stored at key by value.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <param name="token">Value</param>
        /// <returns></returns>
        public async Task<long> StringDecrement(Memory<byte> key, long value, CancellationToken token)
            => long.Parse(await ExecuteForStringResultWithCancellationAsync(DECRBY, key, Encoding.ASCII.GetBytes(value.ToString()), token));

        /// <summary>
        /// Decrement number stored at key by value.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <returns></returns>
        public void StringDecrement(string key, long value, Action<long, string> callback, long context = 0)
            => ExecuteForStringResult(callback, context, DECRBY, key, value.ToString());

        /// <summary>
        /// Decrement number stored at key by value.
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="value">Value</param>
        /// <param name="callback">Callback function when operation completes</param>
        /// <param name="context">Optional context to correlate request to callback</param>
        /// <returns></returns>
        public void StringDecrement(Memory<byte> key, long value, Action<long, string> callback, long context = 0)
            => ExecuteForStringResult(callback, context, DECRBY, key, Encoding.ASCII.GetBytes(value.ToString()));
        #endregion
    }
}