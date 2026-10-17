// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace RateLimitCacheSim
{
    /// <summary>
    /// Shape of a single cache operation.
    /// </summary>
    public enum OpShape
    {
        /// <summary>Lua script performing a single HGET. ~80% of the scripted traffic.</summary>
        ScriptRead,

        /// <summary>Lua script performing HGET + HSET + EXPIRE. ~20% of the scripted traffic.</summary>
        ScriptReadWrite,

        /// <summary>Plain (non-scripted) GET, standing in for the ~3% of traffic that is not EVAL/EVALSHA.</summary>
        PlainGet,
    }

    /// <summary>
    /// The Lua scripts used to model the token-bucket workload.
    /// </summary>
    public static class RateLimitScripts
    {
        /// <summary>
        /// Read-only bucket lookup: a single HGET inside a script.
        /// </summary>
        public const string Read = "return redis.call('HGET', KEYS[1], ARGV[1])";

        /// <summary>
        /// Bucket sync: HGET the current count, write the new count back and refresh the TTL.
        /// </summary>
        public const string ReadWrite = "local v = redis.call('HGET', KEYS[1], ARGV[1])\n" +
            "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])\n" +
            "redis.call('EXPIRE', KEYS[1], ARGV[3])\n" +
            "return v";

        /// <summary>
        /// Returns the script body for a scripted shape.
        /// </summary>
        /// <param name="shape">Operation shape.</param>
        /// <returns>The Lua source.</returns>
        public static string Body(OpShape shape) => shape switch
        {
            OpShape.ScriptRead => Read,
            OpShape.ScriptReadWrite => ReadWrite,
            _ => throw new ArgumentOutOfRangeException(nameof(shape)),
        };
    }
}