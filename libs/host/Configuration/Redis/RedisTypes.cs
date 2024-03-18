// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.ComponentModel;

namespace Garnet
{
    /// <summary>
    /// Legal values for redis.conf tls-auth-clients key
    /// </summary>
    [TypeConverter(typeof(RedisTlsClientsTypeConverter))]
    internal enum RedisTlsAuthClients
    {
        Yes,
        No,
        Optional
    }

    /// <summary>
    /// Legal values for redis.conf loglevel key
    /// </summary>
    [TypeConverter(typeof(RedisLogLevelTypeConverter))]
    internal enum RedisLogLevel
    {
        Debug,
        Verbose,
        Notice,
        Warning,
        Nothing
    }

    /// <summary>
    /// Legal values for redis.conf booleans
    /// </summary>
    [TypeConverter(typeof(RedisBooleanTypeConverter))]
    internal enum RedisBoolean
    {
        Yes,
        No
    }
}