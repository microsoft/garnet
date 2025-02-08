// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.ComponentModel;
using Garnet.server.Auth.Settings;

namespace Garnet
{
    /// <summary>
    /// Legal values for redis.conf enable-debug-command and enable-protected-configs keys.
    /// </summary>
    public enum RedisConnectionProtectionOption
    {
        No = 0, // Block
        Local = 1, // AllowForLocalConnections
        Yes = 2, // AllowForAll
        All = 2 // Garnet Extension.
    }

    public static class RedisConnectionProtectionOptionExtensions
    {
        public static ConnectionProtectionOption ToGarnetOption(this RedisConnectionProtectionOption opt)
        {
            return opt switch
            {
                RedisConnectionProtectionOption.Yes or RedisConnectionProtectionOption.All =>
                    ConnectionProtectionOption.AllowForAll,
                RedisConnectionProtectionOption.Local => ConnectionProtectionOption.AllowForLocalConnections,
                _ => ConnectionProtectionOption.Block
            };
        }
    }

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