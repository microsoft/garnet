// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet
{
    /// <summary>
    /// This class contains all redis.conf settings that have a matching configurable setting in Garnet's Options class.
    /// In order to add a new redis.conf setting:
    /// 1. Verify that the matching Garnet setting is defined in Options.cs
    /// 2. If a custom type is necessary (e.g. enum), use existing or create a new one under RedisTypes.cs 
    /// 3. Add a new property and decorate it with an RedisOptionAttribute, pointing it to the matching Options property name.
    /// 4. If a type conversion is needed to convert between the Redis option and the Garnet option,
    ///    use existing type converters or add a new one under TypeConverters.cs
    /// 5. If a more complex transformer is needed to transform the Redis option and the Garnet option,
    ///    use existing type converters or add a new one under GarnetCustomTransformers.cs, and add its type to the RedisOptionAttribute
    /// </summary>
    internal class RedisOptions
    {
        private const string BindWarning = "Only first IP address specified in bind option is used. All other addresses are ignored.";
        private const string TlsPortWarning = "tls-port option is used only to determine if TLS is enabled. The port value is otherwise ignored.";
        private const string TlsCertFileWarning = @"When using tls-cert-file make sure to first convert your certificate format to .pfx. 
Specify your passphrase in the tls-key-file-pass option (or via the cert-password command line argument), if applicable. 
Specify your subject name via the cert-subject-name command line argument, if applicable.";

        [RedisOption("bind", nameof(Options.Address), BindWarning, typeof(ArrayToFirstItemTransformer<string>))]
        public Option<string[]> Bind { get; set; }

        [RedisOption("port", nameof(Options.Port))]
        public Option<int> Port { get; set; }

        [RedisOption("maxmemory", nameof(Options.MemorySize))]
        public Option<string> MaxMemory { get; set; }

        [RedisOption("logfile", nameof(Options.FileLogger))]
        public Option<string> LogFile { get; set; }

        [RedisOption("dir", nameof(Options.CheckpointDir))]
        public Option<string> Dir { get; set; }

        [RedisOption("cluster-enabled", nameof(Options.EnableCluster))]
        public Option<RedisBoolean> ClusterEnabled { get; set; }

        [RedisOption("requirepass", nameof(Options.Password))]
        public Option<string> RequirePass { get; set; }

        [RedisOption("aclfile", nameof(Options.AclFile))]
        public Option<string> AclFile { get; set; }

        [RedisOption("cluster-node-timeout", nameof(Options.ClusterTimeout))]
        public Option<int> ClusterNodeTimeout { get; set; }

        [RedisOption("tls-port", nameof(Options.EnableTLS), TlsPortWarning, typeof(NonDefaultObjectToBooleanTransformer<int>))]
        public Option<int> TlsPort { get; set; }

        /// <summary>
        /// Note: Garnet currently supports TLS using a .pfx file and passphrase, while Redis supports TLS using .crt and .key files.
        /// In order to use TLS in Garnet while using redis.conf, convert your certificate to .pfx format (more details in the Security section on Garnet's website),
        /// then use the .pfx file path as the `tls-cert-file` value.
        /// If a passphrase was used when creating the original certificate, specify it in the `tls-key-file-pass` parameter
        /// as you would in Redis (or via the `--cert-password` command line argument). When starting the server,
        /// use the `--cert-subject-name` command line argument to set the certificate subject name, if applicable.
        /// </summary>
        [RedisOption("tls-cert-file", nameof(Options.CertFileName), TlsCertFileWarning)]
        public Option<string> TlsCertFile { get; set; }

        [RedisOption("tls-key-file-pass", nameof(Options.CertPassword))]
        public Option<string> TlsKeyFilePass { get; set; }

        [RedisOption("tls-auth-clients", nameof(Options.ClientCertificateRequired))]
        public Option<RedisTlsAuthClients> TlsAuthClients { get; set; }

        [RedisOption("latency-tracking", nameof(Options.LatencyMonitor))]
        public Option<RedisBoolean> LatencyTracking { get; set; }

        [RedisOption("loglevel", nameof(Options.LogLevel))]
        public Option<RedisLogLevel> LogLevel { get; set; }

        [RedisOption("io-threads", nameof(Options.ThreadPoolMinThreads))]
        public Option<int> IoThreads { get; set; }

        [RedisOption("repl-diskless-sync-delay", nameof(Options.ReplicaSyncDelayMs))]
        public Option<int> ReplicaDisklessSyncDelay { get; set; }
    }

    /// <summary>
    /// Wrapper for each RedisOption value, we use this in order to distinguish between an unset value and a value set to default
    /// i.e. if the Option object is null - the value was not set, if the underlying value is default(T), then the value was set to default(T)
    /// This is required in order to determine order of precedence when setting options from multiple sources
    /// </summary>
    /// <typeparam name="T">The underlying type of the option</typeparam>
    internal class Option<T>
    {
        public T Value { get; set; }
    }

    /// <summary>
    /// Attribute defining a RedisOption, must decorate each property in RedisOptions that is deserialized from a redis.conf file
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    internal class RedisOptionAttribute : Attribute
    {
        /// <summary>
        /// Redis key
        /// </summary>
        public string Key { get; }

        /// <summary>
        /// Matching property name in Options.cs
        /// </summary>
        public string GarnetOptionName { get; }

        /// <summary>
        /// Optional custom transformer type, to transform RedisOption property value to Options property value
        /// </summary>
        public Type GarnetCustomTransformer { get; }

        /// <summary>
        /// Optional warning to user when option usage may differ from Redis
        /// </summary>
        public string UsageWarning { get; }

        /// <summary>
        /// Defines a RedisOption
        /// </summary>
        /// <param name="key">Redis Key</param>
        /// <param name="garnetOptionName">Matching property name in Options.cs</param>
        /// <param name="usageWarning">Optional warning to user when option usage may differ from Redis</param>
        /// <param name="garnetCustomTransformer">Optional custom transformer type, to transform RedisOption property value to Options property value</param>
        internal RedisOptionAttribute(string key, string garnetOptionName, string usageWarning = null, Type garnetCustomTransformer = null)
        {
            this.Key = key;
            this.GarnetOptionName = garnetOptionName;
            this.UsageWarning = usageWarning;
            this.GarnetCustomTransformer = garnetCustomTransformer;
        }
    }
}