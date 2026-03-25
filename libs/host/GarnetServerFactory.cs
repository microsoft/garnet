// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Reflection.Emit;
using Garnet.common;
using Garnet.server;
using Garnet.server.Auth.Settings;
using Microsoft.Extensions.Logging;

namespace Garnet
{
    /// <summary>
    /// Factory that creates GarnetServer instances with JIT-optimized configuration.
    /// At runtime, emits a zero-size struct implementing IGarnetServerOptions with hardcoded
    /// constant returns matching the provided GarnetServerOptions. The JIT then specializes
    /// GarnetServer&lt;T&gt; for that struct, inlining all config checks and eliminating dead branches.
    /// </summary>
    public static class GarnetServerFactory
    {
        /// <summary>
        /// Resp protocol version
        /// </summary>
        public const string RedisProtocolVersion = "7.4.3";

        private static readonly ModuleBuilder ModuleBuilder;
        private static readonly object EmitLock = new();
        private static readonly Dictionary<string, Type> EmittedTypes = new();

        static GarnetServerFactory()
        {
            var assembly = AssemblyBuilder.DefineDynamicAssembly(
                new AssemblyName("GarnetDynamicOptions"),
                AssemblyBuilderAccess.Run);
            ModuleBuilder = assembly.DefineDynamicModule("MainModule");

            // Verify at startup that factory covers all IGarnetServerOptions properties.
            // This catches missing properties immediately rather than at runtime struct creation.
            var interfaceProperties = typeof(IGarnetServerOptions).GetProperties(
                System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.FlattenHierarchy);
            var factoryPropertyNames = new HashSet<string>();
            foreach (var p in Properties)
                factoryPropertyNames.Add(p.Name);
            foreach (var ip in interfaceProperties)
            {
                if (!factoryPropertyNames.Contains(ip.Name))
                    throw new InvalidOperationException(
                        $"GarnetServerFactory is missing property '{ip.Name}' from IGarnetServerOptions. " +
                        $"Add it to the Properties array.");
            }
        }

        /// <summary>
        /// Interface property metadata for emission.
        /// </summary>
        private readonly struct PropertyInfo
        {
            public readonly string Name;
            public readonly Type Type;
            public readonly Func<GarnetServerOptions, object> Getter;

            public PropertyInfo(string name, Type type, Func<GarnetServerOptions, object> getter)
            {
                Name = name;
                Type = type;
                Getter = getter;
            }
        }

        /// <summary>
        /// All IGarnetServerOptions properties and how to read them from GarnetServerOptions.
        /// </summary>
        private static readonly PropertyInfo[] Properties =
        [
            // ServerOptions (base class) booleans
            new("EnableStorageTier", typeof(bool), o => o.EnableStorageTier),
            new("CopyReadsToTail", typeof(bool), o => o.CopyReadsToTail),
            new("Recover", typeof(bool), o => o.Recover),
            new("DisablePubSub", typeof(bool), o => o.DisablePubSub),
            new("FailOnRecoveryError", typeof(bool), o => o.FailOnRecoveryError),

            // GarnetServerOptions booleans
            new("DisableObjects", typeof(bool), o => o.DisableObjects),
            new("EnableCluster", typeof(bool), o => o.EnableCluster),
            new("CleanClusterConfig", typeof(bool), o => o.CleanClusterConfig),
            new("FastMigrate", typeof(bool), o => o.FastMigrate),
            new("EnableAOF", typeof(bool), o => o.EnableAOF),
            new("EnableLua", typeof(bool), o => o.EnableLua),
            new("LuaTransactionMode", typeof(bool), o => o.LuaTransactionMode),
            new("WaitForCommit", typeof(bool), o => o.WaitForCommit),
            new("CompactionForceDelete", typeof(bool), o => o.CompactionForceDelete),
            new("LatencyMonitor", typeof(bool), o => o.LatencyMonitor),
            new("QuietMode", typeof(bool), o => o.QuietMode),
            new("EnableIncrementalSnapshots", typeof(bool), o => o.EnableIncrementalSnapshots),
            new("UseFoldOverCheckpoints", typeof(bool), o => o.UseFoldOverCheckpoints),
            new("EnableFastCommit", typeof(bool), o => o.EnableFastCommit),
            new("EnableScatterGatherGet", typeof(bool), o => o.EnableScatterGatherGet),
            new("FastAofTruncate", typeof(bool), o => o.FastAofTruncate),
            new("OnDemandCheckpoint", typeof(bool), o => o.OnDemandCheckpoint),
            new("ReplicaDisklessSync", typeof(bool), o => o.ReplicaDisklessSync),
            new("UseAofNullDevice", typeof(bool), o => o.UseAofNullDevice),
            new("UseRevivBinsPowerOf2", typeof(bool), o => o.UseRevivBinsPowerOf2),
            new("RevivInChainOnly", typeof(bool), o => o.RevivInChainOnly),
            new("ExtensionAllowUnsignedAssemblies", typeof(bool), o => o.ExtensionAllowUnsignedAssemblies),
            new("EnableReadCache", typeof(bool), o => o.EnableReadCache),
            new("ClusterReplicaResumeWithData", typeof(bool), o => o.ClusterReplicaResumeWithData),
            new("AllowMultiDb", typeof(bool), o => o.AllowMultiDb),

            // Non-boolean hot-path fields
            new("MetricsSamplingFrequency", typeof(int), o => o.MetricsSamplingFrequency),
            new("SlowLogThreshold", typeof(int), o => o.SlowLogThreshold),
            new("SlowLogMaxEntries", typeof(int), o => o.SlowLogMaxEntries),
            new("ObjectScanCountLimit", typeof(int), o => o.ObjectScanCountLimit),
            new("MaxDatabases", typeof(int), o => o.MaxDatabases),
            new("EnableDebugCommand", typeof(ConnectionProtectionOption), o => o.EnableDebugCommand),
            new("EnableModuleCommand", typeof(ConnectionProtectionOption), o => o.EnableModuleCommand),
            new("CommitFrequencyMs", typeof(int), o => o.CommitFrequencyMs),
            new("CompactionFrequencySecs", typeof(int), o => o.CompactionFrequencySecs),
            new("IndexResizeFrequencySecs", typeof(int), o => o.IndexResizeFrequencySecs),
            new("AdjustedIndexMaxCacheLines", typeof(int), o => o.AdjustedIndexMaxCacheLines),
            new("AdjustedObjectStoreIndexMaxCacheLines", typeof(int), o => o.AdjustedObjectStoreIndexMaxCacheLines),
            new("ExpiredKeyDeletionScanFrequencySecs", typeof(int), o => o.ExpiredKeyDeletionScanFrequencySecs),
            new("ExpiredObjectCollectionFrequencySecs", typeof(int), o => o.ExpiredObjectCollectionFrequencySecs),
            new("NetworkSendThrottleMax", typeof(int), o => o.NetworkSendThrottleMax),
        ];

        /// <summary>
        /// Creates a GarnetServer using RuntimeServerOptions (AOT-compatible, no Reflection.Emit).
        /// Property values are read from the opts instance at runtime, so the JIT cannot eliminate
        /// branches. Use this when NativeAOT is required or as a simple non-dynamic alternative.
        /// </summary>
        /// <param name="opts">Server options</param>
        /// <param name="loggerFactory">Logger factory</param>
        /// <param name="servers">Optional IGarnetServer instances</param>
        /// <param name="cleanupDir">Whether to clean up data folders on dispose</param>
        /// <returns>A GarnetServer&lt;RuntimeServerOptions&gt; instance</returns>
        public static GarnetServer<RuntimeServerOptions> CreateAotServer(
            GarnetServerOptions opts,
            ILoggerFactory loggerFactory = null,
            IGarnetServer[] servers = null,
            bool cleanupDir = false)
        {
            RuntimeServerOptions.Instance = opts;
            return new GarnetServer<RuntimeServerOptions>(opts, loggerFactory, servers, cleanupDir);
        }

        /// <summary>
        /// Creates a GarnetServer with a JIT-optimized options struct matching the provided configuration.
        /// The returned server has all config branch checks fully inlined and dead code eliminated.
        /// Requires Reflection.Emit (not AOT-compatible). Use CreateAotServer for NativeAOT scenarios.
        /// </summary>
        /// <param name="opts">Server options instance to bake into the struct</param>
        /// <param name="loggerFactory">Logger factory</param>
        /// <param name="servers">Optional IGarnetServer instances</param>
        /// <param name="cleanupDir">Whether to clean up data folders on dispose</param>
        /// <returns>A GarnetServer&lt;T&gt; instance (as IGarnetServerApp) with optimized code paths</returns>
        public static IGarnetServerApp CreateServer(
            GarnetServerOptions opts,
            ILoggerFactory loggerFactory = null,
            IGarnetServer[] servers = null,
            bool cleanupDir = false)
        {
            var structType = GetOrCreateOptionsStruct(opts);
            var serverType = typeof(GarnetServer<>).MakeGenericType(structType);
            return (IGarnetServerApp)Activator.CreateInstance(serverType, opts, loggerFactory, servers, cleanupDir);
        }

        /// <summary>
        /// Creates a GarnetServer from command line arguments, with a JIT-optimized options struct.
        /// Handles argument parsing, logger factory setup, and struct emission.
        /// </summary>
        /// <param name="commandLineArgs">Command line arguments</param>
        /// <param name="loggerFactory">Logger factory (optional, created from args if null)</param>
        /// <param name="cleanupDir">Whether to clean up data folders on dispose</param>
        /// <param name="authenticationSettingsOverride">Override for custom authentication settings</param>
        /// <returns>A GarnetServer&lt;T&gt; instance (as IGarnetServerApp) with optimized code paths</returns>
        public static IGarnetServerApp CreateServer(
            string[] commandLineArgs,
            ILoggerFactory loggerFactory = null,
            bool cleanupDir = false,
            IAuthenticationSettings authenticationSettingsOverride = null)
        {
            Trace.Listeners.Add(new ConsoleTraceListener());

            MemoryLogger initLogger;
            using (var memLogProvider = new MemoryLoggerProvider())
            {
                initLogger = (MemoryLogger)memLogProvider.CreateLogger("ArgParser");
            }

            if (!ServerSettingsManager.TryParseCommandLineArguments(commandLineArgs, out var serverSettings, out _, out _, out var exitGracefully, logger: initLogger))
            {
                if (exitGracefully)
                    Environment.Exit(0);

                FlushMemoryLogger(initLogger, "ArgParser", loggerFactory);

                throw new GarnetException("Encountered an error when initializing Garnet server. Please see log messages above for more details.");
            }

            var disposeLoggerFactory = false;
            if (loggerFactory == null)
            {
                disposeLoggerFactory = true;
            }
            else
            {
                initLogger.LogWarning(
                    $"Received an external ILoggerFactory object. The following configuration options are ignored: {nameof(serverSettings.FileLogger)}, {nameof(serverSettings.LogLevel)}, {nameof(serverSettings.DisableConsoleLogger)}.");
            }

            loggerFactory ??= LoggerFactory.Create(builder =>
            {
                if (!serverSettings.DisableConsoleLogger.GetValueOrDefault())
                {
                    builder.AddSimpleConsole(options =>
                    {
                        options.SingleLine = true;
                        options.TimestampFormat = "hh::mm::ss ";
                    });
                }

                if (serverSettings.FileLogger != null)
                    builder.AddFile(serverSettings.FileLogger);
                builder.SetMinimumLevel(serverSettings.LogLevel);
            });

            // Flush initialization logs from memory logger
            FlushMemoryLogger(initLogger, "ArgParser", loggerFactory);

            var opts = serverSettings.GetServerOptions(loggerFactory.CreateLogger("Options"));
            opts.AuthSettings = authenticationSettingsOverride ?? opts.AuthSettings;

            var server = CreateServer(opts, loggerFactory, cleanupDir: cleanupDir);

            // If the factory created the logger factory, mark it for disposal with the server
            if (disposeLoggerFactory)
            {
                server.DisposeLoggerFactory = true;
            }

            return server;
        }

        /// <summary>
        /// Flushes MemoryLogger entries into a destination logger.
        /// </summary>
        private static void FlushMemoryLogger(MemoryLogger memoryLogger, string categoryName, ILoggerFactory dstLoggerFactory = null)
        {
            if (memoryLogger == null) return;

            var disposeDstLoggerFactory = false;
            if (dstLoggerFactory == null)
            {
                dstLoggerFactory = LoggerFactory.Create(builder => builder.AddSimpleConsole(options =>
                {
                    options.SingleLine = true;
                    options.TimestampFormat = "hh::mm::ss ";
                }).SetMinimumLevel(LogLevel.Information));
                disposeDstLoggerFactory = true;
            }

            var dstLogger = dstLoggerFactory.CreateLogger(categoryName);
            memoryLogger.FlushLogger(dstLogger);

            if (disposeDstLoggerFactory)
                dstLoggerFactory.Dispose();
        }

        /// <summary>
        /// Gets or creates a zero-size struct type with hardcoded constant returns for the given configuration.
        /// Deduplicates by value — identical configurations reuse the same struct type.
        /// Thread-safe: uses a lock to prevent concurrent emission of the same type.
        /// </summary>
        public static Type GetOrCreateOptionsStruct(GarnetServerOptions opts)
        {
            // Build a unique key from all property values for deduplication
            var values = new Dictionary<string, object>();
            var keyParts = new List<string>();

            foreach (var prop in Properties)
            {
                var value = prop.Getter(opts);
                values[prop.Name] = value;
                keyParts.Add($"{prop.Name}={value}");
            }

            // Use the full value string as the dedup key — no hashing, no collisions
            var deduplicationKey = string.Join("|", keyParts);

            lock (EmitLock)
            {
                // Reuse if this exact combination was already emitted
                if (EmittedTypes.TryGetValue(deduplicationKey, out var existingType))
                    return existingType;

                // Use a sequential counter for short, unique, collision-free type names
                var typeName = $"GarnetOpts_{EmittedTypes.Count}";

                // Define a zero-size struct implementing IGarnetServerOptions
                var typeBuilder = ModuleBuilder.DefineType(typeName,
                    TypeAttributes.Public | TypeAttributes.Sealed | TypeAttributes.BeforeFieldInit,
                    typeof(ValueType),
                    [typeof(IGarnetServerOptions)]);

                foreach (var prop in Properties)
                {
                    EmitConstantProperty(typeBuilder, prop.Name, prop.Type, values[prop.Name]);
                }

                var emittedType = typeBuilder.CreateType();
                EmittedTypes[deduplicationKey] = emittedType;
                return emittedType;
            }
        }

        /// <summary>
        /// Emits a static property getter that returns a hardcoded constant.
        /// The JIT inlines this to the constant value, enabling dead code elimination.
        /// </summary>
        private static void EmitConstantProperty(TypeBuilder tb, string name, Type propertyType, object value)
        {
            var method = tb.DefineMethod($"get_{name}",
                MethodAttributes.Public | MethodAttributes.Static | MethodAttributes.HideBySig
                | MethodAttributes.SpecialName | MethodAttributes.Virtual,
                propertyType, Type.EmptyTypes);

            var il = method.GetILGenerator();

            if (propertyType == typeof(bool))
                il.Emit((bool)value ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
            else if (propertyType == typeof(int))
                il.Emit(OpCodes.Ldc_I4, (int)value);
            else if (propertyType == typeof(long))
                il.Emit(OpCodes.Ldc_I8, (long)value);
            else if (propertyType.IsEnum)
                il.Emit(OpCodes.Ldc_I4, Convert.ToInt32(value));
            else
                throw new NotSupportedException($"Property type {propertyType} is not supported for JIT-inlined options");

            il.Emit(OpCodes.Ret);

            var prop = tb.DefineProperty(name, PropertyAttributes.None, propertyType, null);
            prop.SetGetMethod(method);
        }
    }
}