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
    /// constant-returning instance properties. The JIT specializes GarnetServer&lt;T&gt; per struct,
    /// inlines the getters as constants, and eliminates dead branches entirely.
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
            var interfaceProperties = typeof(IGarnetServerOptions).GetProperties(
                BindingFlags.Public | BindingFlags.Instance);
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
        /// Creates a GarnetServer with a JIT-optimized options struct matching the provided configuration.
        /// The returned server has all config branch checks fully inlined and dead code eliminated.
        /// </summary>
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
        /// </summary>
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

            FlushMemoryLogger(initLogger, "ArgParser", loggerFactory);

            var opts = serverSettings.GetServerOptions(loggerFactory.CreateLogger("Options"));
            opts.AuthSettings = authenticationSettingsOverride ?? opts.AuthSettings;

            var server = CreateServer(opts, loggerFactory, cleanupDir: cleanupDir);

            if (disposeLoggerFactory)
            {
                server.DisposeLoggerFactory = true;
            }

            return server;
        }

        /// <summary>
        /// Creates a GarnetServer using RuntimeServerOptions (AOT-compatible, no Reflection.Emit).
        /// Property values are read from the opts instance at runtime — no branch elimination.
        /// </summary>
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
        /// Gets or creates a zero-size struct type with hardcoded constant returns for the given configuration.
        /// Thread-safe. Deduplicates by value — identical configurations reuse the same struct type.
        /// </summary>
        public static Type GetOrCreateOptionsStruct(GarnetServerOptions opts)
        {
            var values = new Dictionary<string, object>();
            var keyParts = new List<string>();

            foreach (var prop in Properties)
            {
                var value = prop.Getter(opts);
                values[prop.Name] = value;
                keyParts.Add($"{prop.Name}={value}");
            }

            var deduplicationKey = string.Join("|", keyParts);

            lock (EmitLock)
            {
                if (EmittedTypes.TryGetValue(deduplicationKey, out var existingType))
                    return existingType;

                var typeName = $"GarnetOpts_{EmittedTypes.Count}";

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
        /// Emits an instance property getter that returns a hardcoded constant.
        /// The JIT inlines this through the struct, seeing the constant value.
        /// </summary>
        private static void EmitConstantProperty(TypeBuilder tb, string name, Type propertyType, object value)
        {
            // Instance virtual method — implements the IGarnetServerOptions interface member
            var method = tb.DefineMethod($"get_{name}",
                MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig
                | MethodAttributes.SpecialName | MethodAttributes.NewSlot | MethodAttributes.Final,
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

            // Map the method to the interface method
            var interfaceMethod = typeof(IGarnetServerOptions).GetProperty(name)?.GetGetMethod();
            if (interfaceMethod != null)
                tb.DefineMethodOverride(method, interfaceMethod);
        }

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
    }
}
