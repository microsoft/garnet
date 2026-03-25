// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;
using Garnet.server.Auth.Settings;

namespace Garnet.server
{
    /// <summary>
    /// Factory that emits zero-size structs implementing IGarnetServerOptions with hardcoded
    /// constant-returning instance properties at runtime via Reflection.Emit.
    ///
    /// When used as a type parameter for generic types (e.g., RespServerSession&lt;T&gt;,
    /// GarnetProvider&lt;T&gt;), the JIT specializes per struct, inlines the getters as constants,
    /// and eliminates dead branches entirely.
    ///
    /// Thread-safe. Deduplicates emitted types by value — identical configurations reuse the
    /// same struct type. Validates at startup that all IGarnetServerOptions properties are covered.
    ///
    /// See GarnetOptionsFactory.Samples.cs for usage examples.
    /// </summary>
    public static class GarnetOptionsFactory
    {
        private static readonly ModuleBuilder ModuleBuilder;
        private static readonly object EmitLock = new();
        private static readonly Dictionary<string, Type> EmittedTypes = new();

        static GarnetOptionsFactory()
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
                        $"GarnetOptionsFactory is missing property '{ip.Name}' from IGarnetServerOptions. " +
                        $"Add it to the Properties array.");
            }
        }

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
        /// Gets or creates a zero-size struct type with hardcoded constant returns for the given
        /// configuration. Thread-safe. Deduplicates by value — identical configs reuse the same type.
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
                    EmitConstantProperty(typeBuilder, prop.Name, prop.Type, values[prop.Name]);

                var emittedType = typeBuilder.CreateType();
                EmittedTypes[deduplicationKey] = emittedType;
                return emittedType;
            }
        }

        /// <summary>
        /// Creates an instance using a type-safe factory callback. The user subclasses
        /// TypedOptionsFactory and writes the constructor call with full compile-time type checking.
        /// The factory handles runtime dispatch to the emitted struct type.
        /// See GarnetOptionsFactory.Samples.cs for usage examples.
        /// </summary>
        /// <typeparam name="TResult">Return type (typically a non-generic base class or interface)</typeparam>
        /// <param name="opts">Server options to bake into the emitted struct</param>
        /// <param name="factory">User-provided factory that constructs the generic type</param>
        public static TResult Create<TResult>(GarnetServerOptions opts, TypedOptionsFactory<TResult> factory)
        {
            var structType = GetOrCreateOptionsStruct(opts);

            // Invoke factory.Create<EmittedStruct>() via reflection on the generic method.
            // This is called once per connection (session creation), not per command — the cost is amortized.
            var createMethod = factory.GetType().GetMethod(nameof(TypedOptionsFactory<TResult>.Create));
            var closedMethod = createMethod.MakeGenericMethod(structType);
            return (TResult)closedMethod.Invoke(factory, null);
        }

        private static void EmitConstantProperty(TypeBuilder tb, string name, Type propertyType, object value)
        {
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
                throw new NotSupportedException($"Property type {propertyType} not supported for JIT-inlined options");

            il.Emit(OpCodes.Ret);

            var prop = tb.DefineProperty(name, PropertyAttributes.None, propertyType, null);
            prop.SetGetMethod(method);

            var interfaceMethod = typeof(IGarnetServerOptions).GetProperty(name)?.GetGetMethod();
            if (interfaceMethod != null)
                tb.DefineMethodOverride(method, interfaceMethod);
        }
    }

    /// <summary>
    /// Abstract base for type-safe factory callbacks used with GarnetOptionsFactory.Create.
    /// Subclass this and override Create to write the constructor call with full compile-time
    /// type checking. The factory handles runtime dispatch to the emitted struct type.
    ///
    /// Example:
    /// <code>
    /// class SessionFactory : TypedOptionsFactory&lt;ServerSessionBase&gt;
    /// {
    ///     public long Id;
    ///     public INetworkSender Sender;
    ///     public StoreWrapper StoreWrapper;
    ///     public SubscribeBroker Broker;
    ///
    ///     public override ServerSessionBase Create&lt;TServerOptions&gt;()
    ///         =&gt; new RespServerSession&lt;TServerOptions&gt;(Id, Sender, StoreWrapper, Broker, null, true);
    /// }
    /// </code>
    /// </summary>
    /// <typeparam name="TResult">Return type (typically a non-generic base class or interface)</typeparam>
    public abstract class TypedOptionsFactory<TResult>
    {
        /// <summary>
        /// Create an instance of a generic type parameterized by TServerOptions.
        /// The compiler type-checks the constructor call; the factory provides the actual TServerOptions.
        /// </summary>
        public abstract TResult Create<TServerOptions>()
            where TServerOptions : struct, IGarnetServerOptions;
    }
}
