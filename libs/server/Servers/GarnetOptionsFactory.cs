// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;

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
    /// same struct type. Properties are discovered from the IGarnetServerOptions interface and
    /// read from GarnetServerOptions by matching property names via reflection.
    ///
    /// See GarnetOptionsFactory.Samples.cs for usage examples.
    /// </summary>
    public static class GarnetOptionsFactory
    {
        private static readonly ModuleBuilder ModuleBuilder;
        private static readonly object EmitLock = new();
        private static readonly Dictionary<string, Type> EmittedTypes = new();

        // Interface properties discovered once at startup via reflection
        private static readonly PropertyInfo[] InterfaceProperties;

        // Matching getters on GarnetServerOptions, indexed by property name
        private static readonly Dictionary<string, Func<GarnetServerOptions, object>> Getters;

        static GarnetOptionsFactory()
        {
            var assembly = AssemblyBuilder.DefineDynamicAssembly(
                new AssemblyName("GarnetDynamicOptions"),
                AssemblyBuilderAccess.Run);
            ModuleBuilder = assembly.DefineDynamicModule("MainModule");

            // Discover all properties from the interface
            InterfaceProperties = typeof(IGarnetServerOptions).GetProperties(
                BindingFlags.Public | BindingFlags.Instance);

            // Build getters by matching interface property names to GarnetServerOptions members
            Getters = new Dictionary<string, Func<GarnetServerOptions, object>>();
            var optsType = typeof(GarnetServerOptions);

            foreach (var ifaceProp in InterfaceProperties)
            {
                // Look for a matching property on GarnetServerOptions (could be inherited from ServerOptions)
                var optsProp = optsType.GetProperty(ifaceProp.Name,
                    BindingFlags.Public | BindingFlags.Instance | BindingFlags.FlattenHierarchy);

                // Fall back to a matching field (GarnetServerOptions uses public fields for most config)
                var optsField = optsProp == null
                    ? optsType.GetField(ifaceProp.Name,
                        BindingFlags.Public | BindingFlags.Instance | BindingFlags.FlattenHierarchy)
                    : null;

                if (optsProp != null)
                {
                    var getter = optsProp.GetGetMethod();
                    Getters[ifaceProp.Name] = opts => getter.Invoke(opts, null);
                }
                else if (optsField != null)
                {
                    Getters[ifaceProp.Name] = opts => optsField.GetValue(opts);
                }
                else
                {
                    throw new InvalidOperationException(
                        $"IGarnetServerOptions property '{ifaceProp.Name}' has no matching property or field " +
                        $"on GarnetServerOptions. Add it to GarnetServerOptions or remove it from the interface.");
                }
            }
        }

        /// <summary>
        /// Gets or creates a zero-size struct type with hardcoded constant returns for the given
        /// configuration. Thread-safe. Deduplicates by value — identical configs reuse the same type.
        /// </summary>
        public static Type GetOrCreateOptionsStruct(GarnetServerOptions opts)
        {
            var values = new Dictionary<string, object>();
            var keyParts = new List<string>();

            foreach (var prop in InterfaceProperties)
            {
                var value = Getters[prop.Name](opts);
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

                foreach (var prop in InterfaceProperties)
                    EmitConstantProperty(typeBuilder, prop.Name, prop.PropertyType, values[prop.Name]);

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
    /// See GarnetOptionsFactory.Samples.cs for usage examples.
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
