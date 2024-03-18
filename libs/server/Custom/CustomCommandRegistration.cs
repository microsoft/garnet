// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;

namespace Garnet.server.Custom
{
    /// <summary>
    /// Base custom command / transaction registration arguments
    /// </summary>
    internal abstract class RegisterArgsBase
    {
        /// <summary>
        /// Custom command / transaction name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Number of parameters required by custom command / transaction
        /// </summary>
        public int NumParams { get; set; }
    }


    /// <summary>
    /// Custom command registration arguments
    /// </summary>
    internal class RegisterCmdArgs : RegisterArgsBase
    {
        public CommandType CommandType { get; set; }

        public long ExpirationTicks { get; set; }
    }

    /// <summary>
    /// Custom transaction registration arguments
    /// </summary>
    internal class RegisterTxnArgs : RegisterArgsBase
    {
    }

    /// <summary>
    /// Factory for registration providers
    /// </summary>
    internal class RegisterCustomCommandProviderFactory
    {
        public static IRegisterCustomCommandProvider GetRegisterCustomCommandProvider(object instance, RegisterArgsBase args)
        {
            if (instance is CustomRawStringFunctions rsf && args is RegisterCmdArgs rsfa)
            {
                return new RegisterRawStringFunctionProvider(rsf, rsfa);
            }

            if (instance is CustomObjectFactory cof && args is RegisterCmdArgs cofa)
            {
                return new RegisterCustomObjectFactoryProvider(cof, cofa);
            }

            if (instance is CustomTransactionProcedure ctp && args is RegisterTxnArgs ctpa)
            {
                return new RegisterCustomTransactionProcedureProvider(ctp, ctpa);
            }

            return null;
        }
    }

    /// <summary>
    /// Registration provider interface
    /// </summary>
    internal interface IRegisterCustomCommandProvider
    {
        /// <summary>
        /// Register custom command instance
        /// </summary>
        /// <param name="customCommandManager">CustomCommandManager used to register custom command instance</param>
        void Register(CustomCommandManager customCommandManager);
    }

    internal abstract class RegisterCustomCommandProviderBase : IRegisterCustomCommandProvider
    {
        /// <summary>
        /// All supported custom command types 
        /// </summary>
        public static Lazy<Type[]> SupportedCustomCommandBaseTypesLazy = new(() =>
        {
            var supportedTypes = new HashSet<Type>();
            foreach (var type in typeof(RegisterCustomCommandProviderBase).Assembly.GetTypes().Where(t =>
                         typeof(RegisterCustomCommandProviderBase).IsAssignableFrom(t) && t.IsClass && !t.IsAbstract))
            {
                var baseType = type.BaseType;
                while (baseType != null && baseType != typeof(RegisterCustomCommandProviderBase))
                {
                    if (baseType.IsGenericType && baseType.GetGenericTypeDefinition() ==
                        typeof(RegisterCustomCommandProviderBase<,>))
                    {
                        var customCmdType = baseType.GetGenericArguments().FirstOrDefault();
                        if (customCmdType != null)
                        {
                            supportedTypes.Add(customCmdType);
                            break;
                        }
                    }

                    baseType = baseType.BaseType;
                }
            }

            return supportedTypes.ToArray();
        });

        public abstract void Register(CustomCommandManager customCommandManager);
    }

    /// <summary>
    /// Base registration provider
    /// </summary>
    /// <typeparam name="T">Type of custom command / transaction</typeparam>
    /// <typeparam name="TArgs">Type of arguments required to register command / transaction</typeparam>
    internal abstract class RegisterCustomCommandProviderBase<T, TArgs> : RegisterCustomCommandProviderBase where TArgs : RegisterArgsBase
    {
        /// <summary>
        /// Arguments required for command / transaction registration
        /// </summary>
        protected TArgs RegisterArgs { get; }

        /// <summary>
        /// Instance of custom command class
        /// </summary>
        protected T Instance { get; }

        protected RegisterCustomCommandProviderBase(T instance, TArgs args)
        {
            this.Instance = instance;
            this.RegisterArgs = args;
        }
    }

    /// <summary>
    /// Base custom command registration provider
    /// </summary>
    /// <typeparam name="T">Type of custom command</typeparam>
    internal abstract class RegisterCustomCmdProvider<T> : RegisterCustomCommandProviderBase<T, RegisterCmdArgs>
    {
        protected RegisterCustomCmdProvider(T instance, RegisterCmdArgs args) : base(instance, args)
        {
        }
    }

    /// <summary>
    /// Base custom transaction registration provider
    /// </summary>
    /// <typeparam name="T">Type of custom transaction</typeparam>
    internal abstract class RegisterCustomTxnProvider<T> : RegisterCustomCommandProviderBase<T, RegisterTxnArgs>
    {
        protected RegisterCustomTxnProvider(T instance, RegisterTxnArgs args) : base(instance, args)
        {
        }
    }

    /// <summary>
    /// RawStringFunction registration provider
    /// </summary>
    internal class RegisterRawStringFunctionProvider : RegisterCustomCmdProvider<CustomRawStringFunctions>
    {
        public RegisterRawStringFunctionProvider(CustomRawStringFunctions instance, RegisterCmdArgs args) : base(instance, args)
        {
        }

        public override void Register(CustomCommandManager customCommandManager)
        {
            customCommandManager.Register(this.RegisterArgs.Name, this.RegisterArgs.NumParams, this.RegisterArgs.CommandType, this.Instance, this.RegisterArgs.ExpirationTicks);
        }
    }

    /// <summary>
    /// CustomObjectFactory registration provider
    /// </summary>
    internal class RegisterCustomObjectFactoryProvider : RegisterCustomCmdProvider<CustomObjectFactory>
    {
        public RegisterCustomObjectFactoryProvider(CustomObjectFactory instance, RegisterCmdArgs args) : base(instance, args)
        {
        }

        public override void Register(CustomCommandManager customCommandManager)
        {
            customCommandManager.Register(this.RegisterArgs.Name, this.RegisterArgs.NumParams, this.RegisterArgs.CommandType, this.Instance);
        }
    }

    /// <summary>
    /// TransactionProcedureProvider registration provider
    /// </summary>
    internal class RegisterCustomTransactionProcedureProvider : RegisterCustomTxnProvider<CustomTransactionProcedure>
    {
        public RegisterCustomTransactionProcedureProvider(CustomTransactionProcedure instance, RegisterTxnArgs args) : base(instance, args)
        {
        }

        public override void Register(CustomCommandManager customCommandManager)
        {
            customCommandManager.Register(this.RegisterArgs.Name, this.RegisterArgs.NumParams, () => this.Instance);
        }
    }
}