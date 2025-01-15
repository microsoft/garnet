// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Abstract base class that all Garnet modules must inherit from.
    /// The module must have a parameterless constructor.
    /// NOTE: This is a feature that is under development.
    /// If taking a dependency on this, please be prepared for breaking changes.
    /// </summary>
    public abstract class ModuleBase
    {
        /// <summary>
        /// Called when the module is loaded.
        /// </summary>
        /// <param name="context">Module load context</param>
        /// <param name="args">Module load args</param>
        public abstract void OnLoad(ModuleLoadContext context, string[] args);
    }

    /// <summary>
    /// Represents the status of a module action.
    /// </summary>
    public enum ModuleActionStatus
    {
        Success,
        Failure,
        AlreadyLoaded,
        AlreadyExists,
        InvalidRegistrationInfo,
    }

    /// <summary>
    /// Provides context for a loading module to invoke initialization and registration methods.
    /// </summary>
    public class ModuleLoadContext
    {
        public readonly ILogger Logger;
        internal string Name;
        internal uint Version;
        internal bool Initialized;

        private readonly CustomCommandManager customCommandManager;

        internal ModuleLoadContext(CustomCommandManager customCommandManager, ILogger logger)
        {
            Debug.Assert(customCommandManager != null);

            Initialized = false;
            this.customCommandManager = customCommandManager;
            Logger = logger;
        }

        /// <summary>
        /// Initializes the module load context
        /// </summary>
        /// <param name="name">Module name</param>
        /// <param name="version">Module version</param>
        /// <returns>Initialization status</returns>
        public ModuleActionStatus Initialize(string name, uint version)
        {
            if (string.IsNullOrEmpty(name))
                return ModuleActionStatus.InvalidRegistrationInfo;

            if (Initialized)
                return ModuleActionStatus.AlreadyLoaded;

            Name = name;
            Version = version;

            if (!customCommandManager.TryAddModule(this))
                return ModuleActionStatus.AlreadyExists;

            Initialized = true;
            return ModuleActionStatus.Success;
        }

        /// <summary>
        /// Registers a raw string custom command
        /// </summary>
        /// <param name="name">Command name</param>
        /// <param name="type">Command type</param>
        /// <param name="customFunctions">Custom raw string function implementation</param>
        /// <param name="commandInfo">Command info</param>
        /// <param name="commandDocs">RESP command docs</param>
        /// <param name="expirationTicks">Expiration ticks for the key</param>
        /// <returns>Registration status</returns>
        public ModuleActionStatus RegisterCommand(string name, CustomRawStringFunctions customFunctions, CommandType type = CommandType.ReadModifyWrite, RespCommandsInfo commandInfo = null, RespCommandDocs commandDocs = null, long expirationTicks = 0)
        {
            if (string.IsNullOrEmpty(name) || customFunctions == null)
                return ModuleActionStatus.InvalidRegistrationInfo;

            customCommandManager.Register(name, type, customFunctions, commandInfo, commandDocs, expirationTicks);

            return ModuleActionStatus.Success;
        }

        /// <summary>
        /// Registers a custom transaction
        /// </summary>
        /// <param name="name">Transaction name</param>
        /// <param name="proc">Transaction procedure implementation</param>
        /// <param name="commandInfo">Command info</param>
        /// <param name="commandDocs">Command docs</param>
        /// <returns>Registration status</returns>
        public ModuleActionStatus RegisterTransaction(string name, Func<CustomTransactionProcedure> proc, RespCommandsInfo commandInfo = null, RespCommandDocs commandDocs = null)
        {
            if (string.IsNullOrEmpty(name) || proc == null)
                return ModuleActionStatus.InvalidRegistrationInfo;

            customCommandManager.Register(name, proc, commandInfo, commandDocs);

            return ModuleActionStatus.Success;
        }

        /// <summary>Registers a custom type</summary>
        /// <param name="factory">Factory to generate new instances of type</param>
        /// <returns>Registration status</returns>
        public ModuleActionStatus RegisterType(CustomObjectFactory factory)
        {
            if (factory == null)
                return ModuleActionStatus.InvalidRegistrationInfo;

            customCommandManager.RegisterType(factory);

            return ModuleActionStatus.Success;
        }

        /// <summary>
        /// Registers a custom object command
        /// </summary>
        /// <param name="name">Command name</param>
        /// <param name="factory">Type factory</param>
        /// <param name="command">Custom object function implementation</param>
        /// <param name="type">Command type</param>
        /// <param name="commandInfo">Command info</param>
        /// <param name="commandDocs">RESP command docs</param>
        /// <returns></returns>
        public ModuleActionStatus RegisterCommand(string name, CustomObjectFactory factory, CustomObjectFunctions command, CommandType type = CommandType.ReadModifyWrite, RespCommandsInfo commandInfo = null, RespCommandDocs commandDocs = null)
        {
            if (string.IsNullOrEmpty(name) || factory == null || command == null)
                return ModuleActionStatus.InvalidRegistrationInfo;

            customCommandManager.Register(name, type, factory, commandInfo, commandDocs, command);

            return ModuleActionStatus.Success;
        }

        /// <summary>
        /// Registers a custom procedure that performs several operations in a non-transactional manner
        /// </summary>
        /// <param name="name">Procedure name</param>
        /// <param name="customScriptProc">Custom procedure implementation</param>
        /// <param name="commandInfo">Command info</param>
        /// <param name="commandDocs">Command docs</param>
        /// <returns>Registration status</returns>
        public ModuleActionStatus RegisterProcedure(string name, Func<CustomProcedure> customScriptProc, RespCommandsInfo commandInfo = null, RespCommandDocs commandDocs = null)
        {
            if (string.IsNullOrEmpty(name) || customScriptProc == null)
                return ModuleActionStatus.InvalidRegistrationInfo;

            customCommandManager.Register(name, customScriptProc, commandInfo, commandDocs);
            return ModuleActionStatus.Success;
        }
    }

    public sealed class ModuleRegistrar
    {
        private static readonly Lazy<ModuleRegistrar> lazy = new(() => new ModuleRegistrar());

        public static ModuleRegistrar Instance { get { return lazy.Value; } }

        private ModuleRegistrar()
        {
            moduleConstructors = new();
        }

        private readonly ConcurrentDictionary<string, ConstructorInfo> moduleConstructors;

        /// <summary>
        /// Load module from a specified assembly and register it with the custom command managers
        /// </summary>
        /// <param name="customCommandManager">Custom command manager instance</param>
        /// <param name="loadedAssembly">Loaded assembly containing module</param>
        /// <param name="moduleArgs">Module arguments</param>
        /// <param name="logger">Logger</param>
        /// <param name="errorMessage">Error message</param>
        /// <returns>True if module loaded successfully</returns>
        public bool LoadModule(CustomCommandManager customCommandManager, Assembly loadedAssembly, string[] moduleArgs, ILogger logger, out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = default;

            // Get types that implement IModule from loaded assemblies
            var loadedTypes = loadedAssembly
                .GetTypes()
                .Where(t => t.IsSubclassOf(typeof(ModuleBase)) && !t.IsAbstract)
                .ToArray();

            if (loadedTypes.Length == 0)
            {
                errorMessage = CmdStrings.RESP_ERR_MODULE_NO_INTERFACE;
                return false;
            }

            if (loadedTypes.Length > 1)
            {
                errorMessage = CmdStrings.RESP_ERR_MODULE_MULTIPLE_INTERFACES;
                return false;
            }

            var moduleType = loadedTypes[0];

            // Uniquely identify the module by assembly and type name
            var moduleAssemblyAnyTypeName = $"{loadedAssembly.FullName}:{moduleType.FullName!}";

            // Check if module was previously loaded
            if (!moduleConstructors.TryGetValue(moduleAssemblyAnyTypeName, out var moduleConstructor))
            {
                // Check that type has empty constructor
                moduleConstructor = moduleType.GetConstructor(Type.EmptyTypes);

                if (moduleConstructor == null)
                {
                    errorMessage = CmdStrings.RESP_ERR_GENERIC_INSTANTIATING_CLASS;
                    return false;
                }

                moduleConstructors.TryAdd(moduleAssemblyAnyTypeName, moduleConstructor);
            }

            // Instantiate module using constructor info
            ModuleBase module;
            try
            {
                module = (ModuleBase)moduleConstructor.Invoke([]);
            }
            catch (Exception)
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_INSTANTIATING_CLASS;
                return false;
            }

            // Register module with custom command manager
            return customCommandManager.RegisterModule(module, moduleArgs, logger, out errorMessage);
        }
    }
}