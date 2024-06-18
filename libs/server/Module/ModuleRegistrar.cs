// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.Logging;

namespace Garnet.server.Module
{
    /// <summary>
    /// Common interface that all Garnet modules must implement.
    /// </summary>
    public interface IModule
    {
        /// <summary>
        /// Called when the module is loaded.
        /// </summary>
        /// <param name="context">Module load context</param>
        /// <param name="moduleArgs">Module load args</param>
        void OnLoad(ModuleLoadContext context, List<string> moduleArgs);
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
        internal string Name;
        internal uint Version;
        internal bool Initialized;

        private ModuleRegistrar moduleRegistrar;
        private CustomCommandManager customCommandManager;

        internal ModuleLoadContext(ModuleRegistrar moduleRegistrar, CustomCommandManager customCommandManager)
        {
            Debug.Assert(moduleRegistrar != null);
            Debug.Assert(customCommandManager != null);

            Initialized = false;
            this.moduleRegistrar = moduleRegistrar;
            this.customCommandManager = customCommandManager;
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

            if (!moduleRegistrar.TryAdd(this))
                return ModuleActionStatus.AlreadyExists;

            Initialized = true;
            return ModuleActionStatus.Success;
        }

        /// <summary>
        /// Registers a raw string custom command
        /// </summary>
        /// <param name="name">Command name</param>
        /// <param name="numParams">Number of parameters</param>
        /// <param name="type">Command type</param>
        /// <param name="customFunctions">Custom raw string function implementation</param>
        /// <param name="commandInfo">Command info</param>
        /// <param name="expirationTicks">Expiration ticks for the key</param>
        /// <returns>Registration status</returns>
        public ModuleActionStatus RegisterCommand(string name, int numParams, CommandType type, CustomRawStringFunctions customFunctions, RespCommandsInfo commandInfo, long expirationTicks = 0)
        {
            if (string.IsNullOrEmpty(name))
                return ModuleActionStatus.InvalidRegistrationInfo;
            if (customFunctions == null)
                return ModuleActionStatus.InvalidRegistrationInfo;
            if (commandInfo == null)
                return ModuleActionStatus.InvalidRegistrationInfo;

            customCommandManager.Register(name, numParams, type, customFunctions, commandInfo, expirationTicks);

            return ModuleActionStatus.Success;
        }

        /// <summary>
        /// Registers a custom transaction
        /// </summary>
        /// <param name="name">Transaction name</param>
        /// <param name="numParams">Number of parameters</param>
        /// <param name="proc">Transaction procedure implemenation</param>
        /// <param name="commandInfo">Command info</param>
        /// <returns>Registration status</returns>
        public ModuleActionStatus RegisterTransaction(string name, int numParams, Func<CustomTransactionProcedure> proc, RespCommandsInfo commandInfo = null)
        {
            if (string.IsNullOrEmpty(name))
                return ModuleActionStatus.InvalidRegistrationInfo;
            if (proc == null)
                return ModuleActionStatus.InvalidRegistrationInfo;

            customCommandManager.Register(name, numParams, proc, commandInfo);

            return ModuleActionStatus.Success;
        }
    }

    internal sealed class ModuleRegistrar
    {
        private static readonly Lazy<ModuleRegistrar> lazy = new Lazy<ModuleRegistrar>(() => new ModuleRegistrar());

        internal static ModuleRegistrar Instance { get { return lazy.Value; } }

        private ModuleRegistrar()
        {
            modules = new();
        }

        private readonly ConcurrentDictionary<string, ModuleLoadContext> modules;

        internal bool LoadModule(CustomCommandManager customCommandManager, Assembly loadedAssembly, List<string> moduleArgs, ILogger logger, out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = default;

            // Get types that implement IModule from loaded assemblies
            var loadedTypes = loadedAssembly
                .GetTypes()
                .Where(t => t.GetInterfaces().Contains(typeof(IModule)))
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

            // Check that type has empty constructor
            var moduleType = loadedTypes[0];
            if (moduleType.GetConstructor(Type.EmptyTypes) == null)
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_INSTANTIATING_CLASS;
                return false;
            }

            var instance = (IModule)Activator.CreateInstance(moduleType);
            if (instance == null)
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_INSTANTIATING_CLASS;
                return false;
            }

            try
            {
                var moduleLoadContext = new ModuleLoadContext(this, customCommandManager);
                instance.OnLoad(moduleLoadContext, moduleArgs);

                if (!moduleLoadContext.Initialized)
                {
                    errorMessage = CmdStrings.RESP_ERR_MODULE_ONLOAD;
                    return false;
                }

                logger?.LogInformation("Module {0} version {1} loaded", moduleLoadContext.Name, moduleLoadContext.Version);
                return true;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error loading module");
                errorMessage = CmdStrings.RESP_ERR_MODULE_ONLOAD;
                return false;
            }
        }

        internal bool TryAdd(ModuleLoadContext moduleLoadContext)
        {
            return modules.TryAdd(moduleLoadContext.Name, moduleLoadContext);
        }
    }
}