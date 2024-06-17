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
    public interface IModule
    {
        void OnLoad(ModuleLoadContext context, List<string> moduleArgs);
    }

    public enum ModuleActionStatus
    {
        Success,
        Failure,
        AlreadyLoaded,
        AlreadyExists,
        InvalidRegistrationInfo,
    }

    public class ModuleLoadContext
    {
        internal string Name;
        internal int Version;

        private bool initialized;
        private ModuleRegistrar moduleRegistrar;
        private CustomCommandManager customCommandManager;

        internal ModuleLoadContext(ModuleRegistrar moduleRegistrar, CustomCommandManager customCommandManager)
        {
            Debug.Assert(moduleRegistrar != null);
            Debug.Assert(customCommandManager != null);

            initialized = false;
            this.moduleRegistrar = moduleRegistrar;
            this.customCommandManager = customCommandManager;
        }

        public ModuleActionStatus Initialize(string name, int version)
        {
            if (initialized)
                return ModuleActionStatus.AlreadyLoaded;

            Name = name;
            Version = version;

            if (!moduleRegistrar.TryAdd(this))
                return ModuleActionStatus.AlreadyExists;

            initialized = true;
            return ModuleActionStatus.Success;
        }

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

        public static ModuleRegistrar Instance { get { return lazy.Value; } }

        private ModuleRegistrar()
        {
            modules = new();
        }

        private readonly ConcurrentDictionary<string, ModuleLoadContext> modules;

        public bool LoadModule(CustomCommandManager customCommandManager, Assembly loadedAssembly, List<string> moduleArgs, ILogger logger, out ReadOnlySpan<byte> errorMessage)
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
                instance.OnLoad(new ModuleLoadContext(this, customCommandManager), moduleArgs);
                return true;
            }
            catch (Exception)
            {
                errorMessage = CmdStrings.RESP_ERR_MODULE_ONLOAD_EXCEPTION;
                return false;
            }
        }

        internal bool TryAdd(ModuleLoadContext moduleLoadContext)
        {
            return modules.TryAdd(moduleLoadContext.Name, moduleLoadContext);
        }
    }
}