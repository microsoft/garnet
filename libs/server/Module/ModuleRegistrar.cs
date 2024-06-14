// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Garnet.server.Module
{
    public interface IModule
    {
        void OnLoad(List<string> moduleArgs);
    }

    internal class ModuleInfo
    {
        internal string Name { get; set; }

        internal object Instance { get; set; }
    }

    internal sealed class ModuleRegistrar
    {
        private static readonly Lazy<ModuleRegistrar> lazy = new Lazy<ModuleRegistrar>(() => new ModuleRegistrar());

        public static ModuleRegistrar Instance { get { return lazy.Value; } }

        private ModuleRegistrar()
        {
        }

        private readonly List<ModuleInfo> _modules = new List<ModuleInfo>();

        public bool LoadModule(Assembly loadedAssembly, List<string> moduleArgs, out ReadOnlySpan<byte> errorMessage)
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
                instance.OnLoad(moduleArgs);
                return true;
            }
            catch (Exception)
            {
                errorMessage = CmdStrings.RESP_ERR_MODULE_ONLOAD_EXCEPTION;
                return false;
            }
        }


        public IEnumerable<ModuleInfo> GetModules()
        {
            return _modules;
        }
    }
}