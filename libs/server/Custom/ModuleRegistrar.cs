// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Garnet.server.Custom
{
    internal class ModuleInfo
    {
        internal string Name { get; set; }

        internal object Instance { get; set; }
    }

    internal class ModuleRegistrar
    {
        private readonly List<ModuleInfo> _modules = new List<ModuleInfo>();

        public void RegisterModule(ModuleInfo module)
        {
            _modules.Add(module);
        }

        public IEnumerable<ModuleInfo> GetModules()
        {
            return _modules;
        }
    }
}
