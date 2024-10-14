// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Reflection;
using Garnet.common;
using Garnet.resources;
using Microsoft.Extensions.Logging;

namespace Garnet.server.Resp
{
    internal class RespCommandDataCommon
    {
        /// <summary>
        /// Safely imports commands data from embedded resource in dynamically loaded/unloaded assembly
        /// </summary>
        /// <typeparam name="TData">Type of IRespCommandData to import</typeparam>
        /// <param name="path">Path to embedded resource</param>
        /// <param name="commandsData">Imported data</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if imported successfully</returns>
        internal static bool TryImportRespCommandsData<TData>(string path,
            out IReadOnlyDictionary<string, TData> commandsData, ILogger logger = null) where TData : class, IRespCommandData<TData>
        {
            // Garnet.resources assembly, where command data is found
            var resourcesAssembly = Assembly.GetAssembly(typeof(ResourceUtils));
            
            var streamProvider =
                StreamProviderFactory.GetStreamProvider(FileLocationType.EmbeddedResource, null, resourcesAssembly);
            var commandsDocsProvider = RespCommandsDataProviderFactory.GetRespCommandsDataProvider<TData>();

            return commandsDocsProvider.TryImportRespCommandsData(path,
                streamProvider, out commandsData, logger);
        }
    }
}