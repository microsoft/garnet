// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Loader;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server.Resp
{
    internal class RespCommandDataCommon
    {
        /// <summary>
        /// Path to Garnet.resources.dll, where command data is found
        /// </summary>
        private static readonly string ResourcesAssemblyPath = Path.Combine(AppContext.BaseDirectory, @"Garnet.resources.dll");

        /// <summary>
        /// Synchronize loading and unloading of resources assembly
        /// </summary>
        private static readonly object ResourcesAssemblyLock = new object();

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
            lock (ResourcesAssemblyLock)
            {
                // Create a new unloadable assembly load context
                var assemblyLoadContext = new AssemblyLoadContext(null, true);

                try
                {
                    // Load the assembly within the context and import the data
                    var assembly = assemblyLoadContext.LoadFromAssemblyPath(ResourcesAssemblyPath);

                    var streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.EmbeddedResource, null, assembly);
                    var commandsDocsProvider = RespCommandsDataProviderFactory.GetRespCommandsDataProvider<TData>();

                    return commandsDocsProvider.TryImportRespCommandsData(path,
                        streamProvider, out commandsData, logger);
                }
                finally
                {
                    // Unload the context
                    assemblyLoadContext.Unload();

                    // Force GC to release the loaded assembly
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
            }
        }
    }
}