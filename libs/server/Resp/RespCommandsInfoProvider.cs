// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace Garnet
{
    /// <summary>
    /// Interface for importing / exporting resp commands info from different file types
    /// </summary>
    public interface IRespCommandsInfoProvider
    {
        /// <summary>
        /// Import resp commands info from path using a stream provider
        /// </summary>
        /// <param name="path">Path to the file containing the serialized resp commands info</param>
        /// <param name="streamProvider">Stream provider to use when reading from the path</param>
        /// <param name="logger">Logger</param>
        /// <param name="commandsInfo">Outputs a read-only dictionary that maps a command name to its matching RespCommandsInfo</param>
        /// <returns>True if import succeeded</returns>
        bool TryImportRespCommandsInfo(string path, IStreamProvider streamProvider, ILogger logger, out IReadOnlyDictionary<string, RespCommandsInfo> commandsInfo);

        /// <summary>
        /// Export resp commands info to path using a stream provider
        /// </summary>
        /// <param name="path">Path to the file to write into</param>
        /// <param name="streamProvider">Stream provider to use when writing to the path</param>
        /// <param name="commandsInfo">Dictionary that maps a command name to its matching RespCommandsInfo</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if export succeeded</returns>
        bool TryExportRespCommandsInfo(string path, IStreamProvider streamProvider, IReadOnlyDictionary<string, RespCommandsInfo> commandsInfo, ILogger logger);
    }

    public class RespCommandsInfoProviderFactory
    {
        /// <summary>
        /// Get an IRespCommandsInfoProvider instance based on its file type
        /// </summary>
        /// <param name="fileType">The resp commands info file type</param>
        /// <returns>IRespCommandsInfoProvider instance</returns>
        /// <exception cref="NotImplementedException"></exception>
        public static IRespCommandsInfoProvider GetRespCommandsInfoProvider(RespCommandsObjectFileType fileType = RespCommandsObjectFileType.Default)
        {
            switch (fileType)
            {
                case RespCommandsObjectFileType.Default:
                    return DefaultRespCommandsInfoProvider.Instance;
                default:
                    throw new NotImplementedException($"No RespCommandsInfoProvider exists for file type: {fileType}.");
            }
        }
    }

    /// <summary>
    /// Default commands info provider (JSON serialized array of RespCommandsInfo objects)
    /// </summary>
    internal class DefaultRespCommandsInfoProvider : IRespCommandsInfoProvider
    {
        private static readonly Lazy<IRespCommandsInfoProvider> LazyInstance;

        public static IRespCommandsInfoProvider Instance => LazyInstance.Value;

        private static readonly JsonSerializerOptions SerializerOptions = new()
        {
            WriteIndented = true,
            Converters = { new JsonStringEnumConverter(), new KeySpecConverter() }
        };

        static DefaultRespCommandsInfoProvider()
        {
            LazyInstance = new(() => new DefaultRespCommandsInfoProvider());
        }

        private DefaultRespCommandsInfoProvider()
        {
        }

        public bool TryImportRespCommandsInfo(string path, IStreamProvider streamProvider, ILogger logger, out IReadOnlyDictionary<string, RespCommandsInfo> commandsInfo)
        {
            using var stream = streamProvider.Read(path);
            using var streamReader = new StreamReader(stream);

            commandsInfo = default;

            try
            {
                var respCommands = JsonSerializer.Deserialize<RespCommandsInfo[]>(streamReader.ReadToEnd(), SerializerOptions)!;

                var tmpRespCommandsInfo = new Dictionary<string, RespCommandsInfo>(StringComparer.OrdinalIgnoreCase);
                foreach (var respCommandsInfo in respCommands)
                {
                    tmpRespCommandsInfo.Add(respCommandsInfo.Name, respCommandsInfo);
                }

                commandsInfo = new ReadOnlyDictionary<string, RespCommandsInfo>(tmpRespCommandsInfo);
            }
            catch (JsonException je)
            {
                logger?.LogError(je, $"An error occurred while parsing resp commands info file (Path: {path}).");
                return false;
            }

            return true;
        }

        public bool TryExportRespCommandsInfo(string path, IStreamProvider streamProvider, IReadOnlyDictionary<string, RespCommandsInfo> commandsInfo, ILogger logger)
        {
            string jsonSettings;

            var commandsInfoToSerialize = commandsInfo.Values.OrderBy(ci => ci.Name).ToArray();
            try
            {
                jsonSettings = JsonSerializer.Serialize(commandsInfoToSerialize, SerializerOptions);
            }
            catch (NotSupportedException e)
            {
                logger?.LogError(e, $"An error occurred while serializing resp commands info file (Path: {path}).");
                return false;
            }

            var data = Encoding.ASCII.GetBytes(jsonSettings);
            streamProvider.Write(path, data);

            return true;
        }
    }

    /// <summary>
    /// Current supported resp commands info file types
    /// </summary>
    public enum RespCommandsObjectFileType
    {
        // Default file format (JSON serialized array of RespCommandsInfo objects)
        Default = 0,
    }
}