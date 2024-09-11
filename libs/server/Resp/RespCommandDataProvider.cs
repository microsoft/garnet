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
using Garnet.server.Resp;
using Microsoft.Extensions.Logging;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace Garnet.server
{
    /// <summary>
    /// An interface for different RESP command data (e.g. RespCommandInfo, RespCommandDocs)
    /// </summary>
    public interface IRespCommandData
    {
        /// <summary>
        /// Garnet's RespCommand enum command representation
        /// </summary>
        public RespCommand Command { get; init; }

        /// <summary>
        /// The command's name
        /// </summary>
        public string Name { get; init; }
    }

    /// <summary>
    /// An interface for different RESP command data (e.g. RespCommandInfo, RespCommandDocs)
    /// </summary>
    public interface IRespCommandData<TSubCommandData> : IRespCommandData where TSubCommandData : IRespCommandData
    {
        /// <summary>
        /// All the command's sub-commands, if any
        /// </summary>
        TSubCommandData[] SubCommands { get; }
    }

    /// <summary>
    /// Interface for importing / exporting RESP commands data from different file types
    /// </summary>
    public interface IRespCommandsDataProvider<TData> where TData : IRespCommandData
    {
        /// <summary>
        /// Import RESP commands data from path using a stream provider
        /// </summary>
        /// <param name="path">Path to the file containing the serialized RESP commands data</param>
        /// <param name="streamProvider">Stream provider to use when reading from the path</param>
        /// <param name="logger">Logger</param>
        /// <param name="commandsData">Outputs a read-only dictionary that maps a command name to its matching data</param>
        /// <returns>True if import succeeded</returns>
        bool TryImportRespCommandsData(string path, IStreamProvider streamProvider, out IReadOnlyDictionary<string, TData> commandsData, ILogger logger = null);

        /// <summary>
        /// Export RESP commands date to path using a stream provider
        /// </summary>
        /// <param name="path">Path to the file to write into</param>
        /// <param name="streamProvider">Stream provider to use when writing to the path</param>
        /// <param name="commandsData">Dictionary that maps a command name to its matching data</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if export succeeded</returns>
        bool TryExportRespCommandsData(string path, IStreamProvider streamProvider, IReadOnlyDictionary<string, TData> commandsData, ILogger logger = null);
    }

    public class RespCommandsDataProviderFactory
    {
        /// <summary>
        /// Get an IRespCommandsDataProvider instance based on its file type
        /// </summary>
        /// <param name="fileType">The RESP commands data file type</param>
        /// <returns>IRespCommandsDataProvider instance</returns>
        /// <exception cref="NotImplementedException"></exception>
        public static IRespCommandsDataProvider<TData> GetRespCommandsDataProvider<TData>(
            RespCommandsDataFileType fileType = RespCommandsDataFileType.Default) where TData : IRespCommandData
        {
            switch (fileType)
            {
                case RespCommandsDataFileType.Default:
                    return DefaultRespCommandsDataProvider<TData>.Instance;
                default:
                    throw new NotImplementedException($"No RespCommandsDataProvider exists for file type: {fileType}.");
            }
        }
    }

    /// <summary>
    /// Default commands data provider (JSON serialized array of data objects)
    /// </summary>
    internal class DefaultRespCommandsDataProvider<TData> : IRespCommandsDataProvider<TData> where TData : IRespCommandData
    {
        private static readonly Lazy<IRespCommandsDataProvider<TData>> LazyInstance;

        public static IRespCommandsDataProvider<TData> Instance => LazyInstance.Value;

        private static readonly JsonSerializerOptions SerializerOptions = new()
        {
            WriteIndented = true,
            Converters = { new JsonStringEnumConverter(), new KeySpecConverter(), new RespCommandArgumentConverter() }
        };

        static DefaultRespCommandsDataProvider()
        {
            LazyInstance = new(() => new DefaultRespCommandsDataProvider<TData>());
        }

        private DefaultRespCommandsDataProvider()
        {
        }

        public bool TryImportRespCommandsData(string path, IStreamProvider streamProvider, out IReadOnlyDictionary<string, TData> commandsData, ILogger logger = null)
        {
            using var stream = streamProvider.Read(path);
            using var streamReader = new StreamReader(stream);

            commandsData = default;

            try
            {
                var respJson = streamReader.ReadToEnd();
                var respCommandsData = JsonSerializer.Deserialize<TData[]>(respJson, SerializerOptions)!;

                var tmpRespCommandsData = new Dictionary<string, TData>(StringComparer.OrdinalIgnoreCase);
                foreach (var data in respCommandsData)
                {
                    tmpRespCommandsData.Add(data.Name, data);
                }

                commandsData = new ReadOnlyDictionary<string, TData>(tmpRespCommandsData);
            }
            catch (JsonException je)
            {
                logger?.LogError(je, "An error occurred while parsing resp command data file (Path: {path}).", path);
                return false;
            }

            return true;
        }

        public bool TryExportRespCommandsData(string path, IStreamProvider streamProvider, IReadOnlyDictionary<string, TData> commandsData, ILogger logger = null)
        {
            string jsonSettings;

            var dataToSerialize = commandsData.Values.OrderBy(ci => ci.Name).ToArray();
            try
            {
                jsonSettings = JsonSerializer.Serialize(dataToSerialize, SerializerOptions);
            }
            catch (NotSupportedException e)
            {
                logger?.LogError(e, "An error occurred while serializing resp commands data file (Path: {path}).", path);
                return false;
            }

            var data = Encoding.ASCII.GetBytes(jsonSettings);
            streamProvider.Write(path, data);

            return true;
        }
    }

    /// <summary>
    /// Current supported RESP commands data file types
    /// </summary>
    public enum RespCommandsDataFileType
    {
        // Default file format (JSON serialized array of data objects)
        Default = 0,
    }
}