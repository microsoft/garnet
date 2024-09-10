//// Copyright (c) Microsoft Corporation.
//// Licensed under the MIT license.

//using System;
//using System.Collections.Generic;
//using System.Collections.ObjectModel;
//using System.IO;
//using System.Linq;
//using System.Text;
//using System.Text.Json;
//using System.Text.Json.Serialization;
//using Garnet.common;
//using Microsoft.Extensions.Logging;
//using JsonSerializer = System.Text.Json.JsonSerializer;

//namespace Garnet.server
//{
//    /// <summary>
//    /// Interface for importing / exporting IRespSerializable objects from different file types
//    /// </summary>
//    public interface IRespSerializableObjectProvider<T>
//    {
//        /// <summary>
//        /// Import IRespSerializable from path using a stream provider
//        /// </summary>
//        /// <param name="path">Path to the file containing the serialized RESP commands info</param>
//        /// <param name="streamProvider">Stream provider to use when reading from the path</param>
//        /// <param name="logger">Logger</param>
//        /// <param name="objectMap">Outputs a read-only dictionary that maps a command name to its matching RespCommandsInfo</param>
//        /// <returns>True if import succeeded</returns>
//        bool TryImportRespCommandsInfo(string path, IStreamProvider streamProvider, out IReadOnlyDictionary<string, T> objectMap, ILogger logger = null);

//        /// <summary>
//        /// Export RESP commands info to path using a stream provider
//        /// </summary>
//        /// <param name="path">Path to the file to write into</param>
//        /// <param name="streamProvider">Stream provider to use when writing to the path</param>
//        /// <param name="objectMap">Dictionary that maps a command name to its matching RespCommandsInfo</param>
//        /// <param name="logger">Logger</param>
//        /// <returns>True if export succeeded</returns>
//        bool TryExportRespCommandsInfo(string path, IStreamProvider streamProvider, IReadOnlyDictionary<string, T> objectMap, ILogger logger = null);
//    }

//    public class RespSerializableObjectProviderFactory
//    {
//        /// <summary>
//        /// Get an IRespCommandsInfoProvider instance based on its file type
//        /// </summary>
//        /// <param name="fileType">The RESP commands info file type</param>
//        /// <returns>IRespCommandsInfoProvider instance</returns>
//        /// <exception cref="NotImplementedException"></exception>
//        public static IRespSerializableObjectProvider<T> GetRespSerializableObjectProvider<T>(RespFileType fileType = RespFileType.Default)
//        {
//            switch (fileType)
//            {
//                case RespFileType.Default:
//                    return DefaultRespCommandsInfoProvider.Instance;
//                default:
//                    throw new NotImplementedException($"No RespCommandsInfoProvider exists for file type: {fileType}.");
//            }
//        }
//    }

//    /// <summary>
//    /// Default commands info provider (JSON serialized array of RespCommandsInfo objects)
//    /// </summary>
//    internal class DefaultRespSerializableObjectProvider<T> : IRespSerializableObjectProvider<T>
//    {
//        private static readonly Lazy<IRespSerializableObjectProvider<T>> LazyInstance;

//        public static IRespSerializableObjectProvider<T> Instance => LazyInstance.Value;

//        private static readonly JsonSerializerOptions SerializerOptions = new()
//        {
//            WriteIndented = true,
//            Converters = { new JsonStringEnumConverter(), new KeySpecConverter() }
//        };

//        static DefaultRespSerializableObjectProvider()
//        {
//            LazyInstance = new(() => new DefaultRespSerializableObjectProvider<T>());
//        }

//        private DefaultRespSerializableObjectProvider()
//        {
//        }

//        public bool TryImportRespCommandsInfo(string path, IStreamProvider streamProvider, out IReadOnlyDictionary<string, RespCommandsInfo> commandsInfo, ILogger logger = null)
//        {
//            using var stream = streamProvider.Read(path);
//            using var streamReader = new StreamReader(stream);

//            commandsInfo = default;

//            try
//            {
//                string respJson = streamReader.ReadToEnd();
//                var respCommands = JsonSerializer.Deserialize<RespCommandsInfo[]>(respJson, SerializerOptions)!;

//                var tmpRespCommandsInfo = new Dictionary<string, RespCommandsInfo>(StringComparer.OrdinalIgnoreCase);
//                foreach (var respCommandsInfo in respCommands)
//                {
//                    tmpRespCommandsInfo.Add(respCommandsInfo.Name, respCommandsInfo);
//                }

//                commandsInfo = new ReadOnlyDictionary<string, RespCommandsInfo>(tmpRespCommandsInfo);
//            }
//            catch (JsonException je)
//            {
//                logger?.LogError(je, "An error occurred while parsing resp commands info file (Path: {path}).", path);
//                return false;
//            }

//            return true;
//        }

//        public bool TryExportRespCommandsInfo(string path, IStreamProvider streamProvider, IReadOnlyDictionary<string, RespCommandsInfo> commandsInfo, ILogger logger = null)
//        {
//            string jsonSettings;

//            var commandsInfoToSerialize = commandsInfo.Values.OrderBy(ci => ci.Name).ToArray();
//            try
//            {
//                jsonSettings = JsonSerializer.Serialize(commandsInfoToSerialize, SerializerOptions);
//            }
//            catch (NotSupportedException e)
//            {
//                logger?.LogError(e, "An error occurred while serializing resp commands info file (Path: {path}).", path);
//                return false;
//            }

//            var data = Encoding.ASCII.GetBytes(jsonSettings);
//            streamProvider.Write(path, data);

//            return true;
//        }

//        public bool TryImportRespCommandsInfo(string path, IStreamProvider streamProvider, out IReadOnlyDictionary<string, T> objectMap,
//            ILogger logger = null) =>
//            throw new NotImplementedException();

//        public bool TryExportRespCommandsInfo(string path, IStreamProvider streamProvider, IReadOnlyDictionary<string, T> objectMap,
//            ILogger logger = null) =>
//            throw new NotImplementedException();
//    }

//    /// <summary>
//    /// Current supported RESP commands info file types
//    /// </summary>
//    public enum RespFileType
//    {
//        // Default file format (JSON serialized array of RespCommandsInfo objects)
//        Default = 0,
//    }
//}