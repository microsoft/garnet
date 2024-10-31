// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet
{
    /// <summary>
    /// Interface for importing / exporting options from different configuration file types
    /// </summary>
    internal interface IConfigProvider
    {
        /// <summary>
        /// Import an Options object from path using a stream provider
        /// </summary>
        /// <param name="path">Path to the config file containing the serialized object</param>
        /// <param name="streamProvider">Stream provider to use when reading from the path</param>
        /// <param name="options">Options object to populate with deserialized options</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if import succeeded</returns>
        bool TryImportOptions(string path, IStreamProvider streamProvider, Options options, ILogger logger);

        /// <summary>
        /// Export an Options object to path using a stream provider
        /// </summary>
        /// <param name="path">Path to the config file to write into</param>
        /// <param name="streamProvider">Stream provider to use when writing to the path</param>
        /// <param name="options">Options object to serialize</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if export succeeded</returns>
        bool TryExportOptions(string path, IStreamProvider streamProvider, Options options, ILogger logger);
    }

    internal class ConfigProviderFactory
    {
        /// <summary>
        /// Get a IConfigProvider instance based on its configuration file type
        /// </summary>
        /// <param name="fileType">The configuration file type</param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public static IConfigProvider GetConfigProvider(ConfigFileType fileType)
        {
            switch (fileType)
            {
                case ConfigFileType.GarnetConf:
                    return GarnetConfigProvider.Instance;
                case ConfigFileType.RedisConf:
                    return RedisConfigProvider.Instance;
                default:
                    throw new NotImplementedException($"No ConfigProvider exists for file type: {fileType}.");
            }
        }
    }

    /// <summary>
    /// Config provider for a garnet.conf file (JSON serialized Options object)
    /// </summary>
    internal class GarnetConfigProvider : IConfigProvider
    {
        private static readonly Lazy<IConfigProvider> LazyInstance;

        public static IConfigProvider Instance => LazyInstance.Value;

        static GarnetConfigProvider()
        {
            LazyInstance = new(() => new GarnetConfigProvider());
        }

        private GarnetConfigProvider()
        {
        }

        public bool TryImportOptions(string path, IStreamProvider streamProvider, Options options, ILogger logger)
        {
            using var stream = streamProvider.Read(path);
            using var streamReader = new StreamReader(stream);

            try
            {
                var jsonSerializerOptions = new JsonSerializerOptions
                {
                    Converters = { new PopulateObjectJsonConverter<Options>(options), new JsonStringEnumConverter() },
                    NumberHandling = JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.WriteAsString,
                };

                var json = streamReader.ReadToEnd();
                
                var jsonReaderOptions = new JsonReaderOptions
                {
                    CommentHandling = JsonCommentHandling.Skip, 
                    AllowTrailingCommas = true
                };

                var jsonReader = new Utf8JsonReader(new ReadOnlySpan<byte>(Encoding.UTF8.GetBytes(json)), jsonReaderOptions);

                // No need fot the return value, as the deserializer populates the existing options instance
                _ = JsonSerializer.Deserialize<Options>(ref jsonReader, jsonSerializerOptions);
            }
            catch (JsonException je)
            {
                logger?.LogError(je, "An error occurred while parsing config file (Path: {path}).", path);
                return false;
            }

            return true;
        }

        public bool TryExportOptions(string path, IStreamProvider streamProvider, Options options, ILogger logger)
        {
            string jsonSettings;
            try
            {
                jsonSettings = JsonSerializer.Serialize(options, new JsonSerializerOptions { WriteIndented = true });
            }
            catch (NotSupportedException e)
            {
                logger?.LogError(e, "An error occurred while serializing config file (Path: {path}).", path);
                return false;
            }

            var data = Encoding.ASCII.GetBytes(jsonSettings);
            streamProvider.Write(path, data);

            return true;
        }
    }

    /// <summary>
    /// Config provider for a redis.conf file
    /// </summary>
    internal class RedisConfigProvider : IConfigProvider
    {
        private static readonly Lazy<IConfigProvider> LazyInstance;

        public static IConfigProvider Instance => LazyInstance.Value;

        static RedisConfigProvider()
        {
            LazyInstance = new(() => new RedisConfigProvider());
        }

        private RedisConfigProvider()
        {
        }

        public bool TryImportOptions(string path, IStreamProvider streamProvider, Options options, ILogger logger)
        {
            using var stream = streamProvider.Read(path);
            using var streamReader = new StreamReader(stream);
            var redisOptions = RedisConfigSerializer.Deserialize(streamReader, logger);
            return RedisConfigSerializer.TryPopulateOptions(redisOptions, options);
        }

        public bool TryExportOptions(string path, IStreamProvider streamProvider, Options options, ILogger logger)
        {
            throw new NotImplementedException();
        }
    }
}