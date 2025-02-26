// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Tsavorite.devices
{
    public class AzureStorageNamedDeviceFactoryCreator : INamedDeviceFactoryCreator
    {
        readonly BlobUtilsV12.ServiceClients pageBlobAccount;
        readonly ILogger logger;

        /// <summary>
        /// Creator of factory for Azure devices
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="logger"></param>
        public AzureStorageNamedDeviceFactoryCreator(string connectionString, ILogger logger = null)
            : this(BlobUtilsV12.GetServiceClients(connectionString), logger)
        {
        }

        /// <summary>
        /// Creator of factory for Azure devices
        /// </summary>
        /// <param name="pageBlobAccount"></param>
        /// <param name="logger"></param>
        AzureStorageNamedDeviceFactoryCreator(BlobUtilsV12.ServiceClients pageBlobAccount, ILogger logger = null)
        {
            this.pageBlobAccount = pageBlobAccount;
            this.logger = logger;
        }

        public INamedDeviceFactory Create(string baseName)
        {
            return new AzureStorageNamedDeviceFactory(baseName, pageBlobAccount, logger);
        }
    }
}