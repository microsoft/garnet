// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Azure.Core;
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
        /// Create instance of factory for Azure devices
        /// </summary>
        /// <param name="serviceUri"></param>
        /// <param name="credential"></param>
        /// <param name="logger"></param>
        public AzureStorageNamedDeviceFactoryCreator(string serviceUri, TokenCredential credential, ILogger logger = null)
            : this(BlobUtilsV12.GetServiceClients(serviceUri, credential), logger)
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