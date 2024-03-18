// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Tsavorite.devices
{
    /// <summary>
    /// Device factory for Azure
    /// </summary>
    public class AzureStorageNamedDeviceFactory : INamedDeviceFactory
    {
        readonly BlobUtilsV12.ServiceClients pageBlobAccount;
        BlobUtilsV12.ContainerClients pageBlobContainer;
        BlobUtilsV12.BlobDirectory pageBlobDirectory;
        readonly ILogger logger;

        /// <summary>
        /// Create instance of factory for Azure devices
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="logger"></param>
        public AzureStorageNamedDeviceFactory(string connectionString, ILogger logger = null)
            : this(BlobUtilsV12.GetServiceClients(connectionString), logger)
        {
        }

        /// <summary>
        /// Create instance of factory for Azure devices
        /// </summary>
        /// <param name="pageBlobAccount"></param>
        /// <param name="logger"></param>
        AzureStorageNamedDeviceFactory(BlobUtilsV12.ServiceClients pageBlobAccount, ILogger logger = null)
        {
            this.pageBlobAccount = pageBlobAccount;
            this.logger = logger;
        }

        /// <inheritdoc />
        public void Initialize(string baseName)
            => InitializeAsync(baseName).GetAwaiter().GetResult();


        async Task InitializeAsync(string baseName)
        {
            var path = baseName.Split('/');
            var containerName = path[0];
            var dirName = string.Join("/", path.Skip(1));

            pageBlobContainer = BlobUtilsV12.GetContainerClients(pageBlobAccount, containerName);
            if (!await pageBlobContainer.WithRetries.ExistsAsync())
                await pageBlobContainer.WithRetries.CreateIfNotExistsAsync();

            pageBlobDirectory = new BlobUtilsV12.BlobDirectory(pageBlobContainer, dirName);
        }


        /// <inheritdoc />
        public void Delete(FileDescriptor fileInfo)
        {
            var dir = fileInfo.directoryName == "" ? pageBlobDirectory : pageBlobDirectory.GetSubDirectory(fileInfo.directoryName);

            if (fileInfo.fileName != null)
            {
                // We delete all files with fileName prefix, since shards have extensions as .0, .1, etc.
                foreach (var blob in dir.GetBlobsAsync(fileInfo.fileName + ".", default).GetAwaiter().GetResult())
                {
                    BlobUtilsV12.ForceDeleteAsync(pageBlobContainer.Default, blob).GetAwaiter().GetResult();
                }
            }
            else
            {
                foreach (var blob in dir.GetBlobsAsync(default).GetAwaiter().GetResult())
                {
                    BlobUtilsV12.ForceDeleteAsync(pageBlobContainer.Default, blob).GetAwaiter().GetResult();
                }
            }
        }

        /// <inheritdoc />
        public IDevice Get(FileDescriptor fileInfo)
        {
            return new AzureStorageDevice(fileInfo.fileName, pageBlobDirectory.GetSubDirectory(fileInfo.directoryName), null, false, false, logger);
        }

        /// <inheritdoc />
        public IEnumerable<FileDescriptor> ListContents(string path)
        {
            var dir = pageBlobDirectory.GetSubDirectory(path);
            var client = pageBlobContainer.Default;

            HashSet<string> directories = new();
            foreach (var item in client.GetBlobs(prefix: $"{dir.Prefix}/")
                .OrderByDescending(f => client.GetBlobClient(f.Name).GetProperties().Value.LastModified)
                )
            {
                // get the directory name
                var name = item.Name.Substring(dir.Prefix.Length + 1);
                // get substring until first slash
                var slash = name.IndexOf('/');
                if (slash > 0)
                {
                    // this is a directory
                    var dirName = name.Substring(0, slash);
                    if (!directories.Contains(dirName))
                    {
                        directories.Add(dirName);

                        // find file name from path
                        var fileName = name.Substring(name.LastIndexOf('/') + 1);

                        yield return new FileDescriptor
                        {
                            directoryName = dirName,
                            fileName = "",
                        };
                    }
                }
                else
                {
                    // this is a file
                    yield return new FileDescriptor
                    {
                        directoryName = "",
                        fileName = name,
                    };
                }
            }
        }
    }
}