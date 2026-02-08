// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.devices
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Core;
    using Azure.Core.Pipeline;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using Azure.Storage.Blobs.Specialized;

    static class BlobUtilsV12
    {
        public class ServerTimeoutPolicy : HttpPipelineSynchronousPolicy
        {
            readonly int timeout;

            public ServerTimeoutPolicy(int timeout)
            {
                this.timeout = timeout;
            }

            public override void OnSendingRequest(HttpMessage message)
            {
                message.Request.Uri.AppendQuery("timeout", timeout.ToString());
            }
        }
        public struct ServiceClients
        {
            public BlobServiceClient Default;
            public BlobServiceClient Aggressive;
            public BlobServiceClient WithRetries;
        }

        internal static ServiceClients GetServiceClients(string connectionString)
        {
            var (aggressiveOptions, defaultOptions, withRetriesOptions) = GetBlobClientOptions();

            return new ServiceClients()
            {
                Default = new BlobServiceClient(connectionString, defaultOptions),
                Aggressive = new BlobServiceClient(connectionString, aggressiveOptions),
                WithRetries = new BlobServiceClient(connectionString, withRetriesOptions),
            };
        }

        internal static ServiceClients GetServiceClients(string serviceUrl, TokenCredential credential)
        {
            var (aggressiveOptions, defaultOptions, withRetriesOptions) = GetBlobClientOptions();
            var serviceUri = new Uri(serviceUrl);

            return new ServiceClients()
            {
                Default = new BlobServiceClient(serviceUri, credential, defaultOptions),
                Aggressive = new BlobServiceClient(serviceUri, credential, aggressiveOptions),
                WithRetries = new BlobServiceClient(serviceUri, credential, withRetriesOptions),
            };
        }

        private static (BlobClientOptions AggressiveOptions, BlobClientOptions DefaultOptions, BlobClientOptions WithRetriesOptions) GetBlobClientOptions()
        {
            var AggressiveOptions = new BlobClientOptions();
            AggressiveOptions.Retry.MaxRetries = 0;
            AggressiveOptions.Retry.NetworkTimeout = TimeSpan.FromSeconds(3);
            AggressiveOptions.AddPolicy(new ServerTimeoutPolicy(2), HttpPipelinePosition.PerCall);

            var DefaultOptions = new BlobClientOptions();
            DefaultOptions.Retry.MaxRetries = 0;
            DefaultOptions.Retry.NetworkTimeout = TimeSpan.FromSeconds(16);
            DefaultOptions.AddPolicy(new ServerTimeoutPolicy(15), HttpPipelinePosition.PerCall);

            var WithRetriesOptions = new BlobClientOptions();
            WithRetriesOptions.Retry.MaxRetries = 10;
            WithRetriesOptions.Retry.Mode = RetryMode.Exponential;
            WithRetriesOptions.Retry.Delay = TimeSpan.FromSeconds(1);
            WithRetriesOptions.Retry.MaxDelay = TimeSpan.FromSeconds(30);

            return (AggressiveOptions, DefaultOptions, WithRetriesOptions);
        }

        public struct ContainerClients
        {
            public BlobContainerClient Default;
            public BlobContainerClient Aggressive;
            public BlobContainerClient WithRetries;
        }

        internal static ContainerClients GetContainerClients(ServiceClients serviceClients, string blobContainerName)
        {
            return new ContainerClients()
            {
                Default = serviceClients.Default.GetBlobContainerClient(blobContainerName),
                Aggressive = serviceClients.Aggressive.GetBlobContainerClient(blobContainerName),
                WithRetries = serviceClients.WithRetries.GetBlobContainerClient(blobContainerName),
            };

        }

        public struct BlockBlobClients
        {
            public BlockBlobClient Default;
            public BlockBlobClient Aggressive;
            public BlockBlobClient WithRetries;

            public string Name => Default?.Name;
        }

        internal static BlockBlobClients GetBlockBlobClients(ContainerClients containerClients, string blobName)
        {
            return new BlockBlobClients()
            {
                Default = containerClients.Default.GetBlockBlobClient(blobName),
                Aggressive = containerClients.Aggressive.GetBlockBlobClient(blobName),
                WithRetries = containerClients.WithRetries.GetBlockBlobClient(blobName),
            };

        }

        public struct PageBlobClients
        {
            public PageBlobClient Default;
            public PageBlobClient Aggressive;
        }

        internal static PageBlobClients GetPageBlobClients(ContainerClients containerClients, string blobName)
        {
            return new PageBlobClients()
            {
                Default = containerClients.Default.GetPageBlobClient(blobName),
                Aggressive = containerClients.Aggressive.GetPageBlobClient(blobName),
            };

        }

        public struct BlobDirectory
        {
            readonly ContainerClients client;
            readonly string prefix;

            public ContainerClients Client => client;
            public string Prefix => prefix;

            public BlobDirectory(ContainerClients client, string prefix)
            {
                this.client = client;
                this.prefix = string.Concat(prefix);
            }

            public BlobDirectory GetSubDirectory(string path)
            {
                return new BlobDirectory(client, $"{prefix}/{path}");
            }

            public BlockBlobClients GetBlockBlobClient(string name)
            {
                return GetBlockBlobClients(client, $"{prefix}/{name}");
            }

            public PageBlobClients GetPageBlobClient(string name)
            {
                return GetPageBlobClients(client, $"{prefix}/{name}");
            }

            public async Task<List<string>> GetBlobsAsync(CancellationToken cancellationToken)
            {
                var list = new List<string>();
                var options = new GetBlobsOptions() { Prefix = prefix };
                await foreach (var blob in client.WithRetries.GetBlobsAsync(options, cancellationToken: cancellationToken))
                {
                    list.Add(blob.Name);
                }
                return list;
            }

            public async Task<List<string>> GetBlobsAsync(string filePrefix, CancellationToken cancellationToken = default)
            {
                var list = new List<string>();
                var options = new GetBlobsOptions() { Prefix = $"{prefix}/{filePrefix}" };
                await foreach (var blob in client.WithRetries.GetBlobsAsync(options, cancellationToken: cancellationToken))
                {
                    list.Add(blob.Name);
                }
                return list;
            }

            public override string ToString()
            {
                return $"{prefix}/";
            }
        }

        /// <summary>
        /// Forcefully deletes a blob.
        /// </summary>
        /// <returns>A task that completes when the operation is finished.</returns>
        public static async Task<bool> ForceDeleteAsync(BlobContainerClient containerClient, string blobName)
        {
            var blob = containerClient.GetBlobClient(blobName);

            try
            {
                await blob.DeleteAsync();
                return true;
            }
            catch (Azure.RequestFailedException e) when (BlobDoesNotExist(e))
            {
                return false;
            }
            catch (Azure.RequestFailedException e) when (CannotDeleteBlobWithLease(e))
            {
                try
                {
                    var leaseClient = new BlobLeaseClient(blob);
                    await leaseClient.BreakAsync(TimeSpan.Zero);
                }
                catch
                {
                    // we ignore exceptions in the lease breaking since there could be races
                }

                // retry the delete
                try
                {
                    await blob.DeleteAsync();
                    return true;
                }
                catch (Azure.RequestFailedException ex) when (BlobDoesNotExist(ex))
                {
                    return false;
                }
            }
        }

        // Lease error codes are documented at https://docs.microsoft.com/en-us/rest/api/storageservices/lease-blob

        public static bool LeaseConflictOrExpired(Azure.RequestFailedException e)
        {
            return e.Status == 409 || e.Status == 412;
        }

        public static bool LeaseConflict(Azure.RequestFailedException e)
        {
            return e.Status == 409;
        }

        public static bool LeaseExpired(Azure.RequestFailedException e)
        {
            return e.Status == 412;
        }

        public static bool CannotDeleteBlobWithLease(Azure.RequestFailedException e)
        {
            return e.Status == 412;
        }

        public static bool PreconditionFailed(Azure.RequestFailedException e)
        {
            return e.Status == 409 || e.Status == 412;
        }

        public static bool BlobDoesNotExist(Azure.RequestFailedException e)
        {
            return e.Status == 404 && e.ErrorCode == BlobErrorCode.BlobNotFound;
        }

        public static bool BlobAlreadyExists(Azure.RequestFailedException e)
        {
            return e.Status == 409;
        }
    }
}