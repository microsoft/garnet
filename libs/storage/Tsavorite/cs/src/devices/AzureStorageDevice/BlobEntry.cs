// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.devices
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;

    // This class bundles a page blob object with a queue and a counter to ensure 
    // 1) BeginCreate is not called more than once
    // 2) No writes are issued before EndCreate
    // The creator of a BlobEntry is responsible for populating the object with an underlying Page Blob. Any subsequent callers
    // either directly write to the created page blob, or queues the write so the creator can clear it after creation is complete.
    // In-progress creation is denoted by a null value on the underlying page blob
    class BlobEntry
    {
        public BlobUtilsV12.PageBlobClients PageBlob { get; private set; }
        public Azure.ETag ETag { get; set; }

        ConcurrentQueue<Action> pendingWrites;
        readonly AzureStorageDevice azureStorageDevice;
        int waitingCount;



        /// <summary>
        /// Creates a new BlobEntry to represent a page blob that already exists in storage.
        /// </summary>
        /// <param name="pageBlob"></param>
        /// <param name="eTag"></param>
        /// <param name="azureStorageDevice"></param>
        public BlobEntry(BlobUtilsV12.PageBlobClients pageBlob, Azure.ETag eTag, AzureStorageDevice azureStorageDevice)
        {
            PageBlob = pageBlob;
            this.azureStorageDevice = azureStorageDevice;
            ETag = eTag;
        }

        /// <summary>
        /// Creates a new BlobEntry to represent a page blob that will be created by <see cref="CreateAsync(long, BlobUtilsV12.PageBlobClients)"/>.
        /// </summary>
        public BlobEntry(AzureStorageDevice azureStorageDevice)
        {
            this.azureStorageDevice = azureStorageDevice;
            pendingWrites = new ConcurrentQueue<Action>();
            waitingCount = 0;
        }

        /// <summary>
        /// Asynchronously invoke create on the given pageBlob.
        /// </summary>
        /// <param name="size">maximum size of the blob</param>
        /// <param name="pageBlob">The page blob to create</param>
        public async Task CreateAsync(long size, BlobUtilsV12.PageBlobClients pageBlob)
        {
            await azureStorageDevice.BlobManager.PerformWithRetriesAsync(
                azureStorageDevice.BlobManager.AsynchronousStorageReadMaxConcurrency,
                true,
                "PageBlobClient.CreateAsync",
                "CreateDevice",
                "",
                pageBlob.Default.Name,
                3000,
                true,
                async (numAttempts) =>
                {
                    var client = (numAttempts > 1) ? pageBlob.Default : pageBlob.Aggressive;

                    var response = await client.CreateAsync(
                    size: size,
                    conditions: new Azure.Storage.Blobs.Models.PageBlobRequestConditions() { IfNoneMatch = Azure.ETag.All },
                    cancellationToken: azureStorageDevice.StorageErrorHandler.Token);

                    ETag = response.Value.ETag;
                    return 1;
                },
                async () =>
                {
                    var response = await pageBlob.Default.GetPropertiesAsync();
                    ETag = response.Value.ETag;
                });

            // At this point the blob is fully created. After this line all consequent writers will write immediately. We just
            // need to clear the queue of pending writers.
            PageBlob = pageBlob;

            // Take a snapshot of the current waiting count. Exactly this many actions will be cleared.
            // Swapping in -1 will inform any stragglers that we are not taking their actions and prompt them to retry (and call write directly)
            int waitingCountSnapshot = Interlocked.Exchange(ref waitingCount, -1);
            Action action;

            // Clear actions
            for (int i = 0; i < waitingCountSnapshot; i++)
            {
                // inserts into the queue may lag behind the creation thread. We have to wait until that happens.
                // This is so rare, that we are probably okay with a busy wait.
                while (!pendingWrites.TryDequeue(out action)) { }
                action();
            }

            // Mark for deallocation for the GC
            pendingWrites = null;
        }

        /// <summary>
        /// Attempts to enqueue an action to be invoked by the creator after creation is done. Should only be invoked when
        /// creation is in-flight. This call is allowed to fail (and return false) if concurrently the creation is complete.
        /// The caller should call the write action directly instead of queueing in this case.
        /// </summary>
        /// <param name="writeAction">The write action to perform</param>
        /// <returns>Whether the action was successfully enqueued</returns>
        public bool TryQueueAction(Action writeAction)
        {
            int currentCount;
            do
            {
                currentCount = waitingCount;

                // If current count became -1, creation is complete. New queue entries will not be processed and we must call the action ourselves.
                if (currentCount == -1) return false;

            } while (Interlocked.CompareExchange(ref waitingCount, currentCount + 1, currentCount) != currentCount);

            // Enqueue last. The creation thread is obliged to wait until it has processed waitingCount many actions.
            // It is extremely unlikely that we will get scheduled out here anyways.
            pendingWrites.Enqueue(writeAction);
            return true;
        }
    }
}