// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.devices
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Storage.Blobs.Specialized;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Provides management of blobs and blob names associated with a partition, and logic for partition lease maintenance and termination.
    /// </summary>
    internal partial class BlobManager : IBlobManager
    {
        readonly CancellationTokenSource shutDownOrTermination;

        BlobUtilsV12.BlockBlobClients leaseBlob;
        BlobLeaseClient leaseClient;
        readonly BlobUtilsV12.BlobDirectory leaseBlobDirectory;
        readonly string LeaseBlobName = "commit-lease";

        readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(45); // max time the lease stays after unclean shutdown
        readonly TimeSpan LeaseRenewal = TimeSpan.FromSeconds(30); // how often we renew the lease
        readonly TimeSpan LeaseSafetyBuffer = TimeSpan.FromSeconds(10); // how much time we want left on the lease before issuing a protected access

        internal TsavoriteTraceHelper TraceHelper { get; private set; }

        /// <summary>
        /// Storage tracer
        /// </summary>
        public TsavoriteTraceHelper StorageTracer => TraceHelper.IsTracingAtMostDetailedLevel ? TraceHelper : null;

        /// <summary>
        /// Error handler for storage accesses
        /// </summary>
        public IStorageErrorHandler StorageErrorHandler { get; private set; }

        static readonly SemaphoreSlim AsynchronousStorageReadMaxConcurrencyStatic = new(Math.Min(100, Environment.ProcessorCount * 10));
        static readonly SemaphoreSlim AsynchronousStorageWriteMaxConcurrencyStatic = new(Math.Min(50, Environment.ProcessorCount * 7));

        /// <inheritdoc />
        public SemaphoreSlim AsynchronousStorageReadMaxConcurrency => AsynchronousStorageReadMaxConcurrencyStatic;

        /// <inheritdoc />
        public SemaphoreSlim AsynchronousStorageWriteMaxConcurrency => AsynchronousStorageWriteMaxConcurrencyStatic;

        internal volatile int LeaseUsers;

        volatile Stopwatch leaseTimer;

        const int MaxRetries = 10;

        readonly bool maintainLease = false;

        /// <summary>
        /// Get delay between retries
        /// </summary>
        /// <param name="numAttempts"></param>
        /// <returns></returns>
        public static TimeSpan GetDelayBetweenRetries(int numAttempts)
            => TimeSpan.FromSeconds(Math.Pow(2, (numAttempts - 1)));

        /// <summary>
        /// Create a blob manager to handle tracing, leases, and storage operations
        /// </summary>
        /// <param name="logger">A logger for logging</param>
        /// <param name="performanceLogger"></param>
        /// <param name="logLevelLimit">A limit on log event level emitted</param>
        /// <param name="errorHandler">A handler for errors encountered in this partition</param>
        /// <param name="maintainLease">Whether lease should be maintained by blob manager</param>
        /// <param name="leaseBlobDirectory">Lease bob is stored in this directory</param>
        /// <param name="leaseBlobName">Name of lease blob (default is commit-lease)</param>
        internal BlobManager(
            ILogger logger,
            ILogger performanceLogger,
            LogLevel logLevelLimit,
            IStorageErrorHandler errorHandler,
            bool maintainLease = false,
            BlobUtilsV12.BlobDirectory leaseBlobDirectory = default,
            string leaseBlobName = null)
        {
            this.maintainLease = maintainLease;
            this.leaseBlobDirectory = leaseBlobDirectory;
            LeaseBlobName = leaseBlobName ?? LeaseBlobName;
            TraceHelper = new TsavoriteTraceHelper(logger, logLevelLimit, performanceLogger);
            StorageErrorHandler = errorHandler ?? new StorageErrorHandler(logger, logLevelLimit, null, null);
            shutDownOrTermination = errorHandler == null ?
                new CancellationTokenSource() :
                CancellationTokenSource.CreateLinkedTokenSource(errorHandler.Token);
            if (maintainLease)
                StartAsync().Wait();
        }


        Task LeaseMaintenanceLoopTask = Task.CompletedTask;
        volatile Task NextLeaseRenewalTask = Task.CompletedTask;

        /// <summary>
        /// Start lease maintenance loop
        /// </summary>
        /// <returns></returns>
        async Task StartAsync()
        {
            leaseBlob = leaseBlobDirectory.GetBlockBlobClient(LeaseBlobName);
            leaseClient = leaseBlob.WithRetries.GetBlobLeaseClient();
            await AcquireOwnership();
        }

        /// <inheritdoc />
        public void HandleStorageError(string where, string message, string blobName, Exception e, bool isFatal, bool isWarning)
        {
            if (blobName == null)
            {
                StorageErrorHandler.HandleError(where, message, e, isFatal, isWarning);
            }
            else
            {
                StorageErrorHandler.HandleError(where, $"{message} blob={blobName}", e, isFatal, isWarning);
            }
        }

        /// <summary>
        /// clean shutdown, wait for everything, then terminate
        /// </summary>
        public async Task StopAsync()
        {
            shutDownOrTermination.Cancel(); // has no effect if already cancelled

            await LeaseMaintenanceLoopTask; // wait for loop to terminate cleanly
        }

        /// <inheritdoc />
        public ValueTask ConfirmLeaseIsGoodForAWhileAsync()
        {
            if (leaseTimer?.Elapsed < LeaseDuration - LeaseSafetyBuffer && !shutDownOrTermination.IsCancellationRequested)
            {
                return default;
            }
            TraceHelper.LeaseProgress("Access is waiting for fresh lease");
            return new ValueTask(NextLeaseRenewalTask);
        }

        public void ConfirmLeaseIsGoodForAWhile()
        {
            if (leaseTimer?.Elapsed < LeaseDuration - LeaseSafetyBuffer && !shutDownOrTermination.IsCancellationRequested)
            {
                return;
            }
            TraceHelper.LeaseProgress("Access is waiting for fresh lease");
            NextLeaseRenewalTask.Wait();
        }

        async Task AcquireOwnership()
        {
            var newLeaseTimer = new Stopwatch();
            int numAttempts = 0;

            while (true)
            {
                StorageErrorHandler.Token.ThrowIfCancellationRequested();
                numAttempts++;

                try
                {
                    newLeaseTimer.Restart();

                    await leaseClient.AcquireAsync(
                        LeaseDuration,
                        null,
                        StorageErrorHandler.Token)
                        .ConfigureAwait(false);
                    TraceHelper.LeaseAcquired();

                    leaseTimer = newLeaseTimer;
                    LeaseMaintenanceLoopTask = Task.Run(MaintenanceLoopAsync);
                    return;
                }
                catch (Azure.RequestFailedException ex) when (BlobUtilsV12.LeaseConflictOrExpired(ex))
                {
                    TraceHelper.LeaseProgress("Waiting for lease");

                    // the previous owner has not released the lease yet, 
                    // try again until it becomes available, should be relatively soon
                    // as the transport layer is supposed to shut down the previous owner when starting this
                    await Task.Delay(TimeSpan.FromSeconds(1), StorageErrorHandler.Token);

                    continue;
                }
                catch (Azure.RequestFailedException ex) when (BlobUtilsV12.BlobDoesNotExist(ex))
                {
                    // Create blob with empty content, then try again
                    await PerformWithRetriesAsync(
                        null,
                        false,
                        "CloudBlockBlob.UploadFromByteArrayAsync",
                        "CreateCommitLog",
                        "",
                        leaseBlob.Default.Name,
                        2000,
                        true,
                        async (numAttempts) =>
                        {
                            try
                            {
                                var client = numAttempts > 2 ? leaseBlob.Default : leaseBlob.Aggressive;
                                await client.UploadAsync(new MemoryStream());
                            }
                            catch (Azure.RequestFailedException ex2) when (BlobUtilsV12.LeaseConflictOrExpired(ex2))
                            {
                                // creation race, try from top
                                TraceHelper.LeaseProgress("Creation race observed, retrying");
                            }

                            return 1;
                        });

                    continue;
                }
                catch (OperationCanceledException) when (StorageErrorHandler.IsTerminated)
                {
                    throw; // o.k. during termination or shutdown
                }
                catch (Exception e) when (StorageErrorHandler.IsTerminated)
                {
                    string message = $"Lease acquisition was canceled";
                    TraceHelper.LeaseProgress(message);
                    throw new OperationCanceledException(message, e);
                }
                catch (Exception ex) when (numAttempts < MaxRetries
                    && !StorageErrorHandler.IsTerminated && BlobUtils.IsTransientStorageError(ex))
                {
                    if (BlobUtils.IsTimeout(ex))
                    {
                        TraceHelper.TsavoritePerfWarning($"Lease acquisition timed out, retrying now");
                    }
                    else
                    {
                        TimeSpan nextRetryIn = GetDelayBetweenRetries(numAttempts);
                        TraceHelper.TsavoritePerfWarning($"Lease acquisition failed transiently, retrying in {nextRetryIn}");
                        await Task.Delay(nextRetryIn);
                    }
                    continue;
                }
                catch (Exception e) when (!Utils.IsFatal(e))
                {
                    StorageErrorHandler.HandleError(nameof(AcquireOwnership), "Could not acquire partition lease", e, true, false);
                    throw;
                }
            }
        }

        public async Task RenewLeaseTask()
        {
            try
            {
                shutDownOrTermination.Token.ThrowIfCancellationRequested();

                var nextLeaseTimer = new Stopwatch();
                nextLeaseTimer.Start();

                TraceHelper.LeaseProgress($"Renewing lease at {leaseTimer.Elapsed.TotalSeconds - LeaseDuration.TotalSeconds}s");
                await leaseClient.RenewAsync(null, StorageErrorHandler.Token).ConfigureAwait(false);
                TraceHelper.LeaseRenewed(leaseTimer.Elapsed.TotalSeconds, leaseTimer.Elapsed.TotalSeconds - LeaseDuration.TotalSeconds);

                if (nextLeaseTimer.ElapsedMilliseconds > 2000)
                {
                    TraceHelper.TsavoritePerfWarning($"RenewLeaseAsync took {nextLeaseTimer.Elapsed.TotalSeconds:F1}s, which is excessive; {leaseTimer.Elapsed.TotalSeconds - LeaseDuration.TotalSeconds}s past expiry");
                }

                leaseTimer = nextLeaseTimer;
            }
            catch (OperationCanceledException) when (StorageErrorHandler.IsTerminated)
            {
                throw; // o.k. during termination or shutdown
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                TraceHelper.LeaseLost(leaseTimer.Elapsed.TotalSeconds, nameof(RenewLeaseTask));
                throw;
            }
        }

        public async Task MaintenanceLoopAsync()
        {
            TraceHelper.LeaseProgress("Started lease maintenance loop");
            try
            {
                while (true)
                {
                    int timeLeft = (int)(LeaseRenewal - leaseTimer.Elapsed).TotalMilliseconds;

                    if (timeLeft <= 0)
                    {
                        NextLeaseRenewalTask = RenewLeaseTask();
                    }
                    else
                    {
                        NextLeaseRenewalTask = LeaseTimer.Instance.Schedule(timeLeft, RenewLeaseTask, shutDownOrTermination.Token);
                    }

                    // wait for successful renewal, or exit the loop as this throws
                    await NextLeaseRenewalTask;
                }
            }
            catch (OperationCanceledException)
            {
                // it's o.k. to cancel while waiting
                TraceHelper.LeaseProgress("Lease renewal loop cleanly canceled");
            }
            catch (Azure.RequestFailedException e) when (e.InnerException != null && e.InnerException is OperationCanceledException)
            {
                // it's o.k. to cancel a lease renewal
                TraceHelper.LeaseProgress("Lease renewal storage operation canceled");
            }
            catch (Azure.RequestFailedException ex) when (BlobUtilsV12.LeaseConflict(ex))
            {
                // We lost the lease to someone else. Terminate ownership immediately.
                StorageErrorHandler.HandleError(nameof(MaintenanceLoopAsync), "Lost partition lease", ex, true, true);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                StorageErrorHandler.HandleError(nameof(MaintenanceLoopAsync), "Could not maintain partition lease", e, true, false);
            }

            TraceHelper.LeaseProgress("Exited lease maintenance loop");

            while (LeaseUsers > 0
                && !StorageErrorHandler.IsTerminated
                && (leaseTimer?.Elapsed < LeaseDuration))
            {
                await Task.Delay(20); // give storage accesses that are in progress and require the lease a chance to complete
            }

            TraceHelper.LeaseProgress("Waited for lease users to complete");

            // release the lease
            try
            {
                TraceHelper.LeaseProgress("Releasing lease");

                await leaseClient.ReleaseAsync(null, StorageErrorHandler.Token).ConfigureAwait(false);
                TraceHelper.LeaseReleased(leaseTimer.Elapsed.TotalSeconds);
            }
            catch (OperationCanceledException)
            {
                // it's o.k. if termination is triggered while waiting
            }
            catch (Azure.RequestFailedException e) when (e.InnerException != null && e.InnerException is OperationCanceledException)
            {
                // it's o.k. if termination is triggered while we are releasing the lease
            }
            catch (Exception e)
            {
                // we swallow, but still report exceptions when releasing a lease
                StorageErrorHandler.HandleError(nameof(MaintenanceLoopAsync), "Could not release partition lease during shutdown", e, false, true);
            }

            StorageErrorHandler.TerminateNormally();

            TraceHelper.LeaseProgress("Blob manager stopped");
        }
    }
}