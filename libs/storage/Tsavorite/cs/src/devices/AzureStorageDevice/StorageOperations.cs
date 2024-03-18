// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.devices
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    internal partial class BlobManager : IBlobManager
    {
        /// <inheritdoc />
        public async Task PerformWithRetriesAsync(
            SemaphoreSlim semaphore,
            bool requireLease,
            string name,
            string intent,
            string data,
            string target,
            int expectedLatencyBound,
            bool isCritical,
            Func<int, Task<long>> operationAsync,
            Func<Task> readETagAsync = null)
        {
            try
            {
                if (semaphore != null)
                {
                    await semaphore.WaitAsync();
                }

                Stopwatch stopwatch = new();
                int numAttempts = 0;
                bool mustReadETagFirst = false;

                while (true) // retry loop
                {
                    numAttempts++;
                    try
                    {
                        if (requireLease)
                        {
                            // we can re-establish the e-tag here because we check the lease afterwards
                            if (mustReadETagFirst)
                            {
                                await readETagAsync().ConfigureAwait(false);
                                mustReadETagFirst = false;
                            }

                            Interlocked.Increment(ref LeaseUsers);
                            await ConfirmLeaseIsGoodForAWhileAsync();
                        }

                        StorageErrorHandler.Token.ThrowIfCancellationRequested();

                        StorageTracer?.TsavoriteStorageProgress($"storage operation {name} ({intent}) started attempt {numAttempts}; target={target} {data}");

                        stopwatch.Restart();

                        long size = await operationAsync(numAttempts).ConfigureAwait(false);

                        stopwatch.Stop();
                        StorageTracer?.TsavoriteStorageProgress($"storage operation {name} ({intent}) succeeded on attempt {numAttempts}; target={target} latencyMs={stopwatch.Elapsed.TotalMilliseconds:F1} {data}");

                        if (stopwatch.ElapsedMilliseconds > expectedLatencyBound)
                        {
                            TraceHelper.TsavoritePerfWarning($"storage operation {name} ({intent}) took {stopwatch.Elapsed.TotalSeconds:F1}s on attempt {numAttempts}, which is excessive; {data}");
                        }

                        TraceHelper.TsavoriteAzureStorageAccessCompleted(intent, size, name, target, stopwatch.Elapsed.TotalMilliseconds, numAttempts);

                        return;
                    }
                    catch (Exception e) when (StorageErrorHandler.IsTerminated)
                    {
                        string message = $"storage operation {name} ({intent}) was canceled";
                        StorageTracer?.TsavoriteStorageProgress(message);
                        throw new OperationCanceledException(message, e);
                    }
                    catch (Exception e) when (BlobUtils.IsTransientStorageError(e) && numAttempts < MaxRetries)
                    {
                        stopwatch.Stop();

                        if (BlobUtils.IsTimeout(e))
                        {
                            TraceHelper.TsavoritePerfWarning($"storage operation {name} ({intent}) timed out on attempt {numAttempts} after {stopwatch.Elapsed.TotalSeconds:F1}s, retrying now; target={target} {data}");
                        }
                        else
                        {
                            TimeSpan nextRetryIn = GetDelayBetweenRetries(numAttempts);
                            HandleStorageError(name, $"storage operation {name} ({intent}) failed transiently on attempt {numAttempts}, retry in {nextRetryIn}s", target, e, false, true);
                            await Task.Delay(nextRetryIn);
                        }
                        continue;
                    }
                    catch (Azure.RequestFailedException ex) when (BlobUtilsV12.PreconditionFailed(ex) && readETagAsync != null)
                    {
                        StorageTracer?.TsavoriteStorageProgress($"storage operation {name} ({intent}) failed precondition on attempt {numAttempts}; target={target} latencyMs={stopwatch.Elapsed.TotalMilliseconds:F1} {data}");
                        mustReadETagFirst = true;
                        continue;
                    }
                    catch (Exception exception)
                    {
                        HandleStorageError(name, $"storage operation {name} ({intent}) failed on attempt {numAttempts}", target, exception, isCritical, StorageErrorHandler.IsTerminated);
                        throw;
                    }
                    finally
                    {
                        if (requireLease)
                        {
                            Interlocked.Decrement(ref LeaseUsers);
                        }
                    }
                }
            }
            finally
            {
                semaphore?.Release();
            }
        }
    }
}