// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace Garnet
{
    public class Worker : BackgroundService
    {
        private bool _isDisposed = false;
        private readonly string[] args;

        private GarnetServer server;

        public Worker(string[] args)
        {
            this.args = args;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                server = new GarnetServer(args);

                // Start the server
                server.Start();

                await Task.Delay(Timeout.Infinite, stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unable to initialize server due to exception: {ex.Message}");
            }
        }

        /// <summary>
        /// Triggered when the application host is performing a graceful shutdown.
        /// </summary>
        /// <param name="cancellationToken">Indicates that the shutdown process should no longer be graceful.</param>
        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            try
            {
                // Wait for existing connections to complete (with timeout)
                // Consider making timeout configurable via configuration file or environment variable.
                // Currently using fixed 30-second timeout to avoid conflicts with existing server arguments.
                var timeout = TimeSpan.FromSeconds(30);
                await WaitForActiveConnectionsToComplete(timeout, cancellationToken);

                // Flush AOF buffer and create checkpoint using Store API
                if (server?.Store != null)
                {
                    try
                    {
                        // CommitAOFAsync returns false if AOF is disabled
                        var commitSuccess = await server.Store.CommitAOFAsync(cancellationToken);

                        if (commitSuccess)
                        {
                            // Wait only if commit was successful
                            await server.Store.WaitForCommitAsync(cancellationToken);
                            Console.WriteLine("AOF operations completed successfully.");
                        }
                        else
                        {
                            Console.WriteLine("AOF commit skipped (likely disabled or unavailable).");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error during graceful AOF operations: {ex.Message}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Force shutdown on cancellation request
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during graceful shutdown: {ex.Message}");
            }
            finally
            {
                // Resource cleanup
                Dispose();
                await base.StopAsync(cancellationToken);
            }
        }

        public override void Dispose()
        {
            if (_isDisposed)
            {
                return;
            }
            server?.Dispose();
            _isDisposed = true;
            GC.SuppressFinalize(this); // This line resolve CA1816 Info: Dispose methods should call SuppressFinalize
        }

        /// <summary>
        /// Waits for active connections to complete within the specified timeout period.
        /// </summary>
        /// <param name="timeout">The timeout duration to wait for connections to complete</param>
        /// <param name="cancellationToken">Cancellation token used to cancel the operation when shutdown is requested</param>
        /// <returns>A task that represents the asynchronous wait operation</returns>
        private async Task WaitForActiveConnectionsToComplete(TimeSpan timeout, CancellationToken cancellationToken)
        {
            if (server?.Metrics == null) return;

            var stopwatch = Stopwatch.StartNew();
            var consecutiveErrors = 0;
            const int maxConsecutiveErrors = 3;

            while (stopwatch.Elapsed < timeout && !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Use Metrics API to get SERVER info metrics
                    var serverMetrics = server.Metrics.GetInfoMetrics(Garnet.common.InfoMetricsType.SERVER);

                    // Find connected_clients metric without LINQ
                    var activeConnections = 0;
                    for (int i = 0; i < serverMetrics.Length; i++)
                    {
                        if (serverMetrics[i].Name == "connected_clients")
                        {
                            if (int.TryParse(serverMetrics[i].Value, out activeConnections))
                                break;
                        }
                    }

                    if (activeConnections == 0)
                    {
                        Console.WriteLine("All connections have been closed gracefully.");
                        break;
                    }

                    Console.WriteLine($"Waiting for {activeConnections} active connections to complete...");
                    consecutiveErrors = 0; // Reset error counter on success
                    await Task.Delay(100, cancellationToken);
                }
                catch (Exception ex)
                {
                    consecutiveErrors++;
                    Console.WriteLine($"Error checking active connections: {ex.Message}");

                    // Break only after multiple consecutive errors
                    if (consecutiveErrors >= maxConsecutiveErrors)
                    {
                        Console.WriteLine($"Too many consecutive errors ({consecutiveErrors}). Stopping connection check.");
                        break;
                    }

                    // Continue with longer delay after error
                    await Task.Delay(500, cancellationToken);
                }
            }

            if (stopwatch.Elapsed >= timeout)
            {
                Console.WriteLine($"Timeout reached after {timeout.TotalSeconds} seconds. Some connections may still be active.");
            }
        }
    }
}