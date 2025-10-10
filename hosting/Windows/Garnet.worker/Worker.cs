// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
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

                // Take checkpoint if tiered storage is enabled, otherwise flush AOF buffer
                if (server != null)
                {
                    try
                    {
                        // Access storeWrapper field using reflection
                        var storeWrapperField = server.GetType().GetField("storeWrapper",
                            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

                        if (storeWrapperField?.GetValue(server) is StoreWrapper storeWrapper)
                        {
                            var enableStorageTier = storeWrapper.serverOptions.EnableStorageTier;

                            if (enableStorageTier)
                            {
                                // Checkpoint takes priority when both tiered storage and AOF are enabled
                                Console.WriteLine("Taking checkpoint for tiered storage...");
                                var checkpointSuccess = storeWrapper.TakeCheckpoint(background: false, logger: null, token: cancellationToken);

                                if (checkpointSuccess)
                                {
                                    Console.WriteLine("Checkpoint completed successfully.");
                                }
                                else
                                {
                                    Console.WriteLine("Checkpoint skipped (another checkpoint in progress).");
                                }
                            }
                            else if (server.Store != null)
                            {
                                // Flush AOF buffer if AOF is enabled
                                // CommitAOFAsync returns false if AOF is disabled
                                var commitSuccess = await server.Store.CommitAOFAsync(cancellationToken);

                                if (commitSuccess)
                                {
                                    // Wait only if commit was successful
                                    _ = await server.Store.WaitForCommitAsync(cancellationToken);
                                    Console.WriteLine("AOF operations completed successfully.");
                                }
                                else
                                {
                                    Console.WriteLine("AOF commit skipped (likely disabled or unavailable).");
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error during graceful shutdown operations: {ex.Message}");
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
                await base.StopAsync(cancellationToken);
                Dispose();
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

            // Simple polling intervals: 50ms -> 300ms -> 1000ms
            var delays = new[] { 50, 300, 1000 };
            var delayIndex = 0;

            while (stopwatch.Elapsed < timeout && !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var activeConnections = GetActiveConnectionCount();

                    if (activeConnections == 0)
                    {
                        Console.WriteLine("All connections have been closed gracefully.");
                        break;
                    }

                    Console.WriteLine($"Waiting for {activeConnections} active connections to complete...");

                    // Use current delay and progress to next one (up to max)
                    var currentDelay = delays[delayIndex];
                    if (delayIndex < delays.Length - 1) delayIndex++;

                    await Task.Delay(currentDelay, cancellationToken);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error checking active connections: {ex.Message}");
                    // Reset to fastest polling on error and wait a bit longer
                    delayIndex = 0;
                    await Task.Delay(500, cancellationToken);
                }
            }

            if (stopwatch.Elapsed >= timeout)
            {
                Console.WriteLine($"Timeout reached after {timeout.TotalSeconds} seconds. Some connections may still be active.");
            }
        }

        /// <summary>
        /// Gets the current number of active connections from server metrics.
        /// </summary>
        /// <returns>Number of active connections, or 0 if not found</returns>
        private int GetActiveConnectionCount()
        {
            if (server == null || server.Metrics == null)
                return 0;
            var serverMetrics = server.Metrics.GetInfoMetrics(Garnet.common.InfoMetricsType.SERVER);

            for (int i = 0; i < serverMetrics.Length; i++)
            {
                if (serverMetrics[i].Name == "connected_clients")
                {
                    return int.TryParse(serverMetrics[i].Value, out var count) ? count : 0;
                }
            }

            return 0;
        }
    }
}