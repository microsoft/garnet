// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace Garnet
{
    public class Worker : BackgroundService
    {
        private bool _isDisposed = false;
        private readonly string[] args;
        private readonly TimeSpan _shutdownTimeout;

        private GarnetServer server;

        /// <param name="args">Command line arguments forwarded to <see cref="GarnetServer"/>.</param>
        /// <param name="shutdownTimeout">
        /// How long to wait for active connections to drain during graceful shutdown.
        /// Must be less than the host <see cref="Microsoft.Extensions.Hosting.HostOptions.ShutdownTimeout"/>
        /// so that data finalization (AOF commit / checkpoint) can also complete within the host budget.
        /// </param>
        public Worker(string[] args, TimeSpan shutdownTimeout)
        {
            this.args = args;
            _shutdownTimeout = shutdownTimeout;
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
                if (server != null)
                {
                    // If cancellation is requested, we will skip the graceful shutdown and proceed to dispose immediately
                    bool isForceShutdown = cancellationToken.IsCancellationRequested;
                    // Perform graceful shutdown with AOF commit and checkpoint when not forced Shutdown From OS.
                    await server.ShutdownAsync(timeout: _shutdownTimeout, noSave: isForceShutdown, token: cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // Force shutdown requested - proceed to dispose
            }
            finally
            {
                // Ensure base class cleanup although cancellationToken is cancelled
                await base.StopAsync(CancellationToken.None).ConfigureAwait(false);
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
            base.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}