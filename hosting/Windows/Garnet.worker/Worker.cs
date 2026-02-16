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
                if (server != null)
                {
                    // If cancellation is requested, we will skip the graceful shutdown and proceed to dispose immediately
                    bool isForceShutdown = cancellationToken.IsCancellationRequested;
                    // Perform graceful shutdown with AOF commit and checkpoint when not forced Shutdown From OS.
                    await server.ShutdownAsync(timeout: TimeSpan.FromSeconds(5), noSave: isForceShutdown, token: cancellationToken).ConfigureAwait(false);
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