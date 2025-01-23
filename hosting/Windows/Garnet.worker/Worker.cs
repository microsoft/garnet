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
            var builder = GarnetApplication.CreateHostBuilder(args);

            var app = builder.Build();

            await app.RunAsync(stoppingToken);
        }

        /// <summary>
        /// Triggered when the application host is performing a graceful shutdown.
        /// </summary>
        /// <param name="cancellationToken">Indicates that the shutdown process should no longer be graceful.</param>
        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            Dispose();
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
        }

        public override void Dispose()
        {
            if (_isDisposed)
            {
                return;
            }
            server?.Dispose();
            _isDisposed = true;
        }
    }
}