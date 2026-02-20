// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.server.Metrics.Latency;
using OpenTelemetry;
using OpenTelemetry.Metrics;

namespace Garnet.server.Metrics
{
    internal sealed class GarnetOpenTelemetryServerMonitor : IDisposable
    {
        private readonly GarnetServerOptions options;
        private readonly GarnetOpenTelemetryServerMetrics serverMetrics;
        private readonly GarnetOpenTelemetrySessionMetrics sessionMetrics;

        public GarnetOpenTelemetryServerMonitor(GarnetServerOptions options, GarnetServerMetrics serverMetrics)
        {
            this.options = options;
            this.serverMetrics = new GarnetOpenTelemetryServerMetrics(serverMetrics);
            this.sessionMetrics = serverMetrics.globalSessionMetrics != null
                ? new GarnetOpenTelemetrySessionMetrics(serverMetrics.globalSessionMetrics)
                : null;

            GarnetOpenTelemetryLatencyMetrics.Initialize(options.LatencyMonitor);
        }

        public void Start()
        {
            if (this.options.OpenTelemetryEndpoint != null)
            {
                Sdk.CreateMeterProviderBuilder()
                    .AddMeter(GarnetOpenTelemetryServerMetrics.MeterName, GarnetOpenTelemetrySessionMetrics.MeterName)
                    .AddOtlpExporter(opts =>
                    {
                        if (this.options.OpenTelemetryEndpoint != null)
                        {
                            opts.Endpoint = this.options.OpenTelemetryEndpoint;
                        }
                        if (this.options.OpenTelemetryExportProtocol.HasValue)
                        {
                            opts.Protocol = this.options.OpenTelemetryExportProtocol.Value;
                        }
                        opts.Protocol = OpenTelemetry.Exporter.OtlpExportProtocol.Grpc;
                        if (this.options.OpenTelemetryExportTimeout.HasValue)
                        {
                            opts.TimeoutMilliseconds = this.options.OpenTelemetryExportTimeout.Value;
                        }
                        if (this.options.OpenTelemetryExportInterval.HasValue)
                        {
                            opts.BatchExportProcessorOptions.ScheduledDelayMilliseconds = this.options.OpenTelemetryExportInterval.Value;
                        }
                    })
                    .Build();
            }
        }

        public void Dispose()
        {
            this.serverMetrics.Dispose();
            this.sessionMetrics?.Dispose();
            GarnetOpenTelemetryLatencyMetrics.DisposeInstance();
        }
    }
}
