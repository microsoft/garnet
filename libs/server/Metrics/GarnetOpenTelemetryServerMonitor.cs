// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Reflection;
using Garnet.server.Metrics.Latency;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;

namespace Garnet.server.Metrics
{
    /// <summary>
    /// Registers OpenTelemetry metrics for Garnet server and manages their lifecycle. This includes server-level metrics, session-level metrics, and latency metrics.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This class acts as the central coordinator for all OpenTelemetry metrics in the Garnet server.
    /// It wraps the raw metrics sources (<see cref="GarnetServerMetrics"/> and <see cref="GarnetSessionMetrics"/>)
    /// into OpenTelemetry-compatible instruments exposed via <see cref="System.Diagnostics.Metrics.Meter"/> instances:
    /// </para>
    /// <list type="bullet">
    ///   <item><description><see cref="GarnetOpenTelemetryServerMetrics"/> — connection-level metrics (active, received, disposed).</description></item>
    ///   <item><description><see cref="GarnetOpenTelemetrySessionMetrics"/> — session-level metrics (commands processed, network I/O, cache lookups).</description></item>
    ///   <item><description><see cref="GarnetOpenTelemetryLatencyMetrics"/> — latency histograms and counters (command latency, bytes/ops per receive call).</description></item>
    /// </list>
    /// <para>
    /// Call <see cref="Start"/> after construction to configure the OTLP exporter and begin metric collection.
    /// The exporter endpoint, protocol, timeout, and interval are controlled by the corresponding
    /// properties on <see cref="GarnetServerOptions"/>.
    /// </para>
    /// <para>
    /// This class implements <see cref="IDisposable"/>; disposing it tears down all underlying meters
    /// and the latency metrics singleton.
    /// </para>
    /// </remarks>
    internal sealed class GarnetOpenTelemetryServerMonitor : IDisposable
    {
        private readonly GarnetServerOptions options;
        private readonly GarnetOpenTelemetryServerMetrics serverMetrics;
        private readonly GarnetOpenTelemetrySessionMetrics sessionMetrics;
        private MeterProvider meterProvider;

        /// <summary>
        /// Initializes a new instance of the <see cref="GarnetOpenTelemetryServerMonitor"/> class,
        /// creating the OpenTelemetry metric wrappers for server and session metrics and initializing
        /// the latency metrics singleton.
        /// </summary>
        /// <param name="options">
        /// The <see cref="GarnetServerOptions"/> that control OpenTelemetry export behavior, including
        /// <see cref="GarnetServerOptions.OpenTelemetryEndpoint"/>,
        /// <see cref="GarnetServerOptions.OpenTelemetryExportProtocol"/>,
        /// <see cref="GarnetServerOptions.OpenTelemetryExportTimeout"/> and 
        /// <see cref="GarnetServerOptions.OpenTelemetryExportInterval"/>
        /// </param>
        /// <param name="serverMetrics">
        /// The <see cref="GarnetServerMetrics"/> instance that provides raw server and session counters.
        /// If <see cref="GarnetServerMetrics.globalSessionMetrics"/> is <c>null</c>, session-level
        /// metrics will not be registered.
        /// </param>
        public GarnetOpenTelemetryServerMonitor(GarnetServerOptions options, GarnetServerMetrics serverMetrics)
        {
            this.options = options;
            this.serverMetrics = new GarnetOpenTelemetryServerMetrics(serverMetrics);
            this.sessionMetrics = serverMetrics.globalSessionMetrics != null
                ? new GarnetOpenTelemetrySessionMetrics(serverMetrics.globalSessionMetrics)
                : null;

            GarnetOpenTelemetryLatencyMetrics.Initialize(options.LatencyMonitor);
        }

        /// <summary>
        /// Initializes and configures OpenTelemetry metrics exporting if an endpoint is specified in the options.
        /// </summary>
        /// <remarks>Call this method to enable OpenTelemetry metrics collection and exporting for the
        /// service. Metrics will be exported using the configured endpoint and protocol. If no endpoint is specified,
        /// metrics exporting will not be enabled.</remarks>
        public void Start()
        {
            if (this.options.OpenTelemetryEndpoint != null)
            {
                this.meterProvider = Sdk.CreateMeterProviderBuilder()
                    .ConfigureResource(rb => rb.AddService("Microsoft.Garnet", serviceVersion: Assembly.GetEntryAssembly()?.GetName()?.Version?.ToString() ?? "unknown"))
                    .AddMeter(GarnetOpenTelemetryServerMetrics.MeterName, GarnetOpenTelemetrySessionMetrics.MeterName, GarnetOpenTelemetryLatencyMetrics.MeterName)
                    .AddOtlpExporter(opts =>
                    {
                        opts.Endpoint = this.options.OpenTelemetryEndpoint;

                        if (this.options.OpenTelemetryExportProtocol.HasValue)
                        {
                            opts.Protocol = this.options.OpenTelemetryExportProtocol.Value;
                        }

                        if (this.options.OpenTelemetryExportTimeout != 0)
                        {
                            opts.TimeoutMilliseconds = this.options.OpenTelemetryExportTimeout;
                        }

                        if (this.options.OpenTelemetryExportInterval != 0)
                        {
                            opts.BatchExportProcessorOptions.ScheduledDelayMilliseconds = this.options.OpenTelemetryExportInterval;
                        }
                    })
                    .Build();
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            this.serverMetrics.Dispose();
            this.sessionMetrics?.Dispose();
            GarnetOpenTelemetryLatencyMetrics.DisposeInstance();
            this.meterProvider?.Dispose();
        }
    }
}