// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Embedded.perftest;
using Garnet;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.DependencyInjection;
using Tsavorite.core;

namespace Embedded.server;

/// <summary>
/// Implements an embedded Garnet RESP server
/// </summary>
internal sealed class GarnetEmbeddedApplication
{
    readonly StoreWrapper store;
    readonly SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>> subscriberBroker;
    readonly GarnetApplication app;

    public GarnetEmbeddedApplication(GarnetApplication app)
    {
        this.app = app;
        this.store = app.Services.GetRequiredService<StoreWrapper>();
        this.subscriberBroker =
            app.Services.GetRequiredService<SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>>>();
    }

    public static new GarnetEmbeddedApplicationBuilder CreateHostBuilder(string[] args, GarnetServerOptions options)
    {
        return new(new GarnetApplicationOptions
        {
            Args = args,
        }, options);
    }

    /// <summary>
    /// Dispose server
    /// </summary>
    public new void Dispose() => app.Dispose();

    public StoreWrapper StoreWrapper => store;

    /// <summary>
    /// Return a direct RESP session to this server
    /// </summary>
    /// <returns>A new RESP server session</returns>
    internal RespServerSession GetRespSession()
    {
        return new RespServerSession(0,
            new EmbeddedNetworkSender(),
            store,
            subscribeBroker: this.subscriberBroker,
            null,
            true);
    }

    internal EmbeddedNetworkHandler GetNetworkHandler()
    {
        var iServer = this.app.Services.GetRequiredService<IGarnetServer>();
        var server = (GarnetServerEmbedded)iServer;

        return server.CreateNetworkHandler();
    }
}