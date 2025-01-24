// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Tsavorite.core;

namespace Embedded.server;

internal sealed class GarnetEmbeddedApplication
{
    public RegisterApi Register => app.Register;

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

    public static GarnetEmbeddedApplicationBuilder CreateHostBuilder(string[] args, GarnetServerOptions options)
    {
        return new(new GarnetApplicationOptions
        {
            Args = args,
        }, options);
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        await app.StopAsync(cancellationToken);
    }

    /// <summary>
    /// Dispose server
    /// </summary>
    public void Dispose() => app.Dispose();

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
        var server = app.Services
            .GetServices<IGarnetServer>()
            .OfType<GarnetServerEmbedded>()
            .FirstOrDefault();

        Console.WriteLine(server);

        return server?.CreateNetworkHandler();
    }
}