// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet;
using Garnet.server;
using Microsoft.Extensions.DependencyInjection;

namespace Embedded.server;

internal class GarnetEmbeddedApplicationBuilder : GarnetApplicationBuilder
{
    internal GarnetEmbeddedApplicationBuilder(GarnetApplicationOptions options, GarnetServerOptions garnetServerOptions)
        : base(options, garnetServerOptions)
    {
    }

    public new GarnetEmbeddedApplication Build()
    {
        var serviceDescriptor = base.Services
            .FirstOrDefault(descriptor => descriptor.ServiceType == typeof(IGarnetServer));

        base.Services.Remove(serviceDescriptor);

        base.Services.AddSingleton<IGarnetServer, GarnetServerEmbedded>();

        var app = base.Build();

        return new GarnetEmbeddedApplication(app);
    }
}