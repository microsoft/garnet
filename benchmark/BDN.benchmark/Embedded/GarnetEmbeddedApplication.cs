// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet;
using Garnet.server;
using Microsoft.Extensions.Hosting;

namespace Embedded.server;

public class GarnetEmbeddedApplication : GarnetApplication
{
    public GarnetEmbeddedApplication(IHost host)
        : base(host)
    {
    }

    public static new GarnetEmbeddedApplicationBuilder CreateHostBuilder(string[] args, GarnetServerOptions options)
    {
        return new GarnetEmbeddedApplicationBuilder(new GarnetApplicationOptions { Args = args, }, options);
    }
}