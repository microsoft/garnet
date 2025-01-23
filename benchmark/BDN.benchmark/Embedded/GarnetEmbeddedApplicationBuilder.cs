// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet;
using Garnet.server;
namespace Embedded.server;

public class GarnetEmbeddedApplicationBuilder : GarnetApplicationBuilder
{
    public GarnetEmbeddedApplicationBuilder(GarnetApplicationOptions options, GarnetServerOptions garnetServerOptions)
        : base(options, garnetServerOptions)
    {
    }

    public new GarnetEmbeddedApplication Build()
    {
        throw new NotImplementedException();
    }

}