// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = GarnetApplication.CreateHostBuilder(args);

builder.Services.AddWindowsService(options =>
{
    options.ServiceName = "Microsoft Garnet Server";
});

var app = builder.Build();
app.Run();