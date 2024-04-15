// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Reflection;
using Garnet;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

class Program
{
    static void Main(string[] args)
    {
        using var loggerFactory = LoggerFactory.Create(builder => builder.AddSimpleConsole(options =>
        {
            options.SingleLine = true;
            options.TimestampFormat = "hh::mm::ss ";
        }));

        ILogger logger = loggerFactory.CreateLogger<Program>();
        CommandInfoUpdater.CommandInfoUpdater.TryUpdateCommandInfo(logger);
    }
}