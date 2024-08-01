// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;
using Garnet.server.Module;
using Microsoft.Extensions.Logging;

namespace GarnetJSON
{
    public class Module : ModuleBase
    {
        public override void OnLoad(ModuleLoadContext context, string[] args)
        {
            var status = context.Initialize("GarnetJSON", 1);
            if (status != ModuleActionStatus.Success)
            {
                context.Logger?.LogError($"Failed to initialize GarnetJSON. Error {status}");
                return;
            }

            var jsonFactory = new JsonObjectFactory();
            context.RegisterType(jsonFactory);
            context.RegisterCommand("JSON.SET", jsonFactory, new JsonSET());
            context.RegisterCommand("JSON.GET", jsonFactory, new JsonGET(), CommandType.Read);
        }
    }
}