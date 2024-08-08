// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;
using Garnet.server.Module;
using Microsoft.Extensions.Logging;

namespace GarnetJSON
{
    /// <summary>
    /// Represents the GarnetJSON module.
    /// </summary>
    public class Module : ModuleBase
    {
        /// <summary>
        /// Initializes the JSON module.
        /// </summary>
        /// <param name="context">The module load context.</param>
        /// <param name="args">The arguments passed to the module.</param>
        public override void OnLoad(ModuleLoadContext context, string[] args)
        {
            var status = context.Initialize("GarnetJSON", 1);
            if (status != ModuleActionStatus.Success)
            {
                context.Logger?.LogError("Failed to initialize GarnetJSON. Error {status}", status);
                return;
            }

            var jsonFactory = new JsonObjectFactory();
            status = context.RegisterType(jsonFactory);
            if (status == ModuleActionStatus.Success)
            {
                context.RegisterCommand("JSON.SET", jsonFactory, new JsonSET(context.Logger));
                context.RegisterCommand("JSON.GET", jsonFactory, new JsonGET(context.Logger), CommandType.Read);
            }
            else
            {
                context.Logger?.LogError("Failed to register JSON object factory. Error {status}", status);
            }
        }
    }
}