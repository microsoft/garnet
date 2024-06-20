// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet;
using Garnet.server.Module;
using Microsoft.Extensions.Logging;

namespace SampleModule
{
    public class SampleModule : ModuleBase
    {
        public override void OnLoad(ModuleLoadContext context, string[] args)
        {
            var status = context.Initialize("SampleModule", 1);
            if (status != ModuleActionStatus.Success)
            {
                context.Logger?.LogError($"Failed to initialize SampleModule. Error {status}");
                return;
            }

            context.RegisterCommand("SampleModule.SETIFPM", new SetIfPMCustomCommand());

            context.RegisterTransaction("SampleModule.READWRITETX", () => new ReadWriteTxn());
        }
    }
}