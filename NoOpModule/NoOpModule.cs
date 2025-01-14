﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;
using Microsoft.Extensions.Logging;

namespace NoOpModule
{
    public class NoOpModule : ModuleBase
    {
        public override void OnLoad(ModuleLoadContext context, string[] args)
        {
            var status = context.Initialize("NoOpModule", 1);
            if (status != ModuleActionStatus.Success)
            {
                context.Logger?.LogError("Failed to initialize NoOpModule. Error {status}", status);
                return;
            }

            context.RegisterCommand("NoOpModule.NOOPCMD", new NoOpCommand(), CommandType.ReadModifyWrite,
                new RespCommandsInfo { Arity = 2 });

            context.RegisterTransaction("NoOpModule.NOOPTXN", () => new NoOpTxn(),
                new RespCommandsInfo { Arity = 1 });

            var factory = new DummyObjectFactory();
            context.RegisterType(factory);

            context.RegisterCommand("NoOpModule.NOOPOBJRMW", factory, new DummyObjectNoOpRMW(),
                CommandType.ReadModifyWrite, new RespCommandsInfo { Arity = 2 });
            context.RegisterCommand("NoOpModule.NOOPOBJREAD", factory, new DummyObjectNoOpRead(), CommandType.Read,
                new RespCommandsInfo { Arity = 2 });

            context.RegisterProcedure("NoOpModule.NOOPPROC", () => new NoOpProc(), new RespCommandsInfo { Arity = 1 });
        }
    }
}
