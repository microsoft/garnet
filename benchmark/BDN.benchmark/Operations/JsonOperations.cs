// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Embedded.server;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for ModuleOperations
    /// </summary>
    [MemoryDiagnoser]
    public class JsonOperations : OperationsBase
    {
        static ReadOnlySpan<byte> JSONGETCMD => "*3\r\n$8\r\nJSON.GET\r\n$2\r\nk3\r\n$1\r\n$\r\n"u8;
        Request jsonGetCmd;

        static ReadOnlySpan<byte> JSONSETCMD => "*4\r\n$8\r\nJSON.SET\r\n$2\r\nk3\r\n$4\r\n$.f2\r\n$1\r\n2\r\n"u8;
        Request jsonSetCmd;

        private void RegisterModules()
        {
            server.Register.NewModule(new NoOpModule.NoOpModule(), [], out _);
            server.Register.NewModule(new GarnetJSON.Module(), [], out _);
        }

        public override void GlobalSetup()
        {
            base.GlobalSetup();
            RegisterModules();
            
            SetupOperation(ref jsonGetCmd, JSONGETCMD);
            SetupOperation(ref jsonSetCmd, JSONSETCMD);

            SlowConsumeMessage("*4\r\n$8\r\nJSON.SET\r\n$2\r\nk3\r\n$1\r\n$\r\n$14\r\n{\"f1\":{\"a\":1}}\r\n"u8);
            SlowConsumeMessage(JSONGETCMD);
            SlowConsumeMessage(JSONSETCMD);
        }

        [Benchmark]
        public void ModuleJsonGetCommand()
        {
            Send(jsonGetCmd);
        }

        [Benchmark]
        public void ModuleJsonSetCommand()
        {
            Send(jsonSetCmd);
        }
    }
}