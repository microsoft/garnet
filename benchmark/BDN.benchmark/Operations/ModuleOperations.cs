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
    public class ModuleOperations : OperationsBase
    {
        static ReadOnlySpan<byte> NOOPCMDREAD => "*2\r\n$22\r\nNoOpModule.NOOPCMDREAD\r\n$2\r\nk1\r\n"u8;
        Request noOpCmdRead;

        static ReadOnlySpan<byte> NOOPCMDRMW => "*2\r\n$21\r\nNoOpModule.NOOPCMDRMW\r\n$2\r\nk1\r\n"u8;
        Request noOpCmdRmw;

        static ReadOnlySpan<byte> NOOPOBJRMW => "*2\r\n$21\r\nNoOpModule.NOOPOBJRMW\r\n$2\r\nk2\r\n"u8;
        Request noOpObjRmw;

        static ReadOnlySpan<byte> NOOPOBJREAD => "*2\r\n$22\r\nNoOpModule.NOOPOBJREAD\r\n$2\r\nk2\r\n"u8;
        Request noOpObjRead;

        static ReadOnlySpan<byte> NOOPPROC => "*1\r\n$19\r\nNoOpModule.NOOPPROC\r\n"u8;
        Request noOpProc;

        static ReadOnlySpan<byte> NOOPTXN => "*1\r\n$18\r\nNoOpModule.NOOPTXN\r\n"u8;
        Request noOpTxn;

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

            SetupOperation(ref noOpCmdRead, NOOPCMDREAD);
            SetupOperation(ref noOpCmdRmw, NOOPCMDRMW);
            SetupOperation(ref noOpObjRead, NOOPOBJREAD);
            SetupOperation(ref noOpObjRmw, NOOPOBJRMW);
            SetupOperation(ref noOpProc, NOOPPROC);
            SetupOperation(ref noOpTxn, NOOPTXN);

            SetupOperation(ref jsonGetCmd, JSONGETCMD);
            SetupOperation(ref jsonSetCmd, JSONSETCMD);

            SlowConsumeMessage("*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$1\r\nc\r\n"u8);
            SlowConsumeMessage(NOOPCMDREAD);
            SlowConsumeMessage(NOOPCMDRMW);
            SlowConsumeMessage(NOOPOBJREAD);
            SlowConsumeMessage(NOOPOBJRMW);
            SlowConsumeMessage(NOOPPROC);
            SlowConsumeMessage(NOOPTXN);
            SlowConsumeMessage("*4\r\n$8\r\nJSON.SET\r\n$2\r\nk3\r\n$1\r\n$\r\n$14\r\n{\"f1\":{\"a\":1}}\r\n"u8);
            SlowConsumeMessage(JSONGETCMD);
            SlowConsumeMessage(JSONSETCMD);
        }

        [Benchmark]
        public void ModuleNoOpRawStringReadCommand()
        {
            Send(noOpCmdRead);
        }

        [Benchmark]
        public void ModuleNoOpRawStringRmwCommand()
        {
            Send(noOpCmdRmw);
        }

        [Benchmark]
        public void ModuleNoOpObjRmwCommand()
        {
            Send(noOpObjRmw);
        }

        [Benchmark]
        public void ModuleNoOpObjReadCommand()
        {
            Send(noOpObjRead);
        }

        [Benchmark]
        public void ModuleNoOpProc()
        {
            Send(noOpProc);
        }

        [Benchmark]
        public void ModuleNoOpTxn()
        {
            Send(noOpTxn);
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