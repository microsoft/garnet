// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Reflection;
using System.Text;
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
        static ReadOnlySpan<byte> NOOPCMD => "*2\r\n$18\r\nNoOpModule.NOOPCMD\r\n$2\r\nk1\r\n"u8;
        Request noOpCmd;

        static ReadOnlySpan<byte> NOOPOBJRMW => "*2\r\n$21\r\nNoOpModule.NOOPOBJRMW\r\n$2\r\nk2\r\n"u8;
        Request noOpObjRmw;

        static ReadOnlySpan<byte> NOOPOBJREAD => "*2\r\n$22\r\nNoOpModule.NOOPOBJREAD\r\n$2\r\nk2\r\n"u8;
        Request noOpObjRead;

        static ReadOnlySpan<byte> NOOPPROC => "*1\r\n$19\r\nNoOpModule.NOOPPROC\r\n"u8;
        Request noOpProc;

        static ReadOnlySpan<byte> NOOPTXN => "*1\r\n$18\r\nNoOpModule.NOOPTXN\r\n"u8;
        Request noOpTxn;

        private void RegisterModules()
        {
            server.Register.NewModule(new NoOpModule.NoOpModule(), [], out _);
        }

        public override void GlobalSetup()
        {
            base.GlobalSetup();
            RegisterModules();

            SetupOperation(ref noOpCmd, NOOPCMD);
            SetupOperation(ref noOpObjRead, NOOPOBJREAD);
            SetupOperation(ref noOpObjRmw, NOOPOBJRMW);
            SetupOperation(ref noOpProc, NOOPPROC);
            SetupOperation(ref noOpTxn, NOOPTXN);

            SlowConsumeMessage("*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$1\r\nc\r\n"u8);
            SlowConsumeMessage(NOOPOBJRMW);
        }

        [Benchmark]
        public void ModuleNoOpRawStringCommand()
        {
            Send(noOpCmd);
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
    }
}
