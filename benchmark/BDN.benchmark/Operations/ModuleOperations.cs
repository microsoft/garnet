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
        static ReadOnlySpan<byte> NOOPCMD => "*2\r\n$18\r\nNoOpModule.NOOPCMD\r\n$1\r\nk\r\n"u8;
        Request noOpCmd;

        public override void GlobalSetup()
        {
            base.GlobalSetup();

            SetupOperation(ref noOpCmd, NOOPCMD);

            var noOpModulePath = Path.Join(moduleLoadPath, "NoOpModule.dll");
            SlowConsumeMessage(Encoding.ASCII.GetBytes(
                $"*3\r\n$6\r\nMODULE\r\n$6\r\nLOADCS\r\n${noOpModulePath.Length}\r\n{noOpModulePath}\r\n"));
            SlowConsumeMessage("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nc"u8);
        }

        [Benchmark]
        public void ModuleNoOpRawStringCommand()
        {
            Send(noOpCmd);
        }
    }
}
